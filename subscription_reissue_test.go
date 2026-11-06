package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	"github.com/Asort97/vpnBot/clients/postgres"
)

type reissueTestStore struct {
	DataStore
	reissueStore
	record postgres.SubscriptionReissue
	fail   bool
	begins int
}

func (s *reissueTestStore) BeginSubscriptionReissue(_ context.Context, userID, expected string, _ map[string]bool) (postgres.SubscriptionReissue, error) {
	if expected == s.record.Generation && s.record.Generation == "" {
		s.begins++
		s.record = postgres.SubscriptionReissue{UserID: userID, Generation: "278bf04e-eec5-4813-acf3-3a245f8196ce", SubID: "new"}
	}
	return s.record, nil
}

func (s *reissueTestStore) GetSubscriptionReissue(_ context.Context, userID string) (postgres.SubscriptionReissue, error) {
	if s.fail {
		return s.record, errors.New("db unavailable")
	}
	if s.record.UserID != userID {
		return postgres.SubscriptionReissue{UserID: userID}, nil
	}
	return s.record, nil
}

const reissueTestUserID = "84"

func TestReissueTokenRevokesLegacyAndPreservesOtherAccounts(t *testing.T) {
	oldStore, oldSecret := userStore, mergedSubSecret
	defer func() { userStore = oldStore; mergedSubSecret = oldSecret }()
	s := &reissueTestStore{}
	userStore = s
	mergedSubSecret = "unit-test-secret"
	legacy := mergedSubscriptionToken(reissueTestUserID)
	legacyCache := mergedSubscriptionCacheKey(reissueTestUserID, mergedSubscriptionRequestVariant{})
	other := mergedSubscriptionToken("42")
	if legacy == "" {
		t.Fatal("legacy token missing")
	}
	s.record = postgres.SubscriptionReissue{UserID: reissueTestUserID, Generation: "generation", Active: false}
	if mergedSubscriptionToken(reissueTestUserID) != "" {
		t.Fatal("unverified credentials published")
	}
	s.record.Active = true
	if mergedSubscriptionCacheKey(reissueTestUserID, mergedSubscriptionRequestVariant{}) == legacyCache {
		t.Fatal("old config cache reused")
	}
	current := mergedSubscriptionToken(reissueTestUserID)
	if current == "" || current == legacy || isValidMergedSubscriptionToken(reissueTestUserID, legacy) {
		t.Fatal("old link still valid")
	}
	if !isValidMergedSubscriptionToken(reissueTestUserID, current) || mergedSubscriptionToken("42") != other {
		t.Fatal("rotation affected other accounts")
	}
	w := httptest.NewRecorder()
	writeAuthorizedSubscriptionEntry(w, mergedSubscriptionCacheEntry{}, "miss", reissueTestUserID, legacy)
	if w.Code != 403 || w.Body.Len() != 0 {
		t.Fatal("in-flight old request exposed reissued credentials")
	}
	s.fail = true
	if mergedSubscriptionToken(reissueTestUserID) != "" || mergedSubscriptionToken("42") != "" {
		t.Fatal("must fail closed when rotation state cannot be verified")
	}
}

func reissuePrimaryFixture(t *testing.T, present bool) *xraySettings {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/panel/api/inbounds/list" {
			fmt.Fprint(w, `{"success":true,"obj":[{"id":1,"protocol":"vless"}]}`)
			return
		}
		if r.URL.Path != "/panel/api/clients/get/tgId/84" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if !present {
			fmt.Fprint(w, `{"success":true,"obj":[]}`)
			return
		}
		fmt.Fprint(w, `{"success":true,"obj":[{"client":{"id":1,"uuid":"old","email":"test","tgId":84},"inboundIds":[1]}]}`)
	}))
	t.Cleanup(server.Close)
	u, _ := url.Parse(server.URL)
	return &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), ""), inboundIDs: []int{1}}
}

func TestReissueWebRequiresExplicitConfirmationForAnyOwner(t *testing.T) {
	primary := reissuePrimaryFixture(t, true)
	setDeviceTestProviders(t)
	oldStore, oldCfg, oldSecret := userStore, xrayCfg, mergedSubSecret
	t.Cleanup(func() { userStore, xrayCfg, mergedSubSecret = oldStore, oldCfg, oldSecret })
	s := &reissueTestStore{}
	userStore, xrayCfg, mergedSubSecret = s, primary, "test-secret"
	h := newWebDevicesHandler(primary, "test-secret")
	call := func(user, action, id string, index int) int {
		w := httptest.NewRecorder()
		body := fmt.Sprintf(`{"user_id":%q,"action":%q,"id":%q}`, user, action, id)
		h(w, deviceWebRequest(body, fmt.Sprintf("%032x", index)))
		return w.Code
	}
	if call(reissueTestUserID, "reissue_status", "", 112) != 200 || s.begins != 0 {
		t.Fatal("viewing status changed credentials")
	}
	if call(reissueTestUserID, "reissue", strings.Repeat("x", 36), 113) != 400 || s.begins != 0 {
		t.Fatal("invalid confirmation accepted")
	}
	if call(reissueTestUserID, "reissue", "initial", 114) != 200 || call(reissueTestUserID, "reissue", "initial", 115) != 200 || s.begins != 1 {
		t.Fatal("confirmation not idempotent")
	}
}

func TestReissueMissingSubscriptionAndCredentialIsolation(t *testing.T) {
	oldStore, oldCfg, oldSecret := userStore, xrayCfg, mergedSubSecret
	t.Cleanup(func() { userStore, xrayCfg, mergedSubSecret = oldStore, oldCfg, oldSecret })
	s := &reissueTestStore{}
	userStore, xrayCfg, mergedSubSecret = s, reissuePrimaryFixture(t, false), "test"
	if _, err := requestSubscriptionReissue(reissueTestUserID, ""); err == nil || s.begins != 0 {
		t.Fatal("missing subscription invalidated")
	}
	state, err := reissuePublicState("42")
	if err != nil || state["allowed"] != true {
		t.Fatal("regular user button missing")
	}
	a := reissueKey("generation", "panel-a", "email")
	if a == reissueKey("generation2", "panel-a", "email") || a == reissueKey("generation", "panel-b", "email") || a != reissueKey("generation", "panel-a", "email") {
		t.Fatal("credential derivation")
	}
}

func TestReissueLockDoesNotBlockAnotherUser(t *testing.T) {
	cfg := reissuePrimaryFixture(t, true)
	unlock := lockReissuePanel("84", cfg)
	done := make(chan struct{})
	go func() { release := lockReissuePanel("42", cfg); release(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		unlock()
		t.Fatal("another user blocked")
	}
	unlock()
}

func TestReissueTelegramRequiresOwnerAndConfirmation(t *testing.T) {
	oldStore, oldCfg, oldSecret := userStore, xrayCfg, mergedSubSecret
	t.Cleanup(func() { userStore, xrayCfg, mergedSubSecret = oldStore, oldCfg, oldSecret })
	s := &reissueTestStore{}
	userStore, xrayCfg, mergedSubSecret = s, reissuePrimaryFixture(t, true), "test"
	bot, tg := deviceTestBot(t)
	q := deviceCallback("reissue:show")
	q.From.ID, q.Message.Chat.ID = 84, 84
	handleReissueCallback(bot, q)
	if s.begins != 0 || !strings.Contains(tg.text, "Перевыпустить подписку?") {
		t.Fatal("view rotated or confirmation missing")
	}
	q.Data = "reissue:confirm:initial"
	q.Message.Chat.ID = 42
	handleReissueCallback(bot, q)
	if s.begins != 0 {
		t.Fatal("foreign chat rotated")
	}
	q.Message.Chat.ID = 84
	handleReissueCallback(bot, q)
	handleReissueCallback(bot, q)
	if s.begins != 1 || s.record.UserID != "84" {
		t.Fatal("regular user confirmation failed or duplicated")
	}
}
