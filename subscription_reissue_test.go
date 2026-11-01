package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

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

func (s *reissueTestStore) GetSubscriptionReissue(context.Context, string) (postgres.SubscriptionReissue, error) {
	if s.fail {
		return s.record, errors.New("db unavailable")
	}
	return s.record, nil
}

func TestReissueTokenRevokesLegacyAndPreservesOtherAccounts(t *testing.T) {
	oldStore, oldSecret := userStore, mergedSubSecret
	defer func() { userStore = oldStore; mergedSubSecret = oldSecret }()
	s := &reissueTestStore{}
	userStore = s
	mergedSubSecret = "unit-test-secret"
	legacy := mergedSubscriptionToken(reissuePilotID)
	legacyCache := mergedSubscriptionCacheKey(reissuePilotID, mergedSubscriptionRequestVariant{})
	other := mergedSubscriptionToken("42")
	if legacy == "" {
		t.Fatal("legacy token missing")
	}
	s.record = postgres.SubscriptionReissue{Generation: "generation", Active: false}
	if mergedSubscriptionToken(reissuePilotID) != "" {
		t.Fatal("unverified credentials published")
	}
	s.record.Active = true
	if mergedSubscriptionCacheKey(reissuePilotID, mergedSubscriptionRequestVariant{}) == legacyCache {
		t.Fatal("old config cache reused")
	}
	current := mergedSubscriptionToken(reissuePilotID)
	if current == "" || current == legacy || isValidMergedSubscriptionToken(reissuePilotID, legacy) {
		t.Fatal("old link still valid")
	}
	if !isValidMergedSubscriptionToken(reissuePilotID, current) || mergedSubscriptionToken("42") != other {
		t.Fatal("pilot affected other accounts")
	}
	w := httptest.NewRecorder()
	writeAuthorizedSubscriptionEntry(w, mergedSubscriptionCacheEntry{}, "miss", reissuePilotID, legacy)
	if w.Code != 403 || w.Body.Len() != 0 {
		t.Fatal("in-flight old request exposed reissued credentials")
	}
	s.fail = true
	if mergedSubscriptionToken(reissuePilotID) != "" || mergedSubscriptionToken("42") != other {
		t.Fatal("must fail closed for pilot only")
	}
}

func TestReissueWebRequiresPilotAndExplicitConfirmation(t *testing.T) {
	primary, _ := devicePanel(t, "reissue", 1, "abcdef")
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
	if call("42", "reissue", "initial", 111) != 403 || s.begins != 0 {
		t.Fatal("non-pilot could rotate")
	}
	if call(reissuePilotID, "reissue_status", "", 112) != 200 || s.begins != 0 {
		t.Fatal("viewing status changed credentials")
	}
	if call(reissuePilotID, "reissue", strings.Repeat("x", 36), 113) != 400 || s.begins != 0 {
		t.Fatal("invalid confirmation accepted")
	}
	if call(reissuePilotID, "reissue", "initial", 114) != 200 || call(reissuePilotID, "reissue", "initial", 115) != 200 || s.begins != 1 {
		t.Fatal("confirmation not idempotent")
	}
}

func TestReissueRestrictedAndCredentialIsolation(t *testing.T) {
	if _, err := requestSubscriptionReissue("42", ""); err == nil {
		t.Fatal("non-pilot allowed")
	}
	state, err := reissuePublicState("42")
	if err != nil || state["allowed"] != false {
		t.Fatal("non-pilot button exposed")
	}
	a := reissueKey("generation", "panel-a", "email")
	if a == reissueKey("generation2", "panel-a", "email") || a == reissueKey("generation", "panel-b", "email") || a != reissueKey("generation", "panel-a", "email") {
		t.Fatal("credential derivation")
	}
}
