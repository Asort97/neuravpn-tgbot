package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	"github.com/Asort97/vpnBot/clients/postgres"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type bonusAdminTestStore struct {
	*deviceTestStore
	calls      int
	key, actor string
	ids        []string
	slots      int
}

func TestBonusSyncUsesPaidAndFreeSlots(t *testing.T) {
	old := userStore
	t.Cleanup(func() { userStore = old })
	userStore = &bonusAdminTestStore{deviceTestStore: &deviceTestStore{}}
	limit := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/panel/api/clients/get/tgId/42":
			w.Write([]byte(`{"success":true,"obj":[{"client":{"uuid":"keep","email":"owned","tgId":42,"subId":"sub","limitHwid":5}}]}`))
		case "/panel/api/clients/get/owned":
			w.Write([]byte(`{"success":true,"obj":{"client":{"uuid":"keep","email":"owned","tgId":42,"subId":"sub"}}}`))
		case "/panel/api/clients/update/owned":
			var body struct {
				Limit int `json:"limitHwid"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			limit = body.Limit
			w.Write([]byte(`{"success":true}`))
		default:
			t.Errorf("unexpected panel request %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	cfg := &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), "")}
	if err := syncPrimaryDeviceLimit(context.Background(), cfg, "42"); err != nil || limit != 10 {
		t.Fatal("native limit omitted bonus", limit, err)
	}
}

func (s *bonusAdminTestStore) GrantDeviceBonus(_ context.Context, key, actor string, ids []string, slots int) (bool, error) {
	s.calls++
	s.key = key
	s.actor = actor
	s.ids = ids
	s.slots = slots
	return true, nil
}
func (s *bonusAdminTestStore) GetDevicePlan(string) (postgres.DevicePlan, error) {
	return postgres.DevicePlan{Enabled: true, ExtraSlots: 2, BonusSlots: 3}, nil
}

func TestBonusCommandValidation(t *testing.T) {
	for _, args := range []string{"", "42", "42 0", "42 -1", "42 96", "42,42 2", "42, 2", "-42 2", "0042 2", "abc 2", "42 2 extra", strings.Repeat("42,", 20) + "43 2"} {
		if _, _, err := parseDeviceBonusArgs(args); err == nil {
			t.Fatal("accepted", args)
		}
	}
	ids, n, err := parseDeviceBonusArgs("42,43 3")
	if err != nil || len(ids) != 2 || n != 3 {
		t.Fatal(ids, n, err)
	}
}

func TestAdminBonusAuthorizationAndBilling(t *testing.T) {
	oldStore, oldAdmins := userStore, adminIDs
	t.Cleanup(func() { userStore, adminIDs = oldStore, oldAdmins })
	s := &bonusAdminTestStore{deviceTestStore: &deviceTestStore{}}
	userStore = s
	adminIDs = []int64{42}
	bot, tg := deviceTestBot(t)
	msg := &tgbotapi.Message{MessageID: 123, From: &tgbotapi.User{ID: 43}, Chat: &tgbotapi.Chat{ID: 43, Type: "private"}, Text: "/addslots 71,72 3", Entities: []tgbotapi.MessageEntity{{Type: "bot_command", Offset: 0, Length: 9}}}
	handleAddDeviceSlots(bot, msg)
	if s.calls != 0 {
		t.Fatal("non-admin grant")
	}
	msg.From.ID = 42
	msg.Chat.ID = 42
	msg.Chat.Type = "group"
	handleAddDeviceSlots(bot, msg)
	if s.calls != 0 {
		t.Fatal("group grant")
	}
	msg.Chat.Type = "private"
	handleAddDeviceSlots(bot, msg)
	if s.calls != 1 || s.key != "tg:42:123" || s.actor != "42" || s.slots != 3 || len(s.ids) != 2 {
		t.Fatal(s)
	}
	if !strings.Contains(tg.text, "лимит 10") || !strings.Contains(tg.text, "навсегда") {
		t.Fatal(tg.text)
	}
	price, err := deviceSurcharge("71")
	if err != nil || price != 100 {
		t.Fatal("bonus was billed", price, err)
	}
}
