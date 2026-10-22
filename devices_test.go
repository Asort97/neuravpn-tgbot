package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type devicePanelFixture struct {
	mu          sync.Mutex
	devices     []xray.HWIDDevice
	deletes     int
	lists       int
	failDelete  bool
	unsupported bool
	foreign     bool
}

func devicePanel(t *testing.T, sub string, id int, fingerprint string) (*xraySettings, *devicePanelFixture) {
	t.Helper()
	f := &devicePanelFixture{devices: []xray.HWIDDevice{{ID: id, Fingerprint: fingerprint, DeviceOS: "Android", DeviceModel: "Pixel", LastSeen: 1000}}}
	email := sub + "+inb5@example.com"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.URL.Path == "/panel/api/clients/get/tgId/42":
			if f.foreign {
				_, _ = w.Write([]byte(`{"success":true,"obj":[]}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": []any{
				map[string]any{"client": map[string]any{"id": 10, "uuid": "uuid", "tgId": 42, "email": email, "subId": sub, "enable": true}},
				map[string]any{"client": map[string]any{"id": 11, "tgId": "42", "email": "alias@example.com", "subId": sub}},
				map[string]any{"client": map[string]any{"id": 12, "tgId": 999, "email": "foreign@example.com", "subId": "foreign"}},
			}})
		case r.URL.Path == "/panel/api/clients/hwids/"+email && r.Method == http.MethodPost:
			f.lists++
			if f.unsupported {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": f.devices})
		case r.URL.Path == "/panel/api/clients/hwids/"+email+"/"+strconv.Itoa(id) && r.Method == http.MethodDelete:
			if f.failDelete {
				w.WriteHeader(503)
				return
			}
			f.deletes++
			f.devices = nil
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			t.Errorf("unexpected/foreign request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(server.Close)
	u, _ := url.Parse(server.URL)
	return &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), ""), inboundIDs: []int{5}}, f
}

func TestDeviceAggregationAcrossMergedPanels(t *testing.T) {
	a, fa := devicePanel(t, "primary", 1, "0123456789ab")
	b, fb := devicePanel(t, "merged", 8, "0123456789ab")
	view := readSubscriptionDevices(context.Background(), []*xraySettings{a, b}, "42")
	if len(view.Devices) != 1 || len(view.Devices[0].Records) != 2 || view.Unavailable != 0 {
		t.Fatalf("same HWID must be grouped across panels: %+v", view)
	}
	if fa.lists != 1 || fb.lists != 1 {
		t.Fatal("same subId aliases must not duplicate panel calls")
	}
	view = readSubscriptionDevices(context.Background(), []*xraySettings{a, b}, "42")
	if len(view.Devices) != 1 {
		t.Fatal("fresh reload failed")
	}
}

func TestDeviceGroupingDoesNotMergeIdenticalModelsWithoutFingerprint(t *testing.T) {
	a, _ := devicePanel(t, "a", 1, "")
	b, _ := devicePanel(t, "b", 1, "")
	view := readSubscriptionDevices(context.Background(), []*xraySettings{a, b}, "42")
	if len(view.Devices) != 2 {
		t.Fatal("model/OS are not a device identity")
	}
}

func TestDevicesUnsupportedPanelIsNotAnEmptySuccess(t *testing.T) {
	a, _ := devicePanel(t, "a", 1, "0123456789ab")
	b, fb := devicePanel(t, "b", 2, "1123456789ab")
	fb.unsupported = true
	view := readSubscriptionDevices(context.Background(), []*xraySettings{a, b}, "42")
	text, _, _ := devicesScreen(view, 0, "")
	if view.Unsupported != 1 || len(view.Devices) != 1 || !strings.Contains(text, "список неполный") {
		t.Fatalf("partial results hidden: %s", text)
	}
}

func TestDevicesPaginationAndSafeLabels(t *testing.T) {
	view := subscriptionDevices{Panels: 1}
	for i := 0; i < 25; i++ {
		view.Devices = append(view.Devices, subscriptionDevice{Key: fmt.Sprint(i), Info: xray.HWIDDevice{DeviceModel: "<phone>" + strings.Repeat("x", 100), LastSeen: 1000}})
	}
	for _, tc := range []struct{ page, count int }{{0, 8}, {1, 8}, {2, 8}, {3, 1}, {999, 1}, {-1, 8}} {
		text, kb, buttons := devicesScreen(view, tc.page, "")
		if len(buttons) != tc.count || strings.Contains(text, "<phone>") {
			t.Fatalf("bad pagination/HTML: page=%d count=%d", tc.page, len(buttons))
		}
		for _, row := range kb.InlineKeyboard {
			for _, button := range row {
				if button.CallbackData != nil && len(*button.CallbackData) > 64 {
					t.Fatal("callback exceeds Telegram limit")
				}
			}
		}
	}
	text, _, _ := devicesScreen(subscriptionDevices{Panels: 1}, 0, "")
	if !strings.Contains(text, "пока не зарегистрированы") {
		t.Fatal("missing empty state")
	}
}

type deviceTelegramFixture struct {
	mu   sync.Mutex
	text string
}

func deviceTestBot(t *testing.T) (*tgbotapi.BotAPI, *deviceTelegramFixture) {
	t.Helper()
	f := &deviceTelegramFixture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Form.Get("text") != "" {
			f.text = r.Form.Get("text")
		}
		if strings.HasSuffix(r.URL.Path, "getMe") {
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"Test","username":"testbot"}}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "answerCallbackQuery") {
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":101,"chat":{"id":42,"type":"private"}}}`))
	}))
	t.Cleanup(server.Close)
	bot, err := tgbotapi.NewBotAPIWithClient("test", server.URL+"/bot%s/%s", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return bot, f
}

func setDeviceTestProviders(t *testing.T, configs ...*xraySettings) {
	t.Helper()
	old, oldSingle := mergedXrayCfgs, mergedXrayCfg
	mergedXrayCfgs, mergedXrayCfg = configs, nil
	userSessionsMu.Lock()
	oldSession := userSessions[42]
	delete(userSessions, 42)
	userSessionsMu.Unlock()
	t.Cleanup(func() {
		mergedXrayCfgs, mergedXrayCfg = old, oldSingle
		userSessionsMu.Lock()
		delete(userSessions, 42)
		if oldSession != nil {
			userSessions[42] = oldSession
		}
		userSessionsMu.Unlock()
	})
}

func deviceCallback(data string) *tgbotapi.CallbackQuery {
	return &tgbotapi.CallbackQuery{ID: "query", Data: data, From: &tgbotapi.User{ID: 42}, Message: &tgbotapi.Message{MessageID: 101, Chat: &tgbotapi.Chat{ID: 42, Type: "private"}}}
}

func TestDeviceCallbackDeletesOnlyOwnedHWIDAcrossAllPanels(t *testing.T) {
	a, fa := devicePanel(t, "a", 1, "0123456789ab")
	b, fb := devicePanel(t, "b", 2, "0123456789ab")
	setDeviceTestProviders(t, b)
	bot, tg := deviceTestBot(t)
	handleCallback(bot, deviceCallback("nav_devices"), a)
	session := getSession(42)
	if len(session.DeviceButtons) != 1 {
		t.Fatalf("expected one merged device, got %d", len(session.DeviceButtons))
	}
	var token string
	for key := range session.DeviceButtons {
		token = key
	}
	handleCallback(bot, deviceCallback("devices_del:"+token), a)
	if fa.deletes != 1 || fb.deletes != 1 || !strings.Contains(tg.text, "ваши устройства: 0") {
		t.Fatalf("delete did not reach all panels: %d %d %s", fa.deletes, fb.deletes, tg.text)
	}
	handleCallback(bot, deviceCallback("devices_del:"+token), a)
	if fa.deletes != 1 || fb.deletes != 1 || !strings.Contains(tg.text, "устарела") {
		t.Fatal("stale callback reused")
	}
}

func TestDeviceCallbackRejectsOtherUserAndForgedToken(t *testing.T) {
	a, fa := devicePanel(t, "a", 1, "0123456789ab")
	setDeviceTestProviders(t)
	bot, _ := deviceTestBot(t)
	q := deviceCallback("nav_devices")
	q.From.ID = 999
	handleCallback(bot, q, a)
	if fa.lists != 0 {
		t.Fatal("foreign user queried device list")
	}
	handleCallback(bot, deviceCallback("devices_del:forged"), a)
	if fa.deletes != 0 {
		t.Fatal("forged button deleted a device")
	}
}

func TestDeviceCallbackDoesNotClaimSuccessOnPartialDeletion(t *testing.T) {
	a, fa := devicePanel(t, "a", 1, "0123456789ab")
	b, fb := devicePanel(t, "b", 2, "0123456789ab")
	fb.failDelete = true
	setDeviceTestProviders(t, b)
	bot, tg := deviceTestBot(t)
	handleCallback(bot, deviceCallback("nav_devices"), a)
	var token string
	for key := range getSession(42).DeviceButtons {
		token = key
	}
	handleCallback(bot, deviceCallback("devices_del:"+token), a)
	if fa.deletes != 1 || fb.deletes != 0 || !strings.Contains(tg.text, "не удалось подтвердить") {
		t.Fatalf("partial deletion hidden: %s", tg.text)
	}
}

func TestDeviceDeletionRechecksOwnershipAndExpiredButtons(t *testing.T) {
	a, fa := devicePanel(t, "a", 1, "0123456789ab")
	setDeviceTestProviders(t)
	bot, tg := deviceTestBot(t)
	handleCallback(bot, deviceCallback("nav_devices"), a)
	var token string
	for key := range getSession(42).DeviceButtons {
		token = key
	}
	fa.foreign = true
	handleCallback(bot, deviceCallback("devices_del:"+token), a)
	if fa.deletes != 0 {
		t.Fatal("device deleted after losing ownership")
	}
	fa.foreign = false
	handleCallback(bot, deviceCallback("nav_devices"), a)
	for key, selection := range getSession(42).DeviceButtons {
		token = key
		selection.ExpiresAt = time.Now().Add(-time.Second)
		getSession(42).DeviceButtons[key] = selection
	}
	handleCallback(bot, deviceCallback("devices_del:"+token), a)
	if fa.deletes != 0 || !strings.Contains(tg.text, "устарела") {
		t.Fatal("expired button must not delete")
	}
}

func TestReadDevicesCancelsUnavailablePanel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	cfg := &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), "")}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	view := readSubscriptionDevices(ctx, []*xraySettings{cfg}, "42")
	if view.Unavailable != 1 {
		t.Fatal("unreachable panel must not become an empty success")
	}
}
