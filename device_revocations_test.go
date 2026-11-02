package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	"github.com/Asort97/vpnBot/clients/postgres"
	"github.com/google/uuid"
)

func (s *deletionTestStore) RevokeAndQueueDeviceDeletion(ctx context.Context, uid, key, name string, panels map[string][]postgres.DeviceDeletionTarget) error {
	if err := s.QueueDeviceDeletion(ctx, uid, key, panels); err != nil {
		return err
	}
	if s.revocations == nil {
		s.revocations = make(map[string][]postgres.DeviceRevocation)
	}
	for _, d := range s.revocations[uid] {
		if d.DeviceKey == key {
			return nil
		}
	}
	s.revocations[uid] = append(s.revocations[uid], postgres.DeviceRevocation{ID: uuid.NewString(), DeviceKey: key, Name: name, RevokedAt: time.Now()})
	return nil
}

func (s *deletionTestStore) DeviceRevoked(_ context.Context, uid, key string) (bool, error) {
	if s.fail {
		return false, errors.New("database unavailable")
	}
	for _, d := range s.revocations[uid] {
		if d.DeviceKey == key || (len(key) >= 17 && d.DeviceKey == key[:17]) {
			return true, nil
		}
	}
	return false, nil
}

func (s *deletionTestStore) RevokedDevices(_ context.Context, uid string) ([]postgres.DeviceRevocation, error) {
	if s.fail {
		return nil, errors.New("database unavailable")
	}
	return append([]postgres.DeviceRevocation{}, s.revocations[uid]...), nil
}

func (s *deletionTestStore) RestoreDevice(_ context.Context, uid, id string) (bool, error) {
	if s.fail {
		return false, errors.New("database unavailable")
	}
	for i, d := range s.revocations[uid] {
		if d.ID != id {
			continue
		}
		s.revocations[uid] = append(s.revocations[uid][:i], s.revocations[uid][i+1:]...)
		jobs := s.jobs[:0]
		for _, j := range s.jobs {
			if j.UserID != uid || j.DeviceKey != d.DeviceKey {
				jobs = append(jobs, j)
			}
		}
		s.jobs = jobs
		return true, nil
	}
	return false, nil
}

func TestDeviceFingerprintMatches3xUI(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-HWID", "  abcdef  ")
	if got := requestDeviceKey(r); got != "hwid:bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721" {
		t.Fatal(got)
	}
	r.Header.Set("X-HWID", "ABCDEF")
	if strings.HasPrefix(requestDeviceKey(r), "hwid:bef57ec7f53a") {
		t.Fatal("case-sensitive HWID folded")
	}
	r.Header.Set("X-HWID", "short")
	if requestDeviceKey(r) != "" {
		t.Fatal("unsupported HWID normalized differently from panel")
	}
}

func TestHWIDSubscriptionCannotBeServedFromSharedHTTPCache(t *testing.T) {
	w := httptest.NewRecorder()
	headers := http.Header{"Cache-Control": {"public, max-age=86400"}, "Etag": {"old"}, "Last-Modified": {"old"}, "Expires": {"future"}}
	writeMergedSubscriptionEntry(w, mergedSubscriptionCacheEntry{headers: headers, body: []byte("configs"), statusCode: 200}, "hit")
	if w.Header().Get("Cache-Control") != "no-store, private" || w.Header().Get("Vary") != "X-HWID" || w.Header().Get("ETag") != "" || w.Header().Get("Expires") != "" {
		t.Fatal("HTTP cache bypasses HWID gate", w.Header())
	}
	if headers.Get("Cache-Control") != "public, max-age=86400" {
		t.Fatal("shared cache entry mutated")
	}
}

func assertRemovedPlaceholder(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	data, err := base64.StdEncoding.DecodeString(w.Body.String())
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatal("more than one config", len(lines))
	}
	u, err := url.Parse(lines[0])
	if err != nil || u.Scheme != "vless" || u.Host != "192.0.2.1:1" || u.Fragment != removedDeviceTitle {
		t.Fatal("invalid placeholder", string(data), err)
	}
}

func TestRevokedDeviceGatePrecedesCachePanelsAndLimits(t *testing.T) {
	setDeviceTestProviders(t)
	s := userStore.(*deletionTestStore)
	s.DataStore = &deviceTestStore{enabled: false}
	oldConfig, oldSecret := xrayCfg, mergedSubSecret
	xrayCfg = &xraySettings{subBaseURL: "http://127.0.0.1:1"}
	mergedSubSecret = "test-secret"
	t.Cleanup(func() { xrayCfg, mergedSubSecret = oldConfig, oldSecret; invalidateMergedSubscriptionCache("42") })
	r := httptest.NewRequest("GET", "/merged-sub/42/"+mergedSubscriptionToken("42"), nil)
	r.Header.Set("X-HWID", "abcdef")
	key := requestDeviceKey(r)[:17]
	if err := s.RevokeAndQueueDeviceDeletion(r.Context(), "42", key, "phone", map[string][]postgres.DeviceDeletionTarget{"offline": nil}); err != nil {
		t.Fatal(err)
	}
	for _, warm := range []bool{false, true} {
		if warm {
			storeMergedSubscriptionCache(mergedSubscriptionCacheKey("42", mergedSubscriptionVariantFromRequest(r)), mergedSubscriptionCacheEntry{body: []byte("REAL_MAIN_AND_MERGED_CONFIGS"), statusCode: 200, headers: make(http.Header)})
		}
		w := httptest.NewRecorder()
		handleMergedSubscription(w, r)
		assertRemovedPlaceholder(t, w)
	}
	// Completion of all native deletion jobs must not remove the persistent ban.
	for _, job := range append([]postgres.DeviceDeletion(nil), s.jobs...) {
		_ = s.CompleteDeviceDeletion(r.Context(), job.ID)
	}
	w := httptest.NewRecorder()
	writeDeviceAuthorizedSubscriptionEntry(w, r, mergedSubscriptionCacheEntry{body: []byte("IN_FLIGHT_CONFIGS"), statusCode: 200, headers: make(http.Header)}, "miss", "42", mergedSubscriptionToken("42"))
	assertRemovedPlaceholder(t, w)
	if !gateRevokedDevice(httptest.NewRecorder(), r, "43") {
		t.Fatal("ban crossed account boundary")
	}
	s.fail = true
	w = httptest.NewRecorder()
	handleMergedSubscription(w, r)
	if w.Code != 503 || strings.Contains(w.Body.String(), "CONFIGS") {
		t.Fatal("DB failure served cached keys")
	}
	s.fail = false
	items, _ := s.RevokedDevices(r.Context(), "42")
	if _, err := restoreDevice(r.Context(), "42", items[0].ID); err != nil {
		t.Fatal(err)
	}
	if !gateRevokedDevice(httptest.NewRecorder(), r, "42") {
		t.Fatal("unblocked device still denied")
	}
}

func TestDeletedDeviceListAndRestoreDoNotContactPanels(t *testing.T) {
	setDeviceTestProviders(t)
	s := userStore.(*deletionTestStore)
	_ = s.RevokeAndQueueDeviceDeletion(context.Background(), "42", "hwid:bef57ec7f53a", "phone", map[string][]postgres.DeviceDeletionTarget{"offline": nil})
	h := newWebDevicesHandler(nil, "test-secret")
	call := func(action, id, uid string) map[string]any {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"user_id": uid, "action": action, "id": id})
		nonce := strings.ReplaceAll(uuid.NewString(), "-", "")
		w := httptest.NewRecorder()
		h(w, deviceWebRequest(string(body), nonce))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	list := call("deleted", "", "42")["devices"].([]any)
	if len(list) != 1 {
		t.Fatal(list)
	}
	id := list[0].(map[string]any)["id"].(string)
	if len(call("deleted", "", "43")["devices"].([]any)) != 0 {
		t.Fatal("foreign list exposed")
	}
	if call("restore", id, "43")["result"] != "absent" {
		t.Fatal("foreign unblock accepted")
	}
	if call("restore", id, "42")["result"] != "restored" || len(s.jobs) != 0 {
		t.Fatal("unblock did not cancel cleanup")
	}
	_ = s.RevokeAndQueueDeviceDeletion(context.Background(), "42", "hwid:bef57ec7f53a", "phone", map[string][]postgres.DeviceDeletionTarget{"offline": nil})
	if call("restore", id, "42")["result"] != "absent" {
		t.Fatal("stale generation unblocked new ban")
	}
}

func TestTelegramRemovedDevicesUnblocksAndRejectsActionConfusion(t *testing.T) {
	setDeviceTestProviders(t)
	s := userStore.(*deletionTestStore)
	_ = s.RevokeAndQueueDeviceDeletion(context.Background(), "42", "hwid:bef57ec7f53a", "phone", map[string][]postgres.DeviceDeletionTarget{"offline": nil})
	bot, tg := deviceTestBot(t)
	handleCallback(bot, deviceCallback("devices_removed:0"), nil)
	if !strings.Contains(tg.text, "удалённые устройства: 1") {
		t.Fatal(tg.text)
	}
	var token string
	for key := range getSession(42).DeviceButtons {
		token = key
	}
	handleCallback(bot, deviceCallback("devices_restore:"+token), nil)
	if len(s.revocations["42"]) != 0 || !strings.Contains(tg.text, "разблокировано") {
		t.Fatal(tg.text)
	}
	getSession(42).DeviceButtons = map[string]deviceSelection{"delete-token": {Key: "hwid:bef57ec7f53a", ExpiresAt: time.Now().Add(time.Minute)}}
	handleCallback(bot, deviceCallback("devices_restore:delete-token"), nil)
	if !strings.Contains(tg.text, "устарела") {
		t.Fatal("delete token was usable for restore")
	}
}

func TestDeletionRequiresStableHWIDAndPreservesName(t *testing.T) {
	cfg, _ := devicePanel(t, "a", 1, "bef57ec7f53a")
	setDeviceTestProviders(t)
	s := userStore.(*deletionTestStore)
	ctx := context.Background()
	if err := queueDeviceDeletion(ctx, []*xraySettings{cfg}, "42", subscriptionDevice{Key: "record:unknown"}); err == nil {
		t.Fatal("claimed revocation without a HWID")
	}
	if len(s.jobs) != 0 {
		t.Fatal("unsupported deletion queued")
	}
	d := subscriptionDevice{Key: "hwid:bef57ec7f53a", Info: xray.HWIDDevice{DeviceModel: "test phone"}}
	s.fail = true
	if err := queueDeviceDeletion(ctx, []*xraySettings{cfg}, "42", d); err == nil {
		t.Fatal("failed storage accepted")
	}
	s.fail = false
	if err := queueDeviceDeletion(ctx, []*xraySettings{cfg}, "42", d); err != nil {
		t.Fatal(err)
	}
	items, _ := s.RevokedDevices(ctx, "42")
	if len(items) != 1 || items[0].Name != "test phone" {
		t.Fatal(items)
	}
}
