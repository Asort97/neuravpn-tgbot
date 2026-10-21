package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
)

func TestSubscriptionStatusUsesAbsolutePanelExpiry(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name                       string
		client                     *xray.Client
		days                       int64
		active, unlimited, pending bool
	}{
		{"missing", nil, 0, false, false, false},
		{"expired", &xray.Client{Enable: true, ExpiryTime: now.Add(-time.Hour).UnixMilli()}, 0, false, false, false},
		{"boundary", &xray.Client{Enable: true, ExpiryTime: now.UnixMilli()}, 0, false, false, false},
		{"less than one day", &xray.Client{Enable: true, ExpiryTime: now.Add(time.Second).UnixMilli()}, 1, true, false, false},
		{"rounded up", &xray.Client{Enable: true, ExpiryTime: now.Add(25 * time.Hour).UnixMilli()}, 2, true, false, false},
		{"disabled with future expiry", &xray.Client{Enable: false, ExpiryTime: now.Add(20 * 24 * time.Hour).UnixMilli()}, 0, false, false, false},
		{"unlimited", &xray.Client{Enable: true}, 0, true, true, false},
		{"first use", &xray.Client{Enable: true, ExpiryTime: -int64(5 * 24 * time.Hour / time.Millisecond)}, 5, true, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subscriptionStatusForClient(tt.client, now)
			if got.Days != tt.days || got.Active != tt.active || got.Unlimited != tt.unlimited || got.PendingActivation != tt.pending {
				t.Fatalf("unexpected status: %+v", got)
			}
			if tt.client != nil && tt.client.ExpiryTime > 0 && (got.ExpiresAt == nil || got.ExpiresAt.UnixMilli() != tt.client.ExpiryTime) {
				t.Fatal("the real expiry date must be retained even when expired")
			}
		})
	}
}

func TestSubscriptionStatusEndpointReadsPanelInsteadOfAccessCache(t *testing.T) {
	oldCfg, oldStore, oldSecret := xrayCfg, userStore, mergedSubSecret
	t.Cleanup(func() { xrayCfg, userStore, mergedSubSecret = oldCfg, oldStore, oldSecret; accessCache.Delete("42") })
	userStore = nil
	mergedSubSecret = "test-status-secret"
	expiry := time.Now().Add(-24 * time.Hour).UnixMilli()
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/panel/api/clients/get/tgId/42" {
			t.Errorf("unexpected panel request: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": []any{map[string]any{
			"client":     map[string]any{"uuid": "test-client", "email": "42@happycat", "tgId": "42", "enable": true, "expiryTime": expiry},
			"inboundIds": []int{1},
		}}})
	}))
	defer panel.Close()
	u, _ := url.Parse(panel.URL)
	xrayCfg = &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), ""), inboundIDs: []int{1}}
	accessCache.Store("42", &accessInfo{daysLeft: 50, client: &xray.Client{Enable: true, ExpiryTime: time.Now().Add(50 * 24 * time.Hour).UnixMilli()}})
	w := httptest.NewRecorder()
	handleMergedSubscription(w, httptest.NewRequest(http.MethodGet, "/merged-sub/42/"+mergedSubscriptionToken("42")+"/status", nil))
	var got subscriptionAccessStatus
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.Days != 0 || got.Active || got.ExpiresAt == nil || got.ExpiresAt.UnixMilli() != expiry {
		t.Fatalf("stale access cache must not affect panel status: %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store, private" {
		t.Fatal("status must not be cached")
	}
	w = httptest.NewRecorder()
	handleMergedSubscription(w, httptest.NewRequest(http.MethodGet, "/merged-sub/42/"+mergedSubscriptionToken("42")+"?status=1", nil))
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.Active {
		t.Fatalf("query status route failed: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	handleMergedSubscription(w, httptest.NewRequest(http.MethodGet, "/merged-sub/42/wrong?status=1", nil))
	if w.Code != http.StatusForbidden {
		t.Fatal("query status requires a signed token")
	}
	w = httptest.NewRecorder()
	handleMergedSubscription(w, httptest.NewRequest(http.MethodGet, "/merged-sub/42/wrong/status", nil))
	if w.Code != http.StatusForbidden {
		t.Fatal("status requires the user's signed token")
	}
	xrayCfg = nil
	w = httptest.NewRecorder()
	handleMergedSubscription(w, httptest.NewRequest(http.MethodGet, "/merged-sub/42/"+mergedSubscriptionToken("42")+"/status", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatal("unavailable panel must not be represented as an expired or active subscription")
	}
}
