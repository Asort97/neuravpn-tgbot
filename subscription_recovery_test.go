package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	xray "github.com/Asort97/vpnBot/clients/Xray"
)

type recoveryStore struct {
	DataStore
	subID  string
	writes int
}

func (s *recoveryStore) GetSubscriptionID(string) (string, error) { return s.subID, nil }
func (s *recoveryStore) CompareAndSwapSubscriptionID(_ context.Context, _, oldID, newID string) error {
	if s.subID == oldID {
		s.subID = newID
		s.writes++
	}
	return nil
}

func TestPrimarySubscriptionRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		oldCode, newCode                 int
		owner, subID                     string
		wantLookup, wantRetry, wantWrite int
	}{
		{"stale owned ID", 404, 200, "42", "current", 1, 1, 1},
		{"native denial preserved", 404, 403, "42", "current", 1, 1, 0},
		{"forbidden not retried", 403, 200, "42", "current", 0, 0, 0},
		{"unchanged ID not looped", 404, 200, "42", "old", 1, 0, 0},
		{"foreign owner rejected", 404, 200, "99", "current", 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := userStore
			store := &recoveryStore{subID: "old"}
			userStore = store
			t.Cleanup(func() { userStore = previous })
			lookups, retries := 0, 0
			panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/s-39fj3r9f3j/old":
					w.WriteHeader(tc.oldCode)
				case "/panel/api/clients/get/tgId/42":
					lookups++
					json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": []any{map[string]any{"client": map[string]any{"tgId": tc.owner, "subId": tc.subID, "enable": true, "uuid": "unchanged"}}}})
				case "/s-39fj3r9f3j/current":
					retries++
					if r.Header.Get("X-Hwid") != "same-device" {
						t.Error("HWID dropped on retry")
					}
					w.Header().Set("X-Hwid-Active", "true")
					w.WriteHeader(tc.newCode)
					w.Write([]byte("fresh-configs"))
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer panel.Close()
			u, _ := url.Parse(panel.URL)
			cfg := &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), ""), subBaseURL: panel.URL}
			req := httptest.NewRequest("GET", panel.URL+"/s-39fj3r9f3j/old", nil)
			req.Header.Set("X-Hwid", "same-device")
			body, headers, code, err := fetchPrimarySubscriptionBody(context.Background(), cfg, "42", req.URL.String(), req)
			wantCode := tc.oldCode
			if tc.wantRetry > 0 {
				wantCode = tc.newCode
			}
			if err != nil || code != wantCode || lookups != tc.wantLookup || retries != tc.wantRetry || store.writes != tc.wantWrite {
				t.Fatalf("code=%d lookup=%d retry=%d writes=%d err=%v", code, lookups, retries, store.writes, err)
			}
			if tc.wantWrite > 0 && (store.subID != "current" || string(body) != "fresh-configs" || headers.Get("X-Hwid-Active") != "true") {
				t.Fatal("recovery lost metadata or response")
			}
		})
	}
}
