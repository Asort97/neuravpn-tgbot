package xray

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestHWIDLimitPreservesRecordAndChecksOwner(t *testing.T) {
	posts := 0
	client := hwidTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Write([]byte(`{"success":true,"obj":{"client":{"uuid":"keep-uuid","email":"owned","tgId":42,"limitHwid":0,"enable":true,"expiryTime":123456,"unknown":{"keep":true}}}}`))
			return
		}
		posts++
		var v map[string]any
		if json.NewDecoder(r.Body).Decode(&v) != nil || v["id"] != "keep-uuid" || v["expiryTime"] != float64(123456) || v["limitHwid"] != float64(5) || v["unknown"] == nil {
			t.Error("client settings overwritten")
		}
		w.Write([]byte(`{"success":true}`))
	})
	if err := client.SetHWIDLimit(context.Background(), "owned", "99", 5); err == nil || posts != 0 {
		t.Fatal("foreign limit changed")
	}
	if err := client.SetHWIDLimit(context.Background(), "owned", "42", 5); err != nil || posts != 1 {
		t.Fatal(err)
	}
	b, err := json.Marshal(Client{LimitHwid: 7})
	var c Client
	if err != nil || json.Unmarshal(b, &c) != nil || c.LimitHwid != 7 {
		t.Fatal("limit lost in client update")
	}
}

func hwidTestClient(t *testing.T, handler http.HandlerFunc) *XRayClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	u, _ := url.Parse(server.URL)
	return New("", "", u.Hostname(), u.Port(), "")
}

func TestHWIDListAndSingleDelete(t *testing.T) {
	deletes := 0
	client := hwidTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing panel authentication")
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/panel/api/clients/hwids/a+b@example.com":
			_, _ = w.Write([]byte(`{"success":true,"obj":[{"id":123,"firstSeen":1000,"lastSeen":2000,"deviceOs":"android","deviceModel":"Pixel","fingerprint":"0123456789ab"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/panel/api/clients/hwids/a+b@example.com/123":
			deletes++
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			t.Errorf("unexpected API call %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	})
	client.SetAPIToken("test-token")
	devices, err := client.ListHWIDDevices(context.Background(), "a+b@example.com")
	if err != nil || len(devices) != 1 || devices[0].ID != 123 || devices[0].DeviceModel != "Pixel" {
		t.Fatalf("unexpected list: %+v %v", devices, err)
	}
	if err := client.DeleteHWIDDevice(context.Background(), "a+b@example.com", 123); err != nil || deletes != 1 {
		t.Fatalf("delete failed: %v", err)
	}
	if err := client.DeleteHWIDDevice(context.Background(), "a+b@example.com", 0); err == nil || deletes != 1 {
		t.Fatal("invalid ID reached panel")
	}
}

func TestHWIDListRejectsFailuresAndUnsupported(t *testing.T) {
	for _, tc := range []struct {
		code        int
		body        string
		unsupported bool
	}{
		{404, "missing", true}, {405, "unsupported", true}, {503, "unavailable", false}, {200, `{"success":false}`, false}, {200, `{}`, false}, {200, `<html>login</html>`, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			client := hwidTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.code); _, _ = w.Write([]byte(tc.body)) })
			client.SetAPIToken("test")
			_, err := client.ListHWIDDevices(context.Background(), "user")
			if err == nil || errors.Is(err, ErrHWIDUnsupported) != tc.unsupported {
				t.Fatalf("wrong error: %v", err)
			}
		})
	}
}

func TestHWIDDeleteDoesNotReplayAmbiguousSuccess(t *testing.T) {
	calls := 0
	client := hwidTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`<html>unexpected</html>`))
	})
	if err := client.DeleteHWIDDevice(context.Background(), "user", 123); err == nil || calls != 1 {
		t.Fatalf("ambiguous deletion replayed: calls=%d err=%v", calls, err)
	}
}

func TestHWIDListDeadline(t *testing.T) {
	client := hwidTestClient(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := client.ListHWIDDevices(ctx, "user")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline not propagated: %v", err)
	}
}
