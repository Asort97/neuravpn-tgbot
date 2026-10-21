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

func TestSubscriptionClientLegacyListFallback(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.URL.Path {
		case "/panel/api/clients/get/tgId/42":
			w.WriteHeader(http.StatusNotFound)
		case "/panel/api/inbounds/list":
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": []any{
				map[string]any{"id": 1, "settings": `{"clients":[{"tgId":42,"enable":true,"expiryTime":100}]}`},
				map[string]any{"id": 2, "settings": `{"clients":[{"tgId":"42","enable":true,"expiryTime":200}]}`},
				map[string]any{"id": 3, "settings": `{"clients":[{"tgId":"42","enable":true,"expiryTime":999}]}`},
			}})
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := New("", "", u.Hostname(), u.Port(), "")
	got, err := client.GetSubscriptionClient(context.Background(), "42", []int{1, 2})
	if err != nil || got == nil || got.ExpiryTime != 200 || requests != 2 {
		t.Fatalf("expected newest selected-inbound record in two requests: %+v, %v, requests=%d", got, err, requests)
	}
}

func TestSubscriptionClientCancelsSlowPanel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := New("", "", u.Hostname(), u.Port(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := client.GetSubscriptionClient(ctx, "42", nil)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("slow panel request must be canceled: %v", err)
	}
}

func TestSubscriptionClientDoesNotReportAPIFailureAsMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":false,"msg":"database unavailable"}`))
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := New("", "", u.Hostname(), u.Port(), "")
	if _, err := client.GetSubscriptionClient(context.Background(), "42", nil); err == nil {
		t.Fatal("API failure must not become an expired subscription")
	}
}
