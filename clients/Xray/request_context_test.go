package xray

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestScopedClientCancelsRequestsWithoutChangingOriginal(t *testing.T) {
	var slow atomic.Bool
	slow.Store(true)
	stopped := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("scoped client lost authentication")
		}
		if slow.Load() {
			<-r.Context().Done()
			stopped <- struct{}{}
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"obj":[]}`))
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	base := New("", "", u.Hostname(), u.Port(), "")
	base.SetAPIToken("test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	scoped, err := base.WithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.GetAllInbounds(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected canceled list request, got %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("server request was not canceled")
	}
	slow.Store(false)
	if _, err := base.GetAllInbounds(); err != nil {
		t.Fatalf("scoped cancellation changed shared client: %v", err)
	}
}

func TestScopedClientCancelsResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	base := New("", "", u.Hostname(), u.Port(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	scoped, err := base.WithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scoped.GetClientRecordByEmail("test@example.com"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("response body must use scoped deadline: %v", err)
	}
}

func TestScopedClientCancelsLogin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	base := New("test", "test", u.Hostname(), u.Port(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	scoped, err := base.WithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := scoped.LoginToServer(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("login must use scoped deadline: %v", err)
	}
}

func TestScopedClientDoesNotWaitPastDeadlineForAuthentication(t *testing.T) {
	base := New("", "", "example.com", "443", "")
	base.authMu.Lock()
	defer base.authMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := base.WithContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("authentication lock must not block cancellation: %v", err)
	}
}
