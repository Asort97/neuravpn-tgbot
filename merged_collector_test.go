package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
)

func TestMergedCollectorKeepsReadyNodeWhenAnotherTimesOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	stopped := make(chan struct{})
	got, err := collectMergedProviderLinks(ctx, "test", 2, func(ctx context.Context, index int) ([]string, error) {
		if index == 0 {
			<-ctx.Done()
			defer close(stopped)
			return nil, ctx.Err()
		}
		return []string{"vless://netherlands"}, nil
	})
	if err != nil || !reflect.DeepEqual(got, []string{"vless://netherlands"}) {
		t.Fatalf("healthy node lost on timeout: links=%v err=%v", got, err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out worker did not stop")
	}
}

func TestMergedCollectorPreservesConfiguredOrderAndDeduplicates(t *testing.T) {
	lastStarted := make(chan struct{})
	got, err := collectMergedProviderLinks(context.Background(), "test", 3, func(_ context.Context, index int) ([]string, error) {
		switch index {
		case 0:
			<-lastStarted
			return []string{" vless://first ", "vless://shared", ""}, nil
		case 1:
			return nil, errors.New("offline")
		default:
			close(lastStarted)
			return []string{"vless://shared", "vless://last"}, nil
		}
	})
	want := []string{"vless://first", "vless://shared", "vless://last"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("links=%v err=%v, want %v", got, err, want)
	}
}

func TestMergedCollectorDoesNotReportAllFailuresAsSuccess(t *testing.T) {
	for _, count := range []int{0, 2} {
		links, err := collectMergedProviderLinks(context.Background(), "test", count, func(context.Context, int) ([]string, error) {
			return nil, errors.New("offline")
		})
		if err == nil || len(links) != 0 {
			t.Fatalf("count=%d links=%v err=%v", count, links, err)
		}
	}
}

func TestMergedCollectorAllowsWorkerToFinishAfterReturn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	done := make(chan struct{})
	links, err := collectMergedProviderLinks(ctx, "test", 1, func(context.Context, int) ([]string, error) {
		<-release
		defer close(done)
		return []string{"vless://too-late"}, nil
	})
	close(release)
	if err == nil || len(links) != 0 {
		t.Fatalf("late node must not become success: links=%v err=%v", links, err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late worker blocked")
	}
}

func TestMergedProvidersReadAndRepairIndependentlyOfHungNode(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing-client", true: "missing-attachment"}[incomplete], func(t *testing.T) {
			oldConfigs, oldLegacy, oldStore := mergedXrayCfgs, mergedXrayCfg, userStore
			defer func() { mergedXrayCfgs, mergedXrayCfg, userStore = oldConfigs, oldLegacy, oldStore }()
			mergedXrayCfg = nil
			userStore = nil
			slowStopped := make(chan struct{}, 1)
			slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
				select {
				case slowStopped <- struct{}{}:
				default:
				}
			}))
			defer slow.Close()
			var attached atomic.Bool
			attached.Store(!incomplete)
			var mutations atomic.Int32
			client := xray.Client{ID: "test-client", Email: "42@happycat", TgID: "42", Enable: true, SubID: "test-sub"}
			settings, _ := json.Marshal(xray.InboundSettings{Clients: []xray.Client{client}, Decryption: "none"})
			inbounds := []xray.InboundData{
				{ID: 1, Enable: true, Port: 443, Protocol: "vless", Remark: "NL-1", Settings: string(settings), StreamSettings: `{"network":"tcp","security":"reality","realitySettings":{"serverNames":["example.com"],"shortIds":["abcd"],"settings":{"publicKey":"test-public-key","fingerprint":"chrome"}}}`},
				{ID: 2, Enable: true, Port: 8443, Protocol: "vless", Remark: "NL-2", Settings: string(settings), StreamSettings: `{"network":"tcp","security":"reality","realitySettings":{"serverNames":["example.com"],"shortIds":["abcd"],"settings":{"publicKey":"test-public-key","fingerprint":"chrome"}}}`},
			}
			fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				var obj any
				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
					mutations.Add(1)
					attached.Store(true)
				case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/clients/update/"):
					mutations.Add(1)
				case strings.Contains(r.URL.Path, "/clients/get/"):
					ids := []int{1}
					if attached.Load() {
						ids = []int{1, 2}
					}
					obj = map[string]any{"client": map[string]any{"uuid": client.ID, "email": client.Email, "tgId": 42, "subId": client.SubID, "enable": true}, "inboundIds": ids}
				case r.URL.Path == "/panel/api/inbounds/list":
					obj = inbounds
				case r.URL.Path == "/panel/api/inbounds/get/1":
					obj = inbounds[0]
				case r.URL.Path == "/panel/api/inbounds/get/2":
					obj = inbounds[1]
				default:
					t.Errorf("unexpected API call: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": obj})
			}))
			defer fast.Close()
			makeConfig := func(server *httptest.Server) *xraySettings {
				u, _ := url.Parse(server.URL)
				c := xray.New("", "", u.Hostname(), u.Port(), "")
				c.SetAPIToken("test-token")
				return &xraySettings{client: c, inboundIDs: []int{1, 2}, serverAddress: "netherlands.example", serverPort: 443, serverName: "example.com", publicKey: "test-public-key", shortID: "abcd"}
			}
			mergedXrayCfgs = []*xraySettings{makeConfig(slow), makeConfig(fast)}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			links, err := buildAllMergedProviderLinksContext(ctx, "42", "test-sub", &accessInfo{client: &client, expireAt: time.Now().Add(time.Hour)}, true)
			if err != nil || len(links) != 2 || !strings.Contains(links[0], "netherlands.example:443") || !strings.Contains(links[1], "netherlands.example:8443") {
				t.Fatalf("healthy provider must survive timeout: links=%v err=%v", links, err)
			}
			if incomplete && (!attached.Load() || mutations.Load() != 2) {
				t.Fatalf("repair was blocked by hung node: attached=%v mutations=%d", attached.Load(), mutations.Load())
			}
			if !incomplete && mutations.Load() != 0 {
				t.Fatal("existing client must remain read-only")
			}
			select {
			case <-slowStopped:
			case <-time.After(time.Second):
				t.Fatal("hung HTTP request was not canceled")
			}
		})
	}
}
