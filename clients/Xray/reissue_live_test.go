package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Opt-in integration test against an isolated, short-lived native panel client.
func TestReissueNative(t *testing.T) {
	base := os.Getenv("REISSUE_QA_PANEL")
	if base == "" {
		t.Skip("explicit live QA credentials required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(address)
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
	}
	jar, _ := cookiejar.New(nil)
	c := &XRayClient{serverURL: base, apiToken: os.Getenv("REISSUE_QA_TOKEN"), username: os.Getenv("REISSUE_QA_USER"), password: os.Getenv("REISSUE_QA_PASSWORD"), httpClient: &http.Client{Transport: transport, Timeout: 10 * time.Second, Jar: jar}}
	if err := c.loginWithContext(ctx); err != nil {
		t.Fatal("QA login failed")
	}
	uid := "9000000000043"
	existing, err := c.ReissueRecords(ctx, uid)
	if err != nil || len(existing) != 0 {
		t.Fatal("QA ID not vacant", err)
	}
	inbounds, err := c.getAllInboundsContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tcp *InboundData
	hyID := 0
	wgID := 0
	for i := range inbounds {
		in := &inbounds[i]
		var st map[string]any
		_ = json.Unmarshal([]byte(in.StreamSettings), &st)
		if in.Enable && in.Protocol == "vless" && (st["network"] == "tcp" || st["network"] == "raw") && st["security"] == "reality" {
			tcp = in
		}
		if in.Enable && in.Protocol == "hysteria" && in.Port == 10010 {
			hyID = in.ID
		}
		if in.Enable && in.Protocol == "amneziawg" {
			wgID = in.ID
		}
	}
	if tcp == nil || hyID == 0 {
		t.Fatal("VLESS and Hysteria QA inbounds unavailable")
	}
	oldKey, newKey, oldSub, newSub := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	email := "reissue-qa-" + uuid.NewString() + "@example.invalid"
	ids := []int{tcp.ID, hyID}
	if wgID != 0 {
		ids = append(ids, wgID)
	}
	data, _ := json.Marshal(map[string]any{"client": map[string]any{"id": oldKey, "auth": oldKey, "password": oldKey, "email": email, "tgId": 9000000000043, "subId": oldSub, "enable": true, "expiryTime": time.Now().Add(48 * time.Hour).UnixMilli(), "limitHwid": 0, "totalGB": 1073741824}, "inboundIds": ids})
	code, body, err := c.doAPIRequestContext(ctx, "POST", base+"/panel/api/clients/add", data, map[string]string{"Content-Type": "application/json"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = checkAPISuccess("QA create", code, body); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.DeleteClientByEmail(email, false); err != nil {
			t.Error("QA cleanup failed", err)
		}
	}()
	var stream map[string]any
	_ = json.Unmarshal([]byte(tcp.StreamSettings), &stream)
	reality := stream["realitySettings"].(map[string]any)
	settings := reality["settings"].(map[string]any)
	stream["realitySettings"] = map[string]any{"serverName": reality["serverNames"].([]any)[0], "shortId": reality["shortIds"].([]any)[0], "publicKey": settings["publicKey"], "fingerprint": "chrome"}
	delete(stream, "externalProxy")
	var hy map[string]any
	if json.Unmarshal([]byte(os.Getenv("REISSUE_QA_HY_TEMPLATE")), &hy) != nil {
		t.Fatal("HY QA template missing")
	}
	delete(hy, "sendThrough")
	makeOutbound := func(protocol, key string) map[string]any {
		if protocol == "vless" {
			address := tcp.Listen
			if address == "" || address == "0.0.0.0" || address == "::" {
				address = "127.0.0.1"
			}
			return map[string]any{"protocol": "vless", "streamSettings": stream, "settings": map[string]any{"vnext": []any{map[string]any{"address": address, "port": tcp.Port, "users": []any{map[string]any{"id": key, "encryption": "none"}}}}}}
		}
		b, _ := json.Marshal(hy)
		var clone map[string]any
		_ = json.Unmarshal(b, &clone)
		hyStream, ok := clone["streamSettings"].(map[string]any)
		if !ok {
			t.Fatal("HY stream settings missing")
		}
		hySettings, ok := hyStream["hysteriaSettings"].(map[string]any)
		if !ok || hySettings["auth"] == nil {
			t.Fatal("HY client authentication missing")
		}
		// Client authentication is separate from the shared UDP obfuscation password.
		hySettings["auth"] = key
		return clone
	}
	probe := func(protocol, key string) bool {
		t.Helper()
		folder := t.TempDir()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		config := map[string]any{"log": map[string]any{"loglevel": "none"}, "inbounds": []any{map[string]any{"listen": "127.0.0.1", "port": port, "protocol": "socks", "settings": map[string]any{"auth": "noauth"}}}, "outbounds": []any{makeOutbound(protocol, key)}}
		b, _ := json.Marshal(config)
		path := filepath.Join(folder, "config.json")
		if err = os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, "/usr/local/x-ui/bin/xray-linux-amd64", "run", "-config", path)
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
		time.Sleep(500 * time.Millisecond)
		result, err := exec.CommandContext(ctx, "curl", "--socks5-hostname", "127.0.0.1:"+strconv.Itoa(port), "--max-time", "5", "-s", "-o", "/dev/null", "-w", "%{http_code}", "http://www.gstatic.com/generate_204").Output()
		return err == nil && strings.TrimSpace(string(result)) == "204"
	}
	for _, p := range []string{"vless", "hysteria"} {
		if !probe(p, oldKey) {
			t.Fatal("QA baseline connection failed", p)
		}
	}
	before, err := c.ReissueRecords(ctx, uid)
	if err != nil || len(before) != 1 {
		t.Fatal(err)
	}
	if err = c.ApplyReissue(ctx, before[0], newKey, newSub); err != nil {
		t.Fatal(err)
	}
	after, err := c.ReissueRecords(ctx, uid)
	if err != nil || len(after) != 1 {
		t.Fatal(err)
	}
	if err = VerifyReissue(before[0], after[0], newKey, newSub); err != nil {
		t.Fatal(err)
	}
	if wgID != 0 {
		updatedInbounds, err := c.getAllInboundsContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var config struct {
			Clients []map[string]json.RawMessage `json:"clients"`
		}
		settings := ""
		for _, inbound := range updatedInbounds {
			if inbound.ID == wgID {
				settings = inbound.Settings
			}
		}
		if err = json.Unmarshal([]byte(settings), &config); err != nil {
			t.Fatal(err)
		}
		_, public := reissueTunnelKeys(newKey)
		found := false
		for _, peer := range config.Clients {
			if rawString(peer["email"]) == email {
				found = rawString(peer["publicKey"]) == public && public != rawString(before[0].Client["publicKey"])
			}
		}
		if !found {
			t.Fatal("AWG inbound peer was not replaced")
		}
		fmt.Println("LIVE_REISSUE_AWG_PEER_REPLACED")
	}
	for _, p := range []string{"vless", "hysteria"} {
		ok := false
		for i := 0; i < 3; i++ {
			if probe(p, newKey) {
				ok = true
				break
			}
			time.Sleep(time.Second)
		}
		if !ok {
			t.Fatal("new key failed", p)
		}
		if probe(p, oldKey) {
			t.Fatal("old key still connects", p)
		}
		fmt.Println("LIVE_REISSUE", p, "new key connected; old key rejected")
	}
	fmt.Println("LIVE_REISSUE_ACCOUNT_ID_EXPIRY_QUOTA_ATTACHMENTS_PRESERVED")
}
