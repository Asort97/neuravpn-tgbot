package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

func pilotURIs() []byte {
	v := "?encryption=none&security=reality&sni=example.org&pbk=public&sid=abcd&fp=firefox&type="
	return []byte("vless://owner@lv.staticdeliverycdn.com:10001" + v + "tcp&flow=xtls-rprx-vision#one\n" +
		"vless://owner@lv.staticdeliverycdn.com:10002" + v + "tcp#two\n" +
		"vless://owner@lv.staticdeliverycdn.com:443" + v + "xhttp&path=%2Ftest&extra=%7B%22mode%22%3A%22auto%22%7D#three\n" +
		"hysteria2://secret@213.21.200.218:10010/?sni=example.org&obfs=salamander&obfs-password=mask#hy\n" +
		"vless://secondary@rupanel.staticdeliverycdn.com:443" + v + "xhttp&path=%2F#secondary\n")
}

func TestAutoSelectPilotScope(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "")
	if err := os.Unsetenv("SUBSCRIPTION_AUTOSELECT_ENABLED"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		env, user, ua string
		convert       bool
	}{
		{"", autoSelectPilotUser, "Happ/4.3", false},
		{autoSelectPilotUser, "5550418384", "Happ/4.3", false},
		{autoSelectPilotUser, autoSelectPilotUser, "v2rayNG", false},
		{autoSelectPilotUser, autoSelectPilotUser, "Happ/4.3", true},
	} {
		t.Run(tc.env+tc.user+tc.ua, func(t *testing.T) {
			t.Setenv("SUBSCRIPTION_AUTOSELECT_PILOT_USER_ID", tc.env)
			in := pilotURIs()
			headers := http.Header{"Subscription-Userinfo": {"expire=123"}, "Content-Type": {"text/plain"}}
			out, h, err := autoSelectSubscription(tc.user, tc.ua, "", in, headers)
			if err != nil {
				t.Fatal(err)
			}
			if tc.convert {
				if !json.Valid(out) || h.Get("X-Neura-Autoselect") != "enabled" {
					t.Fatal("missing JSON pilot")
				}
				if h.Get("Subscription-Userinfo") != "expire=123" || headers.Get("Content-Type") != "text/plain" {
					t.Fatal("headers lost or source mutated")
				}
			} else if !bytes.Equal(in, out) || h.Get("Content-Type") != "text/plain" {
				t.Fatal("non-pilot response modified")
			}
		})
	}
}

func TestAutoSelectDenialAndFailurePreserveOriginal(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	for _, header := range []string{"X-Hwid-Max-Devices-Reached", "X-Hwid-Not-Supported"} {
		in := pilotURIs()
		out, _, err := autoSelectSubscription("5550418384", "Happ", "", in, http.Header{header: {"true"}})
		if err != nil || !bytes.Equal(out, in) {
			t.Fatal("denied response modified")
		}
	}
	for _, in := range [][]byte{[]byte("unavailable"), bytes.ReplaceAll(pilotURIs(), []byte("lv.staticdeliverycdn.com"), []byte("unrelated.example.org")), append(pilotURIs(), []byte("trojan://secret@example.org:443\n")...)} {
		out, _, err := autoSelectSubscription("5550418384", "Happ", "", in, nil)
		if err == nil || !bytes.Equal(out, in) {
			t.Fatal("failed conversion must preserve original response")
		}
	}
}

func TestAutoSelectProfilesAndFreshCredentials(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		input := bytes.ReplaceAll(pilotURIs(), []byte("owner@"), []byte("renewed@"))
		if encoded {
			input = []byte(base64.StdEncoding.EncodeToString(input))
		}
		out, err := buildAutoSelectSubscription(input)
		if err != nil {
			t.Fatal(err)
		}
		var configs []map[string]any
		if json.Unmarshal(out, &configs) != nil || len(configs) != 6 {
			t.Fatal("ordinary profiles lost")
		}
		if bytes.Contains(out, []byte("owner")) || !bytes.Contains(out, []byte("renewed")) {
			t.Fatal("stale credentials")
		}
		auto := configs[0]
		members := auto["outbounds"].([]any)
		if len(members) != 23 || members[22].(map[string]any)["protocol"] != "blackhole" {
			t.Fatal("bad fallback")
		}
		if bytes.Contains(out, []byte(`"freedom"`)) {
			t.Fatal("direct fallback")
		}
		bal := auto["routing"].(map[string]any)["balancers"].([]any)[0].(map[string]any)
		if bal["fallbackTag"] != "unavailable" {
			t.Fatal("not fail closed")
		}
		if configs[5]["remarks"] != "secondary" {
			t.Fatal("merged node lost")
		}
	}
}

func TestAutoSelectFullIPPoolPreservesProfiles(t *testing.T) {
	out, err := buildAutoSelectSubscription(pilotURIs())
	if err != nil {
		t.Fatal(err)
	}
	var configs []map[string]any
	if err = json.Unmarshal(out, &configs); err != nil {
		t.Fatal(err)
	}
	pool := make(map[string]bool)
	for _, ip := range autoSelectPilotIPs {
		if pool[ip] || net.ParseIP(ip) == nil {
			t.Fatal("invalid or duplicate pool address")
		}
		pool[ip] = true
	}
	if len(pool) != 7 {
		t.Fatal("expected approved seven-address pool")
	}
	auto := configs[0]
	observer := auto["observatory"].(map[string]any)
	if observer["probeInterval"] != "60s" || observer["enableConcurrency"] != true {
		t.Fatal("incorrect observation interval")
	}
	members := auto["outbounds"].([]any)
	seen := make(map[string]bool)
	for i, value := range members[:21] {
		member := value.(map[string]any)
		tag := member["tag"].(string)
		if seen[tag] {
			t.Fatal("duplicate tag")
		}
		seen[tag] = true
		endpoint := member["settings"].(map[string]any)["vnext"].([]any)[0].(map[string]any)
		ip := endpoint["address"].(string)
		if !pool[ip] || !strings.HasSuffix(tag, "-ip-"+ip) {
			t.Fatal("unapproved address or missing IP in tag")
		}
		original := configs[1+i/7]["outbounds"].([]any)[0].(map[string]any)
		originalEndpoint := original["settings"].(map[string]any)["vnext"].([]any)[0].(map[string]any)
		if originalEndpoint["address"] != "lv.staticdeliverycdn.com" {
			t.Fatal("ordinary profile address changed")
		}
		if endpoint["port"] != originalEndpoint["port"] || !reflect.DeepEqual(endpoint["users"], originalEndpoint["users"]) ||
			!reflect.DeepEqual(member["streamSettings"], original["streamSettings"]) {
			t.Fatal("credentials, SNI, transport or port changed")
		}
	}
	hy := members[21].(map[string]any)
	if hy["protocol"] != "hysteria" || hy["settings"].(map[string]any)["address"] != "213.21.200.218" || hy["tag"] != "auto-hysteria2-ip-213.21.200.218" {
		t.Fatal("Hysteria endpoint changed")
	}
	if configs[5]["outbounds"].([]any)[0].(map[string]any)["settings"].(map[string]any)["vnext"].([]any)[0].(map[string]any)["address"] != "rupanel.staticdeliverycdn.com" {
		t.Fatal("merged node changed")
	}
}

func TestAutoSelectIPMemberDoesNotAlias(t *testing.T) {
	source := map[string]any{"tag": "auto-bypass-1", "settings": map[string]any{"vnext": []any{map[string]any{"address": "original", "users": []any{map[string]any{"id": "owner"}}}}}}
	member, err := autoSelectIPMember(source, "213.21.200.223")
	if err != nil {
		t.Fatal(err)
	}
	member["settings"].(map[string]any)["vnext"].([]any)[0].(map[string]any)["users"].([]any)[0].(map[string]any)["id"] = "changed"
	encoded, _ := json.Marshal(source)
	if bytes.Contains(encoded, []byte("changed")) || bytes.Contains(encoded, []byte("213.21.200.223")) {
		t.Fatal("source mutated")
	}
}

func TestAutoSelectURIValidation(t *testing.T) {
	for _, replacement := range [][2]string{
		{"security=reality", "security=none"}, {"obfs=salamander", "obfs=unknown"},
		{"sni=example.org&obfs=", "sni=example.org&insecure=1&obfs="},
		{"type=xhttp", "type=unknown"}, {"%7B%22mode%22%3A%22auto%22%7D", "bad-json"},
	} {
		if _, err := buildAutoSelectSubscription([]byte(strings.Replace(string(pilotURIs()), replacement[0], replacement[1], 1))); err == nil {
			t.Fatal("unsupported settings accepted")
		}
	}
}

func TestAutoSelectGlobalCapabilityAndKillSwitch(t *testing.T) {
	for _, tc := range []struct {
		name, enabled, user, ua, accept string
		convert                         bool
	}{
		{"customer-happ", "true", "10001", "Happ/4.3", "text/plain", true},
		{"future-customer", "true", "90000000000", "Happ/4.3", "", true},
		{"custom-client", "true", "10002", "NeuraVPN/1.0", autoSelectMediaType, true},
		{"custom-weight", "true", "10002", "NeuraVPN/1.0", "text/plain, " + autoSelectMediaType + ";q=0.8", true},
		{"no-capability", "true", "10002", "NeuraVPN/1.0", "application/json", false},
		{"legacy-client", "true", "10002", "v2rayNG/1.10", "*/*", false},
		{"disabled-type", "true", "10002", "NeuraVPN/1.0", autoSelectMediaType + ";q=0", false},
		{"invalid-weight", "true", "10002", "NeuraVPN/1.0", autoSelectMediaType + ";q=invalid", false},
		{"nan-weight", "true", "10002", "NeuraVPN/1.0", autoSelectMediaType + ";q=NaN", false},
		{"kill-switch-owner", "false", autoSelectPilotUser, "Happ", "", false},
		{"invalid-switch", "typo", autoSelectPilotUser, "Happ", "", false},
		{"no-user", "true", "", "Happ", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", tc.enabled)
			t.Setenv("SUBSCRIPTION_AUTOSELECT_PILOT_USER_ID", autoSelectPilotUser)
			input := pilotURIs()
			out, h, err := autoSelectSubscription(tc.user, tc.ua, tc.accept, input, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.convert {
				if !json.Valid(out) || h.Get("X-Neura-Autoselect") != "enabled" {
					t.Fatal("missing JSON")
				}
			} else if !bytes.Equal(input, out) {
				t.Fatal("legacy or disabled output changed")
			}
		})
	}
}

func TestAutoSelectPartialEntitlements(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(string(pilotURIs())), "\n")
	for _, tc := range []struct {
		name       string
		indexes    []int
		candidates int
	}{
		{"no-hysteria", []int{0, 1, 2, 4}, 21},
		{"no-xhttp", []int{0, 1, 3, 4}, 15},
		{"one-main-route", []int{2, 4}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var input []string
			for _, i := range tc.indexes {
				input = append(input, lines[i])
			}
			out, err := buildAutoSelectSubscription([]byte(strings.Join(input, "\n")))
			if err != nil {
				t.Fatal(err)
			}
			var configs []map[string]any
			if json.Unmarshal(out, &configs) != nil || len(configs) != len(input)+1 {
				t.Fatal("individual profiles lost")
			}
			members := configs[0]["outbounds"].([]any)
			if len(members) != tc.candidates+1 {
				t.Fatal("unauthorized candidates added")
			}
			for _, member := range members[:len(members)-1] {
				o := member.(map[string]any)
				if tc.name == "no-hysteria" && o["protocol"] == "hysteria" {
					t.Fatal("Hysteria access invented")
				}
				if tc.name == "no-xhttp" && strings.HasPrefix(o["tag"].(string), "auto-bypass-3") {
					t.Fatal("XHTTP access invented")
				}
			}
		})
	}
}

func TestAutoSelectGlobalCredentialsArePerUser(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	for _, id := range []string{"111111", "222222"} {
		input := bytes.ReplaceAll(pilotURIs(), []byte("owner@"), []byte("key-"+id+"@"))
		input = bytes.ReplaceAll(input, []byte("secret@"), []byte("auth-"+id+"@"))
		out, _, err := autoSelectSubscription(id, "Happ", "", input, nil)
		if err != nil {
			t.Fatal(err)
		}
		other := "111111"
		if id == other {
			other = "222222"
		}
		if !bytes.Contains(out, []byte("key-"+id)) || !bytes.Contains(out, []byte("auth-"+id)) || bytes.Contains(out, []byte("key-"+other)) || bytes.Contains(out, []byte("auth-"+other)) {
			t.Fatal("credentials crossed between users")
		}
	}
}

func TestAutoSelectCapabilityAndUserSeparateCache(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "https://example.org/merged-sub/test/token", nil)
	r.Header.Set("User-Agent", "NeuraVPN/1.0")
	r.Header.Set("X-HWID", "test-install-id")
	legacy := mergedSubscriptionCacheKey("111111", mergedSubscriptionVariantFromRequest(r))
	r.Header.Set("Accept", autoSelectMediaType)
	jsonKey := mergedSubscriptionCacheKey("111111", mergedSubscriptionVariantFromRequest(r))
	otherUser := mergedSubscriptionCacheKey("222222", mergedSubscriptionVariantFromRequest(r))
	if legacy == jsonKey || jsonKey == otherUser {
		t.Fatal("format or account shares subscription cache")
	}
}

func TestAutoSelectGlobalRevocationStillPrecedesCache(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	TestRevokedDeviceGatePrecedesCachePanelsAndLimits(t)
}

func TestAutoSelectPrivateLiveFixture(t *testing.T) {
	path := os.Getenv("AUTOSELECT_PRIVATE_FIXTURE")
	if path == "" {
		t.Skip("private live fixture not configured")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("fixture unavailable")
	}
	out, err := buildAutoSelectSubscription(body)
	if err != nil {
		t.Fatal(err)
	}
	var configs []map[string]any
	if json.Unmarshal(out, &configs) != nil || len(configs) != 7 {
		t.Fatal("expected auto plus six supported existing profiles")
	}
	if dest := os.Getenv("AUTOSELECT_PRIVATE_OUTPUT"); dest != "" {
		if os.WriteFile(dest, out, 0600) != nil {
			t.Fatal("cannot write private verification artifact")
		}
	}
}
