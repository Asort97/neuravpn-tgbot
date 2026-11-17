package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestWhitelistSelection(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_PER_BYPASS_IP_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_BS_AUTOSELECT_DISABLED", "")
	base := string(pilotURIs())
	one := "vless://first-key@bs.example:443?encryption=none&security=reality&type=xhttp&pbk=pk&sni=one.example&path=%2Fone#🔓%20Белые%20списки%201\n"
	two := "vless://second-key@bs.example:8443?encryption=none&security=reality&type=xhttp&pbk=pk&sni=two.example&path=%2Ftwo#🔓%20Белые%20списки%202\n"
	ws := "vless://ws-key@bs.example:19447?encryption=none&security=tls&type=ws&sni=ws.example&host=ws.example&path=%2Fpilot#Белые%20списки%20WS\n"
	get := func(user, tail string) []map[string]any {
		t.Helper()
		body, _, err := autoSelectSubscription(user, "Happ", "", []byte(base+tail), http.Header{})
		var profiles []map[string]any
		if err != nil || json.Unmarshal(body, &profiles) != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		return profiles
	}
	withWS := get(autoSelectPilotUser, ws+two+one)
	if !reflect.DeepEqual(withWS, get("5550418384", strings.Replace(ws, "#", "#🔓%20", 1)+two+one)) {
		t.Fatal("ordinary subscriber did not receive the same group")
	}
	if !reflect.DeepEqual(get(autoSelectPilotUser, one+two), get(autoSelectPilotUser, one+two+strings.Replace(ws, "sni=ws.example", "sni=", 1))) {
		t.Fatal("invalid optional WS affected other profiles")
	}
	merged := withWS[len(withWS)-1]["outbounds"].([]any)
	if len(withWS) != len(get(autoSelectPilotUser, one+two)) || len(merged) != 3 {
		t.Fatal("WS should appear only inside the group")
	}
	newRoute := merged[2].(map[string]any)
	stream := newRoute["streamSettings"].(map[string]any)
	if newRoute["tag"] != "bs-ws" || stream["security"] != "tls" || stream["network"] != "ws" || stream["tlsSettings"].(map[string]any)["allowInsecure"] != false {
		t.Fatal("incorrect WS pilot")
	}
	for _, tail := range []string{one + two, two + one} {
		before := get(autoSelectPilotUser, "")
		after := get(autoSelectPilotUser, tail)
		if len(after) != len(before)+1 || !reflect.DeepEqual(before, after[:len(before)]) {
			t.Fatal("unrelated profiles changed")
		}
		p := after[len(before)]
		candidates := p["outbounds"].([]any)
		if p["remarks"] != "🔓 Белые списки 1" || len(candidates) != 2 || p["burstObservatory"] == nil {
			t.Fatal("incorrect group")
		}
		for i, key := range []string{"first-key", "second-key"} {
			out, _ := json.Marshal(candidates[i])
			if !bytes.Contains(out, []byte(key)) || candidates[i].(map[string]any)["tag"] != []string{"bs-1", "bs-2"}[i] {
				t.Fatal("incorrect member")
			}
		}
	}
	for _, tc := range []struct{ user, tail string }{
		{autoSelectPilotUser, one}, {autoSelectPilotUser, two}, {autoSelectPilotUser, one + one + two},
	} {
		before := get(tc.user, "")
		after := get(tc.user, tc.tail)
		if len(after) != len(before)+strings.Count(tc.tail, "\n") {
			t.Fatal("unexpected profile removal")
		}
	}
	t.Setenv("SUBSCRIPTION_BS_AUTOSELECT_DISABLED", "true")
	if len(get("5550418384", one+two+ws)) != len(get("5550418384", ""))+3 {
		t.Fatal("group rollback lost individual routes")
	}
}
