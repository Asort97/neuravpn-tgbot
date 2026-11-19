package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestPerBypassIPSelectionOwnerOnly(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	baseline, err := buildAutoSelectSubscription(pilotURIs())
	if err != nil {
		t.Fatal(err)
	}
	var original []map[string]any
	if err := json.Unmarshal(baseline, &original); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{autoSelectPilotUser, "5550418384"} {
		body, _, err := autoSelectSubscription(user, "Happ", "", pilotURIs(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if user != autoSelectPilotUser {
			if !bytes.Equal(body, baseline) {
				t.Fatal("other user changed")
			}
			continue
		}
		var got []map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(original) {
			t.Fatal("profile count changed")
		}
		for i := range got {
			if i == 0 || i > 3 {
				if !reflect.DeepEqual(got[i], original[i]) {
					t.Fatalf("unrelated profile %d changed", i)
				}
				continue
			}
			if got[i]["remarks"] != original[i]["remarks"] {
				t.Fatal("name changed")
			}
			outs := got[i]["outbounds"].([]any)
			if len(outs) != 8 {
				t.Fatal("expected seven IPs and blackhole")
			}
			src := original[i]["outbounds"].([]any)[0].(map[string]any)
			endpoint := src["settings"].(map[string]any)["vnext"].([]any)[0].(map[string]any)
			for j, ip := range autoSelectPilotIPs {
				out := outs[j].(map[string]any)
				ep := out["settings"].(map[string]any)["vnext"].([]any)[0].(map[string]any)
				if ep["address"] != ip || ep["port"] != endpoint["port"] || !reflect.DeepEqual(ep["users"], endpoint["users"]) || !reflect.DeepEqual(out["streamSettings"], src["streamSettings"]) || !reflect.DeepEqual(out["mux"], src["mux"]) {
					t.Fatal("parameters changed beyond IP")
				}
			}
			assertHappFastStart(t, got[i])
		}
	}
	for _, tc := range []struct {
		ua string
		h  http.Header
	}{
		{"v2rayNG", nil}, {"Happ", http.Header{"X-Hwid-Max-Devices-Reached": {"true"}}},
	} {
		body, _, err := autoSelectSubscription(autoSelectPilotUser, tc.ua, "", pilotURIs(), tc.h)
		if err != nil || !bytes.Equal(body, pilotURIs()) {
			t.Fatal("unsupported or denied response changed")
		}
	}
}

func assertHappFastStart(t *testing.T, p map[string]any) {
	t.Helper()
	observer := obj(p["burstObservatory"])
	want := map[string]any{"destination": "https://speed.cloudflare.com/__down?bytes=4096", "httpMethod": "GET", "interval": "15s", "timeout": "4s", "sampling": float64(1)}
	if p["observatory"] != nil || !reflect.DeepEqual(observer["pingConfig"], want) {
		t.Fatal("wrong startup checks", p["remarks"])
	}
	sel := observer["subjectSelector"].([]any)[0].(string)
	outs := p["outbounds"].([]any)
	for _, o := range outs[:7] {
		if !strings.HasPrefix(obj(o)["tag"].(string), sel) || obj(o)["protocol"] != "vless" {
			t.Fatal("untested candidate")
		}
	}
	b := obj(obj(p["routing"])["balancers"].([]any)[0])
	if b["fallbackTag"] != obj(outs[7])["tag"] || obj(outs[7])["protocol"] != "blackhole" ||
		!reflect.DeepEqual(b["strategy"], map[string]any{"type": "leastLoad", "settings": map[string]any{"expected": float64(1)}}) {
		t.Fatal("wrong strategy or fallback")
	}
}
