package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestHysteriaVariantsPilot(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	for _, user := range []string{autoSelectPilotUser, "5550418384"} {
		t.Setenv("SUBSCRIPTION_HYSTERIA_VARIANTS_PILOT", "false")
		before, _, err := autoSelectSubscription(user, "Happ", "", pilotURIs(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("SUBSCRIPTION_HYSTERIA_VARIANTS_PILOT", "true")
		after, _, err := autoSelectSubscription(user, "Happ", "", pilotURIs(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var a, b []map[string]any
		json.Unmarshal(before, &a)
		json.Unmarshal(after, &b)
		for i := range a {
			if user != autoSelectPilotUser || i != 4 {
				if !reflect.DeepEqual(a[i], b[i]) {
					t.Fatalf("unrelated profile changed: %s/%d", user, i)
				}
				continue
			}
			outs := b[i]["outbounds"].([]any)
			if len(outs) != 22 {
				t.Fatal("expected 21 variants and fail-closed fallback")
			}
			src := a[i]["outbounds"].([]any)[0].(map[string]any)["streamSettings"].(map[string]any)
			for j := 0; j < 21; j++ {
				out := outs[j].(map[string]any)
				st := out["streamSettings"].(map[string]any)
				if out["settings"].(map[string]any)["port"] != float64(10010+j/7) || out["settings"].(map[string]any)["address"] != autoSelectPilotIPs[j%7] {
					t.Fatal("port mismatch")
				}
				if !reflect.DeepEqual(st["tlsSettings"], src["tlsSettings"]) || !reflect.DeepEqual(st["hysteriaSettings"], src["hysteriaSettings"]) {
					t.Fatal("TLS or auth changed")
				}
				if j/7 == 1 && st["finalmask"] != nil {
					t.Fatal("plain variant has mask")
				}
				if j/7 == 2 {
					m := st["finalmask"].(map[string]any)["udp"].([]any)[0].(map[string]any)["settings"].(map[string]any)
					if m["packetSize"] != "512-1200" || m["password"] != "mask" {
						t.Fatal("bad gecko mask")
					}
				}
			}
			if b[i]["observatory"].(map[string]any)["probeInterval"] != "45s" {
				t.Fatal("wrong interval")
			}
		}
	}
	in := pilotURIs()
	for _, tc := range []struct {
		ua string
		h  http.Header
	}{{"v2rayNG", nil}, {"Happ", http.Header{"X-Hwid-Max-Devices-Reached": {"true"}}}} {
		out, _, err := autoSelectSubscription(autoSelectPilotUser, tc.ua, "", in, tc.h)
		if err != nil || string(out) != string(in) {
			t.Fatal("capability/authorization gate bypassed")
		}
	}
}

func TestHysteriaAuxiliaryCollectorLinks(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_HYSTERIA_VARIANTS_PILOT", "true")
	base := pilotURIs()
	want, _, err := autoSelectSubscription(autoSelectPilotUser, "Happ", "", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	input := append(append([]byte{}, base...), []byte("hysteria2://secret@213.21.200.218:10011?sni=example.org#plain\nhysteria2://secret@213.21.200.218:10012?sni=example.org&obfs=gecko#gecko\n")...)
	got, _, err := autoSelectSubscription(autoSelectPilotUser, "Happ", "", input, nil)
	if err != nil || string(got) != string(want) {
		t.Fatal("auxiliary exports broke JSON selection", err)
	}
}
