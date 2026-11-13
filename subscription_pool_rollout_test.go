package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestGlobalPerProfilePools(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_PER_BYPASS_IP_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_HYSTERIA_VARIANTS_ENABLED", "true")
	baseline, err := buildAutoSelectSubscription(pilotURIs())
	if err != nil {
		t.Fatal(err)
	}
	var original []map[string]any
	json.Unmarshal(baseline, &original)
	for _, uid := range []string{autoSelectPilotUser, "5550418384"} {
		for _, ua := range []string{"Happ/4.8", "NeuraVPN/1"} {
			body, _, err := autoSelectSubscription(uid, ua, autoSelectMediaType, pilotURIs(), nil)
			if err != nil {
				t.Fatal(err)
			}
			var profiles []map[string]any
			json.Unmarshal(body, &profiles)
			if !reflect.DeepEqual(profiles[0], original[0]) || !reflect.DeepEqual(profiles[5], original[5]) {
				t.Fatal("global auto or merged node changed")
			}
			for i := 1; i <= 4; i++ {
				want := 8
				if i == 4 {
					want = 22
				}
				if len(profiles[i]["outbounds"].([]any)) != want {
					t.Fatal("incorrect candidate count", uid, i)
				}
				if profiles[i]["observatory"].(map[string]any)["probeInterval"] != "45s" {
					t.Fatal("incorrect interval")
				}
			}
		}
	}
	raw := append(append([]byte{}, pilotURIs()...), []byte("hy2://secret@213.21.200.218:10012?sni=example.org&obfs=gecko#aux\n")...)
	for _, input := range [][]byte{raw, []byte(base64.StdEncoding.EncodeToString(raw))} {
		body, _, err := autoSelectSubscription("5550418384", "v2rayNG", "", input, nil)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		if !looksLikeSubscriptionText(text) {
			text, _, err = decodeSubscriptionPayload(text)
			if err != nil {
				t.Fatal(err)
			}
		}
		if strings.Contains(text, ":10012") || !strings.Contains(text, ":10010") {
			t.Fatal("legacy auxiliary filtering failed")
		}
		denied, _, err := autoSelectSubscription("5550418384", "Happ", "", input, http.Header{"X-Hwid-Max-Devices-Reached": {"true"}})
		if err != nil || string(denied) != string(input) {
			t.Fatal("denied response modified")
		}
	}
}
