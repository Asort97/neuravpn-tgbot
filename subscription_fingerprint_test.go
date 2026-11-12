package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestLVFingerprintURIFormats(t *testing.T) {
	input := bytes.ReplaceAll(pilotURIs(), []byte("fp=firefox"), []byte("fp=chrome"))
	for _, enc := range []*base64.Encoding{nil, base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		body := input
		if enc != nil {
			body = []byte(enc.EncodeToString(body))
		}
		out, err := normalizeLVSubscriptionFingerprint(body)
		if err != nil {
			t.Fatal(err)
		}
		again, err := normalizeLVSubscriptionFingerprint(out)
		if err != nil || !bytes.Equal(out, again) {
			t.Fatal("not idempotent")
		}
		if enc != nil {
			out, err = enc.DecodeString(string(out))
			if err != nil {
				t.Fatal("encoding changed", err)
			}
		}
		before, after := strings.Split(string(input), "\n"), strings.Split(string(out), "\n")
		if len(before) != len(after) {
			t.Fatal("profile count changed")
		}
		for i := range before {
			if i >= 3 {
				if before[i] != after[i] {
					t.Fatal("non-LV profile changed")
				}
				continue
			}
			u, _ := url.Parse(before[i])
			v, _ := url.Parse(after[i])
			q := u.Query()
			q.Set("fp", "firefox")
			if !reflect.DeepEqual(q, v.Query()) {
				t.Fatal("query differs beyond fingerprint")
			}
			u.RawQuery, v.RawQuery = "", ""
			if !reflect.DeepEqual(u, v) {
				t.Fatal("endpoint, credentials or name changed")
			}
		}
	}
}

func TestLVFingerprintScopeAndMissingValue(t *testing.T) {
	base := "vless://owner@lv.staticdeliverycdn.com:443?security=reality&type=xhttp&fp=chrome#main"
	for _, other := range []string{
		strings.Replace(base, "lv.staticdeliverycdn.com", "rupanel.staticdeliverycdn.com", 1),
		strings.Replace(base, "lv.staticdeliverycdn.com", "netherlands.staticdeliverycdn.com", 1),
		strings.Replace(base, ":443?", ":10006?", 1),
		strings.Replace(base, "security=reality", "security=tls", 1),
		strings.Replace(base, "type=xhttp", "type=ws", 1),
		"vless://removed@127.0.0.1:1?security=none#device-removed",
	} {
		out, err := normalizeLVSubscriptionFingerprint([]byte(other))
		if err != nil || string(out) != other {
			t.Fatal("unrelated link changed")
		}
	}
	for _, fp := range []string{"", "&fp=chrome&fp=safari", "&fp=randomized"} {
		out, err := normalizeLVSubscriptionFingerprint([]byte(strings.Replace(base, "&fp=chrome", fp, 1)))
		if err != nil {
			t.Fatal(err)
		}
		u, _ := url.Parse(string(out))
		if !reflect.DeepEqual(u.Query()["fp"], []string{"firefox"}) {
			t.Fatal("fingerprint not normalized")
		}
	}
	for _, unsupported := range []string{"<html>error</html>", `[{"outbounds":[]}]`} {
		out, err := normalizeLVSubscriptionFingerprint([]byte(unsupported))
		if err == nil || string(out) != unsupported {
			t.Fatal("unsupported body not preserved")
		}
	}
}

func TestLVFingerprintAllUsersAndAutoCandidates(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	input := bytes.ReplaceAll(pilotURIs(), []byte("fp=firefox"), []byte("fp=chrome"))
	for _, user := range []string{autoSelectPilotUser, "5550418384", "new-user"} {
		out, _, err := autoSelectSubscription(user, "Happ/4.8", "", input, http.Header{})
		if err != nil {
			t.Fatal(err)
		}
		var configs []map[string]any
		if err := json.Unmarshal(out, &configs); err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, c := range configs {
			for _, raw := range c["outbounds"].([]any) {
				o := raw.(map[string]any)
				if o["protocol"] != "vless" {
					continue
				}
				s := o["streamSettings"].(map[string]any)["realitySettings"].(map[string]any)
				if c["remarks"] == "secondary" {
					if s["fingerprint"] != "chrome" {
						t.Fatal("BS fingerprint changed")
					}
				} else {
					if s["fingerprint"] != "firefox" {
						t.Fatal("LV fingerprint not applied")
					}
					count++
				}
			}
		}
		if count != 24 {
			t.Fatalf("expected 21 candidates + 3 profiles, got %d", count)
		}
	}
}
