package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func relayTags(t *testing.T, body []byte) (tags map[string]map[string]any, all []string) {
	t.Helper()
	var profiles []map[string]any
	if err := json.Unmarshal(body, &profiles); err != nil {
		t.Fatal(err)
	}
	tags = map[string]map[string]any{}
	for _, p := range profiles {
		for _, v := range p["outbounds"].([]any) {
			o := v.(map[string]any)
			tag, _ := o["tag"].(string)
			if strings.Contains(tag, "-ip-203.0.113.7") {
				tags[tag] = o
			}
			all = append(all, tag)
		}
	}
	return tags, all
}

func TestRelayCandidatesOwnerFirst(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_HYSTERIA_VARIANTS_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_RELAY_IPS", "203.0.113.7, not-an-ip")
	accept := autoSelectMediaTypeV2

	owner, _, err := autoSelectSubscription(autoSelectPilotUser, "NeuraVPN/1.0.20", accept, pilotURIs(), http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	relays, _ := relayTags(t, owner)
	if len(relays) == 0 {
		t.Fatal("owner must get relay candidates")
	}
	for tag, o := range relays {
		if !strings.HasPrefix(tag, "auto-bypass-") && !strings.HasPrefix(tag, "auto-hy") {
			t.Fatalf("unexpected relay candidate %s", tag)
		}
		ep := relayEndpoint(o)
		if ep["address"] != "203.0.113.7" {
			t.Fatalf("%s must dial the relay, got %v", tag, ep["address"])
		}
		if !relayForwardedPorts[o["protocol"].(string)][ep["port"].(float64)] {
			t.Fatalf("%s uses a port the relay does not forward: %v", tag, ep["port"])
		}
	}

	user, _, err := autoSelectSubscription("5550418384", "NeuraVPN/1.0.20", accept, pilotURIs(), http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if relays, _ := relayTags(t, user); len(relays) != 0 {
		t.Fatal("other users wait for SUBSCRIPTION_RELAY_ALL")
	}
	t.Setenv("SUBSCRIPTION_RELAY_ALL", "true")
	user, _, err = autoSelectSubscription("5550418384", "NeuraVPN/1.0.20", accept, pilotURIs(), http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if relays, _ := relayTags(t, user); len(relays) == 0 {
		t.Fatal("SUBSCRIPTION_RELAY_ALL must offer relays to everyone")
	}
}

func TestRelayCloneKeepsCredentialsAndIsIdempotent(t *testing.T) {
	body := []byte(`[{"remarks":"x","outbounds":[
		{"tag":"auto-bypass-1-ip-198.51.100.1","protocol":"vless","settings":{"vnext":[{"address":"198.51.100.1","port":10001,"users":[{"id":"u1","flow":"xtls-rprx-vision"}]}]},"streamSettings":{"security":"reality","realitySettings":{"publicKey":"pk","shortId":"ab","serverName":"s.example"}}},
		{"tag":"auto-bypass-1-ip-198.51.100.2","protocol":"vless","settings":{"vnext":[{"address":"198.51.100.2","port":10001,"users":[{"id":"u1"}]}]}},
		{"tag":"auto-bypass-9-ip-198.51.100.1","protocol":"vless","settings":{"vnext":[{"address":"198.51.100.1","port":2053,"users":[{"id":"u1"}]}]}},
		{"tag":"auto-hysteria2-ip-198.51.100.1","protocol":"hysteria","settings":{"address":"198.51.100.1","port":10010,"version":2}},
		{"tag":"unavailable","protocol":"blackhole"}]}]`)
	out, err := addRelayCandidates(body, []string{"203.0.113.7"})
	if err != nil {
		t.Fatal(err)
	}
	relays, all := relayTags(t, out)
	if len(relays) != 2 {
		t.Fatalf("want one vless and one hysteria relay copy, got %v", all)
	}
	v := relays["auto-bypass-1-ip-203.0.113.7"]
	users := relayEndpoint(v)["users"].([]any)
	if users[0].(map[string]any)["id"] != "u1" || v["streamSettings"].(map[string]any)["realitySettings"].(map[string]any)["publicKey"] != "pk" {
		t.Fatal("relay copy must keep credentials and REALITY settings")
	}
	if _, ok := relays["auto-hysteria2-ip-203.0.113.7"]; !ok {
		t.Fatal("hysteria candidate must get a relay copy")
	}
	again, err := addRelayCandidates(out, []string{"203.0.113.7"})
	if err != nil {
		t.Fatal(err)
	}
	if _, all2 := relayTags(t, again); len(all2) != len(all) {
		t.Fatal("adding relays twice must not duplicate candidates")
	}
}
