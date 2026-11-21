package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

func TestNeuraVPNAppGetsJSONTuning(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_PER_BYPASS_IP_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_PROBE_TUNING_ENABLED", "true")
	agent := "NeuraVPN/1.0.16 (Android 14; build 16)"
	accept := autoSelectMediaTypeV2 + ", */*;q=0.5"
	out, h, err := autoSelectSubscription("5550418384", agent, accept, pilotURIs(), http.Header{})
	if err != nil || h.Get("X-Neura-Autoselect") != "enabled" {
		t.Fatal("JSON-capable app must receive autoselect profiles", err)
	}
	out = applyProbeTuning(applyHappHelloProfile(out, "5550418384", agent), "5550418384", agent)
	var profiles []map[string]any
	if err := json.Unmarshal(out, &profiles); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("tlshello")) {
		t.Fatal("app profiles must fragment the ClientHello")
	}
	balancers := obj(profiles[0]["routing"])["balancers"].([]any)
	if obj(balancers[0])["fallbackTag"] == "unavailable" {
		t.Fatal("app must not start on a blackhole fallback")
	}

	// Older app builds only accept links; they must keep getting the original body.
	old, h, err := autoSelectSubscription("5550418384", "NeuraVPN/1.0.15 (Android 14; build 15)", "*/*", pilotURIs(), http.Header{})
	if err != nil || h.Get("X-Neura-Autoselect") != "" || !bytes.Equal(old, pilotURIs()) {
		t.Fatal("link-only app builds must keep plain links")
	}
	if !bytes.Equal(applyHappHelloProfile(old, "1", "NeuraVPN/1.0.15"), old) {
		t.Fatal("plain links must stay untouched")
	}
}

// Released NeuraVPN builds (Android <= 1.0.19, Windows <= 1.0.14) ask for the
// bare JSON media type but cannot import current balancers. They must keep
// plain links so their subscription always refreshes.
func TestReleasedNeuraVPNBuildsKeepLinks(t *testing.T) {
	cases := []struct{ ua, accept string }{
		{"NeuraVPN/1.0.17 (Android; build 17)", autoSelectMediaType},
		{"NeuraVPN/1.0.13 (Windows; build 13)", autoSelectMediaType},
		{"NeuraVPN/1.0.18 (Android; build 18)", autoSelectMediaType + ", */*;q=0.5"},
		{"NeuraVPN/1.0.17 (Android; build 17)", autoSelectMediaType + ";v=1"},
		{"NeuraVPN/1.0.15 (Android; build 15)", "*/*"},
	}
	for _, c := range cases {
		if supportsAutoSelect(c.ua, c.accept) {
			t.Fatalf("%s with %q must get links", c.ua, c.accept)
		}
	}
	if !supportsAutoSelect("NeuraVPN/1.0.20 (Android; build 20)", autoSelectMediaTypeV2+", */*;q=0.5") {
		t.Fatal("v=2 client must get JSON")
	}
	for _, ua := range []string{"Happ/4.8", "INCY/1.2"} {
		if !supportsAutoSelect(ua, "*/*") {
			t.Fatalf("%s must keep JSON", ua)
		}
	}
}
