package main

import (
	"bytes"
	"net/http"
	"testing"
)

func incyTuned(t *testing.T, user string) bool {
	t.Helper()
	agent := "INCY/1.4"
	out, h, err := autoSelectSubscription(user, agent, "*/*", pilotURIs(), http.Header{})
	if err != nil || h.Get("X-Neura-Autoselect") != "enabled" {
		t.Fatal("INCY must keep receiving autoselect JSON", err)
	}
	out = applyProbeTuning(applyHappHelloProfile(out, user, agent), user, agent)
	return bytes.Contains(out, []byte("tlshello"))
}

func TestIncyTuningRollout(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_PROBE_TUNING_ENABLED", "true")
	if !incyTuned(t, autoSelectPilotUser) {
		t.Fatal("owner must get the Happ fragmentation in INCY")
	}
	if incyTuned(t, "5550418384") {
		t.Fatal("other INCY users wait for SUBSCRIPTION_INCY_TUNING=all")
	}
	t.Setenv("SUBSCRIPTION_INCY_TUNING", "all")
	if !incyTuned(t, "5550418384") {
		t.Fatal("all INCY users get fragmentation when enabled")
	}
	t.Setenv("SUBSCRIPTION_INCY_TUNING", "off")
	if incyTuned(t, autoSelectPilotUser) {
		t.Fatal("off must disable INCY tuning for everyone")
	}
}
