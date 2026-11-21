package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestProbeTuningOwnerOnly(t *testing.T) {
	t.Setenv("SUBSCRIPTION_PROBE_TUNING_ENABLED", "")
	t.Setenv("SUBSCRIPTION_PROBE_TUNING_PILOT", "")
	b, err := buildAutoSelectSubscription(pilotURIs())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range [][2]string{{"123", "Happ/4.8"}, {"123", "Incy"}} {
		if !bytes.Equal(applyProbeTuning(b, tc[0], tc[1]), b) {
			t.Fatal("changed for", tc)
		}
	}
	got := applyProbeTuning(b, autoSelectPilotUser, "Happ/4.8")
	var ps []map[string]any
	if err := json.Unmarshal(got, &ps); err != nil {
		t.Fatal(err)
	}
	auto := ps[0]
	if auto["observatory"] != nil {
		t.Fatal("observatory must be replaced")
	}
	ping := obj(obj(auto["burstObservatory"])["pingConfig"])
	if ping["destination"] != tunedProbeURL || ping["interval"] != "1m" {
		t.Fatal(ping)
	}
	bal := obj(obj(auto["routing"])["balancers"].([]any)[0])
	if sel, _ := bal["selector"].([]any); len(sel) != 1 || sel[0] != "auto-bypass-3-" {
		t.Fatal("auto profile must select XHTTP members only", bal["selector"])
	}
	if subj := obj(auto["burstObservatory"])["subjectSelector"].([]any); len(subj) != 1 || subj[0] != "auto-bypass-3-" {
		t.Fatal("probes must cover XHTTP members only", subj)
	}
	fb, _ := bal["fallbackTag"].(string)
	if fb == "" || fb == "unavailable" {
		t.Fatal("fallback must be a real outbound", fb)
	}
	found := false
	for _, o := range auto["outbounds"].([]any) {
		if obj(o)["tag"] == fb && obj(o)["protocol"] != "blackhole" {
			found = true
		}
	}
	if !found {
		t.Fatal("fallback tag missing from outbounds", fb)
	}
	t.Setenv("SUBSCRIPTION_PROBE_TUNING_PILOT", "false")
	if !bytes.Equal(applyProbeTuning(b, autoSelectPilotUser, "Happ/4.8"), b) {
		t.Fatal("pilot switch must disable tuning")
	}
}
