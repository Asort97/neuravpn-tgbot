package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRecoveryPilot(t *testing.T) {
	b, err := buildAutoSelectSubscription(pilotURIs())
	if err != nil {
		t.Fatal(err)
	}
	b = applyHappHelloProfile(b, autoSelectPilotUser, "Happ/4.8")
	for _, tc := range [][2]string{{"123", "Happ"}, {autoSelectPilotUser, "Incy"}, {autoSelectPilotUser, "notHapp"}} {
		if got := appendHappRecoveryPilot(b, tc[0], tc[1]); !bytes.Equal(b, got) {
			t.Fatal("non-pilot changed", tc)
		}
	}
	before := append([]byte(nil), b...)
	got := appendHappRecoveryPilot(b, autoSelectPilotUser, "Happ/4.8")
	if !bytes.Equal(before, b) || !bytes.HasPrefix(got, b[:len(b)-1]) {
		t.Fatal("existing profiles changed")
	}
	if !bytes.Equal(got, appendHappRecoveryPilot(got, autoSelectPilotUser, "Happ")) {
		t.Fatal("not idempotent")
	}
	var ps []map[string]any
	if err := json.Unmarshal(got, &ps); err != nil {
		t.Fatal(err)
	}
	p := ps[len(ps)-1]
	if p["remarks"] != recoveryPilotTitle {
		t.Fatal("missing pilot")
	}
	ping := obj(obj(p["burstObservatory"])["pingConfig"])
	if ping["httpMethod"] != "GET" || ping["destination"] != recoveryProbeURL || ping["interval"] != "3m" {
		t.Fatal(ping)
	}
	tags := map[any]bool{}
	for _, v := range p["outbounds"].([]any) {
		o := obj(v)
		if tags[o["tag"]] || o["protocol"] == "freedom" {
			t.Fatal("duplicate or direct outbound")
		}
		tags[o["tag"]] = true
	}
	if len(tags) != 2*len(autoSelectPilotIPs)+2 || !tags["unavailable"] || !tags["trial-udp"] {
		t.Fatal(tags)
	}
	outs := p["outbounds"].([]any)
	for i := 0; i < 2*len(autoSelectPilotIPs); i += 2 {
		a, z := obj(outs[i]), obj(outs[i+1])
		if obj(a["streamSettings"])["finalmask"] == nil || obj(z["streamSettings"])["finalmask"] != nil || endpoint(obj(a["settings"]))["address"] != endpoint(obj(z["settings"]))["address"] {
			t.Fatal("missing pair")
		}
	}
	for _, bad := range []string{"[]", "denied", `[{"remarks":"device removed"}]`} {
		if string(appendHappRecoveryPilot([]byte(bad), autoSelectPilotUser, "Happ")) != bad {
			t.Fatal("denial changed")
		}
	}
}
