package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestHappFixedLVPilot(t *testing.T) {
	body, err := buildAutoSelectSubscription(pilotURIs())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ user, ua string }{{"other", "Happ"}, {autoSelectPilotUser, "Incy"}} {
		if !bytes.Equal(body, appendHappFixedLVPilot(body, tc.user, tc.ua)) {
			t.Fatal("pilot leaked outside owner/Happ gate")
		}
	}
	var old, got []map[string]any
	out := appendHappFixedLVPilot(body, autoSelectPilotUser, "Happ/4")
	if json.Unmarshal(body, &old) != nil || json.Unmarshal(out, &got) != nil ||
		len(got) != len(old)+1 || !reflect.DeepEqual(old, got[:len(old)]) {
		t.Fatal("original profiles changed or new profile missing")
	}
	p := got[len(old)]
	outs := p["outbounds"].([]any)
	b := obj(p["routing"])
	if len(outs) != 1 || p["remarks"] != fixedLVTitle || p["observatory"] != nil || p["burstObservatory"] != nil || b["balancers"] != nil {
		t.Fatal("fixed profile must have exactly one outbound and no probes")
	}
	tag := "auto-bypass-3-ip-" + fixedLVAddress
	var source any
	for _, o := range old[0]["outbounds"].([]any) {
		if obj(o)["tag"] == tag {
			source = o
		}
	}
	if !reflect.DeepEqual(outs[0], source) || obj(b["rules"].([]any)[0])["outboundTag"] != tag ||
		!bytes.Equal(appendHappFixedLVPilot(out, autoSelectPilotUser, "Happ"), out) {
		t.Fatal("wrong source, route or duplicate")
	}
	missing := bytes.ReplaceAll(body, []byte(tag), []byte("absent"))
	if !bytes.Equal(missing, appendHappFixedLVPilot(missing, autoSelectPilotUser, "Happ")) {
		t.Fatal("invented an unavailable member")
	}
}
