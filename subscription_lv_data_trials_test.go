package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestHappLVDataTrials(t *testing.T) {
	body := []byte(`[{"remarks":"Обход 2 🇱🇻 Латвия","routing":{"balancers":[{"fallbackTag":"unavailable"}]},"observatory":{},"outbounds":[{"tag":"auto-bypass-2-ip-a","protocol":"vless"},{"tag":"auto-bypass-2-ip-b","protocol":"vless"},{"tag":"unavailable","protocol":"blackhole"}]}]`)
	for _, tc := range []struct{ user, agent string }{{"other", "Happ"}, {autoSelectPilotUser, "Incy"}} {
		if !bytes.Equal(body, appendHappLVDataTrials(body, tc.user, tc.agent)) {
			t.Fatal("trial escaped owner/client gate")
		}
	}
	out := appendHappLVDataTrials(body, autoSelectPilotUser, "Happ/4")
	var docs []json.RawMessage
	if json.Unmarshal(out, &docs) != nil || len(docs) != 2 || !bytes.Equal(docs[0], body[1:len(body)-1]) {
		t.Fatal("original modified or copy missing")
	}
	var p map[string]any
	if err := json.Unmarshal(docs[1], &p); err != nil {
		t.Fatal(err)
	}
	pc := obj(obj(p["burstObservatory"])["pingConfig"])
	if pc["httpMethod"] != "GET" || pc["destination"] != recoveryProbeURL || pc["timeout"] != "2s" || p["observatory"] != nil ||
		!bytes.Equal(appendHappLVDataTrials(out, autoSelectPilotUser, "Happ"), out) {
		t.Fatal("wrong trial or duplicate")
	}
	for _, other := range []string{"Германия ТЕСТ", "Обход HYSTERIA 🇱🇻 Латвия", "Белые списки 1"} {
		b := bytes.ReplaceAll(body, []byte("Обход 2 🇱🇻 Латвия"), []byte(other))
		if !bytes.Equal(b, appendHappLVDataTrials(b, autoSelectPilotUser, "Happ")) {
			t.Fatal(other)
		}
	}
}
