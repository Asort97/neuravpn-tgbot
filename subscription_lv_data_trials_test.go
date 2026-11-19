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
	if json.Unmarshal(out, &docs) != nil || len(docs) != 3 || !bytes.Equal(docs[0], body[1:len(body)-1]) {
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
	var fast map[string]any
	if err := json.Unmarshal(docs[2], &fast); err != nil {
		t.Fatal(err)
	}
	ping := obj(obj(fast["burstObservatory"])["pingConfig"])
	if fast["remarks"] != "Латвия · Обход 2 · быстрый старт" || ping["httpMethod"] != "GET" ||
		ping["destination"] != "https://speed.cloudflare.com/__down?bytes=4096" ||
		ping["interval"] != "15s" || ping["timeout"] != "4s" || ping["sampling"] != float64(1) {
		t.Fatal("wrong fast-start probe")
	}
	for _, key := range []string{"routing", "outbounds"} {
		a, _ := json.Marshal(p[key])
		b, _ := json.Marshal(fast[key])
		if !bytes.Equal(a, b) {
			t.Fatalf("fast start changed %s", key)
		}
	}
	for _, other := range []string{"Германия ТЕСТ", "Обход HYSTERIA 🇱🇻 Латвия", "Белые списки 1"} {
		b := bytes.ReplaceAll(body, []byte("Обход 2 🇱🇻 Латвия"), []byte(other))
		if !bytes.Equal(b, appendHappLVDataTrials(b, autoSelectPilotUser, "Happ")) {
			t.Fatal(other)
		}
	}
}
