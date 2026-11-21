package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func signedReport(t *testing.T, secret []byte, at time.Time, report map[string]any) *http.Request {
	t.Helper()
	body, _ := json.Marshal(report)
	ts := strconv.FormatInt(at.Unix(), 10)
	r := httptest.NewRequest(http.MethodPost, "/neura-hello/report", bytes.NewReader(body))
	r.Header.Set("X-Neura-Timestamp", ts)
	r.Header.Set("X-Neura-Signature", neuraHelloSignature(secret, ts, body))
	return r
}

func TestNeuraHelloIngestAcceptsOnlySignedFreshReports(t *testing.T) {
	secret := []byte("s3cret")
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	store := newNodeStore(filepath.Join(t.TempDir(), "state.json"))
	h := handleNeuraHelloReport(secret, store, func() time.Time { return now })
	report := map[string]any{"node": "lv", "cpus": 4, "load1": 0.6, "disk_pct": 63,
		"conns": map[string]int{"443": 3000, "10001": 1200, "bad": 5}, "clients": 2100,
		"hello_at": "2026-11-21T11:08:06Z", "hello": map[string]any{"443": map[string]int{"flows": 600, "stalled": 6}}}

	w := httptest.NewRecorder()
	h(w, signedReport(t, secret, now, report))
	if w.Code != http.StatusNoContent {
		t.Fatalf("signed report rejected: %d", w.Code)
	}
	nodes, _, _ := store.snapshot()
	got := nodes["lv"].Last
	if got.totalConns() != 4200 || got.Conns["bad"] != 0 || got.Time != now {
		t.Fatalf("report not stored as expected: %+v", got)
	}

	for name, req := range map[string]*http.Request{
		"wrong secret": signedReport(t, []byte("other"), now, report),
		"stale":        signedReport(t, secret, now.Add(-10*time.Minute), report),
		"bad node":     signedReport(t, secret, now, map[string]any{"node": "../etc"}),
	} {
		w := httptest.NewRecorder()
		h(w, req)
		if w.Code == http.StatusNoContent {
			t.Fatalf("%s must be rejected", name)
		}
	}
	tampered := signedReport(t, secret, now, report)
	tampered.Body = httpBody(`{"node":"lv","conns":{"443":1}}`)
	w = httptest.NewRecorder()
	h(w, tampered)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("tampered body must fail signature, got %d", w.Code)
	}
}

func httpBody(s string) *readCloser { return &readCloser{strings.NewReader(s)} }

type readCloser struct{ *strings.Reader }

func (readCloser) Close() error { return nil }

func TestNodeStorePersistsAndTrimsHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := newNodeStore(path)
	start := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 30; i++ {
		store.record(nodeReport{Node: "bs", Time: start.Add(time.Duration(i) * time.Hour), Conns: map[string]int{"443": i}})
	}
	nodes, _, _ := store.snapshot()
	if n := len(nodes["bs"].History); n != 25 {
		t.Fatalf("history must keep one day, got %d points", n)
	}
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	reloaded := newNodeStore(path)
	reloaded.load()
	again, _, _ := reloaded.snapshot()
	if again["bs"].Last.totalConns() != 29 {
		t.Fatal("state must survive a restart")
	}
}

func TestNeuraAlerterRaisesAndClears(t *testing.T) {
	var sent []string
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	a := newNeuraAlerter(func(s string) { sent = append(sent, s) }, func() time.Time { return now })

	fresh := func(r nodeReport) nodeState {
		r.Time = now
		return nodeState{Last: r}
	}
	a.evaluate(map[string]nodeState{
		"lv": fresh(nodeReport{Node: "lv", CPUs: 4, DiskPct: 91,
			Scans: []helloScan{{ASN: "31214", Name: "TIS-DIALOG-AS", Flows: 140, IPs: 139}}}),
		"bs": {Last: nodeReport{Node: "bs", Time: now.Add(-7 * time.Minute)}},
	})
	joined := strings.Join(sent, "\n")
	for _, want := range []string{"нода bs молчит 7 мин", "диск на lv заполнен на 91%", "AS31214"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing alert %q in:\n%s", want, joined)
		}
	}
	// Repeated evaluation within the repeat window stays quiet.
	n := len(sent)
	a.evaluate(map[string]nodeState{"lv": fresh(nodeReport{Node: "lv", CPUs: 4, DiskPct: 91,
		Scans: []helloScan{{ASN: "31214", Name: "TIS-DIALOG-AS", Flows: 140, IPs: 139}}})})
	if len(sent) != n {
		t.Fatalf("alerts must not repeat immediately: %v", sent[n:])
	}
	// Recovery is announced once.
	a.evaluate(map[string]nodeState{
		"lv": fresh(nodeReport{Node: "lv", CPUs: 4, DiskPct: 60}),
		"bs": fresh(nodeReport{Node: "bs", CPUs: 2}),
	})
	joined = strings.Join(sent[n:], "\n")
	if !strings.Contains(joined, "нода bs снова на связи") || !strings.Contains(joined, "диск на lv в норме") {
		t.Fatalf("recovery not announced: %s", joined)
	}
}

func TestNeuraAlerterDetectsConnectionDrop(t *testing.T) {
	var sent []string
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	a := newNeuraAlerter(func(s string) { sent = append(sent, s) }, func() time.Time { return now })
	history := []nodePoint{}
	for m := 60; m >= 16; m-- {
		history = append(history, nodePoint{T: now.Add(-time.Duration(m) * time.Minute).Unix(), Conns: 8000})
	}
	for m := 4; m >= 0; m-- {
		history = append(history, nodePoint{T: now.Add(-time.Duration(m) * time.Minute).Unix(), Conns: 900})
	}
	state := nodeState{Last: nodeReport{Node: "lv", Time: now, CPUs: 4, Conns: map[string]int{"443": 900}}, History: history}
	a.evaluate(map[string]nodeState{"lv": state})
	if len(sent) != 1 || !strings.Contains(sent[0], "соединения на lv упали") {
		t.Fatalf("drop not detected: %v", sent)
	}
	// Recovery to 70% of the baseline clears it.
	for i := len(history) - 5; i < len(history); i++ {
		history[i].Conns = 7000
	}
	a.evaluate(map[string]nodeState{"lv": state})
	if len(sent) != 2 || !strings.Contains(sent[1], "восстановились") {
		t.Fatalf("recovery not announced: %v", sent)
	}
}

func TestRenderNodesGroupsCascadeAndHighlightsProblems(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 8, 0, 0, time.UTC)
	text := renderNodes(map[string]nodeState{
		"lv": {Last: nodeReport{Node: "lv", Time: now.Add(-30 * time.Second), CPUs: 4, Load1: 2.4, MemPct: 38, DiskPct: 15,
			RxMbps: 80, TxMbps: 78, Clients: 352, Conns: map[string]int{"443": 11078, "10001": 524, "10002": 350},
			HelloAt: "2026-11-21T15:08:06Z", Hello: map[string]helloPort{"443": {Flows: 478, Stalled: 5, Frag: 400}, "10002": {Flows: 300, Stalled: 120}}},
			History: []nodePoint{{T: now.Unix(), Conns: 12006}}},
		"ru-entry": {Last: nodeReport{Node: "ru-entry", Time: now, CPUs: 1, Clients: 20, Conns: map[string]int{"8443": 336}}},
		"nl-exit":  {Last: nodeReport{Node: "nl-exit", Time: now, CPUs: 1, Clients: 1, Conns: map[string]int{"443": 39}}},
		"bot":      {Last: nodeReport{Node: "bot", Time: now.Add(-9 * time.Minute), CPUs: 1, DiskPct: 88, Conns: map[string]int{"443": 1}}},
		"de-relay": {Last: nodeReport{Node: "de-relay", Time: now, CPUs: 1}},
	}, map[string]int{"lv": 57, "ru-entry": 2}, now, now)
	for _, want := range []string{
		"👥 онлайн 59 · 🔴 на связи 4 из 5 нод",
		"<b>🇱🇻 Латвия — обходы</b>",
		"онлайн 57 · 11 952 соединения · 352 клиента",
		"🔌 443 · 11 078 │ 10001 · 524 │ 10002 · 350",
		"📈 пик за сутки 12 006",
		"🧊 зависают (18:08): 443 1% · 10002 40% ⚠️",
		"<b>🔗 Каскад 🇷🇺 → 🇳🇱</b>\n🟢 вход 🇷🇺 · ru-entry\n   онлайн 2 · 336 соединений · 20 клиентов",
		"   ↓\n🟢 выход 🇳🇱 · nl-exit\n   39 туннелей от входа",
		"🔴 bot · 9 мин назад",
		"💾 88% ⚠️",
		"<b>Прочие</b>\n🟢 de-relay",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
	if strings.Index(text, "Латвия") > strings.Index(text, "Каскад") || strings.Index(text, "Каскад") > strings.Index(text, "Бот") {
		t.Fatalf("groups out of order:\n%s", text)
	}
}
