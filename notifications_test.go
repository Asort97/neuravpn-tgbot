package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	"github.com/Asort97/vpnBot/clients/lifecycle"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
)

func testNotices(t *testing.T) *noticeEngine {
	t.Helper()
	e := &noticeEngine{path: filepath.Join(t.TempDir(), "notifications.json")}
	if err := e.load(); err != nil {
		t.Fatal(err)
	}
	return e
}
func yesNotice() bool     { return true }
func ignoreNotice(string) {}

func TestNoticeTemplates(t *testing.T) {
	kinds := []string{"unclaimed_1", "unclaimed_3", "unclaimed_7", "help_20m", "help_24h", "trial_help", "trial_d1", "d1", "d3", "expired", "expired_d3", "expired_d7", "expired_d30", "payment_5m", "payment_24h", "autopay_failed", "incident", "recovered"}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			s := noticeTemplate(kind, kind)
			if !strings.Contains(s.Text, "<b>") || !strings.Contains(s.Text, "<tg-emoji ") {
				t.Fatal("missing headline/custom emoji")
			}
			plain := stripHTML(s.Text)
			if plain != strings.ToLower(plain) {
				t.Fatal("not lowercase", plain)
			}
			green, optout := 0, false
			for _, row := range s.Keyboard.InlineKeyboard {
				for _, b := range row {
					if b.Style == "success" {
						green++
					} else if b.Style != "" {
						t.Fatal("nonprimary styled")
					}
					if b.Text != strings.ToLower(b.Text) {
						t.Fatal("button not lowercase")
					}
					if b.CallbackData != nil && *b.CallbackData == "notice_optout" {
						optout = true
					}
				}
			}
			want := 1
			if kind == "incident" {
				want = 0
			}
			if green != want {
				t.Fatalf("green=%d", green)
			}
			if optout != s.Promo {
				t.Fatal("optout classification")
			}
			tracked := noticeTrackedKeyboard(s.Keyboard, "0123456789abcdef")
			for _, row := range tracked.InlineKeyboard {
				for _, b := range row {
					if b.CallbackData != nil && (!strings.HasPrefix(*b.CallbackData, "notice:") || len(*b.CallbackData) > 64) {
						t.Fatal("untracked or too long callback")
					}
				}
			}
		})
	}
}

func TestNoticeWindowsAndStops(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	for _, days := range []int{1, 3, 7} {
		entered := now.Add(-time.Duration(days) * 24 * time.Hour)
		u := lifecycle.User{}
		got := lifecycleKinds(u, entered, false, time.Time{}, time.Time{}, now)
		if len(got) != 1 {
			t.Fatalf("day %d: %v", days, got)
		}
		for _, stop := range []lifecycle.User{{Bonus: true}, {Paid: true}, {Referred: true}} {
			if got = lifecycleKinds(stop, entered, false, time.Time{}, time.Time{}, now); len(got) > 0 {
				t.Fatal("chain not stopped")
			}
		}
		if got = lifecycleKinds(u, entered, true, time.Time{}, time.Time{}, now); len(got) > 0 {
			t.Fatal("referral entry not suppressed")
		}
	}
	if len(lifecycleKinds(lifecycle.User{}, now.Add(-100*24*time.Hour), false, time.Time{}, time.Time{}, now)) != 0 {
		t.Fatal("historical catchup")
	}
	for _, tc := range []struct {
		age  time.Duration
		kind string
	}{{0, "expired"}, {3 * 24 * time.Hour, "expired_d3"}, {7 * 24 * time.Hour, "expired_d7"}, {30 * 24 * time.Hour, "expired_d30"}} {
		got := lifecycleKinds(lifecycle.User{Paid: true}, now, false, time.Time{}, now.Add(-tc.age), now)
		if !reflect.DeepEqual(got, []string{tc.kind}) {
			t.Fatal(got)
		}
	}
	got := lifecycleKinds(lifecycle.User{Paid: true}, now, false, time.Time{}, now.Add(20*24*time.Hour), now)
	if len(got) != 0 {
		t.Fatal("renewed user targeted")
	}
	got = lifecycleKinds(lifecycle.User{Autopay: true}, now, false, time.Time{}, now.Add(12*time.Hour), now)
	if len(got) != 0 {
		t.Fatal("manual offer with autopay")
	}
	for _, tc := range []struct {
		age  time.Duration
		kind string
	}{{20 * time.Minute, "help_20m"}, {24 * time.Hour, "help_24h"}} {
		got = lifecycleKinds(lifecycle.User{Paid: true}, now, false, now.Add(-tc.age), now.Add(30*24*time.Hour), now)
		if !reflect.DeepEqual(got, []string{tc.kind}) {
			t.Fatal(got)
		}
	}
}

func TestNoticeRollingCaps(t *testing.T) {
	now := time.Now()
	u := &noticeUser{Records: map[string]*noticeRecord{}}
	if !noticePromoAllowed(u, now) {
		t.Fatal("initial cap")
	}
	u.Records["a"] = &noticeRecord{Status: "sent", Promo: true, At: now.Add(-23 * time.Hour)}
	if noticePromoAllowed(u, now) {
		t.Fatal("daily cap")
	}
	u.Records["a"].At = now.Add(-25 * time.Hour)
	u.Records["b"] = &noticeRecord{Status: "sent", Promo: true, At: now.Add(-72 * time.Hour)}
	u.Records["c"] = &noticeRecord{Status: "sent", Promo: true, At: now.Add(-120 * time.Hour)}
	if noticePromoAllowed(u, now) {
		t.Fatal("weekly cap")
	}
	u.Records["c"].Promo = false
	if !noticePromoAllowed(u, now) {
		t.Fatal("service consumed quota")
	}
	u.Records["a"].Status = "failed"
	u.Records["b"].Status = "failed"
	if !noticePromoAllowed(u, now) {
		t.Fatal("failed delivery consumed quota")
	}
	u.OptOut = true
	if noticePromoAllowed(u, now) {
		t.Fatal("optout ignored")
	}
}

func TestNoticeDeliveryPersistenceAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		msg    int
		status string
	}{{"success", nil, 42, "sent"}, {"rejected", errors.New("Bad Request: invalid payload"), 0, "failed"}, {"timeout", errors.New("connection timeout"), 0, "unknown"}, {"blocked", errors.New("Forbidden: bot was blocked by the user"), 0, "blocked"}, {"empty", nil, 0, "unknown"}} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNotices(t)
			var logs []string
			calls := 0
			send := func(string, rawInlineKeyboardMarkup) (int, error) {
				calls++
				r := e.snapshot("42").Records["test"]
				if r.Status != "sending" || !r.At.IsZero() {
					t.Fatal("marked delivered before Telegram")
				}
				return tc.msg, tc.err
			}
			s := noticeTemplate("expired", "test")
			ok := e.deliver("42", s, yesNotice, send, func(v string) { logs = append(logs, v) })
			if ok != (tc.status == "sent") {
				t.Fatal("bad result")
			}
			reopened := &noticeEngine{path: e.path}
			if err := reopened.load(); err != nil {
				t.Fatal(err)
			}
			r := reopened.snapshot("42").Records["test"]
			if r.Status != tc.status {
				t.Fatal(r.Status)
			}
			if (r.MessageID > 0) != (tc.status == "sent") {
				t.Fatal("false delivery")
			}
			if len(logs) != 1 || strings.HasPrefix(logs[0], "доставлено:") != (tc.status == "sent") {
				t.Fatal("false admin log", logs)
			}
			reopened.deliver("42", s, yesNotice, send, ignoreNotice)
			if calls != 1 {
				t.Fatal("duplicate after restart")
			}
		})
	}
}

func TestNoticeStopsAndRecheck(t *testing.T) {
	e := testNotices(t)
	count := 0
	send := func(string, rawInlineKeyboardMarkup) (int, error) { count++; return 1, nil }
	s := noticeTemplate("unclaimed_1", "a")
	e.deliver("42", s, func() bool { return false }, send, ignoreNotice)
	if count != 0 || len(e.snapshot("42").Records) != 0 {
		t.Fatal("stale candidate sent")
	}
	if err := e.change("42", func(u *noticeUser) { u.OptOut = true }); err != nil {
		t.Fatal(err)
	}
	e.deliver("42", s, yesNotice, send, ignoreNotice)
	if count != 0 {
		t.Fatal("optout")
	}
	if !e.deliver("42", noticeTemplate("expired", "expiry"), yesNotice, send, ignoreNotice) {
		t.Fatal("service suppressed by optout")
	}
	e.mu.Lock()
	e.state.IncidentActive = true
	e.mu.Unlock()
	e.deliver("43", s, yesNotice, send, ignoreNotice)
	e.deliver("43", noticeTemplate("help_24h", "help"), yesNotice, send, ignoreNotice)
	if count != 1 {
		t.Fatal("outage pause")
	}
	if !e.deliver("43", noticeTemplate("incident", "incident:1"), yesNotice, send, ignoreNotice) {
		t.Fatal("incident suppressed")
	}
}

func TestNoticeAtomicQuotaConcurrent(t *testing.T) {
	e := testNotices(t)
	var calls atomic.Int32
	var wg sync.WaitGroup
	for _, kind := range []string{"unclaimed_1", "unclaimed_3"} {
		wg.Add(1)
		go func(kind string) {
			defer wg.Done()
			e.deliver("42", noticeTemplate(kind, kind), yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { calls.Add(1); return 5, nil }, ignoreNotice)
		}(kind)
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("quota race", calls.Load())
	}
}

func TestNoticeSlowUserDoesNotLockAllUsers(t *testing.T) {
	e := testNotices(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		e.deliver("42", noticeTemplate("expired", "slow"), func() bool { close(entered); <-release; return false }, nil, ignoreNotice)
	}()
	<-entered
	other := make(chan bool, 1)
	go func() {
		other <- e.deliver("43", noticeTemplate("expired", "fast"), yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { return 3, nil }, ignoreNotice)
	}()
	select {
	case ok := <-other:
		if !ok {
			t.Fatal("second user blocked")
		}
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("global lock during upstream check")
	}
	close(release)
	<-done
}

func TestNoticePersistenceRequiredBeforeSending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &noticeEngine{path: filepath.Join(path, "notifications.json"), ready: true, state: noticeState{Users: map[string]*noticeUser{}}}
	sent := false
	e.deliver("42", noticeTemplate("expired", "test"), yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { sent = true; return 1, nil }, ignoreNotice)
	if sent {
		t.Fatal("sent without reservation")
	}
}

func TestNoticeExactInvoiceStatus(t *testing.T) {
	b := &noticeBill{ID: "current"}
	id := "623290294"
	for _, tc := range []struct {
		paymentID, status string
		paid              bool
		owner             any
		want              bool
	}{{"current", "pending", false, float64(623290294), true}, {"current", "canceled", false, id, true}, {"current", "succeeded", true, id, false}, {"current", "waiting_for_capture", true, id, false}, {"current", "waiting_for_capture", false, id, false}, {"old", "succeeded", true, id, false}, {"other", "pending", false, id, false}, {"current", "pending", false, "another", false}, {"current", "unknown", false, id, false}, {"current", "pending", true, id, false}} {
		p := &yookassa.YooKassaPaymentResponse{ID: tc.paymentID, Status: tc.status, Paid: tc.paid, Metadata: map[string]interface{}{"chat_id": tc.owner}}
		if got := exactUnpaid(p, b, id); got != tc.want {
			t.Fatalf("%+v: %v", tc, got)
		}
	}
}

func TestNoticeKeyboardDoesNotMutateOriginal(t *testing.T) {
	s := noticeTemplate("unclaimed_1", "x")
	original := *s.Keyboard.InlineKeyboard[1][0].CallbackData
	_ = noticeTrackedKeyboard(s.Keyboard, "token")
	if *s.Keyboard.InlineKeyboard[1][0].CallbackData != original {
		t.Fatal("mutated template")
	}
}

func TestNoticeConnectionIncludesOtherInbounds(t *testing.T) {
	old, olds := mergedXrayCfg, mergedXrayCfgs
	mergedXrayCfg = nil
	mergedXrayCfgs = nil
	t.Cleanup(func() { mergedXrayCfg = old; mergedXrayCfgs = olds })
	for _, tc := range []struct {
		body             string
		connected, known bool
	}{{`{"down":900000}`, true, true}, {`null`, false, false}, {`{"down":0}`, false, true}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/panel/api/clients/get/tgId/42":
				w.Write([]byte(`{"success":true,"obj":[{"client":{"tgId":"42","email":"inb5","enable":true}},{"client":{"tgId":"42","email":"inb6","enable":true}}]}`))
			case "/panel/api/clients/traffic/inb5":
				w.Write([]byte(`{"success":true,"obj":{"up":0,"down":0}}`))
			case "/panel/api/clients/traffic/inb6":
				w.Write([]byte(`{"success":true,"obj":` + tc.body + `}`))
			default:
				t.Errorf("unexpected request %s", r.URL.Path)
				w.WriteHeader(500)
			}
		}))
		u, _ := url.Parse(server.URL)
		cfg := &xraySettings{client: xray.New("", "", u.Hostname(), u.Port(), ""), inboundIDs: []int{5, 6}}
		connected, known := noticeConnectionKnown(cfg, "42")
		server.Close()
		if connected != tc.connected || known != tc.known {
			t.Fatalf("%s: connected=%v known=%v", tc.body, connected, known)
		}
	}
}

func TestNoticeSecondCheckHonorsNewOptout(t *testing.T) {
	e := testNotices(t)
	sent := false
	e.deliver("42", noticeTemplate("unclaimed_1", "a"), func() bool {
		if err := e.change("42", func(u *noticeUser) { u.OptOut = true }); err != nil {
			t.Fatal(err)
		}
		return true
	}, func(string, rawInlineKeyboardMarkup) (int, error) { sent = true; return 1, nil }, ignoreNotice)
	if sent {
		t.Fatal("optout changed during lookup ignored")
	}
}

func TestNoticePendingChargeSuppressesSecondPayment(t *testing.T) {
	e := testNotices(t)
	_ = e.change("42", func(u *noticeUser) { u.PendingAutopay = &noticeAutopay{ID: "pending"} })
	for _, kind := range []string{"d1", "expired", "payment_24h", "autopay_failed"} {
		if e.deliver("42", noticeTemplate(kind, kind), yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { t.Fatal("second payment suggested"); return 0, nil }, ignoreNotice) {
			t.Fatal("sent")
		}
	}
	if !e.deliver("42", noticeTemplate("incident", "incident:1"), yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { return 1, nil }, ignoreNotice) {
		t.Fatal("important incident blocked")
	}
}
