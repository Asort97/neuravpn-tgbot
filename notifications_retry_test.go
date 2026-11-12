package main

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func TestNoticeRetriesBoundedAcrossRestartWithoutChatSpam(t *testing.T) {
	e := testNotices(t)
	s := noticeTemplate("expired_d7", "expired_d7:test")
	calls, audits := 0, 0
	send := func(string, rawInlineKeyboardMarkup) (int, error) {
		calls++
		return 0, &tgbotapi.Error{Code: 429, Message: "Too Many Requests", ResponseParameters: tgbotapi.ResponseParameters{RetryAfter: 1800}}
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if e.deliver("42", s, yesNotice, send, func(string) { audits++ }) {
			t.Fatal("false delivery")
		}
		reopened := &noticeEngine{path: e.path}
		if err := reopened.load(); err != nil {
			t.Fatal(err)
		}
		e = reopened
		r := e.snapshot("42").Records[s.Key]
		if r.Attempts != attempt || r.LastError != "Too Many Requests" || r.Status != "failed" || !r.At.IsZero() {
			t.Fatal("retry state lost", r)
		}
		e.mu.Lock()
		if e.eligibleLocked("42", s, time.Now()) {
			t.Fatal("early retry")
		}
		if attempt < 3 {
			minimum := 30 * time.Minute
			if attempt == 2 {
				minimum = time.Hour
			}
			if r.NextAttemptAt.Sub(r.AttemptAt) < minimum {
				t.Fatal("backoff/retry_after ignored")
			}
			if !e.eligibleLocked("42", s, r.NextAttemptAt) {
				t.Fatal("retry never eligible")
			}
			e.user("42").Records[s.Key].NextAttemptAt = time.Now().Add(-time.Second)
		} else if e.eligibleLocked("42", s, time.Now().Add(30*24*time.Hour)) {
			t.Fatal("retry limit ignored")
		}
		e.mu.Unlock()
	}
	if calls != 3 || audits != 0 {
		t.Fatal("unexpected retry or admin chat spam", calls, audits)
	}
}

func TestNoticePermanentFailureDoesNotRetry(t *testing.T) {
	for _, err := range []error{
		errors.New("Bad Request: chat not found"),
		errors.New("Forbidden: user is deactivated"),
		&tgbotapi.Error{Code: 400, Message: "invalid keyboard"},
		&tgbotapi.Error{Code: 403, Message: "bot can't initiate conversation with a user"},
	} {
		e := testNotices(t)
		s := noticeTemplate("expired_d7", "test")
		e.deliver("42", s, yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { return 0, err }, func(string) { t.Fatal("failure leaked to admin chat") })
		reopened := &noticeEngine{path: e.path}
		if err := reopened.load(); err != nil {
			t.Fatal(err)
		}
		reopened.mu.Lock()
		allowed := reopened.eligibleLocked("42", s, time.Now().Add(30*24*time.Hour))
		reopened.mu.Unlock()
		if allowed || !reopened.snapshot("42").Records[s.Key].PermanentFailure {
			t.Fatal("permanent rejection retried")
		}
	}
}

func TestNoticeLegacyFailureBackoffAndSuccess(t *testing.T) {
	e := testNotices(t)
	s := noticeTemplate("expired_d7", "legacy")
	now := time.Now()
	e.mu.Lock()
	e.user("42").Records[s.Key] = &noticeRecord{Key: s.Key, Kind: s.Kind, Status: "failed", AttemptAt: now}
	if e.eligibleLocked("42", s, now.Add(5*time.Minute)) {
		t.Fatal("old fast retry retained")
	}
	if !e.eligibleLocked("42", s, now.Add(15*time.Minute)) {
		t.Fatal("legacy record cannot retry")
	}
	e.user("42").Records[s.Key].AttemptAt = now.Add(-time.Hour)
	e.mu.Unlock()
	audits := 0
	if !e.deliver("42", s, yesNotice, func(string, rawInlineKeyboardMarkup) (int, error) { return 7, nil }, func(string) { audits++ }) {
		t.Fatal("recovery failed")
	}
	r := e.snapshot("42").Records[s.Key]
	if audits != 1 || r.Attempts != 2 || r.Status != "sent" || r.MessageID != 7 {
		t.Fatal("bad recovery")
	}
}

func TestNoticeErrorDoesNotExposeBotToken(t *testing.T) {
	token := "123456:test-secret"
	t.Setenv("TG_BOT_TOKEN", token)
	for _, err := range []error{
		&url.Error{Op: "Post", URL: "https://api.telegram.org/bot" + token + "/sendMessage", Err: errors.New("connection reset")},
		errors.New("failure " + token + "\nnext line"),
	} {
		s := noticeDeliveryError(err)
		if strings.Contains(s, token) || strings.Contains(s, "\n") || strings.Contains(s, "api.telegram.org") {
			t.Fatal("unsafe error log")
		}
	}
}
