package main

import (
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBroadcastEffectParsingAndPreviewAliases(t *testing.T) {
	for _, cmd := range []string{"notify-test", "notify_test", "notifytest"} {
		if !isNotifyCommandName(cmd) || !isNotifyTestCommandName(cmd) {
			t.Fatal(cmd)
		}
	}
	if isNotifyTestCommandName("notify") {
		t.Fatal("broadcast treated as preview")
	}
	for _, tc := range []struct{ in, body, effect string }{{"hello 🎉", "hello 🎉", ""}, {"--effect confetti <b>hello</b>\nworld", "<b>hello</b>\nworld", confettiEffectID}, {"--effect 🎉", "", confettiEffectID}} {
		body, effect, err := parseBroadcastEffect(tc.in)
		if err != nil || body != tc.body || effect != tc.effect {
			t.Fatal(tc, body, effect, err)
		}
	}
	for _, in := range []string{"--effect", "--effect other hi", "--effect=confetti hi"} {
		if _, _, err := parseBroadcastEffect(in); err == nil {
			t.Fatal(in)
		}
	}
}

func TestBroadcastEffectPayloadAndSingleDelivery(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "getMe") {
			w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"test"}}`))
			return
		}
		calls++
		r.ParseForm()
		if r.Form.Get("chat_id") != "623290294" || r.Form.Get("message_effect_id") != confettiEffectID || r.Form.Get("text") != "<b>test</b>" {
			t.Error(r.Form)
		}
		w.Write([]byte(`{"ok":false,"error_code":400,"description":"effect rejected"}`))
	}))
	defer server.Close()
	bot, err := tgbotapi.NewBotAPIWithClient("test", server.URL+"/bot%s/%s", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	src := &tgbotapi.Message{MessageID: 5, Chat: &tgbotapi.Chat{ID: 42}, Text: "original"}
	if err := sendBroadcastPayload(bot, 623290294, src, "<b>test</b>", false, confettiEffectID); err == nil || calls != 1 {
		t.Fatal("error silently retried", err, calls)
	}
	method, p, err := broadcastEffectParams(42, src, "", true, confettiEffectID)
	if err != nil || method != "copyMessage" || p["from_chat_id"] != "42" || p["message_id"] != "5" || p["parse_mode"] != "" {
		t.Fatal(method, p, err)
	}
	src.Photo = []tgbotapi.PhotoSize{{FileID: "photo"}}
	method, p, err = broadcastEffectParams(42, src, "caption", false, confettiEffectID)
	if err != nil || method != "sendPhoto" || p["photo"] != "photo" || p["caption"] != "caption" {
		t.Fatal(method, p, err)
	}
	if _, _, err = broadcastEffectParams(-42, src, "test", false, confettiEffectID); err == nil {
		t.Fatal("group accepted")
	}
}
