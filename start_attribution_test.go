package main

import (
	"path/filepath"
	"strings"
	"testing"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func TestTikTokStartAttribution(t *testing.T) {
	msg := &tgbotapi.Message{Text: "/start ad_tiktok", Entities: []tgbotapi.MessageEntity{{Type: "bot_command", Offset: 0, Length: 6}}}
	tag := extractAdTag(msg)
	if tag != "tiktok" {
		t.Fatal(tag)
	}
	for _, fresh := range []bool{false, true} {
		if !strings.Contains(startLogAction(fresh, "", "42", tag), "ИЗ ТИКТОКА") {
			t.Fatal("missing TikTok source")
		}
	}
	if startLogAction(true, "43", "42", "") != "новый пользователь по рефералке" {
		t.Fatal("referral log changed")
	}
	if startLogAction(true, "42", "42", "") != "новый пользователь" {
		t.Fatal("self referral changed")
	}
	if startLogAction(false, "", "42", "other") != "start" {
		t.Fatal("unrelated campaign mislabeled")
	}
	if strings.Contains(startLogAction(false, "", "42", "<b>tiktok</b>"), "<b>") {
		t.Fatal("untrusted tag in HTML log")
	}
	stats := newAdStatsStore(filepath.Join(t.TempDir(), "ads.json"))
	if count, added := stats.record(tag, "42"); count != 1 || !added {
		t.Fatal("new start not counted")
	}
	if count, added := stats.record(tag, "42"); count != 1 || added {
		t.Fatal("same user counted twice")
	}
	loaded := newAdStatsStore(stats.path)
	loaded.load()
	if !loaded.data["tiktok"]["42"] {
		t.Fatal("source not persisted")
	}
}

func TestTikTokTrackedTelegramLog(t *testing.T) {
	bot, tg := deviceTestBot(t)
	logAction(bot, 42, "tester", startLogAction(false, "", "42", "tiktok"), false)
	if len(tg.logs) == 0 || !strings.Contains(tg.logs[len(tg.logs)-1], "ИЗ ТИКТОКА") {
		t.Fatal("source absent from tracked chat log")
	}
}
