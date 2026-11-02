package main

import (
	"strings"
	"testing"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func TestDeviceActionLogEscapesDeviceName(t *testing.T) {
	bot, tg := deviceTestBot(t)
	logDeviceAction(bot, &tgbotapi.User{ID: 42, UserName: "tester"}, "удаление", "не удалось сохранить изменение", "<phone>\n& tablet")
	if len(tg.logs) == 0 {
		t.Fatal("missing tracked event")
	}
	text := tg.logs[len(tg.logs)-1]
	if !strings.Contains(text, "&lt;phone&gt; &amp; tablet") || strings.Contains(text, "<phone>") {
		t.Fatal("device name not HTML escaped", text)
	}
}

func TestFailedDeviceDeletionIsNotLoggedAsSuccess(t *testing.T) {
	cfg, _ := devicePanel(t, "a", 1, "0123456789ab")
	setDeviceTestProviders(t)
	bot, tg := deviceTestBot(t)
	handleCallback(bot, deviceCallback("nav_devices"), cfg)
	var token string
	for key := range getSession(42).DeviceButtons {
		token = key
	}
	userStore.(*deletionTestStore).fail = true
	handleCallback(bot, deviceCallback("devices_del:"+token), cfg)
	if len(tg.logs) == 0 || !strings.Contains(tg.logs[len(tg.logs)-1], "удаление — не удалось сохранить изменение") {
		t.Fatal("failed deletion not tracked", tg.logs)
	}
}
