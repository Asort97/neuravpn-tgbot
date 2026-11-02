package main

import (
	"html"
	"log"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func logDeviceAction(bot *tgbotapi.BotAPI, user *tgbotapi.User, action, result, name string) {
	name = strings.Join(strings.Fields(name), " ")
	runes := []rune(name)
	if len(runes) > 100 {
		name = string(runes[:100])
	}
	text := "устройство: " + action + " — " + result
	if name != "" {
		text += " · " + name
	}
	log.Printf("[device-action] source=bot user=%d action=%s result=%q device=%q", user.ID, action, result, name)
	logAction(bot, user.ID, user.UserName, html.EscapeString(text), false)
}
