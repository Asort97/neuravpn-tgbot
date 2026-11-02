package main

import (
	"context"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	"github.com/Asort97/vpnBot/clients/postgres"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
)

func removedDevicesScreen(devices []postgres.DeviceRevocation, page int, notice string) (string, tgbotapi.InlineKeyboardMarkup, map[string]deviceSelection) {
	pages := max(1, (len(devices)+devicesPageSize-1)/devicesPageSize)
	page = min(max(page, 0), pages-1)
	text := fmt.Sprintf("<b>удалённые устройства: %d</b>\n", len(devices))
	if notice != "" {
		text += "\n" + html.EscapeString(notice) + "\n"
	}
	rows := [][]tgbotapi.InlineKeyboardButton{}
	buttons := make(map[string]deviceSelection)
	for i := page * devicesPageSize; i < min(len(devices), (page+1)*devicesPageSize); i++ {
		d := devices[i]
		text += fmt.Sprintf("\n%d. %s\nудалено: %s UTC\n", i+1, html.EscapeString(d.Name), d.RevokedAt.UTC().Format("02.01.2006 15:04"))
		token := uuid.NewString()
		buttons[token] = deviceSelection{RestoreID: d.ID, Page: page, ExpiresAt: time.Now().Add(30 * time.Minute)}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("разблокировать · "+d.Name, "devices_restore:"+token)))
	}
	if pages > 1 {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("←", "devices_removed:"+strconv.Itoa(max(0, page-1))),
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%d / %d", page+1, pages), "devices_removed:"+strconv.Itoa(page)),
			tgbotapi.NewInlineKeyboardButtonData("→", "devices_removed:"+strconv.Itoa(min(pages-1, page+1)))))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("обновить", "devices_removed:"+strconv.Itoa(page))), tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("назад к устройствам", "nav_devices")))
	return text, tgbotapi.NewInlineKeyboardMarkup(rows...), buttons
}

// Caller has authenticated the private chat and holds DevicesMu.
func handleRemovedDevicesCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	uid := strconv.FormatInt(cq.From.ID, 10)
	page, notice := 0, ""
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if strings.HasPrefix(cq.Data, "devices_restore:") {
		token := strings.TrimPrefix(cq.Data, "devices_restore:")
		selection, ok := session.DeviceButtons[token]
		delete(session.DeviceButtons, token)
		if !ok || selection.RestoreID == "" || time.Now().After(selection.ExpiresAt) {
			notice = "эта кнопка устарела. список обновлён."
		} else {
			page = selection.Page
			if _, err := restoreDevice(ctx, uid, selection.RestoreID); err != nil {
				notice = "не удалось разблокировать устройство. попробуйте ещё раз."
			} else {
				notice = "устройство разблокировано. обновите подписку в приложении. лимит устройств продолжает действовать."
			}
		}
	} else {
		page, _ = strconv.Atoi(strings.TrimPrefix(cq.Data, "devices_removed:"))
	}
	devices, err := revokedDevices(ctx, uid)
	if err != nil {
		keyboard := tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("повторить", "devices_removed:0"), tgbotapi.NewInlineKeyboardButtonData("назад", "nav_devices")))
		_ = updateSessionText(bot, cq.Message.Chat.ID, session, stateDevices, "не удалось загрузить удалённые устройства. попробуйте ещё раз.", "", keyboard)
		return
	}
	text, keyboard, buttons := removedDevicesScreen(devices, page, notice)
	session.DeviceButtons = buttons
	_ = updateSessionText(bot, cq.Message.Chat.ID, session, stateDevices, text, "HTML", keyboard)
}
