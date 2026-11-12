package main

import (
	"log"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// Do not encourage a second charge while the first charge has an uncertain result.
func rememberPendingAutopay(id, paymentID string, expiry, attemptAt time.Time) {
	if err := notices.change(id, func(u *noticeUser) { u.PendingAutopay = &noticeAutopay{ID: paymentID, At: attemptAt, Expiry: expiry} }); err != nil {
		log.Printf("pending autopay notification hold could not be saved user=%s: %v", id, err)
	}
}
func processNoticePendingAutopay(bot *tgbotapi.BotAPI) {
	notices.mu.Lock()
	pending := map[string]noticeAutopay{}
	for id, u := range notices.state.Users {
		if u.PendingAutopay != nil {
			pending[id] = *u.PendingAutopay
		}
	}
	notices.mu.Unlock()
	for id, p := range pending {
		u, err := notificationFacts(id)
		if err != nil {
			continue
		}
		paid := u.LastPaidAt.After(p.At)
		canceled := false
		if !paid && p.ID != "" && yookassaClient != nil {
			payment, e := yookassaClient.GetYooKassaPaymentStatus(p.ID)
			if e != nil || payment == nil || payment.ID != p.ID || paymentUserID(payment.Metadata["chat_id"]) != id {
				continue
			}
			canceled = payment.Status == "canceled" && !payment.Paid
		}
		if !paid && !canceled {
			continue
		}
		matched := false
		if err := notices.change(id, func(n *noticeUser) {
			if n.PendingAutopay != nil && n.PendingAutopay.At.Equal(p.At) {
				n.PendingAutopay = nil
				matched = true
			}
		}); err != nil {
			continue
		}
		if !matched {
			continue
		}
		if canceled {
			if err := userStore.DisableAutopay(id); err == nil {
				sendAutopayFailureNotice(bot, id, p.Expiry)
			}
		}
	}
}

// Only an admin who checked the provider can release a hold with no payment ID.
func handleNoticeBillingClear(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) bool {
	if msg == nil || !msg.IsCommand() || msg.Command() != "notification_payment_checked" {
		return false
	}
	if msg.From == nil || !isAdmin(msg.From.ID) {
		return true
	}
	a := strings.Fields(msg.CommandArguments())
	id := int64(0)
	if len(a) == 2 {
		id, _ = strconv.ParseInt(a[0], 10, 64)
	}
	if id <= 0 || a[1] != "confirm" {
		_, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "после ручной проверки счёта в юкассе: /notification_payment_checked <id> confirm"))
		return true
	}
	err := notices.change(a[0], func(u *noticeUser) { u.PendingAutopay = nil })
	text := "пауза платёжных предложений снята; списаний и начислений команда не делает"
	if err != nil {
		text = "не удалось сохранить состояние"
	} else {
		logAction(bot, msg.From.ID, msg.From.UserName, "проверка автоплатежа подтверждена для "+a[0], false)
	}
	_, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, text))
	return true
}
