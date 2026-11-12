package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func handleNoticeCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, cfg *xraySettings) {
	if cq.From == nil || cq.Message == nil || cq.Message.Chat == nil || cq.Message.Chat.ID != cq.From.ID {
		return
	}
	parts := strings.Split(cq.Data, ":")
	if len(parts) != 3 || notices.load() != nil {
		ackCallback(bot, cq, "уведомление недоступно")
		return
	}
	id := strconv.FormatInt(cq.From.ID, 10)
	action := ""
	for name, code := range noticeActions {
		if code == parts[2] {
			action = name
			break
		}
	}
	notices.mu.Lock()
	u := notices.user(id)
	var found *noticeRecord
	for _, r := range u.Records {
		if r.Token == parts[1] && r.Status == "sent" && r.MessageID == cq.Message.MessageID {
			found = r
			break
		}
	}
	if found == nil || action == "" {
		notices.mu.Unlock()
		ackCallback(bot, cq, "уведомление недоступно")
		return
	}
	kind, key := found.Kind, found.Key
	if found.Clicks == nil {
		found.Clicks = map[string]time.Time{}
	}
	found.Clicks[action] = time.Now().UTC()
	if action == "notice_optout" {
		u.OptOut = true
	}
	err := notices.save()
	notices.mu.Unlock()
	if err != nil {
		ackCallback(bot, cq, "не удалось сохранить, попробуйте ещё раз")
		return
	}
	sendUserNotificationAdminLog(bot, cq.From.ID, cq.From.UserName, "нажатие: "+kind+" · "+action)
	if action == "notice_optout" {
		ackCallback(bot, cq, "предложения отключены. сообщения об оплате и доступе останутся")
		return
	}
	if action == "notice_retry" || action == "notice_check" {
		bill := currentNoticeBill(id)
		if bill == nil || key != kind+":"+bill.ID {
			ackCallback(bot, cq, "этот счёт заменён новым. откройте раздел оплаты")
			return
		}
		if action == "notice_check" {
			copy := *cq
			copy.Data = "check_payment"
			handleCheckPayment(bot, &copy, getSession(cq.From.ID), cfg)
			return
		}
		// Refresh provider state; never create another invoice if this one was paid.
		payment, e := yookassaClient.GetYooKassaPaymentStatus(bill.ID)
		if e != nil || payment == nil {
			ackCallback(bot, cq, "проверка оплаты недоступна, попробуйте позже")
			return
		}
		if !exactUnpaid(payment, bill, id) {
			ackCallback(bot, cq, "проверьте оплату кнопкой «я оплатил»")
			return
		}
		if payment.Status == "pending" {
			b := rawURLButton("оплатить", bill.URL, "5344015205531686528")
			b.Style = "success"
			_, _ = sendMessageRaw(bot, cq.From.ID, "<b>оплата подписки</b>\n\nоткройте страницу вашего счёта.", "HTML", rawInlineKeyboardMarkup{InlineKeyboard: [][]rawInlineKeyboardButton{{b}, {rawCallbackButton("я оплатил", "check_payment", "", "")}}})
			ackCallback(bot, cq, "")
			return
		}
		plan, ok := ratePlanByID[bill.Plan]
		if !ok {
			ackCallback(bot, cq, "выберите тариф заново")
			return
		}
		// A canceled invoice cannot be paid: show the plan at its current price.
		handleRateSelection(bot, cq, getSession(cq.From.ID), plan)
		return
	}
	copy := *cq
	copy.Data = action
	handleCallback(bot, &copy, cfg)
}

func handleIncidentCommand(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) bool {
	if msg == nil || !msg.IsCommand() || msg.Command() != "incident" {
		return false
	}
	if msg.From == nil || !isAdmin(msg.From.ID) {
		return true
	}
	args := strings.Fields(msg.CommandArguments())
	if len(args) != 2 || (args[0] != "open" && args[0] != "resolved") || args[1] != "confirm" {
		_, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "подтверждённая авария: /incident open confirm\nподтверждённое восстановление: /incident resolved confirm\nкоманда запускает сервисные уведомления пользователям"))
		return true
	}
	if notices.load() != nil {
		_, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "не удалось прочитать состояние уведомлений"))
		return true
	}
	notices.mu.Lock()
	old := notices.state
	changed := false
	if args[0] == "open" && !old.IncidentActive {
		notices.state.IncidentID = strconv.FormatInt(time.Now().UnixNano(), 36)
		notices.state.IncidentActive = true
		notices.state.IncidentAt = time.Now().UTC()
		notices.state.RecoveredAt = time.Time{}
		changed = true
	} else if args[0] == "resolved" && old.IncidentActive {
		notices.state.IncidentActive = false
		notices.state.RecoveredAt = time.Now().UTC()
		changed = true
	}
	err := notices.save()
	if err != nil {
		notices.state = old
	}
	notices.mu.Unlock()
	text := "состояние уже установлено"
	if err != nil {
		text = "не удалось сохранить состояние"
	} else if changed {
		text = "состояние подтверждено, уведомления поставлены в обработку"
		select {
		case noticeWake <- struct{}{}:
		default:
		}
		logAction(bot, msg.From.ID, msg.From.UserName, fmt.Sprintf("авария: %s", args[0]), false)
	}
	_, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, text))
	return true
}

func handleNoticeMembership(bot *tgbotapi.BotAPI, update *tgbotapi.ChatMemberUpdated) {
	if update == nil || update.Chat.Type != "private" {
		return
	}
	status := update.NewChatMember.Status
	if status != "kicked" && status != "member" {
		return
	}
	id := strconv.FormatInt(update.Chat.ID, 10)
	blocked := status == "kicked"
	if err := notices.change(id, func(u *noticeUser) { u.Blocked = blocked }); err != nil {
		return
	}
	label := "событие: бот разблокирован"
	if blocked {
		label = "событие: бот заблокирован"
	}
	sendUserNotificationAdminLog(bot, update.Chat.ID, update.Chat.UserName, label)
}
