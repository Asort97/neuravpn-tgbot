package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func parseDeviceBonusArgs(args string) ([]string, int, error) {
	fields := strings.Fields(args)
	if len(fields) != 2 {
		return nil, 0, fmt.Errorf("expected IDs and slots")
	}
	slots, err := strconv.Atoi(fields[1])
	if err != nil || slots < 1 || slots > 95 {
		return nil, 0, fmt.Errorf("invalid slots")
	}
	ids := strings.Split(fields[0], ",")
	if len(ids) > 20 {
		return nil, 0, fmt.Errorf("too many IDs")
	}
	seen := map[string]bool{}
	for _, id := range ids {
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n <= 0 || strconv.FormatInt(n, 10) != id || seen[id] {
			return nil, 0, fmt.Errorf("invalid ID")
		}
		seen[id] = true
	}
	return ids, slots, nil
}

func handleAddDeviceSlots(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	if msg == nil || msg.From == nil || msg.Chat == nil {
		return
	}
	send := func(text string) { _, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, text)) }
	if !isAdmin(msg.From.ID) {
		send("Команда доступна только администратору.")
		return
	}
	if !msg.Chat.IsPrivate() || msg.Chat.ID != msg.From.ID {
		send("Используйте команду в личном чате с ботом.")
		return
	}
	ids, slots, err := parseDeviceBonusArgs(msg.CommandArguments())
	if err != nil || msg.MessageID <= 0 {
		send("/addslots ID 3 — добавить 3 бесплатных слота навсегда.\nНесколько пользователей: /addslots 123,456 3\nДо 20 ID за раз, от 1 до 95 слотов. У пользователя должно быть не более 95 бесплатных бонусных слотов.")
		return
	}
	s, ok := userStore.(interface {
		GrantDeviceBonus(context.Context, string, string, []string, int) (bool, error)
	})
	if !ok {
		send("Начисление слотов временно недоступно.")
		return
	}
	send("добавление постоянных слотов...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	applied, err := s.GrantDeviceBonus(ctx, fmt.Sprintf("tg:%d:%d", msg.Chat.ID, msg.MessageID), strconv.FormatInt(msg.From.ID, 10), ids, slots)
	cancel()
	if err != nil {
		send("Не удалось подтвердить начисление. Проверьте, что все ID зарегистрированы и бонус не превысит 95. При ошибке связи не отправляйте новую команду, пока не проверите результат: она будет отдельным начислением.")
		return
	}
	var text strings.Builder
	if applied {
		fmt.Fprintf(&text, "Добавлено по %d бесплатных слотов навсегда.\n", slots)
	} else {
		text.WriteString("Это сообщение уже обработано. Повторного начисления нет.\n")
	}
	for _, id := range ids {
		invalidateMergedSubscriptionCache(id)
		p, err := currentDevicePlan(id)
		if err != nil {
			fmt.Fprintf(&text, "\n%s: бонус сохранён, итоговый лимит временно недоступен.", id)
			continue
		}
		if p.Enabled {
			fmt.Fprintf(&text, "\n%s: лимит %d (5 + %d бесплатных + %d платных).", id, p.Limit(), p.BonusSlots, p.ExtraSlots)
		} else {
			fmt.Fprintf(&text, "\n%s: бесплатный бонус %d сохранён; ограничения сейчас отключены.", id, p.BonusSlots)
		}
	}
	select {
	case devicePlanSyncWake <- struct{}{}:
	default:
	}
	text.WriteString("\n\nЦена, автопродление и оплаченные дни не меняются. Синхронизация с панелью идёт в фоне; при недоступности будет повторена.")
	send(text.String())
}
