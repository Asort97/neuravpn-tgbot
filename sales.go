package main

import (
	"fmt"
	"html"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Asort97/vpnBot/clients/postgres"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var saleZone = time.FixedZone("МСК", 3*60*60)
var validSaleName = regexp.MustCompile(`^[A-Z0-9][A-Z0-9_-]{0,47}$`)

const saleUsage = "использование: /create_sales BLACKFRIDAY 50% 2026-11-30 23:59\nвремя московское; скидка от 1 до 99%.\n/sales — текущая акция\n/stop_sales BLACKFRIDAY — остановить"

func parseSaleArgs(args string, now time.Time) (postgres.Sale, error) {
	p := strings.Fields(args)
	if len(p) != 4 {
		return postgres.Sale{}, fmt.Errorf("%s", saleUsage)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(p[1], "%"))
	end, timeErr := time.ParseInLocation("2006-01-02 15:04", p[2]+" "+p[3], saleZone)
	name := strings.ToUpper(p[0])
	if err != nil || timeErr != nil || n < 1 || n > 99 || !validSaleName.MatchString(name) || !end.After(now) {
		return postgres.Sale{}, fmt.Errorf("неверное название, процент или дата окончания в прошлом.\n%s", saleUsage)
	}
	return postgres.Sale{Name: name, Percent: n, EndsAt: end.UTC()}, nil
}

type salesStore interface {
	GetActiveSale() (*postgres.Sale, error)
	CreateSale(postgres.Sale, string) error
	StopSale(string) (bool, error)
}

func currentSale() (*postgres.Sale, error) {
	if s, ok := userStore.(interface {
		GetActiveSale() (*postgres.Sale, error)
	}); ok {
		return s.GetActiveSale()
	}
	return nil, nil
}

func loadPlanSale(p RatePlan) (RatePlan, error) {
	s, err := currentSale()
	p.SalePercent = 0
	if s != nil && err == nil {
		p.SalePercent = s.Percent
	}
	return p, err
}

func planPaymentAmount(p RatePlan, promo int) float64 {
	base := p.Amount + float64(p.DeviceExtra*50)
	amount := base
	if promo > 0 && promo <= 100 {
		amount = math.Round(p.Amount*(1-float64(promo)/100)*100)/100 + float64(p.DeviceExtra*50)
	}
	if p.SalePercent > 0 {
		amount = min(amount, postgres.SaleAmount(base, p.SalePercent))
	}
	return amount
}

func activePaymentPromo(userID string) (int, error) {
	if s, ok := userStore.(interface {
		GetActivePromocode(string) (*struct {
			Code          string
			Discount      int
			DiscountUntil time.Time
		}, error)
	}); ok {
		p, err := s.GetActivePromocode(userID)
		if err != nil {
			return 0, err
		}
		if p != nil {
			return p.Discount, nil
		}
	}
	return 0, nil
}

func salePrice(amount float64) (float64, error) {
	s, err := currentSale()
	if err != nil {
		return 0, err
	}
	if s != nil {
		amount = postgres.SaleAmount(amount, s.Percent)
	}
	return amount, nil
}

func handleSalesCommand(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, session *UserSession, command, args string) {
	if msg == nil || msg.Chat == nil || msg.From == nil {
		return
	}
	reply := func(text string) {
		_ = updateSessionText(bot, msg.Chat.ID, session, stateMenu, text, "HTML", mainMenuInlineKeyboard())
	}
	if msg.From == nil || !isAdmin(msg.From.ID) {
		reply("только для администратора")
		return
	}
	if !msg.Chat.IsPrivate() || msg.Chat.ID != msg.From.ID {
		reply("управляйте акциями в личном чате с ботом")
		return
	}
	s, ok := userStore.(salesStore)
	if !ok {
		reply("акции недоступны для этой базы данных")
		return
	}
	var text string
	switch command {
	case "create_sales":
		sale, err := parseSaleArgs(args, time.Now())
		if err != nil {
			reply(html.EscapeString(err.Error()))
			return
		}
		if err = s.CreateSale(sale, strconv.FormatInt(msg.From.ID, 10)); err != nil {
			log.Printf("create sale failed: %v", err)
			reply("не удалось создать акцию. возможно, уже действует другая: /sales")
			return
		}
		text = fmt.Sprintf("<b>акция включена</b>\n%s · %d%%\nдо %s мск\nавтоматически на все покупки и автосписания. скидки не суммируются; применяется более выгодная цена.\nпосле окончания вернутся обычные цены. ранее созданные счета не меняются.", sale.Name, sale.Percent, sale.EndsAt.In(saleZone).Format("02.01.2006 15:04"))
	case "stop_sales":
		if !validSaleName.MatchString(strings.ToUpper(strings.TrimSpace(args))) {
			reply(saleUsage)
			return
		}
		changed, err := s.StopSale(args)
		if err != nil {
			reply("не удалось остановить акцию")
			return
		}
		if !changed {
			reply("активная акция с таким названием не найдена")
			return
		}
		text = "<b>акция остановлена</b>\n" + html.EscapeString(strings.ToUpper(strings.TrimSpace(args)))
	case "sales":
		sale, err := s.GetActiveSale()
		if err != nil {
			reply("не удалось проверить акцию")
			return
		}
		if sale == nil {
			reply("активных акций нет\n" + saleUsage)
			return
		}
		text = fmt.Sprintf("<b>текущая акция</b>\n%s · %d%%\nдо %s мск", sale.Name, sale.Percent, sale.EndsAt.In(saleZone).Format("02.01.2006 15:04"))
	}
	reply(text)
	if command != "sales" {
		log.Printf("sale admin=%d action=%s args=%s", msg.From.ID, command, args)
		if logChatID != 0 {
			m := tgbotapi.NewMessage(logChatID, text+fmt.Sprintf("\nадминистратор: %d", msg.From.ID))
			m.ParseMode = "HTML"
			if _, err := bot.Send(m); err != nil {
				log.Printf("sale audit failed: %v", err)
			}
		}
	}
}
