package main

import (
	"encoding/json"
	"errors"
	"github.com/Asort97/vpnBot/clients/postgres"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"testing"
	"time"
)

func TestSaleDevicePaymentUsesSavedOrderAmount(t *testing.T) {
	var p yookassa.YooKassaPaymentResponse
	if err := json.Unmarshal([]byte(`{"id":"p1","paid":true,"status":"succeeded","amount":{"value":"25.00","currency":"RUB"},"metadata":{"product_type":"device_slot","device_order_id":"o1","chat_id":42}}`), &p); err != nil {
		t.Fatal(err)
	}
	if !validDeviceSlotPayment(&p, "o1", "42", "p1", 25) {
		t.Fatal("discounted order rejected")
	}
	for _, amount := range []float64{0, 50, 24.99, 100} {
		if validDeviceSlotPayment(&p, "o1", "42", "p1", amount) {
			t.Fatal("wrong amount accepted", amount)
		}
	}
	if validDeviceSlotPayment(&p, "o2", "42", "p1", 25) || validDeviceSlotPayment(&p, "o1", "43", "p1", 25) {
		t.Fatal("wrong order owner accepted")
	}
}

func TestParseSaleArgs(t *testing.T) {
	now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	s, err := parseSaleArgs("blackfriday 50% 2026-11-30 23:59", now)
	if err != nil || s.Name != "BLACKFRIDAY" || s.Percent != 50 || s.EndsAt.Format(time.RFC3339) != "2026-11-30T20:59:00Z" {
		t.Fatal(s, err)
	}
	for _, args := range []string{"", "SALE 50%", "SALE 0 2026-11-30 23:59", "SALE 100 2026-11-30 23:59", "<b> 50 2026-11-30 23:59", "SALE 50 2026-11-31 23:59", "SALE 50 2026-11-30 24:01", "SALE 50 2026-11-13 03:00", "SALE 50% 2026-11-30 23:59 extra"} {
		if _, err := parseSaleArgs(args, now); err == nil {
			t.Fatal("accepted", args)
		}
	}
}

func TestSalePlanPricesDoNotStack(t *testing.T) {
	p := RatePlan{Amount: 149, DeviceExtra: 2, SalePercent: 50}
	if v := planPaymentAmount(p, 0); v != 124.5 {
		t.Fatal(v)
	}
	if v := planPaymentAmount(p, 25); v != 124.5 {
		t.Fatal("stacked", v)
	}
	if v := planPaymentAmount(p, 90); v != 114.9 {
		t.Fatal("better promo ignored", v)
	}
	if n := starsAmountForPlan(p); n != 125 {
		t.Fatal(n)
	}
	p.SalePercent = 5
	if n := starsAmountForPlan(p); n != 234 {
		t.Fatal("Stars discount lost", n)
	}
	p.SalePercent = 0
	if v := planPaymentAmount(p, 25); v != 211.75 {
		t.Fatal("existing promo changed", v)
	}
}

type saleTestStore struct {
	*deviceTestStore
	sale           *postgres.Sale
	err            error
	creates, stops int
}

func (s *saleTestStore) GetActiveSale() (*postgres.Sale, error) { return s.sale, s.err }
func (s *saleTestStore) CreateSale(v postgres.Sale, _ string) error {
	s.creates++
	s.sale = &v
	return s.err
}
func (s *saleTestStore) StopSale(string) (bool, error) { s.stops++; s.sale = nil; return true, s.err }

func TestSalesAdminAuthorizationAndPricingErrors(t *testing.T) {
	oldStore, oldAdmins, oldLog := userStore, adminIDs, logChatID
	defer func() { userStore, adminIDs, logChatID = oldStore, oldAdmins, oldLog }()
	s := &saleTestStore{deviceTestStore: &deviceTestStore{}}
	userStore = s
	adminIDs = []int64{42}
	logChatID = 0
	bot, _ := deviceTestBot(t)
	msg := &tgbotapi.Message{From: &tgbotapi.User{ID: 43}, Chat: &tgbotapi.Chat{ID: 43, Type: "private"}}
	args := "BLACKFRIDAY 50% " + time.Now().Add(48*time.Hour).In(saleZone).Format("2006-01-02 15:04")
	handleSalesCommand(bot, msg, &UserSession{}, "create_sales", args)
	handleSalesCommand(bot, msg, &UserSession{}, "stop_sales", "BLACKFRIDAY")
	if s.creates != 0 || s.stops != 0 {
		t.Fatal("nonadmin changed sale")
	}
	msg.From.ID = 42
	msg.Chat.ID = 42
	msg.Chat.Type = "group"
	handleSalesCommand(bot, msg, &UserSession{}, "create_sales", args)
	if s.creates != 0 {
		t.Fatal("group changed sale")
	}
	msg.Chat.Type = "private"
	handleSalesCommand(bot, msg, &UserSession{}, "create_sales", args)
	if s.creates != 1 || s.sale.Percent != 50 {
		t.Fatal(s)
	}
	if price, err := salePrice(50); err != nil || price != 25 {
		t.Fatal(price, err)
	}
	s.err = errors.New("db unavailable")
	if _, err := salePrice(50); err == nil {
		t.Fatal("charged full price on lookup failure")
	}
}
