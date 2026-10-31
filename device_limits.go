package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Asort97/vpnBot/clients/postgres"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type devicePlanStore interface {
	GetDevicePlan(string) (postgres.DevicePlan, error)
	CreateDeviceSlotOrder(string) (postgres.DeviceSlotOrder, error)
	GetDeviceSlotOrder(string, string) (postgres.DeviceSlotOrder, error)
	SetDeviceOrderPayment(string, string, string) error
	ApplyDeviceSlotOrder(string, string, string) (bool, error)
	RemoveExtraDeviceSlot(string, int) error
	GetEnabledDevicePlans() ([]string, error)
	GetPendingDeviceSlotOrders() ([]postgres.DeviceSlotOrder, error)
	EnableAutopayWithDevices(string, int) error
	MarkDevicePlanSynced(string, int) error
}

func starsPlanPayload(p RatePlan) string {
	if p.DeviceExtra == 0 {
		return starsPayloadPrefix + p.ID
	}
	return starsPayloadPrefix + p.ID + ":d" + strconv.Itoa(p.DeviceExtra)
}

func parseStarsPlan(id string) (RatePlan, bool) {
	parts := strings.Split(id, ":d")
	if len(parts) > 2 {
		return RatePlan{}, false
	}
	p, ok := ratePlanByID[parts[0]]
	if !ok {
		return p, false
	}
	if len(parts) == 2 {
		n, err := strconv.Atoi(parts[1])
		if err != nil || n < 0 || n > 95 {
			return RatePlan{}, false
		}
		p.DeviceExtra = n
	}
	return p, true
}

func currentDevicePlan(userID string) (postgres.DevicePlan, error) {
	if s, ok := userStore.(devicePlanStore); ok {
		return s.GetDevicePlan(userID)
	}
	return postgres.DevicePlan{}, nil
}

func deviceSurcharge(userID string) (int, error) {
	p, err := currentDevicePlan(userID)
	if err != nil {
		return 0, err
	}
	return p.ExtraSlots * postgres.DeviceSlotPrice, nil
}

func reduceDeviceSlots(userID string) error {
	s, ok := userStore.(devicePlanStore)
	if !ok {
		return errors.New("управление местами недоступно")
	}
	p, err := s.GetDevicePlan(userID)
	if err != nil {
		return err
	}
	if !p.Enabled || p.ExtraSlots < 1 {
		return errors.New("нет дополнительных мест")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	view := readSubscriptionDevices(ctx, []*xraySettings{xrayCfg}, userID)
	if view.Unavailable+view.Unsupported > 0 {
		return errors.New("не удалось проверить число устройств")
	}
	if len(view.Devices) > 4+p.ExtraSlots {
		return errors.New("сначала удалите лишнее устройство из списка")
	}
	if err = s.RemoveExtraDeviceSlot(userID, p.ExtraSlots); err != nil {
		return err
	}
	invalidateMergedSubscriptionCache(userID)
	return syncPrimaryDeviceLimit(ctx, xrayCfg, userID)
}

func filterLimitedDeviceView(view subscriptionDevices, primary *xraySettings, userID string) subscriptionDevices {
	p, err := currentDevicePlan(userID)
	if err != nil || !p.Enabled || primary == nil || primary.client == nil {
		return view
	}
	filtered := make([]subscriptionDevice, 0, len(view.Devices))
	for _, d := range view.Devices {
		for _, r := range d.Records {
			if r.Config.client.PanelIdentity() == primary.client.PanelIdentity() {
				filtered = append(filtered, d)
				break
			}
		}
	}
	view.Devices = filtered
	return view
}

func syncPrimaryDeviceLimit(ctx context.Context, primary *xraySettings, userID string) error {
	p, err := currentDevicePlan(userID)
	if err != nil {
		return err
	}
	if !p.Enabled {
		return nil
	}
	if primary == nil || primary.client == nil {
		return errors.New("primary panel unavailable")
	}
	clients, err := primary.client.GetSubscriptionClients(ctx, userID, primary.inboundIDs)
	if err != nil {
		return err
	}
	if len(clients) == 0 {
		// Accounts without a subscription must not starve the bounded worker.
		// Their first subscription fetch still performs the native gate check.
		if s, ok := userStore.(devicePlanStore); ok {
			return s.MarkDevicePlanSynced(userID, p.ExtraSlots)
		}
		return nil
	}
	for _, c := range clients {
		if c.TgID != userID || c.Email == "" {
			return errors.New("invalid subscription owner")
		}
		if c.LimitHwid != 5+p.ExtraSlots {
			if err = primary.client.SetHWIDLimit(ctx, c.Email, userID, 5+p.ExtraSlots); err != nil {
				return err
			}
		}
	}
	if s, ok := userStore.(devicePlanStore); ok {
		return s.MarkDevicePlanSynced(userID, p.ExtraSlots)
	}
	return nil
}

func limitPlaceholder(w http.ResponseWriter, title string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Profile-Title", "base64:"+base64.StdEncoding.EncodeToString([]byte(title)))
	w.Header().Set("X-Neura-Device-Limit", "true")
	// Documentation-only IP, port 1 and an unrelated UUID; never expose a real node.
	link := "vless://00000000-0000-4000-8000-000000000000@192.0.2.1:1?encryption=none&security=none&type=tcp#" + url.PathEscape(title) + "\n"
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(base64.StdEncoding.EncodeToString([]byte(link))))
}

func hwidDenial(headers http.Header) string {
	if strings.EqualFold(headers.Get("X-Hwid-Max-Devices-Reached"), "true") {
		return "Достигнут лимит устройств"
	}
	if strings.EqualFold(headers.Get("X-Hwid-Not-Supported"), "true") {
		return "Приложение не передаёт HWID"
	}
	return ""
}

// A denied device must never receive stale cached configs or merged-node links.
// The primary panel owns the single device registry for the whole merged URL.
func gateDeviceSubscription(w http.ResponseWriter, r *http.Request, userID string) bool {
	p, err := currentDevicePlan(userID)
	if err != nil {
		http.Error(w, "device status unavailable", 503)
		return false
	}
	if !p.Enabled {
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	upstream, _, _, err := buildSubscriptionURLForMergedRequest(xrayCfg, userID)
	if err != nil {
		http.Error(w, "subscription unavailable", 503)
		return false
	}
	request := r.Clone(ctx)
	request.URL = &url.URL{}
	request.Header = r.Header.Clone()
	request.Header.Set("Accept", "text/plain")
	request.Header.Set("User-Agent", "v2rayNG/1.10")
	_, headers, code, err := fetchPrimarySubscriptionBody(ctx, xrayCfg, userID, upstream, request)
	if err != nil {
		http.Error(w, "device verification unavailable", 503)
		return false
	}
	// Newly created primary clients may not have been visited by the worker yet.
	// Never release their first configs until the native HWID gate is enabled.
	if code == http.StatusOK && !strings.EqualFold(headers.Get("X-Hwid-Active"), "true") && hwidDenial(headers) == "" {
		if err = syncPrimaryDeviceLimit(ctx, xrayCfg, userID); err != nil {
			http.Error(w, "device verification unavailable", 503)
			return false
		}
		_, headers, code, err = fetchPrimarySubscriptionBody(ctx, xrayCfg, userID, upstream, request)
		if err != nil {
			http.Error(w, "device verification unavailable", 503)
			return false
		}
	}
	if title := hwidDenial(headers); title != "" {
		invalidateMergedSubscriptionCache(userID)
		limitPlaceholder(w, title)
		return false
	}
	if code != http.StatusOK || !strings.EqualFold(headers.Get("X-Hwid-Active"), "true") {
		http.Error(w, "device verification unavailable", 503)
		return false
	}
	return true
}

func createDeviceSlotPayment(userID string) (postgres.DeviceSlotOrder, error) {
	s, ok := userStore.(devicePlanStore)
	if !ok {
		return postgres.DeviceSlotOrder{}, errors.New("оплата мест недоступна")
	}
	plan, err := s.GetDevicePlan(userID)
	if err != nil || !plan.Enabled {
		return postgres.DeviceSlotOrder{}, errors.New("докупка мест пока недоступна для этого аккаунта")
	}
	info, err := ensureXrayAccessFresh(xrayCfg, userID, fallbackEmail(userID), 0, false)
	if err != nil || info == nil || info.client == nil || !info.client.Enable || (info.client.ExpiryTime > 0 && info.expireAt.Before(time.Now())) {
		return postgres.DeviceSlotOrder{}, errors.New("сначала продлите VPN-подписку")
	}
	o, err := s.CreateDeviceSlotOrder(userID)
	if err != nil {
		return o, errors.New("докупка мест пока недоступна для этого аккаунта")
	}
	if o.URL != "" {
		return o, nil
	}
	uid, err := strconv.ParseInt(userID, 10, 64)
	if err != nil {
		return o, err
	}
	email, _ := userStore.GetEmail(userID)
	if email == "" {
		email = fallbackEmail(userID)
	}
	meta := map[string]any{"product_type": "device_slot", "device_order_id": o.ID}
	payment, err := yookassaClient.CreatePaymentWithKey(50, "Дополнительное устройство до окончания VPN-подписки", uid, "device_slot", meta, email, false, "device-slot-"+o.ID)
	if err != nil {
		return o, errors.New("не удалось создать платёж; повторите попытку")
	}
	link, _ := payment.Confirmation["confirmation_url"].(string)
	u, parseErr := url.Parse(link)
	if parseErr != nil || u.Scheme != "https" || u.Host == "" || payment.ID == "" {
		return o, errors.New("платёжная система не вернула ссылку")
	}
	if err = s.SetDeviceOrderPayment(o.ID, payment.ID, link); err != nil {
		return o, err
	}
	o.PaymentID, o.URL = payment.ID, link
	return o, nil
}

func applyDeviceSlotPayment(orderID, userID, paymentID string) (bool, error) {
	s, ok := userStore.(devicePlanStore)
	if !ok {
		return false, errors.New("device billing unavailable")
	}
	o, err := s.GetDeviceSlotOrder(orderID, userID)
	if err != nil {
		return false, err
	}
	if o.PaymentID != "" && o.PaymentID != paymentID {
		return false, errors.New("device payment mismatch")
	}
	if o.Applied {
		return false, nil
	}
	paid, err := yookassaClient.GetYooKassaPaymentStatus(paymentID)
	if err != nil {
		return false, err
	}
	if !validDeviceSlotPayment(paid, orderID, userID, paymentID) {
		return false, errors.New("payment is not a confirmed device slot purchase")
	}
	applied, err := s.ApplyDeviceSlotOrder(orderID, userID, paymentID)
	if err == nil {
		invalidateMergedSubscriptionCache(userID)
	}
	return applied, err
}

func validDeviceSlotPayment(p *yookassa.YooKassaPaymentResponse, orderID, userID, paymentID string) bool {
	return p != nil && p.ID == paymentID && p.Paid && p.Status == "succeeded" && fmt.Sprint(p.Amount["value"]) == "50.00" && fmt.Sprint(p.Amount["currency"]) == "RUB" && fmt.Sprint(p.Metadata["product_type"]) == "device_slot" && fmt.Sprint(p.Metadata["device_order_id"]) == orderID && paymentUserID(p.Metadata["chat_id"]) == userID
}

func paymentUserID(value any) string {
	if n, ok := value.(float64); ok {
		return strconv.FormatFloat(n, 'f', 0, 64)
	}
	return fmt.Sprint(value)
}

func startDevicePlanWorker() {
	go func() {
		for {
			s, ok := userStore.(devicePlanStore)
			if !ok {
				return
			}
			orders, err := s.GetPendingDeviceSlotOrders()
			if err == nil {
				for _, o := range orders {
					_, _ = applyDeviceSlotPayment(o.ID, o.UserID, o.PaymentID)
				}
			}
			ids, err := s.GetEnabledDevicePlans()
			if err == nil {
				for _, id := range ids {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					err = syncPrimaryDeviceLimit(ctx, xrayCfg, id)
					cancel()
					if err != nil {
						log.Printf("[devices] limit sync user=%s: %v", id, err)
					}
				}
			}
			time.Sleep(time.Minute)
		}
	}()
}

func devicePlanSummary(userID string) (string, postgres.DevicePlan) {
	p, err := currentDevicePlan(userID)
	if err != nil {
		return "\nЛимит временно недоступен.\n", p
	}
	if !p.Enabled {
		return "\nДля вас лимит устройств пока не включён. Доплачивать не нужно.\n", p
	}
	return fmt.Sprintf("\nЛимит: <b>%d устройств</b> (5 включено).\nДополнительных мест: %d · +%d ₽ при каждом продлении.\n", 5+p.ExtraSlots, p.ExtraSlots, p.ExtraSlots*50), p
}

func handleDevicePlanCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery) {
	if cq.From == nil || cq.Message == nil || cq.Message.Chat == nil || !cq.Message.Chat.IsPrivate() || cq.From.ID != cq.Message.Chat.ID {
		return
	}
	ackCallback(bot, cq, "")
	uid := strconv.FormatInt(cq.From.ID, 10)
	session := getSession(cq.From.ID)
	if !session.DevicesMu.TryLock() {
		return
	}
	defer session.DevicesMu.Unlock()
	bindSessionToCallbackMessage(session, cq)
	summary, p := devicePlanSummary(uid)
	text := "<b>Дополнительные устройства</b>\n" + summary
	var rows [][]tgbotapi.InlineKeyboardButton
	if p.Enabled {
		text += "\nОдно место: 50 ₽ до окончания текущей подписки. При каждом продлении +50 ₽ за место, независимо от срока тарифа. Дни не добавляются. После изменения числа мест автопродление отключится; его можно включить заново."
		if cq.Data == "device_plan:reduce_confirm" {
			err := reduceDeviceSlots(uid)
			if err != nil {
				text += "\n\nНе удалось убрать место: проверьте, что число устройств меньше нового лимита, и обновите список."
			} else {
				text += "\n\nОдно дополнительное место убрано. Автопродление отключено."
			}
		} else if cq.Data == "device_plan:reduce" {
			text += "\n\n<b>Убрать одно дополнительное место сейчас?</b> Лимит уменьшится на 1. Оплата за текущий срок не возвращается. При следующем продлении цена снизится на 50 ₽."
			rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Подтверждаю, убрать место", "device_plan:reduce_confirm")))
		} else if cq.Data == "device_plan:buy" {
			o, err := createDeviceSlotPayment(uid)
			if err != nil {
				text += "\n\nНе удалось создать платёж. Попробуйте позже или проверьте срок подписки."
			} else {
				rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonURL("Оплатить 50 ₽", o.URL)))
				text += "\n\nПосле оплаты место добавится автоматически. Нажмите «Устройства», чтобы обновить список."
			}
		} else {
			rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Добавить место · 50 ₽", "device_plan:buy")))
		}
		if p.ExtraSlots > 0 && cq.Data == "device_plan:show" {
			rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Убрать дополнительное место", "device_plan:reduce")))
		}
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Устройства", "nav_devices")))
	_ = updateSessionText(bot, cq.From.ID, session, stateDevices, text, "HTML", tgbotapi.NewInlineKeyboardMarkup(rows...))
}
