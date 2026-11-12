package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Asort97/vpnBot/clients/lifecycle"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const notificationStatePath = "database/notifications.json"

type noticeRecord struct {
	Key, Kind, Token, Status string
	Promo                    bool
	At, AttemptAt            time.Time
	MessageID                int
	Clicks                   map[string]time.Time
}
type noticeBill struct {
	ID, Plan, URL string
	CreatedAt     time.Time
}
type noticeAutopay struct {
	ID         string
	At, Expiry time.Time
}
type noticeUser struct {
	EnteredAt                              time.Time
	Referred, OptOut, Blocked              bool
	ConnectedAt, PaidAt                    time.Time
	Bill                                   *noticeBill
	AutopayFailureExpiry, AutopayFailureAt time.Time
	PendingAutopay                         *noticeAutopay
	Records                                map[string]*noticeRecord
}
type noticeState struct {
	Users                   map[string]*noticeUser
	IncidentID              string
	IncidentActive          bool
	IncidentAt, RecoveredAt time.Time
}
type noticeEngine struct {
	mu    sync.Mutex
	locks sync.Map
	path  string
	state noticeState
	ready bool
}

var notices = &noticeEngine{path: notificationStatePath}

func (e *noticeEngine) load() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ready {
		return nil
	}
	e.state.Users = map[string]*noticeUser{}
	b, err := os.ReadFile(e.path)
	if err == nil {
		if err = json.Unmarshal(b, &e.state); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if e.state.Users == nil {
		e.state.Users = map[string]*noticeUser{}
	}
	e.ready = true
	return nil
}
func (e *noticeEngine) user(id string) *noticeUser {
	u := e.state.Users[id]
	if u == nil {
		u = &noticeUser{}
		e.state.Users[id] = u
	}
	if u.Records == nil {
		u.Records = map[string]*noticeRecord{}
	}
	return u
}
func (e *noticeEngine) save() error {
	if !e.ready {
		return errors.New("notification store unavailable")
	}
	if err := os.MkdirAll(filepath.Dir(e.path), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(e.state)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(e.path), ".notifications-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, e.path)
}
func (e *noticeEngine) change(id string, fn func(*noticeUser)) error {
	if err := e.load(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	fn(e.user(id))
	return e.save()
}
func recordNoticeEntry(id string, referred bool) {
	if err := notices.change(id, func(u *noticeUser) {
		if u.EnteredAt.IsZero() {
			u.EnteredAt = time.Now().UTC()
		}
		u.Referred = u.Referred || referred
		u.Blocked = false
	}); err != nil {
		log.Printf("notification entry persist: %v", err)
	}
}
func rememberNoticeBill(id string, b noticeBill) error {
	return notices.change(id, func(u *noticeUser) { u.Bill = &b })
}
func currentNoticeBill(id string) *noticeBill {
	if notices.load() != nil {
		return nil
	}
	notices.mu.Lock()
	defer notices.mu.Unlock()
	b := notices.user(id).Bill
	if b == nil {
		return nil
	}
	copy := *b
	return &copy
}

func (e *noticeEngine) snapshot(id string) noticeUser {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, _ := json.Marshal(e.user(id))
	var u noticeUser
	_ = json.Unmarshal(b, &u)
	return u
}
func (e *noticeEngine) incidentMatches(id string, active bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state.IncidentID == id && e.state.IncidentActive == active
}

func (e *noticeEngine) eligibleLocked(id string, s noticeSpec, now time.Time) bool {
	u := e.user(id)
	if u.PendingAutopay != nil && (strings.HasPrefix(s.Kind, "payment_") || strings.HasPrefix(s.Kind, "expired") || s.Kind == "d1" || s.Kind == "d3" || s.Kind == "trial_d1" || s.Kind == "autopay_failed") {
		return false
	}
	if strings.HasPrefix(s.Kind, "payment_") {
		for _, r := range u.Records {
			if strings.HasPrefix(r.Kind, "payment_") && r.Status == "sent" && now.Sub(r.At) < time.Hour {
				return false
			}
		}
	}
	if u.Blocked || s.Text == "" || (s.Pause && e.state.IncidentActive) || (s.Promo && !noticePromoAllowed(u, now)) {
		return false
	}
	if r := u.Records[s.Key]; r != nil && (r.Status != "failed" || now.Sub(r.AttemptAt) < 5*time.Minute) {
		return false
	}
	return true
}

func noticePromoAllowed(u *noticeUser, now time.Time) bool {
	if u.OptOut || u.Blocked {
		return false
	}
	week := 0
	for _, r := range u.Records {
		// Unknown deliveries reserve quota too, without being recorded as delivered.
		if !r.Promo || (r.Status != "sent" && r.Status != "sending" && r.Status != "unknown") {
			continue
		}
		at := r.At
		if at.IsZero() {
			at = r.AttemptAt
		}
		if now.Sub(at) < 24*time.Hour {
			return false
		}
		if now.Sub(at) < 7*24*time.Hour {
			week++
		}
	}
	return week < 3
}

type noticeSpec struct {
	Kind, Key, Text string
	Promo, Pause    bool
	Keyboard        rawInlineKeyboardMarkup
}

func noticeEmoji(id, fallback string) string {
	return `<tg-emoji emoji-id="` + id + `">` + fallback + `</tg-emoji>`
}
func noticeTemplate(kind, key string) noticeSpec {
	s := noticeSpec{Kind: kind, Key: key, Pause: true}
	wave := noticeEmoji("5346299917679757635", "👋")
	wifi := noticeEmoji("5346325906526868503", "📶")
	money := noticeEmoji("5344015205531686528", "💰")
	clock := noticeEmoji("5345823764720426390", "⏰")
	warning := noticeEmoji("5264991913274019640", "⚠️")
	button := func(text, data, style string) rawInlineKeyboardButton {
		return rawCallbackButton(text, data, style, "")
	}
	support := button("поддержка", "nav_support", "")
	pay := func(label string) {
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{button(label, "nav_topup", "success")}, {support}}
	}
	help := func() {
		s.Text = wifi + " <b>получилось подключиться?</b>\n\nесли что-то не выходит, выберите устройство или напишите нам — поможем разобраться."
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{button("инструкция по подключению", "nav_instructions", "success")}, {support}}
	}
	switch kind {
	case "unclaimed_1", "unclaimed_3", "unclaimed_7":
		s.Promo = true
		s.Text = wave + " <b>попробуйте neuravpn бесплатно</b>\n\nподпишитесь на наш канал и получите <b>5 дней доступа</b>, чтобы спокойно всё проверить."
		check := "проверить подписку · +5 дней"
		if kind == "unclaimed_3" {
			s.Text = wifi + " <b>ваши 5 дней всё ещё доступны</b>\n\nподпишитесь на канал и проверьте подписку — добавим пробный доступ."
			check = "проверить подписку"
		}
		if kind == "unclaimed_7" {
			s.Text = wave + " <b>если захотите попробовать — мы рядом</b>\n\nбесплатные 5 дней всё ещё можно получить за подписку на наш канал."
			check = "проверить подписку"
		}
		channel := rawURLButton("подписаться на канал", channelURLEff, "")
		channel.Style = "success"
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{channel}, {button(check, "claim_sub_bonus", "")}}
	case "help_20m":
		s.Text = onboardingInstructionText()
		s.Keyboard = onboardingInstructionKeyboardRaw()
		s.Keyboard.InlineKeyboard = append([][]rawInlineKeyboardButton{{button("инструкция по подключению", "nav_instructions", "success")}}, s.Keyboard.InlineKeyboard...)
	case "help_24h", "trial_help":
		help()
	case "trial_d1":
		s.Promo = true
		s.Text = clock + " <b>пробный период скоро закончится</b>\n\nесли neuravpn вам подошёл, продлите подписку, чтобы остаться на связи."
		pay("выбрать тариф")
	case "d1", "d3":
		s.Text = clock + " <b>подписка скоро закончится</b>\n\nпродлите доступ, чтобы остаться на связи."
		pay("продлить доступ")
	case "expired":
		s.Text = clock + " <b>ваш доступ закончился</b>\n\nподписку можно продлить в боте."
		pay("продлить доступ")
	case "expired_d3":
		s.Promo = true
		s.Text = wifi + " <b>доступ закончился 3 дня назад</b>\n\nесли vpn ещё нужен — продлите подписку, и он снова заработает."
		pay("продлить доступ")
	case "expired_d7":
		s.Promo = true
		s.Text = wifi + " <b>нужна помощь с neuravpn?</b>\n\nесли возникли сложности с подключением, напишите нам — постараемся помочь."
		pay("продлить доступ")
	case "expired_d30":
		s.Promo = true
		s.Text = wave + " <b>будем рады вашему возвращению</b>\n\nесли vpn снова понадобится, подписку можно продлить в боте."
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{button("посмотреть тарифы", "nav_topup", "success")}, {button("+15 дней за друга", "nav_referral", "")}}
	case "payment_5m", "payment_24h":
		s.Text = money + " <b>не получилось завершить оплату?</b>\n\nможно попробовать ещё раз. если возникла ошибка — напишите нам."
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{button("попробовать оплатить", "notice_retry", "success")}, {button("я оплатил", "notice_check", "")}, {support}}
	case "autopay_failed":
		s.Pause = false
		s.Text = warning + " <b>не удалось продлить подписку автоматически</b>\n\nавтопродление отключено. чтобы сохранить доступ, можно продлить подписку вручную."
		pay("продлить вручную")
	case "incident":
		s.Pause = false
		s.Text = warning + " <b>сейчас возможны проблемы с подключением</b>\n\nмы уже разбираемся. сообщим, когда работа восстановится."
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{support}}
	case "recovered":
		s.Pause = false
		s.Text = wifi + " <b>подключение восстановлено</b>\n\nпопробуйте подключиться ещё раз. если проблема осталась — напишите нам."
		s.Keyboard.InlineKeyboard = [][]rawInlineKeyboardButton{{button("подключить vpn", "nav_get_vpn", "success")}, {support}}
	}
	if s.Promo {
		s.Keyboard.InlineKeyboard = append(s.Keyboard.InlineKeyboard, []rawInlineKeyboardButton{button("не присылать предложения", "notice_optout", "")})
	}
	return s
}

var noticeActions = map[string]string{"nav_support": "s", "nav_topup": "p", "nav_instructions": "i", "nav_referral": "r", "nav_get_vpn": "v", "claim_sub_bonus": "b", "notice_optout": "o", "notice_retry": "t", "notice_check": "c", "android": "a", "ios": "j", "windows": "w", "macos": "m", "disable_autopay": "d"}

func noticeTrackedKeyboard(k rawInlineKeyboardMarkup, token string) rawInlineKeyboardMarkup {
	out := rawInlineKeyboardMarkup{}
	for _, row := range k.InlineKeyboard {
		copyRow := append([]rawInlineKeyboardButton(nil), row...)
		for i, b := range copyRow {
			if b.CallbackData != nil {
				if code, ok := noticeActions[*b.CallbackData]; ok {
					cb := "notice:" + token + ":" + code
					copyRow[i].CallbackData = &cb
				}
			}
		}
		out.InlineKeyboard = append(out.InlineKeyboard, copyRow)
	}
	return out
}

// The reservation is persisted before Telegram. A crash/ambiguous network failure
// leaves an unknown delivery, not a false success and not an automatic duplicate.
func (e *noticeEngine) deliver(id string, s noticeSpec, recheck func() bool, send func(string, rawInlineKeyboardMarkup) (int, error), audit func(string)) bool {
	if e.load() != nil {
		return false
	}
	lock, _ := e.locks.LoadOrStore(id, &sync.Mutex{})
	userLock := lock.(*sync.Mutex)
	userLock.Lock()
	defer userLock.Unlock()
	e.mu.Lock()
	now := time.Now().UTC()
	eligible := e.eligibleLocked(id, s, now)
	e.mu.Unlock()
	if !eligible || !recheck() {
		return false
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return false
	}
	e.mu.Lock()
	if !e.eligibleLocked(id, s, time.Now().UTC()) {
		e.mu.Unlock()
		return false
	}
	u := e.user(id)
	r := &noticeRecord{Key: s.Key, Kind: s.Kind, Token: hex.EncodeToString(raw[:]), Status: "sending", Promo: s.Promo, AttemptAt: now}
	u.Records[s.Key] = r
	if err := e.save(); err != nil {
		delete(u.Records, s.Key)
		log.Printf("notification reservation: %v", err)
		e.mu.Unlock()
		return false
	}
	e.mu.Unlock()
	msg, err := send(s.Text, noticeTrackedKeyboard(s.Keyboard, r.Token))
	e.mu.Lock()
	if err != nil || msg <= 0 {
		r.Status = "unknown"
		var te *tgbotapi.Error
		// sendMessageRaw returns API descriptions rather than a typed error.
		if errors.As(err, &te) || isDefiniteTelegramFailure(err) {
			r.Status = "failed"
		}
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "blocked") {
			u.Blocked = true
			r.Status = "blocked"
		}
		if saveErr := e.save(); saveErr != nil {
			log.Printf("notification failure persist: %v", saveErr)
		}
		status := r.Status
		e.mu.Unlock()
		audit("ошибка доставки: " + s.Kind + " · " + status)
		return false
	}
	r.Status = "sent"
	r.At = time.Now().UTC()
	r.MessageID = msg
	if err = e.save(); err != nil {
		log.Printf("notification delivered but state write failed user=%s: %v", id, err)
	}
	e.mu.Unlock()
	audit("доставлено: " + s.Kind)
	return true
}
func isDefiniteTelegramFailure(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "bad request") || strings.Contains(s, "forbidden") || strings.Contains(s, "too many requests") || strings.Contains(s, "bot was blocked") || strings.Contains(s, "chat not found")
}
func sendLifecycleNotice(bot *tgbotapi.BotAPI, id string, s noticeSpec, check func() bool) bool {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 || bot == nil {
		return false
	}
	return notices.deliver(id, s, check, func(text string, k rawInlineKeyboardMarkup) (int, error) {
		return sendMessageRaw(bot, n, text, "HTML", k)
	}, func(kind string) { sendUserNotificationAdminLog(bot, n, "", kind) })
}

func exactUnpaid(p *yookassa.YooKassaPaymentResponse, b *noticeBill, id string) bool {
	return p != nil && b != nil && p.ID == b.ID && paymentUserID(p.Metadata["chat_id"]) == id && !p.Paid && (p.Status == "pending" || p.Status == "canceled")
}

func notificationFacts(id string) (lifecycle.User, error) {
	s, ok := userStore.(lifecycle.Store)
	if !ok {
		return lifecycle.User{}, errors.New("notification snapshots unsupported")
	}
	return s.NotificationUser(id)
}
