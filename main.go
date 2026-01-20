package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io/fs"
	"log"
	"math"
	"math/rand"
	"net/mail"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	instruct "github.com/Asort97/vpnBot/clients/instruction"
	pgstore "github.com/Asort97/vpnBot/clients/postgres"
	sqlite "github.com/Asort97/vpnBot/clients/sqLite"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	startTrialDays    = 7
	channelBonusDays  = 7
	referralBonusDays = 15
	channelUsername   = "@neuravpn"
	channelURL        = "https://t.me/neuravpn"
)

const startText = `добро пожаловать!

этот бот поможет подключить neuravpn с понятными инструкциями для любой платформы.

перед покупкой основного тарифа мы предлагаем пробный период - 7 дней.
попробуйте. мы не заставляем.

гарантируем стабильный и бесперебойный доступ ко всем заблокированным ресурсам
без ограничения исходной скорости вашего интернета.
можете проверить.

<a href="https://t.me/neuravpn">наш новостной канал</a>`

// Runtime-overridable channel settings (safer than hardcoded constants for production)
var (
	channelUsernameEff = channelUsername
	channelURLEff      = channelURL
	channelChatIDEff   int64
	adStats            = newAdStatsStore(filepath.Join("database", "ad_stats.json"))
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// throttling map (keyed by user id and action key)
var lastActionKey = make(map[int64]map[string]time.Time)

type SessionState string

const (
	stateMenu         SessionState = "menu"
	stateGetVPN       SessionState = "get_vpn"
	stateTopUp        SessionState = "top_up"
	stateChoosePay    SessionState = "choose_payment"
	stateStatus       SessionState = "status"
	stateInstructions SessionState = "instructions"
	stateCollectEmail SessionState = "collect_email"
	stateEditEmail    SessionState = "edit_email"
)

type RatePlan struct {
	ID     string
	Title  string
	Amount float64
	Days   int
}

type adStatsStore struct {
	mu   sync.RWMutex
	path string
	data map[string]map[string]bool // adTag -> set of userIDs counted
}

func newAdStatsStore(path string) *adStatsStore {
	return &adStatsStore{
		path: path,
		data: make(map[string]map[string]bool),
	}
}

func (s *adStatsStore) load() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) > 0 {
		return
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			s.data = make(map[string]map[string]bool)
			return
		}
		log.Printf("ad stats read error: %v", err)
		return
	}
	var raw map[string][]string
	if err := json.Unmarshal(b, &raw); err != nil {
		log.Printf("ad stats unmarshal error: %v", err)
		s.data = make(map[string]map[string]bool)
		return
	}
	for tag, users := range raw {
		if s.data[tag] == nil {
			s.data[tag] = make(map[string]bool)
		}
		for _, u := range users {
			s.data[tag][u] = true
		}
	}
}

func (s *adStatsStore) saveLocked() {
	raw := make(map[string][]string)
	for tag, set := range s.data {
		for u := range set {
			raw[tag] = append(raw[tag], u)
		}
	}
	b, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		log.Printf("ad stats marshal error: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		log.Printf("ad stats mkdir error: %v", err)
		return
	}
	if err := os.WriteFile(s.path, b, 0o644); err != nil {
		log.Printf("ad stats write error: %v", err)
	}
}

func (s *adStatsStore) record(tag, userID string) (newCount int, isNew bool) {
	s.load()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[tag] == nil {
		s.data[tag] = make(map[string]bool)
	}
	if s.data[tag][userID] {
		return len(s.data[tag]), false
	}
	s.data[tag][userID] = true
	s.saveLocked()
	return len(s.data[tag]), true
}

func (s *adStatsStore) statsForChannel(channel string) map[string]int {
	s.load()
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]int)
	prefix := channel + "/"
	for tag, users := range s.data {
		if strings.HasPrefix(tag, prefix) {
			res[tag] = len(users)
		}
	}
	return res
}

type DataStore interface {
	AddDays(userID string, days int64) error
	GetDays(userID string) (int64, error)
	SetDays(userID string, days int64) error
	GetEmail(userID string) (string, error)
	SetEmail(userID, email string) error
	EnsureSubscriptionID(userID string) (string, error)
	GetSubscriptionID(userID string) (string, error)
	AcceptPrivacy(userID string, at time.Time) error
	IsNewUser(userID string) bool
	IsStartBonusClaimed(userID string) (bool, error)
	ClaimStartBonus(userID string, source string, at time.Time) (bool, error)
	RecordReferral(newUserID, referrerID string) error
	GetReferralsCount(userID string) int
}

var ratePlans = []RatePlan{
	{ID: "30d", Title: "30 дней", Amount: 100, Days: 30},
	{ID: "60d", Title: "60 дней", Amount: 150, Days: 60},
	{ID: "90d", Title: "90 дней", Amount: 200, Days: 90},
	{ID: "365d", Title: "365 дней", Amount: 650, Days: 365},
}

var ratePlanByID = func() map[string]RatePlan {
	m := make(map[string]RatePlan)
	for _, p := range ratePlans {
		m[p.ID] = p
	}
	return m
}()

type UserSession struct {
	MessageID     int
	State         SessionState
	ContentType   string
	PendingPlanID string
	LastAccess    string
	LastLink      string
	CertFileName  string
	CertFileBytes []byte
}

type xraySettings struct {
	client        *xray.XRayClient
	inboundID     int
	inboundIDs    []int
	serverAddress string
	serverPort    int
	serverName    string
	publicKey     string
	shortID       string
	spiderX       string
	subBaseURL    string
}

type accessInfo struct {
	client   *xray.Client
	expireAt time.Time
	daysLeft int64
	link     string
}

var (
	yookassaClient *yookassa.YooKassaClient
	userStore      DataStore
	xrayCfg        *xraySettings
	oldXrayCfg     *xraySettings
	privacyURL     string
	adminIDs       []int64
	logChatID      int64 = -1003334019708
	userSessions         = make(map[int64]*UserSession)
)

var (
	expiryReminderMu    sync.Mutex
	expiryReminderState = make(map[int64]map[string]string)
)

const expiryReminderStatePath = "database/reminder_state.json"

func canProceedKey(userID int64, key string, interval time.Duration) bool {
	now := time.Now()
	if lastActionKey[userID] == nil {
		lastActionKey[userID] = make(map[string]time.Time)
	}
	if t, ok := lastActionKey[userID][key]; ok {
		if now.Sub(t) < interval {
			return false
		}
	}
	lastActionKey[userID][key] = now
	return true
}

func getSession(chatID int64) *UserSession {
	if s, ok := userSessions[chatID]; ok {
		return s
	}
	s := &UserSession{}
	userSessions[chatID] = s
	return s
}

func isSubscribedToChannel(bot *tgbotapi.BotAPI, userID int64) (bool, error) {
	// Самый надежный вариант - использовать числовой ChatID (можно задать через ENV CHANNEL_CHAT_ID).
	if channelChatIDEff != 0 {
		memberCfg := tgbotapi.GetChatMemberConfig{
			ChatConfigWithUser: tgbotapi.ChatConfigWithUser{
				ChatID: channelChatIDEff,
				UserID: userID,
			},
		}
		member, err := bot.GetChatMember(memberCfg)
		if err != nil {
			return false, err
		}
		switch member.Status {
		case "creator", "administrator", "member":
			return true, nil
		default:
			return false, nil
		}
	}

	uname := strings.TrimSpace(channelUsernameEff)
	if uname == "" {
		return false, fmt.Errorf("channel username is empty")
	}

	// Try resolve chat by username (with and without '@')
	tryGetChat := func(username string) (*tgbotapi.Chat, error) {
		chatCfg := tgbotapi.ChatInfoConfig{ChatConfig: tgbotapi.ChatConfig{SuperGroupUsername: strings.TrimSpace(username)}}
		chat, err := bot.GetChat(chatCfg)
		if err != nil {
			return nil, err
		}
		return &chat, nil
	}

	chat, err := tryGetChat(uname)
	if err != nil {
		alt := ""
		if strings.HasPrefix(uname, "@") {
			alt = strings.TrimPrefix(uname, "@")
		} else {
			alt = "@" + uname
		}
		chat, err = tryGetChat(alt)
		if err != nil {
			return false, err
		}
	}

	memberCfg := tgbotapi.GetChatMemberConfig{ChatConfigWithUser: tgbotapi.ChatConfigWithUser{ChatID: chat.ID, UserID: userID}}
	member, err := bot.GetChatMember(memberCfg)
	if err != nil {
		return false, err
	}
	switch member.Status {
	case "creator", "administrator", "member":
		return true, nil
	default:
		return false, nil
	}
}

func ensureXrayAccess(cfg *xraySettings, telegramUser string, email string, addDays int64, createIfMissing bool) (*accessInfo, error) {
	if cfg == nil || cfg.client == nil {
		return nil, fmt.Errorf("xray not configured")
	}
	// Determine target inbound IDs: if list is empty, try to load all inbounds dynamically
	inboundIDs := cfg.inboundIDs
	if len(inboundIDs) == 0 {
		inbounds, err := cfg.client.GetAllInbounds()
		if err != nil {
			return nil, err
		}
		for _, ib := range inbounds {
			// Only include enabled VLESS protocol inbounds
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				inboundIDs = append(inboundIDs, ib.ID)
			}
		}
		// Fallback to single configured inbound if nothing matched
		if len(inboundIDs) == 0 && cfg.inboundID > 0 {
			inboundIDs = append(inboundIDs, cfg.inboundID)
		}
	}

	if len(inboundIDs) == 0 {
		return nil, fmt.Errorf("no inbounds available to ensure client")
	}

	if !createIfMissing && addDays == 0 {
		// Still attempt to read primary client without creating on others
		c, err := cfg.client.GetClientByTelegram(inboundIDs[0], telegramUser)
		if err != nil {
			return nil, err
		}
		if c == nil {
			return nil, nil
		}
		// Normalize minimal fields
		if strings.TrimSpace(c.Email) == "" {
			c.Email = telegramUser
		}
		exp := time.UnixMilli(c.ExpiryTime)
		info := &accessInfo{client: c, expireAt: exp}
		if !exp.IsZero() {
			remain := time.Until(exp)
			if remain > 0 {
				info.daysLeft = int64(remain.Hours()/24 + 0.999)
			}
		}
		if cfg.serverAddress != "" && cfg.publicKey != "" && cfg.serverName != "" && cfg.shortID != "" && cfg.serverPort > 0 {
			info.link = cfg.client.GenerateVLESSLink(c, cfg.serverAddress, cfg.serverPort, cfg.serverName, cfg.publicKey, cfg.shortID, cfg.spiderX)
		}
		_ = userStore.SetDays(telegramUser, info.daysLeft)
		return info, nil
	}

	// Secure subscription id per user
	subID, _ := userStore.EnsureSubscriptionID(telegramUser)
	primaryClient, expireAt, err := cfg.client.EnsureClientAcrossInbounds(inboundIDs, telegramUser, email, addDays, subID)
	if err != nil {
		return nil, err
	}

	daysLeft := int64(0)
	if !expireAt.IsZero() {
		remain := time.Until(expireAt)
		if remain > 0 {
			daysLeft = int64(remain.Hours()/24 + 0.999)
		}
	}
	_ = userStore.SetDays(telegramUser, daysLeft)

	link := ""
	if cfg.serverAddress != "" && cfg.publicKey != "" && cfg.serverName != "" && cfg.shortID != "" && cfg.serverPort > 0 {
		link = cfg.client.GenerateVLESSLink(primaryClient, cfg.serverAddress, cfg.serverPort, cfg.serverName, cfg.publicKey, cfg.shortID, cfg.spiderX)
	}

	return &accessInfo{
		client:   primaryClient,
		expireAt: expireAt,
		daysLeft: daysLeft,
		link:     link,
	}, nil
}

func fallbackEmail(userID string) string {
	if userStore == nil {
		return fmt.Sprintf("%s@happycat", userID)
	}
	if email, err := userStore.GetEmail(userID); err == nil && strings.TrimSpace(email) != "" {
		return email
	}
	return fmt.Sprintf("%s@happycat", userID)
}

func formatExpiryUTC(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("02.01.2006 15:04 UTC")
}

func collectExpiryByTgID(cfg *xraySettings) (map[int64]time.Time, error) {
	if cfg == nil || cfg.client == nil {
		return nil, fmt.Errorf("xray not configured")
	}

	inboundIDs := cfg.inboundIDs
	if len(inboundIDs) == 0 {
		inbounds, err := cfg.client.GetAllInbounds()
		if err != nil {
			return nil, err
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				inboundIDs = append(inboundIDs, ib.ID)
			}
		}
	}
	if len(inboundIDs) == 0 {
		return nil, fmt.Errorf("no inbounds available for reminder")
	}

	result := make(map[int64]time.Time)
	for _, inboundID := range inboundIDs {
		clients, err := cfg.client.GetInboundById(inboundID)
		if err != nil {
			return nil, err
		}
		for _, c := range clients {
			if !c.Enable || c.ExpiryTime <= 0 {
				continue
			}
			tgID := strings.TrimSpace(c.TgID)
			if tgID == "" {
				continue
			}
			id, err := strconv.ParseInt(tgID, 10, 64)
			if err != nil {
				continue
			}
			exp := time.UnixMilli(c.ExpiryTime)
			if existing, ok := result[id]; !ok || exp.After(existing) {
				result[id] = exp
			}
		}
	}
	return result, nil
}

func shouldSendExpiryReminder(userID int64, stage string, expiry time.Time) bool {
	expiryReminderMu.Lock()
	defer expiryReminderMu.Unlock()
	if expiryReminderState[userID] == nil {
		expiryReminderState[userID] = make(map[string]string)
	}
	expKey := expiry.UTC().Format(time.RFC3339Nano)
	if stage == "expired" {
		if expiryReminderState[userID][stage] != "" {
			return false
		}
	} else if expiryReminderState[userID][stage] == expKey {
		return false
	}
	expiryReminderState[userID][stage] = expKey
	_ = saveExpiryReminderState()
	return true
}

func clearExpiryReminderStage(userID int64, stage string) {
	expiryReminderMu.Lock()
	defer expiryReminderMu.Unlock()
	if expiryReminderState[userID] == nil {
		return
	}
	if _, ok := expiryReminderState[userID][stage]; !ok {
		return
	}
	delete(expiryReminderState[userID], stage)
	if len(expiryReminderState[userID]) == 0 {
		delete(expiryReminderState, userID)
	}
	_ = saveExpiryReminderState()
}

func loadExpiryReminderState() {
	expiryReminderMu.Lock()
	defer expiryReminderMu.Unlock()

	data, err := os.ReadFile(expiryReminderStatePath)
	if err != nil {
		return
	}
	var raw map[string]map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Printf("expiry reminder load failed: %v", err)
		return
	}
	out := make(map[int64]map[string]string)
	for k, v := range raw {
		id, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			continue
		}
		out[id] = v
	}
	expiryReminderState = out
}

func saveExpiryReminderState() error {
	if err := os.MkdirAll(filepath.Dir(expiryReminderStatePath), 0o755); err != nil {
		return err
	}
	raw := make(map[string]map[string]string)
	for k, v := range expiryReminderState {
		raw[strconv.FormatInt(k, 10)] = v
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return os.WriteFile(expiryReminderStatePath, data, 0o644)
}

func startExpiryReminder(bot *tgbotapi.BotAPI, cfg *xraySettings) {
	go func() {
		ticker := time.NewTicker(12 * time.Hour)
		defer ticker.Stop()

		for {
			func() {
				expiries, err := collectExpiryByTgID(cfg)
				if err != nil {
					log.Printf("expiry reminder: %v", err)
					return
				}
				now := time.Now().UTC()

				for userID, exp := range expiries {
					remain := exp.Sub(now)
					daysLeft := int64(0)
					if remain > 0 {
						daysLeft = int64(remain.Hours()/24 + 0.999)
						clearExpiryReminderStage(userID, "expired")
					}

					key := ""
					if daysLeft == 3 {
						key = "d3"
					} else if daysLeft == 1 {
						key = "d1"
					} else if daysLeft <= 0 {
						key = "expired"
					} else {
						continue
					}

					if !shouldSendExpiryReminder(userID, key, exp) {
						continue
					}

					expStr := formatExpiryUTC(exp)
					text := ""
					if daysLeft <= 0 {
						text = fmt.Sprintf("⏰ ваш доступ к neuravpn закончился.\nдействовал до: %s\nпродлите в разделе «оплата».", expStr)
					} else {
						text = fmt.Sprintf("⏰ ваш доступ к neuravpn заканчивается через %d дн.\nдействует до: %s\nпродлите в разделе «оплата».", daysLeft, expStr)
					}

					msg := tgbotapi.NewMessage(userID, text)
					_, _ = bot.Send(msg)
				}
			}()

			<-ticker.C
		}
	}()
}

func sendAccess(info *accessInfo, telegramUserID string, chatID int64, addedDays int, userID int64, cfg *xraySettings, bot *tgbotapi.BotAPI, session *UserSession) error {
	if info == nil || info.client == nil {
		return fmt.Errorf("no access info")
	}

	exp := "—"
	if !info.expireAt.IsZero() {
		exp = formatExpiryUTC(info.expireAt)
	}

	left := "0 дней"
	if info.daysLeft > 0 {
		left = fmt.Sprintf("%d дней", info.daysLeft)
	}
	combined := fmt.Sprintf("%s · %s", left, exp)

	keyLine := "ключ будет сгенерирован позже"
	subURL := generateSubscriptionURL(cfg, info.client)
	if strings.TrimSpace(subURL) != "" {
		keyLine = fmt.Sprintf("<code>%s</code>", subURL)
	} else if strings.TrimSpace(info.link) != "" {
		keyLine = fmt.Sprintf("<code>%s</code>", info.link)
	}

	text := fmt.Sprintf(`🔌 подключить neuravpn

наше приложение для ios, android и windows сейчас в разработке, но мы нашли оптимальный компромисс.
временный доступ осуществляется через сторонние клиенты — они отлично работают!

<b>ваш ключ (подключение к neuravpn):</b>
%s
(нажмите чтобы скопировать)
перейдите в раздел «инструкции» — мы подробно объясним, что и куда нужно вставить.

оставшееся время / действует до:
%s
`, keyLine, combined)
	if addedDays > 0 {
		text += fmt.Sprintf("\n\n✨ Начислено: +%d дней", addedDays)
	}

	session.LastAccess = text
	session.LastLink = info.link

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🛠 инструкции", "nav_instructions"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("👤 профиль", "nav_status"),
			tgbotapi.NewInlineKeyboardButtonData("🏠 меню", "nav_menu"),
		),
	)
	return updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
}

func issuePlanAccess(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, plan RatePlan, cfg *xraySettings, telegramUser string, numericUserID int64) error {
	info, err := ensureXrayAccess(cfg, telegramUser, fallbackEmail(telegramUser), int64(plan.Days), true)
	if err != nil {
		return err
	}
	return sendAccess(info, telegramUser, chatID, plan.Days, numericUserID, cfg, bot, session)
}

func updateSessionText(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, state SessionState, text string, parseMode string, keyboard tgbotapi.InlineKeyboardMarkup) error {
	if session.MessageID != 0 {
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, session.MessageID, text, keyboard)
		if parseMode != "" {
			edit.ParseMode = parseMode
		}
		edit.DisableWebPagePreview = true
		if _, err := bot.Send(edit); err == nil {
			instruct.ResetState(chatID)
			session.State = state
			session.ContentType = "text"
			return nil
		}
	}
	return replaceSessionWithText(bot, chatID, session, state, text, parseMode, keyboard)
}

func replaceSessionWithText(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, state SessionState, text string, parseMode string, keyboard tgbotapi.InlineKeyboardMarkup) error {
	if session.MessageID != 0 {
		_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, session.MessageID))
	}
	instruct.ResetState(chatID)
	msg := tgbotapi.NewMessage(chatID, text)
	if parseMode != "" {
		msg.ParseMode = parseMode
	}
	msg.ReplyMarkup = keyboard
	msg.DisableWebPagePreview = true

	sent, err := bot.Send(msg)
	if err != nil {
		return err
	}

	session.MessageID = sent.MessageID
	session.State = state
	session.ContentType = "text"
	return nil
}

func replaceSessionWithDocument(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, state SessionState, file tgbotapi.FileBytes, caption string, parseMode string, keyboard tgbotapi.InlineKeyboardMarkup) error {
	if session.MessageID != 0 {
		_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, session.MessageID))
	}
	instruct.ResetState(chatID)

	doc := tgbotapi.NewDocument(chatID, file)
	doc.Caption = caption
	if parseMode != "" {
		doc.ParseMode = parseMode
	}
	doc.ReplyMarkup = keyboard

	sent, err := bot.Send(doc)
	if err != nil {
		return err
	}

	session.MessageID = sent.MessageID
	session.State = state
	session.ContentType = "document"
	return nil
}

func mainMenuInlineKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🔌 подключить VPN", "nav_get_vpn"),
			tgbotapi.NewInlineKeyboardButtonData("👤 профиль/оплата", "nav_status"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🎁 +15 дней", "nav_referral"),
			tgbotapi.NewInlineKeyboardButtonData("📞 поддержка", "nav_support"),
		),
	)
}

func composeMenuText() string {
	base := strings.TrimSpace(startText)
	if strings.Contains(base, "%") {
		base = strings.TrimSpace(fmt.Sprintf(startText, startTrialDays, channelBonusDays))
	}
	if base == "" {
		return "Добро пожаловать! Используйте меню ниже, чтобы подключить VPN."
	}
	return base
}

func showMainMenu(bot *tgbotapi.BotAPI, chatID int64, session *UserSession) error {
	text := composeMenuText()
	return updateSessionText(bot, chatID, session, stateMenu, text, "HTML", mainMenuInlineKeyboard())
}

func sendChannelBonusOffer(bot *tgbotapi.BotAPI, chatID int64) {
	text := fmt.Sprintf(
		"кстати, у нас есть новостной канал.\n\nесли подпишешься — добавим +%d дней доступа.",
		channelBonusDays,
	)
	msg := tgbotapi.NewMessage(chatID, text)
	msg.DisableWebPagePreview = true
	msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("подписаться", channelURLEff),
			tgbotapi.NewInlineKeyboardButtonData("проверить", "claim_sub_bonus"),
		),
	)
	_, _ = bot.Send(msg)
}

func rateKeyboard() tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	var row []tgbotapi.InlineKeyboardButton
	for _, p := range ratePlans {
		label := fmt.Sprintf("%d дней — %.0f₽", p.Days, p.Amount)
		row = append(row, tgbotapi.NewInlineKeyboardButtonData(label, "rate_"+p.ID))
		if len(row) == 2 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_status"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

const (
	starsCurrency      = "XTR"
	starsPayloadPrefix = "stars:"
)

func starsAmountForPlan(plan RatePlan) int {
	n := int(math.Round(plan.Amount * 1.5))
	if n < 1 {
		n = 1
	}
	return n
}

func choosePayKeyboard(plan RatePlan) tgbotapi.InlineKeyboardMarkup {
	stars := starsAmountForPlan(plan)
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("⭐ звёздами (%d ⭐)", stars), "pay_stars_"+plan.ID),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("💳 картой (%.0f ₽)", plan.Amount), "pay_card_"+plan.ID),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_topup"),
			tgbotapi.NewInlineKeyboardButtonData("🏠 меню", "nav_menu"),
		),
	)
}

func sendStarsInvoice(bot *tgbotapi.BotAPI, chatID int64, plan RatePlan) error {
	stars := starsAmountForPlan(plan)
	prices := []tgbotapi.LabeledPrice{
		{Label: plan.Title, Amount: stars},
	}

	payload := starsPayloadPrefix + plan.ID
	startParam := "neuravpn_" + plan.ID
	inv := tgbotapi.NewInvoice(
		chatID,
		"NeuraVPN",
		fmt.Sprintf("Доступ к NeuraVPN на %d дней.", plan.Days),
		payload,
		"",
		startParam,
		starsCurrency,
		prices,
	)
	// tgbotapi sends nil SuggestedTipAmounts as JSON null, but Telegram expects an array.
	inv.SuggestedTipAmounts = []int{}

	_, err := bot.Send(inv)
	return err
}

func createStarsInvoiceLink(bot *tgbotapi.BotAPI, plan RatePlan) (string, error) {
	stars := starsAmountForPlan(plan)
	prices := []tgbotapi.LabeledPrice{
		{Label: plan.Title, Amount: stars},
	}

	payload := starsPayloadPrefix + plan.ID
	startParam := "neuravpn_" + plan.ID

	pricesBytes, err := json.Marshal(prices)
	if err != nil {
		return "", err
	}

	// Telegram requires suggested_tip_amounts to be an array, not null.
	tipsBytes, err := json.Marshal([]int{})
	if err != nil {
		return "", err
	}

	params := tgbotapi.Params{
		"title":                 "NeuraVPN",
		"description":           fmt.Sprintf("Доступ к NeuraVPN на %d дней.", plan.Days),
		"payload":               payload,
		"provider_token":        "",
		"currency":              starsCurrency,
		"prices":                string(pricesBytes),
		"start_parameter":       startParam,
		"suggested_tip_amounts": string(tipsBytes),
	}

	resp, err := bot.MakeRequest("createInvoiceLink", params)
	if err != nil {
		return "", err
	}

	var link string
	if err := json.Unmarshal(resp.Result, &link); err != nil {
		return "", err
	}
	return link, nil
}

func main() {
	yookassaApiKey := os.Getenv("YOOKASSA_API_KEY")
	yookassaStoreID := os.Getenv("YOOKASSA_STORE_ID")
	botToken := os.Getenv("TG_BOT_TOKEN")
	privacyURL = os.Getenv("PRIVACY_URL")
	dbDSN := strings.TrimSpace(os.Getenv("DB_DSN"))

	// Optional: override channel settings without code changes
	if v := strings.TrimSpace(os.Getenv("CHANNEL_USERNAME")); v != "" {
		if !strings.HasPrefix(v, "@") {
			v = "@" + v
		}
		channelUsernameEff = v
		if strings.TrimSpace(channelURLEff) == strings.TrimSpace(channelURL) {
			channelURLEff = "https://t.me/" + strings.TrimPrefix(v, "@")
		}
	}
	if v := strings.TrimSpace(os.Getenv("CHANNEL_URL")); v != "" {
		channelURLEff = v
	}
	if v := strings.TrimSpace(os.Getenv("CHANNEL_CHAT_ID")); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			channelChatIDEff = id
		}
	}

	// Parse admin IDs
	adminIDsStr := os.Getenv("ADMIN_IDS")
	if adminIDsStr != "" {
		for _, idStr := range strings.Split(adminIDsStr, ",") {
			idStr = strings.TrimSpace(idStr)
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				adminIDs = append(adminIDs, id)
			}
		}
	}
	// Добавляем дополнительного получателя уведомлений (по запросу): 7968465778
	adminIDs = append(adminIDs, 7968465778)

	xrayUser := os.Getenv("XRAY_USERNAME")
	xrayPass := os.Getenv("XRAY_PASSWORD")
	xrayHost := os.Getenv("XRAY_HOST")
	xrayPort := os.Getenv("XRAY_PORT")
	xrayBasePath := os.Getenv("XRAY_WEB_BASE_PATH")
	inboundID, _ := strconv.Atoi(os.Getenv("XRAY_INBOUND_ID"))
	// Optional: comma-separated inbound IDs to target; if empty, we will use dynamic discovery via GetAllInbounds
	inboundIDsStr := strings.TrimSpace(os.Getenv("XRAY_INBOUND_IDS"))
	var inboundIDs []int
	if inboundIDsStr != "" {
		for _, p := range strings.Split(inboundIDsStr, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if id, err := strconv.Atoi(p); err == nil {
				inboundIDs = append(inboundIDs, id)
			}
		}
	} else if inboundID > 0 {
		inboundIDs = append(inboundIDs, inboundID)
	}
	serverPort, _ := strconv.Atoi(os.Getenv("XRAY_SERVER_PORT"))

	xClient := xray.New(xrayUser, xrayPass, xrayHost, xrayPort, xrayBasePath)
	if err := xClient.LoginToServer(); err != nil {
		log.Fatalf("login to xray failed: %v", err)
	}

	xrayCfg = &xraySettings{
		client:        xClient,
		inboundID:     inboundID,
		inboundIDs:    inboundIDs,
		serverAddress: os.Getenv("XRAY_SERVER_ADDRESS"),
		serverPort:    serverPort,
		serverName:    os.Getenv("XRAY_SERVER_NAME"),
		publicKey:     os.Getenv("XRAY_PUBLIC_KEY"),
		shortID:       os.Getenv("XRAY_SHORT_ID"),
		spiderX:       os.Getenv("XRAY_SPIDER_X"),
		subBaseURL:    strings.TrimSpace(os.Getenv("SUB_BASE_URL")),
	}

	// Setup old Xray connection for migration
	oldXrayHost := os.Getenv("OLD_XRAY_HOST")
	if strings.TrimSpace(oldXrayHost) != "" {
		oldXrayUser := os.Getenv("OLD_XRAY_USERNAME")
		oldXrayPass := os.Getenv("OLD_XRAY_PASSWORD")
		oldXrayPort := os.Getenv("OLD_XRAY_PORT")
		oldXrayBasePath := os.Getenv("OLD_XRAY_WEB_BASE_PATH")
		oldInboundIDsStr := strings.TrimSpace(os.Getenv("OLD_XRAY_INBOUND_IDS"))
		var oldInboundIDs []int
		if oldInboundIDsStr != "" {
			for _, p := range strings.Split(oldInboundIDsStr, ",") {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				if id, err := strconv.Atoi(p); err == nil {
					oldInboundIDs = append(oldInboundIDs, id)
				}
			}
		}

		oldXClient := xray.New(oldXrayUser, oldXrayPass, oldXrayHost, oldXrayPort, oldXrayBasePath)
		if err := oldXClient.LoginToServer(); err != nil {
			log.Printf("⚠️  old xray connection failed (migration unavailable): %v", err)
		} else {
			oldXrayCfg = &xraySettings{
				client:     oldXClient,
				inboundID:  0,
				inboundIDs: oldInboundIDs,
			}
			log.Println("✅ Old Xray server connected for migration")
		}
	}

	yookassaClient = yookassa.New(yookassaStoreID, yookassaApiKey)
	var storeCloser func()
	if dbDSN != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pgStore, err := pgstore.New(ctx, dbDSN)
		if err != nil {
			log.Fatalf("postgres store init failed: %v", err)
		}
		userStore = pgStore
		storeCloser = pgStore.Close
		log.Println("📦 Storage: PostgreSQL")
	} else {
		path := "database/data.json"
		userStore = sqlite.New(path)
		log.Printf("📦 Storage: JSON (%s)", path)
	}
	if storeCloser != nil {
		defer storeCloser()
	}
	if userStore == nil {
		log.Fatal("storage is not configured")
	}

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatalf("bot init error: %v", err)
	}

	loadExpiryReminderState()
	startExpiryReminder(bot, xrayCfg)

	// Профилактический re-login к XRAY раз в час
	go func() {
		for {
			time.Sleep(1 * time.Hour)
			if err := xClient.LoginToServer(); err != nil {
				msg := tgbotapi.NewMessage(logChatID, "⚠️ Релогин к Xray завершился ошибкой")
				_, _ = bot.Send(msg)
				log.Printf("[XRAY] re-login failed: %v", err)
			} else {
				// msg := tgbotapi.NewMessage(logChatID, "✅ Релогин к Xray прошел успешно")
				// _, _ = bot.Send(msg)
				log.Printf("[XRAY] re-login success")
			}
		}
	}()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	log.Println("🚀 Бот запущен в асинхронном режиме")

	for update := range updates {
		// Обрабатываем каждый update в отдельной горутине для параллельности
		go func(upd tgbotapi.Update) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("❌ Panic в обработчике update: %v", r)
				}
			}()

			if upd.PreCheckoutQuery != nil {
				handlePreCheckout(bot, upd.PreCheckoutQuery)
				return
			}
			if msg := upd.Message; msg != nil {
				handleIncomingMessage(bot, msg, xrayCfg)
				return
			}
			if cq := upd.CallbackQuery; cq != nil && cq.Message != nil {
				handleCallback(bot, cq, xrayCfg)
			}
		}(update)
	}
}

// generateSubscriptionURL builds a subscription URL using fixed path and client's SubID.
// Format: <SUB_BASE_URL>/s-39fj3r9f3j/<subID>
func generateSubscriptionURL(cfg *xraySettings, c *xray.Client) string {
	if cfg == nil || c == nil {
		return ""
	}
	base := cfg.subBaseURL
	if strings.TrimSpace(base) == "" {
		// fallback hardcoded base as per request if env not set
		base = "https://sub.staticdeliverycdn.com:2096"
	}
	subID := strings.TrimSpace(c.SubID)
	if subID == "" {
		// derive from telegram id
		subID = "sub" + strings.TrimSpace(c.TgID)
	}
	// fixed path segment '/s-39fj3r9f3j/' followed by subID
	if !strings.HasPrefix(base, "http") {
		base = "https://" + base
	}
	return fmt.Sprintf("%s/s-39fj3r9f3j/%s", strings.TrimRight(base, "/"), subID)
}

// Admin-only: sync clients across all inbounds, creating missing ones.
func handleSyncInbounds(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	chatID := msg.Chat.ID
	// Admin check
	isAdmin := false
	for _, id := range adminIDs {
		if id == msg.From.ID {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		m := tgbotapi.NewMessage(chatID, "⛔️ Только для админа")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	// Load target inbounds
	inboundIDs := xrCfg.inboundIDs
	if len(inboundIDs) == 0 {
		inbounds, err := xrCfg.client.GetAllInbounds()
		if err != nil {
			msg := tgbotapi.NewMessage(chatID, "ошибка загрузки инбаундов: "+err.Error())
			msg.ParseMode = "HTML"
			_, _ = bot.Send(msg)
			return
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				inboundIDs = append(inboundIDs, ib.ID)
			}
		}
	}
	if len(inboundIDs) == 0 {
		msg := tgbotapi.NewMessage(chatID, "нет доступных инбаундов для синхронизации")
		msg.ParseMode = "HTML"
		_, _ = bot.Send(msg)
		return
	}

	// Collect user IDs from storage
	var userIDs []string
	if pg, ok := userStore.(interface{ GetAllUserIDs() ([]string, error) }); ok {
		ids, err := pg.GetAllUserIDs()
		if err != nil {
			msg := tgbotapi.NewMessage(chatID, "ошибка получения пользователей: "+err.Error())
			msg.ParseMode = "HTML"
			_, _ = bot.Send(msg)
			return
		}
		userIDs = ids
	} else if sq, ok := userStore.(interface {
		GetAllUsers() map[string]sqlite.UserData
	}); ok {
		for id := range sq.GetAllUsers() {
			userIDs = append(userIDs, id)
		}
	}
	if len(userIDs) == 0 {
		msg := tgbotapi.NewMessage(chatID, "пользователи не найдены в хранилище")
		msg.ParseMode = "HTML"
		_, _ = bot.Send(msg)
		return
	}

	created := 0
	updated := 0
	failed := 0
	for _, uid := range userIDs {
		email := fallbackEmail(uid)
		// Ensure client across all inbounds without changing expiry (daysToAdd=0)
		// ensure secure subID per user
		subID, _ := userStore.EnsureSubscriptionID(uid)
		c, _, err := xrCfg.client.EnsureClientAcrossInbounds(inboundIDs, uid, email, 0, subID)
		if err != nil {
			failed++
			continue
		}
		if c != nil {
			// We cannot easily distinguish created vs updated without deeper signals; increment updated
			updated++
		} else {
			created++
		}
		// avoid flooding server
		time.Sleep(20 * time.Millisecond)
	}

	text := fmt.Sprintf("Синхронизация завершена. Обновлено: %d, создано: %d, ошибок: %d", updated, created, failed)
	m := tgbotapi.NewMessage(chatID, text)
	m.ParseMode = "HTML"
	_, _ = bot.Send(m)
}

// Admin-only: migrate users from DB to new Xray server with their current days balance.
func handleMigrateUsers(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	chatID := msg.Chat.ID
	// Admin check
	isAdmin := false
	for _, id := range adminIDs {
		if id == msg.From.ID {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		m := tgbotapi.NewMessage(chatID, "⛔️ Только для админа")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	// Send initial message
	initialMsg := tgbotapi.NewMessage(chatID, "🔄 Начинаю миграцию пользователей на новый сервер...")
	initialMsg.ParseMode = "HTML"
	sentMsg, _ := bot.Send(initialMsg)

	// Load target inbounds
	inboundIDs := xrCfg.inboundIDs
	if len(inboundIDs) == 0 {
		inbounds, err := xrCfg.client.GetAllInbounds()
		if err != nil {
			msg := tgbotapi.NewMessage(chatID, "❌ Ошибка загрузки инбаундов: "+err.Error())
			msg.ParseMode = "HTML"
			_, _ = bot.Send(msg)
			return
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				inboundIDs = append(inboundIDs, ib.ID)
			}
		}
	}
	if len(inboundIDs) == 0 {
		msg := tgbotapi.NewMessage(chatID, "❌ Нет доступных инбаундов для миграции")
		msg.ParseMode = "HTML"
		_, _ = bot.Send(msg)
		return
	}

	// Collect user IDs from storage
	var userIDs []string
	if pg, ok := userStore.(interface{ GetAllUserIDs() ([]string, error) }); ok {
		ids, err := pg.GetAllUserIDs()
		if err != nil {
			msg := tgbotapi.NewMessage(chatID, "❌ Ошибка получения пользователей: "+err.Error())
			msg.ParseMode = "HTML"
			_, _ = bot.Send(msg)
			return
		}
		userIDs = ids
	} else if sq, ok := userStore.(interface {
		GetAllUsers() map[string]sqlite.UserData
	}); ok {
		for id := range sq.GetAllUsers() {
			userIDs = append(userIDs, id)
		}
	}
	if len(userIDs) == 0 {
		msg := tgbotapi.NewMessage(chatID, "❌ Пользователи не найдены в хранилище")
		msg.ParseMode = "HTML"
		_, _ = bot.Send(msg)
		return
	}

	migrated := 0
	failed := 0
	skipped := 0

	progressMsg := fmt.Sprintf("📊 Миграция:\nВсего пользователей: %d\nОбработано: 0", len(userIDs))
	edit := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, progressMsg)
	edit.ParseMode = "HTML"
	_, _ = bot.Send(edit)

	for idx, uid := range userIDs {
		// Get user's current days from DB
		days, err := userStore.GetDays(uid)
		if err != nil {
			log.Printf("skip user %s: cannot get days: %v", uid, err)
			skipped++
			continue
		}

		// If user has no days, skip migration (or create with 0 days based on requirement)
		if days <= 0 {
			skipped++
			// Update progress every 10 users
			if (idx+1)%10 == 0 {
				progressMsg = fmt.Sprintf("📊 Миграция:\nВсего: %d | Обработано: %d\n✅ Мигрировано: %d | ⏭ Пропущено: %d | ❌ Ошибок: %d",
					len(userIDs), idx+1, migrated, skipped, failed)
				edit := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, progressMsg)
				edit.ParseMode = "HTML"
				_, _ = bot.Send(edit)
			}
			continue
		}

		email := fallbackEmail(uid)

		// Create client on new server with their current days balance
		subID, _ := userStore.EnsureSubscriptionID(uid)
		_, _, err = xrCfg.client.EnsureClientAcrossInbounds(inboundIDs, uid, email, days, subID)
		if err != nil {
			log.Printf("migration failed for user %s: %v", uid, err)
			failed++
			continue
		}

		migrated++

		// Update progress every 10 users or at the end
		if (idx+1)%10 == 0 || idx == len(userIDs)-1 {
			progressMsg = fmt.Sprintf("📊 Миграция:\nВсего: %d | Обработано: %d\n✅ Мигрировано: %d | ⏭ Пропущено: %d | ❌ Ошибок: %d",
				len(userIDs), idx+1, migrated, skipped, failed)
			edit := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, progressMsg)
			edit.ParseMode = "HTML"
			_, _ = bot.Send(edit)
		}

		// Avoid flooding server
		time.Sleep(50 * time.Millisecond)
	}

	finalText := fmt.Sprintf("✅ <b>Миграция завершена!</b>\n\n"+
		"📊 <b>Статистика:</b>\n"+
		"├ Всего пользователей: %d\n"+
		"├ ✅ Успешно мигрировано: %d\n"+
		"├ ⏭ Пропущено (нет дней): %d\n"+
		"└ ❌ Ошибок: %d\n\n"+
		"Все пользователи с активными днями перенесены на новый сервер с установкой SubID.",
		len(userIDs), migrated, skipped, failed)

	finalMsg := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, finalText)
	finalMsg.ParseMode = "HTML"
	_, _ = bot.Send(finalMsg)
}

// Admin-only: migrate expiry times from old Xray server to update days in new DB.
func handleMigrateExpiryFromOld(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	// Admin check
	isAdmin := false
	for _, id := range adminIDs {
		if id == msg.From.ID {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		m := tgbotapi.NewMessage(chatID, "⛔️ Только для админа")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	if oldXrayCfg == nil || oldXrayCfg.client == nil {
		m := tgbotapi.NewMessage(chatID, "❌ Старый Xray сервер не настроен (переменные OLD_XRAY_* отсутствуют)")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	initialMsg := tgbotapi.NewMessage(chatID, "🔄 Загружаю данные об истечении доступа со старого сервера...")
	initialMsg.ParseMode = "HTML"
	sentMsg, _ := bot.Send(initialMsg)

	// Determine target inbounds on old server
	oldInboundIDs := oldXrayCfg.inboundIDs
	if len(oldInboundIDs) == 0 {
		inbounds, err := oldXrayCfg.client.GetAllInbounds()
		if err != nil {
			msg := tgbotapi.NewMessage(chatID, "❌ Ошибка загрузки инбаундов старого сервера: "+err.Error())
			msg.ParseMode = "HTML"
			_, _ = bot.Send(msg)
			return
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				oldInboundIDs = append(oldInboundIDs, ib.ID)
			}
		}
	}

	if len(oldInboundIDs) == 0 {
		m := tgbotapi.NewMessage(chatID, "❌ Не найдены инбаунды на старом сервере")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	// Collect all clients with their expiryTime from old server
	type oldClientInfo struct {
		tgID       string
		expiryTime int64
	}
	oldClientsMap := make(map[string]oldClientInfo) // key: tgID

	for _, inboundID := range oldInboundIDs {
		clients, err := oldXrayCfg.client.GetInboundById(inboundID)
		if err != nil {
			log.Printf("failed to load old inbound %d: %v", inboundID, err)
			continue
		}
		for _, c := range clients {
			tgID := strings.TrimSpace(c.TgID)
			if tgID == "" {
				continue
			}
			// Keep the client with latest expiry for each tgID
			if old, exists := oldClientsMap[tgID]; !exists || c.ExpiryTime > old.expiryTime {
				oldClientsMap[tgID] = oldClientInfo{
					tgID:       tgID,
					expiryTime: c.ExpiryTime,
				}
			}
		}
	}

	if len(oldClientsMap) == 0 {
		m := tgbotapi.NewMessage(chatID, "❌ Клиенты не найдены на старом сервере")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	progressMsg := fmt.Sprintf("📊 Миграция сроков:\nВсего клиентов: %d\nОбработано: 0", len(oldClientsMap))
	edit := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, progressMsg)
	edit.ParseMode = "HTML"
	_, _ = bot.Send(edit)

	updated := 0
	skipped := 0
	failed := 0
	idx := 0

	for _, oldClient := range oldClientsMap {
		idx++
		tgID := oldClient.tgID

		// Calculate days remaining
		expireAt := time.UnixMilli(oldClient.expiryTime)
		remain := time.Until(expireAt)
		daysLeft := int64(0)
		if remain > 0 {
			daysLeft = int64(remain.Hours()/24 + 0.999)
		}

		if daysLeft <= 0 {
			skipped++
		} else {
			// Update days in new DB (replace, not add)
			if err := userStore.SetDays(tgID, daysLeft); err != nil {
				log.Printf("failed to update days for user %s: %v", tgID, err)
				failed++
				continue
			}

			// Also update expiryTime on new Xray server for all inbounds (SET exact expiry, not ADD days)
			newInboundIDs := xrayCfg.inboundIDs
			if len(newInboundIDs) == 0 {
				// fallback to first inbound if not configured
				if xrayCfg.inboundID > 0 {
					newInboundIDs = []int{xrayCfg.inboundID}
				}
			}

			expireAt := time.Now().Add(time.Duration(daysLeft) * 24 * time.Hour)
			success := true
			for _, inboundID := range newInboundIDs {
				// Get existing client
				c, err := xrayCfg.client.GetClientByTelegram(inboundID, tgID)
				if err != nil || c == nil {
					continue // client may not exist on this inbound yet
				}
				// Set exact expiryTime (don't add, replace)
				c.ExpiryTime = expireAt.UnixMilli()
				if err := xrayCfg.client.UpdateClient(inboundID, *c); err != nil {
					log.Printf("failed to update expiry on inbound %d for user %s: %v", inboundID, tgID, err)
					success = false
				}
			}

			if success {
				updated++
			} else {
				failed++
			}
		}

		// Update progress every 20 users
		if idx%20 == 0 || idx == len(oldClientsMap) {
			progressMsg = fmt.Sprintf("📊 Миграция сроков:\nВсего: %d | Обработано: %d\n✅ Обновлено: %d | ⏭ Истекло: %d | ❌ Ошибок: %d",
				len(oldClientsMap), idx, updated, skipped, failed)
			edit := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, progressMsg)
			edit.ParseMode = "HTML"
			_, _ = bot.Send(edit)
		}

		time.Sleep(20 * time.Millisecond)
	}

	finalText := fmt.Sprintf("✅ <b>Миграция сроков завершена!</b>\n\n"+
		"📊 <b>Статистика:</b>\n"+
		"├ Всего клиентов на старом сервере: %d\n"+
		"├ ✅ Успешно обновлено дней в БД: %d\n"+
		"├ ⏭ Истекло (срок ≤ 0): %d\n"+
		"└ ❌ Ошибок: %d\n\n"+
		"Дни в новой БД синхронизированы со старого сервера.",
		len(oldClientsMap), updated, skipped, failed)

	finalMsg := tgbotapi.NewEditMessageText(chatID, sentMsg.MessageID, finalText)
	finalMsg.ParseMode = "HTML"
	_, _ = bot.Send(finalMsg)
}

func handleIncomingMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	chatID := msg.Chat.ID
	session := getSession(chatID)

	// Команда рассылки для админа
	if msg.IsCommand() && msg.Command() == "notify" {
		isAdmin := false
		for _, id := range adminIDs {
			if id == msg.From.ID {
				isAdmin = true
				break
			}
		}
		if !isAdmin {
			_ = updateSessionText(bot, chatID, session, stateMenu, "⛔️ Только для админа", "HTML", mainMenuInlineKeyboard())
			return
		}
		// Определяем источник контента: сам месседж или реплай на медиа
		sourceMsg := msg
		if msg.ReplyToMessage != nil {
			sourceMsg = msg.ReplyToMessage
		}

		// Текст рассылки (из аргументов команды или подписи)
		broadcastText := strings.TrimSpace(msg.CommandArguments())
		if broadcastText == "" {
			// Попробуем извлечь текст из подписи, если там была команда
			cap := strings.TrimSpace(sourceMsg.Caption)
			if strings.HasPrefix(cap, "/notify") {
				after := strings.TrimSpace(cap)
				// Уберём токен команды (/notify или /notify@bot)
				if sp := strings.Index(after, " "); sp >= 0 {
					broadcastText = strings.TrimSpace(after[sp+1:])
				} else {
					broadcastText = ""
				}
			}
		}

		// Есть ли медиа в исходном сообщении
		hasPhoto := sourceMsg.Photo != nil && len(sourceMsg.Photo) > 0
		hasAnim := sourceMsg.Animation != nil
		hasDoc := sourceMsg.Document != nil
		hasVideo := sourceMsg.Video != nil
		hasMedia := hasPhoto || hasAnim || hasDoc || hasVideo

		// Если нет медиа и нет текста — просим текст
		if !hasMedia && strings.TrimSpace(broadcastText) == "" {
			_ = updateSessionText(bot, chatID, session, stateMenu, "Укажите текст для рассылки: /notify <текст>", "HTML", mainMenuInlineKeyboard())
			return
		}

		go func() {
			var userIDs []string
			var err error
			// Для Postgres
			if pg, ok := userStore.(interface{ GetAllUserIDs() ([]string, error) }); ok {
				userIDs, err = pg.GetAllUserIDs()
			} else if sq, ok := userStore.(interface{ GetAllUsers() map[string]interface{} }); ok {
				for id := range sq.GetAllUsers() {
					userIDs = append(userIDs, id)
				}
			} else if sq, ok := userStore.(interface {
				GetAllUsers() map[string]sqlite.UserData
			}); ok {
				for id := range sq.GetAllUsers() {
					userIDs = append(userIDs, id)
				}
			} else {
				err = fmt.Errorf("userStore не поддерживает массовое получение id")
			}
			if err != nil {
				msg := tgbotapi.NewMessage(chatID, "Ошибка получения пользователей: "+err.Error())
				msg.ParseMode = "HTML"
				_, _ = bot.Send(msg)
				return
			}
			count := 0
			for _, uid := range userIDs {
				id, err := strconv.ParseInt(uid, 10, 64)
				if err != nil {
					continue
				}
				if hasMedia {
					// Сначала пробуем copyMessage (с переопределением подписи при необходимости)
					cm := tgbotapi.NewCopyMessage(id, sourceMsg.Chat.ID, sourceMsg.MessageID)
					if strings.TrimSpace(broadcastText) != "" {
						cm.Caption = broadcastText
						cm.ParseMode = "HTML"
					} else if strings.HasPrefix(strings.TrimSpace(sourceMsg.Caption), "/notify") {
						// Если исходная подпись содержала команду, очищаем её у получателей
						cm.Caption = ""
					}
					if _, err = bot.Send(cm); err != nil {
						// Fallback на отправку по FileID в зависимости от типа
						switch {
						case hasAnim:
							cfg := tgbotapi.NewAnimation(id, tgbotapi.FileID(sourceMsg.Animation.FileID))
							cfg.Caption = broadcastText
							cfg.ParseMode = "HTML"
							_, err = bot.Send(cfg)
						case hasPhoto:
							p := sourceMsg.Photo[len(sourceMsg.Photo)-1]
							cfg := tgbotapi.NewPhoto(id, tgbotapi.FileID(p.FileID))
							cfg.Caption = broadcastText
							cfg.ParseMode = "HTML"
							_, err = bot.Send(cfg)
						case hasVideo:
							cfg := tgbotapi.NewVideo(id, tgbotapi.FileID(sourceMsg.Video.FileID))
							cfg.Caption = broadcastText
							cfg.ParseMode = "HTML"
							_, err = bot.Send(cfg)
						case hasDoc:
							cfg := tgbotapi.NewDocument(id, tgbotapi.FileID(sourceMsg.Document.FileID))
							cfg.Caption = broadcastText
							cfg.ParseMode = "HTML"
							_, err = bot.Send(cfg)
						default:
							// На всякий случай — текстом
							m := tgbotapi.NewMessage(id, broadcastText)
							m.ParseMode = "HTML"
							_, err = bot.Send(m)
						}
					}
				} else {
					m := tgbotapi.NewMessage(id, broadcastText)
					m.ParseMode = "HTML"
					_, err = bot.Send(m)
				}
				if err == nil {
					count++
				}
				// Не спамим слишком быстро
				time.Sleep(30 * time.Millisecond)
			}
			msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("Рассылка завершена. Доставлено: %d", count))
			msg.ParseMode = "HTML"
			_, _ = bot.Send(msg)
		}()
		_ = updateSessionText(bot, chatID, session, stateMenu, "Рассылка запущена", "HTML", mainMenuInlineKeyboard())
		return
	}

	if msg.SuccessfulPayment != nil {
		planID := session.PendingPlanID
		if payload := strings.TrimSpace(msg.SuccessfulPayment.InvoicePayload); strings.HasPrefix(payload, starsPayloadPrefix) {
			planID = strings.TrimPrefix(payload, starsPayloadPrefix)
		}
		plan, ok := ratePlanByID[planID]
		if !ok {
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Платёж есть, но тариф не ясен. Напиши в поддержку.", "", mainMenuInlineKeyboard())
			return
		}
		if err := handleSuccessfulPayment(bot, msg, xrCfg, plan, session); err != nil {
			log.Printf("handleSuccessfulPayment error: %v", err)
		}
		return
	}

	if msg.IsCommand() {
		switch msg.Command() {
		case "start":
			handleStart(bot, msg, session, xrCfg)
		case "adlink":
			handleAdLink(bot, msg)
		case "adcheck":
			handleAdCheck(bot, msg)
		case "sync_inbounds":
			handleSyncInbounds(bot, msg, xrCfg)
		case "migrate_users":
			handleMigrateUsers(bot, msg, xrCfg)
		case "migrate_expiry_from_old":
			handleMigrateExpiryFromOld(bot, msg)
		case "topup", "пополнить", "пополнить_баланс":
			handleTopUp(bot, &tgbotapi.CallbackQuery{Message: msg}, session)
		case "getvpn", "vpn", "подключить", "получитьvpn":
			handleGetVPN(bot, &tgbotapi.CallbackQuery{Message: msg, From: msg.From}, session, xrCfg)
		case "status", "profile", "профиль":
			handleStatus(bot, &tgbotapi.CallbackQuery{Message: msg, From: msg.From}, session, xrCfg)
		case "instructions", "инструкции":
			handleInstructionsMenu(bot, &tgbotapi.CallbackQuery{Message: msg}, session)
		case "referral", "рефералы":
			handleReferral(bot, &tgbotapi.CallbackQuery{Message: msg, From: msg.From}, session)
		case "support", "поддержка":
			handleSupport(bot, &tgbotapi.CallbackQuery{Message: msg, From: msg.From}, session)
		default:
			_ = showMainMenu(bot, chatID, session)
		}
		return
	}

	if session.State == stateCollectEmail {
		userID := strconv.FormatInt(msg.From.ID, 10)
		addr, err := mail.ParseAddress(strings.TrimSpace(msg.Text))
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(bot, chatID, session, stateCollectEmail, "Неверный e-mail, пример: name@example.com", "HTML", mainMenuInlineKeyboard())
			return
		}
		_ = userStore.SetEmail(userID, addr.Address)
		_ = userStore.AcceptPrivacy(userID, time.Now())

		plan, ok := ratePlanByID[session.PendingPlanID]
		if !ok {
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Тариф не найден, выбери заново.", "HTML", rateKeyboard())
			return
		}
		if err := startPaymentForPlan(bot, chatID, session, plan); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось создать платёж.", "", mainMenuInlineKeyboard())
		}
		return
	}

	if session.State == stateEditEmail {
		userID := strconv.FormatInt(msg.From.ID, 10)
		addr, err := mail.ParseAddress(strings.TrimSpace(msg.Text))
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(bot, chatID, session, stateEditEmail, "Неверный e-mail.", "HTML", mainMenuInlineKeyboard())
			return
		}
		_ = userStore.SetEmail(userID, addr.Address)
		handleStatus(bot, &tgbotapi.CallbackQuery{Message: msg}, session, xrCfg)
		return
	}
}

func handleStart(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, session *UserSession, xrCfg *xraySettings) {
	chatID := msg.Chat.ID
	userID := strconv.FormatInt(msg.From.ID, 10)
	args := msg.CommandArguments()
	referrerID := ""
	if args != "" && strings.HasPrefix(args, "ref_") {
		referrerID = strings.TrimPrefix(args, "ref_")
	}

	if args != "" && strings.HasPrefix(args, "ad_") {
		adTag := strings.TrimPrefix(args, "ad_")
		adStats.record(adTag, userID)
	}

	isNew := userStore.IsNewUser(userID)
	if isNew && referrerID != "" && referrerID != userID {
		if err := userStore.RecordReferral(userID, referrerID); err == nil {
			_ = userStore.AddDays(referrerID, 15)
			_, _ = ensureXrayAccess(xrayCfg, referrerID, fallbackEmail(referrerID), 15, true)
			if ok, _ := userStore.ClaimStartBonus(userID, "referral", time.Now()); ok {
				_ = userStore.AddDays(userID, 7)
				_, _ = ensureXrayAccess(xrayCfg, userID, fallbackEmail(userID), 7, true)
			}
			if refChatID, err := strconv.ParseInt(referrerID, 10, 64); err == nil {
				refDays, _ := userStore.GetDays(referrerID)
				refCount := userStore.GetReferralsCount(referrerID)
				newUserName := msg.From.UserName
				if newUserName == "" {
					newUserName = fmt.Sprintf("ID:%s", userID)
				} else {
					newUserName = fmt.Sprintf("@%s", newUserName)
				}
				refMsg := fmt.Sprintf("🎉 <b>по вашей реферальной ссылке зарегистрировался %s!</b>\n\n🎁 <b>вам начислено: +15 дней</b>\n👥 <b>всего рефералов:</b> %d\n⏱ <b>баланс:</b> %d дн.", newUserName, refCount, refDays)
				nmsg := tgbotapi.NewMessage(refChatID, refMsg)
				nmsg.ParseMode = "HTML"
				_, _ = bot.Send(nmsg)

				adminMsg := fmt.Sprintf("🆕 <b>Реферал:</b> Пользователь <code>%s</code> (ID:%s) перешёл по ссылке пользователя <code>%s</code> (ID:%s)\nПригласившему начислено +15 дней.", newUserName, userID, referrerID, referrerID)
				amsg := tgbotapi.NewMessage(logChatID, adminMsg)
				amsg.ParseMode = "HTML"
				_, _ = bot.Send(amsg)
			}
		}
	}

	session.PendingPlanID = ""
	_ = showMainMenu(bot, chatID, session)

	if claimed, err := userStore.IsStartBonusClaimed(userID); err == nil && !claimed {
		sendChannelBonusOffer(bot, chatID)
	}
}

func handleAdLink(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	isAdmin := false
	for _, id := range adminIDs {
		if id == msg.From.ID {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		m := tgbotapi.NewMessage(chatID, "доступ только для админов")
		_, _ = bot.Send(m)
		return
	}

	args := strings.Fields(msg.CommandArguments())
	if len(args) < 1 {
		m := tgbotapi.NewMessage(chatID, "использование: /adlink <канал или @канал> [ид_поста]\nпример: /adlink @mychannel 123 или /adlink @mychannel (тогда сгенерирую случайный id)")
		_, _ = bot.Send(m)
		return
	}
	channel := strings.TrimPrefix(args[0], "@")
	if channel == "" {
		m := tgbotapi.NewMessage(chatID, "укажи канал, например @mychannel")
		_, _ = bot.Send(m)
		return
	}
	postID := ""
	if len(args) > 1 {
		postID = args[1]
	} else {
		postID = randomSlug(8)
	}

	tag := fmt.Sprintf("%s/%s", channel, postID)
	startParam := "ad_" + tag
	link := fmt.Sprintf("https://t.me/%s?start=%s", bot.Self.UserName, startParam)

	text := fmt.Sprintf("Рекламная ссылка:\nканал: @%s\nпост: %s\nstart: %s\n\n%s", channel, postID, startParam, link)
	m := tgbotapi.NewMessage(chatID, text)
	m.DisableWebPagePreview = true
	_, _ = bot.Send(m)
}

func handleAdCheck(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	isAdmin := false
	for _, id := range adminIDs {
		if id == msg.From.ID {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		m := tgbotapi.NewMessage(chatID, "доступ только для админов")
		_, _ = bot.Send(m)
		return
	}

	args := strings.Fields(msg.CommandArguments())
	if len(args) < 1 {
		m := tgbotapi.NewMessage(chatID, "использование: /adcheck <канал|@канал>\nпример: /adcheck @mychannel")
		_, _ = bot.Send(m)
		return
	}
	channel := strings.TrimPrefix(args[0], "@")
	stats := adStats.statsForChannel(channel)
	if len(stats) == 0 {
		m := tgbotapi.NewMessage(chatID, fmt.Sprintf("нет данных по каналу @%s", channel))
		_, _ = bot.Send(m)
		return
	}

	// sort tags by count desc
	type item struct {
		tag   string
		count int
	}
	var items []item
	for tag, c := range stats {
		items = append(items, item{tag: tag, count: c})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].count > items[j].count })

	var b strings.Builder
	b.WriteString(fmt.Sprintf("Статистика по @%s:\n", channel))
	for _, it := range items {
		post := strings.TrimPrefix(it.tag, channel+"/")
		link := fmt.Sprintf("https://t.me/%s", it.tag)
		if post != "" {
			link = fmt.Sprintf("https://t.me/%s/%s", channel, post)
		}
		b.WriteString(fmt.Sprintf("• пост %s — %d переходов (%s)\n", post, it.count, link))
	}
	m := tgbotapi.NewMessage(chatID, b.String())
	m.DisableWebPagePreview = true
	_, _ = bot.Send(m)
}

func handleReferralStats(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userID := strconv.FormatInt(msg.From.ID, 10)
	link := fmt.Sprintf("https://t.me/%s?start=ref_%s", bot.Self.UserName, userID)
	count := userStore.GetReferralsCount(userID)
	text := fmt.Sprintf("Твоя ссылка:\n%s\n\nПривлёк: %d\nБонусов: %d дней", link, count, count*15)
	reply := tgbotapi.NewMessage(chatID, text)
	reply.ParseMode = "HTML"
	bot.Send(reply)
}

func handleCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	session := getSession(chatID)
	data := cq.Data
	ackText := ""

	// Логирование действия для админов (не логируем навигацию по инструкциям и шаги)
	username := cq.From.UserName
	userID := int64(cq.From.ID)
	actionName := getActionName(data)
	if !(strings.HasPrefix(data, "win_prev_") || strings.HasPrefix(data, "win_next_") ||
		strings.HasPrefix(data, "android_prev_") || strings.HasPrefix(data, "android_next_") ||
		strings.HasPrefix(data, "ios_prev_") || strings.HasPrefix(data, "ios_next_") ||
		strings.HasPrefix(data, "macos_prev_") || strings.HasPrefix(data, "macos_next_") || data == "ios_current" ||
		data == "windows" || data == "android" || data == "ios" || data == "macos" || data == "nav_status") {
		notifyAdmins(bot, userID, username, actionName)
	}

	switch {
	case data == "nav_menu":
		_ = showMainMenu(bot, chatID, session)
	case data == "nav_get_vpn":
		handleGetVPN(bot, cq, session, xrCfg)
	case data == "nav_topup":
		handleTopUp(bot, cq, session)
	case data == "nav_status":
		handleStatus(bot, cq, session, xrCfg)
	case data == "nav_referral":
		handleReferral(bot, cq, session)
	case data == "nav_support":
		handleSupport(bot, cq, session)
	case data == "edit_email":
		handleEditEmail(bot, cq, session)
	case data == "nav_instructions":
		handleInstructionsMenu(bot, cq, session)
	case data == "claim_sub_bonus":
		handleClaimSubscriptionBonus(bot, cq, session, xrCfg)
		return
	case data == "copy_key":
		{
			userIDStr := strconv.FormatInt(cq.From.ID, 10)
			info, _ := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), 0, true)
			link := ""
			if info != nil && info.client != nil {
				link = generateSubscriptionURL(xrCfg, info.client)
				if strings.TrimSpace(link) == "" {
					link = info.link
				}
			}
			if strings.TrimSpace(link) == "" {
				ackCallback(bot, cq, "ключ недоступен, попробуйте позже")
				return
			}
			msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("`%s`", escapeMarkdownV2(link)))
			msg.ParseMode = "MarkdownV2"
			msg.DisableWebPagePreview = true
			_, _ = bot.Send(msg)
			ackCallback(bot, cq, "нажми на моноспейс, чтобы скопировать")
			return
		}
	case data == "windows":
		if err := startInstructionFlow(bot, chatID, session, xrCfg, instruct.Windows, 0); err != nil {
			log.Printf("windows instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "android":
		if err := startInstructionFlow(bot, chatID, session, xrCfg, instruct.Android, 0); err != nil {
			log.Printf("android instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "ios":
		if err := startInstructionFlow(bot, chatID, session, xrCfg, instruct.IOS, 0); err != nil {
			log.Printf("ios instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "macos":
		if err := startInstructionFlow(bot, chatID, session, xrCfg, instruct.MacOS, 0); err != nil {
			log.Printf("macos instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case strings.HasPrefix(data, "win_prev_"):
		// win_prev_<currentStep>
		parts := strings.Split(data, "win_prev_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionWindows(chatID, bot, n-1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("windows prev step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "win_next_"):
		parts := strings.Split(data, "win_next_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionWindows(chatID, bot, n+1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("windows next step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "android_prev_"):
		parts := strings.Split(data, "android_prev_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionAndroid(chatID, bot, n-1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("android prev step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "android_next_"):
		parts := strings.Split(data, "android_next_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionAndroid(chatID, bot, n+1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("android next step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "ios_prev_"):
		parts := strings.Split(data, "ios_prev_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionIos(chatID, bot, n-1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("ios prev step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "ios_next_"):
		parts := strings.Split(data, "ios_next_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionIos(chatID, bot, n+1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("ios next step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "macos_prev_"):
		parts := strings.Split(data, "macos_prev_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionMacOS(chatID, bot, n-1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("macos prev step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "macos_next_"):
		parts := strings.Split(data, "macos_next_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionMacOS(chatID, bot, n+1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("macos next step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "pay_stars_"):
		id := strings.TrimPrefix(data, "pay_stars_")
		p, ok := ratePlanByID[id]
		if !ok {
			ackCallback(bot, cq, "тариф не найден")
			return
		}
		session.PendingPlanID = p.ID
		link, err := createStarsInvoiceLink(bot, p)
		if err != nil {
			log.Printf("createStarsInvoiceLink error: %v", err)
			ackCallback(bot, cq, "не удалось создать ссылку")
			return
		}
		stars := starsAmountForPlan(p)
		text := fmt.Sprintf(
			"💰 покупка доступа\n\nсрок: %d дней\nцена: %.0f ₽ или %d ⭐\n\nнажми «оплатить ⭐».",
			p.Days, p.Amount, stars,
		)
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("оплатить ⭐", link),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_topup"),
				tgbotapi.NewInlineKeyboardButtonData("🏠 меню", "nav_menu"),
			),
		)
		_ = updateSessionText(bot, chatID, session, stateChoosePay, text, "", kb)
		ackCallback(bot, cq, "готово")
		return
	case strings.HasPrefix(data, "pay_card_"):
		id := strings.TrimPrefix(data, "pay_card_")
		p, ok := ratePlanByID[id]
		if !ok {
			ackCallback(bot, cq, "тариф не найден")
			return
		}
		session.PendingPlanID = p.ID

		userID := strconv.FormatInt(cq.From.ID, 10)
		if email, _ := userStore.GetEmail(userID); strings.TrimSpace(email) == "" {
			text := "📧 Для оплаты картой нужен e-mail для чека.\nОтправь e-mail следующим сообщением (пример: name@example.com).\n\n" +
				"<b>Продолжая и вводя e-mail, ты соглашаешься с <a href=\"https://telegra.ph/POLZOVATELSKOE-SOGLASHENIE-PUBLICHNAYA-OFERTA-SERVISA-HAPPY-CAT-VPN-10-27\">пользовательским соглашением</a>.</b>"
			kb := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_topup"),
					tgbotapi.NewInlineKeyboardButtonData("🏠 меню", "nav_menu"),
				),
			)
			_ = updateSessionText(bot, chatID, session, stateCollectEmail, text, "HTML", kb)
			ackCallback(bot, cq, "пришли e-mail")
			return
		}

		if err := startPaymentForPlan(bot, chatID, session, p); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "𢙴 Не удалось создать платёж.", "", mainMenuInlineKeyboard())
			ackCallback(bot, cq, "ошибка оплаты")
			return
		}
		ackCallback(bot, cq, "счёт создан")
		return
	case strings.HasPrefix(data, "rate_"):
		id := strings.TrimPrefix(data, "rate_")
		if p, ok := ratePlanByID[id]; ok {
			handleRateSelection(bot, cq, session, p)
			// Уведомление в лог-чат о выборе тарифа
			planMsg := fmt.Sprintf("💸 Пользователь ID:%d выбрал тариф: %s (%d дн., %.0f₽)", userID, p.Title, p.Days, p.Amount)
			m := tgbotapi.NewMessage(logChatID, planMsg)
			m.ParseMode = "HTML"
			m.DisableWebPagePreview = true
			_, _ = bot.Send(m)
			return
		}
	case data == "check_payment":
		handleCheckPayment(bot, cq, session, xrCfg)

	}

	ackCallback(bot, cq, ackText)
}

// small helper for callback answers
func ackCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, text string) {
	cfg := tgbotapi.CallbackConfig{CallbackQueryID: cq.ID}
	if strings.TrimSpace(text) != "" {
		cfg.Text = text
	}
	_, _ = bot.Request(cfg)
}

// escapeMarkdownV2 escapes a string for safe use inside MarkdownV2 code spans.
func escapeMarkdownV2(s string) string {
	replacer := strings.NewReplacer(
		"_", "\\_",
		"*", "\\*",
		"[", "\\[",
		"]", "\\]",
		"(", "\\(",
		")", "\\)",
		"~", "\\~",
		"`", "\\`",
		">", "\\>",
		"#", "\\#",
		"+", "\\+",
		"-", "\\-",
		"=", "\\=",
		"|", "\\|",
		"{", "\\{",
		"}", "\\}",
		".", "\\.",
		"!", "\\!",
	)
	return replacer.Replace(s)
}

func randomSlug(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(letters[rand.Intn(len(letters))])
	}
	return b.String()
}

func handleTopUp(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = ""
	var builder strings.Builder
	builder.WriteString("💰 покупка доступа\nчем больше период — тем выгоднее!\n\nвыберите период ниже.\nпосле выбора можно оплатить картой или звёздами.\n\nтарифы:\n")
	for _, plan := range ratePlans {
		builder.WriteString(fmt.Sprintf("• %d дней — %.0f ₽\n", plan.Days, plan.Amount))
	}
	header := strings.TrimSuffix(builder.String(), "\n")
	_ = updateSessionText(bot, chatID, session, stateTopUp, header, "HTML", rateKeyboard())
}
func handleRateSelection(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, plan RatePlan) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = plan.ID

	stars := starsAmountForPlan(plan)
	text := fmt.Sprintf(
		"💰 покупка доступа\n\nсрок: %d дней\nцена: %.0f ₽ или %d ⭐\n\nвыберите способ оплаты:",
		plan.Days, plan.Amount, stars,
	)
	_ = updateSessionText(bot, chatID, session, stateChoosePay, text, "HTML", choosePayKeyboard(plan))
	ackCallback(bot, cq, "выберите способ оплаты")
}
func startPaymentForPlan(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, plan RatePlan) error {
	metadata := map[string]interface{}{
		"plan_id":     plan.ID,
		"plan_title":  plan.Title,
		"plan_days":   plan.Days,
		"plan_amount": plan.Amount,
	}

	email, _ := userStore.GetEmail(strconv.FormatInt(chatID, 10))
	newID, _, err := yookassaClient.SendVPNPayment(bot, chatID, session.MessageID, plan.Amount, plan.Title, metadata, email)
	if err != nil {
		return err
	}
	session.MessageID = newID
	session.State = stateTopUp
	session.PendingPlanID = plan.ID
	instruct.ResetState(chatID)
	return nil
}

func handleCheckPayment(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	payment, ok, err := yookassaClient.FindSucceededPayment(chatID)
	if err != nil {
		log.Printf("FindSucceededPayment error: %v", err)
		ackCallback(bot, cq, "Ошибка проверки платежа")
		return
	}
	if !ok || payment == nil {
		ackCallback(bot, cq, "Платеж не найден, попробуй позже (5-10 сек)")
		return
	}

	yookassaClient.ClearPayments(chatID)
	meta := payment.Metadata
	plan := resolvePlanFromMetadata(meta, session)
	if plan.Title == "" {
		ackCallback(bot, cq, "Тариф в платеже не найден")
		return
	}

	fake := &tgbotapi.Message{Chat: cq.Message.Chat, From: cq.From}
	if err := handleSuccessfulPayment(bot, fake, xrCfg, plan, session); err != nil {
		log.Printf("handleSuccessfulPayment error: %v", err)
		ackCallback(bot, cq, "Не удалось выдать доступ")
		return
	}

	ackCallback(bot, cq, fmt.Sprintf("Платеж за %s подтверждён", plan.Title))
}

func handleClaimSubscriptionBonus(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)
	userIDStr := strconv.FormatInt(userID, 10)

	if claimed, err := userStore.IsStartBonusClaimed(userIDStr); err == nil && claimed {
		ackCallback(bot, cq, "бонус уже получен")
		return
	}

	ok, err := isSubscribedToChannel(bot, userID)
	if err != nil {
		log.Printf("subscription check failed: %v", err)
		ackCallback(bot, cq, "не удалось проверить подписку")
		return
	}
	if !ok {
		ackCallback(bot, cq, "сначала подпишитесь на канал")
		return
	}

	if ok, err := userStore.ClaimStartBonus(userIDStr, "channel", time.Now()); err != nil {
		log.Printf("claim start bonus failed: %v", err)
		ackCallback(bot, cq, "не удалось начислить бонус")
		return
	} else if !ok {
		ackCallback(bot, cq, "бонус уже получен")
		return
	}

	if err := userStore.AddDays(userIDStr, int64(channelBonusDays)); err != nil {
		ackCallback(bot, cq, "не удалось начислить дни")
		return
	}

	info, err := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), int64(channelBonusDays), true)
	if err != nil {
		log.Printf("ensureXrayAccess bonus error: %v", err)
		ackCallback(bot, cq, "не удалось выдать доступ")
		return
	}

	_ = sendAccess(info, userIDStr, chatID, channelBonusDays, userID, xrCfg, bot, session)
	ackCallback(bot, cq, "бонус выдан")
}

func handlePreCheckout(bot *tgbotapi.BotAPI, pcq *tgbotapi.PreCheckoutQuery) {
	ok := true
	errMsg := ""
	if payload := strings.TrimSpace(pcq.InvoicePayload); strings.HasPrefix(payload, starsPayloadPrefix) {
		id := strings.TrimPrefix(payload, starsPayloadPrefix)
		if _, exists := ratePlanByID[id]; !exists {
			ok = false
			errMsg = "тариф не найден"
		} else if strings.TrimSpace(pcq.Currency) != starsCurrency {
			ok = false
			errMsg = "неверная валюта"
		}
	}
	ans := tgbotapi.PreCheckoutConfig{
		PreCheckoutQueryID: pcq.ID,
		OK:                 ok,
		ErrorMessage:       errMsg,
	}
	if _, err := bot.Request(ans); err != nil {
		log.Printf("precheckout answer error: %v", err)
	}
}

func handleGetVPN(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)
	telegramUser := fmt.Sprint(userID)

	info, err := ensureXrayAccess(xrCfg, telegramUser, fallbackEmail(telegramUser), 0, true)
	if err != nil {
		log.Printf("ensureXrayAccess error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не удалось получить доступ. Напиши в поддержку.", "", mainMenuInlineKeyboard())
		return
	}

	if err := sendAccess(info, telegramUser, chatID, 0, userID, xrCfg, bot, session); err != nil {
		log.Printf("sendAccess error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не получилось отправить ссылку.", "", mainMenuInlineKeyboard())
		return
	}

	// sendMessageToAdmin(fmt.Sprintf("user id:%d запросил VPN", cq.From.ID), cq.From.UserName, bot, userID)
}

func handleStatus(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)
	userIDStr := strconv.FormatInt(userID, 10)

	info, _ := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), 0, false)
	days, _ := userStore.GetDays(userIDStr)
	if info != nil && info.daysLeft > 0 {
		days = info.daysLeft
	}

	email, _ := userStore.GetEmail(userIDStr)
	if strings.TrimSpace(email) == "" {
		email = "не указан"
	}
	emailEsc := html.EscapeString(email)
	refCount := userStore.GetReferralsCount(userIDStr)
	refBonus := refCount * referralBonusDays

	header := fmt.Sprintf(
		"👤 профиль\n🪪 id: <code>%d</code>\n📧 mail: %s\n🎁 рефералы: %d (дней: %d)",
		userID, emailEsc, refCount, refBonus,
	)

	var accessBlock string
	if days > 0 {
		expTime := time.Time{}
		if info != nil && !info.expireAt.IsZero() {
			expTime = info.expireAt
		} else {
			expTime = time.Now().UTC().Add(time.Duration(days) * 24 * time.Hour)
		}
		expStr := formatExpiryUTC(expTime)
		accessBlock = fmt.Sprintf(
			"\n\nу вас есть доступ к neuravpn 🟢\nон активен ещё %d дней\nдо %s\n\nесли хотите продлить доступ - переходите в раздел «оплата»\nтам все очень дешево!",
			days, expStr,
		)
	} else {
		accessBlock = "\n\nу вас нет доступа к neuravpn 🔴\n\nесли хотите продлить доступ - переходите в раздел «оплата»\nтам все очень дешево!"
	}

	profileText := header + accessBlock

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("💰 оплата", "nav_topup"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✏️ e-mail", "edit_email"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ меню", "nav_menu"),
		),
	)

	_ = updateSessionText(bot, chatID, session, stateStatus, profileText, "HTML", kb)
}
func buildStatusText(cfg *xraySettings, userID int) (string, error) {
	telegramUser := fmt.Sprint(userID)
	// Не создаём запись и клиента при простом просмотре статуса
	info, _ := ensureXrayAccess(cfg, telegramUser, fallbackEmail(telegramUser), 0, false)
	days, _ := userStore.GetDays(strconv.Itoa(userID))
	if info != nil && info.daysLeft > 0 {
		days = info.daysLeft
	}
	statusEmoji := "🔴"
	statusText := "Не активна"
	if days > 0 {
		statusEmoji = "🟢"
		statusText = "Активна"
	}
	exp := "-"
	if info != nil && !info.expireAt.IsZero() {
		exp = formatExpiryUTC(info.expireAt)
	}
	// Always show subscription URL instead of raw VLESS
	subURL := ""
	if info != nil && info.client != nil {
		subURL = generateSubscriptionURL(cfg, info.client)
	}
	linkLine := ""
	if strings.TrimSpace(subURL) != "" {
		linkLine = fmt.Sprintf("\n\n<b>🔗 подписка</b>\n<code>%s</code>", subURL)
	}
	return fmt.Sprintf("💳 <b>подписка</b>\n<b>├ %s статус:</b> %s\n<b>├ ⏱ остаток:</b> %d дн.\n<b>└ 📅 действует до:</b> %s%s", statusEmoji, statusText, days, exp, linkLine), nil
}

func handleEditEmail(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "✏️ отправь новый e-mail сообщением."
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_status"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateEditEmail, text, "HTML", kb)
	ackCallback(bot, cq, "жду e-mail")
}

func handleInstructionsMenu(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	instruct.ResetState(chatID)
	text := "📖 инструкции\nвыбери платформу:"
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🖥️ Windows", "windows"),
			tgbotapi.NewInlineKeyboardButtonData("📱 Android", "android"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🍏 iOS", "ios"),
			tgbotapi.NewInlineKeyboardButtonData("💻 MacOS", "macos"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateInstructions, text, "HTML", kb)
}
func startInstructionFlow(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, xrCfg *xraySettings, platform instruct.InstructType, step int) error {
	prevMessageID := session.MessageID
	instruct.ResetState(chatID)

	var (
		msgID int
		err   error
	)

	userIDStr := strconv.FormatInt(chatID, 10)
	key := ""
	if xrCfg != nil {
		if info, _ := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), 0, true); info != nil {
			link := generateSubscriptionURL(xrCfg, info.client)
			if strings.TrimSpace(link) == "" {
				link = info.link
			}
			key = link
		}
	}
	instruct.SetInstructionKey(chatID, key)

	switch platform {
	case instruct.Windows:
		msgID, err = instruct.InstructionWindows(chatID, bot, step)
	case instruct.Android:
		msgID, err = instruct.InstructionAndroid(chatID, bot, step)
	case instruct.IOS:
		msgID, err = instruct.InstructionIos(chatID, bot, step)
	case instruct.MacOS:
		msgID, err = instruct.InstructionMacOS(chatID, bot, step)
	default:
		return fmt.Errorf("unsupported instruction platform: %v", platform)
	}

	if err != nil {
		return err
	}

	if prevMessageID != 0 {
		_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, prevMessageID))
	}
	session.MessageID = msgID
	session.State = stateInstructions
	session.ContentType = "photo"
	return nil
}

func handleSuccessfulPayment(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings, plan RatePlan, session *UserSession) error {
	chatID := msg.Chat.ID
	userID := int64(msg.From.ID)
	telegramUser := fmt.Sprint(userID)

	waitingText := fmt.Sprintf("готовлю доступ по тарифу %s...", plan.Title)
	_ = updateSessionText(bot, chatID, session, stateTopUp, waitingText, "HTML", mainMenuInlineKeyboard())

	if err := issuePlanAccess(bot, chatID, session, plan, xrCfg, telegramUser, userID); err != nil {
		return err
	}

	session.PendingPlanID = ""

	// Уведомление пригласившему, если он есть, и начисление уже выполнено в handleStart
	// Здесь отправим информативное сообщение о покупке пригласившему (если переход был по реферальной ссылке)
	// Определить пригласившего напрямую здесь сложно без хранения связи; пропустим если неизвестно

	// Уведомление в лог-чат о покупке тарифа
	adminText := fmt.Sprintf("💳 <b>покупка тарифа</b>\n👤 пользователь: <code>%d</code>\n📦 тариф: <b>%s</b> (%d дн.)", userID, plan.Title, plan.Days)
	m := tgbotapi.NewMessage(logChatID, adminText)
	m.ParseMode = "HTML"
	m.DisableWebPagePreview = true
	_, _ = bot.Send(m)

	// Оставляем существующее короткое уведомление
	sendMessageToAdmin(fmt.Sprintf("платёж от %d за %s", msg.From.ID, plan.Title), msg.From.UserName, bot, userID)
	return nil
}

// resolvePlanFromMetadata reads plan fields from YooKassa metadata or pending session plan.
func resolvePlanFromMetadata(meta map[string]interface{}, session *UserSession) RatePlan {
	if meta == nil {
		if p, ok := ratePlanByID[session.PendingPlanID]; ok {
			return p
		}
		return RatePlan{}
	}

	plan := RatePlan{}
	if v, ok := meta["plan_id"]; ok {
		plan.ID = fmt.Sprint(v)
	}
	if v, ok := meta["plan_title"]; ok {
		plan.Title = fmt.Sprint(v)
	}
	if v, ok := meta["plan_days"]; ok {
		switch val := v.(type) {
		case float64:
			plan.Days = int(val)
		case string:
			if n, err := strconv.Atoi(val); err == nil {
				plan.Days = n
			}
		}
	}
	if v, ok := meta["plan_amount"]; ok {
		switch val := v.(type) {
		case float64:
			plan.Amount = val
		case string:
			if n, err := strconv.ParseFloat(val, 64); err == nil {
				plan.Amount = n
			}
		}
	}

	if plan.ID != "" {
		if p, ok := ratePlanByID[plan.ID]; ok {
			if plan.Title == "" {
				plan.Title = p.Title
			}
			if plan.Days == 0 {
				plan.Days = p.Days
			}
			if plan.Amount == 0 {
				plan.Amount = p.Amount
			}
		}
	}
	return plan
}

func sendMessageToAdmin(text string, username string, bot *tgbotapi.BotAPI, id int64) {
	if id == 623290294 || id == 6365653009 {
		return
	}
	var userLink string
	if username != "" {
		userLink = fmt.Sprintf("<a href=\"https://t.me/%s\">@%s</a>", html.EscapeString(username), html.EscapeString(username))
	} else {
		userLink = fmt.Sprintf("<a href=\"tg://user?id=%d\">профиль пользователя</a>", id)
	}
	newText := fmt.Sprintf("%s:\n%s", userLink, html.EscapeString(text))
	msg := tgbotapi.NewMessage(logChatID, newText)
	msg.ParseMode = "HTML"
	msg.DisableWebPagePreview = true
	_, _ = bot.Send(msg)
}

func getActionName(data string) string {
	actionMap := map[string]string{
		"nav_menu":         "🏠 меню",
		"nav_get_vpn":      "🔌 подключить VPN",
		"nav_topup":        "💰 покупка доступа",
		"nav_status":       "👤 профиль",
		"nav_referral":     "🎁 +15 дней",
		"nav_support":      "📞 поддержка",
		"nav_instructions": "🛠 инструкции",
		"edit_email":       "✍️ e-mail",
		"windows":          "инструкция Windows",
		"android":          "инструкция Android",
		"ios":              "инструкция iOS",
		"macos":            "инструкция MacOS",
		"copy_key":         "копировать ключ",
		"check_payment":    "проверить оплату",
		"claim_sub_bonus":  "получить бонус за подписку",
	}

	if strings.HasPrefix(data, "rate_") {
		return "выбор тарифа"
	}
	if strings.HasPrefix(data, "win_prev_") || strings.HasPrefix(data, "android_prev_") || strings.HasPrefix(data, "ios_prev_") || strings.HasPrefix(data, "macos_prev_") {
		return "инструкция: назад"
	}
	if strings.HasPrefix(data, "win_next_") || strings.HasPrefix(data, "android_next_") || strings.HasPrefix(data, "ios_next_") || strings.HasPrefix(data, "macos_next_") {
		return "инструкция: дальше"
	}

	if name, ok := actionMap[data]; ok {
		return name
	}

	return data
}

func notifyAdmins(bot *tgbotapi.BotAPI, userID int64, username, action string) {
	userLink := fmt.Sprintf(`<a href="tg://user?id=%d">ID:%d</a>`, userID, userID)
	if username != "" {
		userLink = fmt.Sprintf(`<a href="https://t.me/%s">@%s</a> (ID:%d)`, username, username, userID)
	}
	text := fmt.Sprintf("📊 действие: <b>%s</b>\nпользователь: %s", action, userLink)
	msg := tgbotapi.NewMessage(logChatID, text)
	msg.ParseMode = "HTML"
	msg.DisableWebPagePreview = true
	_, _ = bot.Send(msg)
}

// Simple referral stats screen
func handleReferral(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	userID := strconv.FormatInt(cq.From.ID, 10)
	link := fmt.Sprintf("https://t.me/%s?start=ref_%s", bot.Self.UserName, userID)
	count := userStore.GetReferralsCount(userID)
	bonus := count * referralBonusDays
	shareText := url.QueryEscape("подключай vpn, опробовав его бесплатно 7 дней!")
	shareText = strings.ReplaceAll(shareText, "+", "%20")
	shareURL := fmt.Sprintf("https://t.me/share/url?url=%s&text=%s", url.QueryEscape(link), shareText)

	text := fmt.Sprintf(
		"🎁 +15 дней к доступу\n\n"+
			"кстати, у нас есть реферальная программа.\nприводишь друга → получаешь +15 дней доступа.\n\n"+
			"🔗 твоя ссылка\n<code>%s</code>\n\n"+
			"пришло друзей: %d\nнакопленный бонус: %d дней.",
		link, count, bonus,
	)
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("поделиться ссылкой", shareURL),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
}
func handleSupport(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "📞 поддержка\n\nесть вопросы или предложения? пиши: @asortiment97\nответим лично, никаких почтовых ящиков."
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
	ackCallback(bot, cq, "поддержка")
}
