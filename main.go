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
	rawkbd "github.com/Asort97/vpnBot/clients/rawkbd"
	sqlite "github.com/Asort97/vpnBot/clients/sqLite"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	qrcode "github.com/skip2/go-qrcode"
)

const (
	startTrialDays    = 7
	channelBonusDays  = 7
	referralBonusDays = 15
	channelUsername   = "@neuravpn"
	channelURL        = "https://t.me/neuravpn"
)
const startText = `<tg-emoji emoji-id="5346299917679757635">👋</tg-emoji> добро пожаловать!

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
	adStats            = newAdStatsStore(resolveAdStatsPath())
	logSessionMu       sync.Mutex
	logSessions        = make(map[int64]*logSession) // key: userID
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func resolveAdStatsPath() string {
	if p := strings.TrimSpace(os.Getenv("AD_STATS_PATH")); p != "" {
		return p
	}
	exe, err := os.Executable()
	if err != nil {
		return filepath.Join("database", "ad_stats.json")
	}
	base := filepath.Dir(exe)
	return filepath.Join(base, "database", "ad_stats.json")
}

// throttling map (keyed by user id and action key)
var lastActionKey = make(map[int64]map[string]time.Time)

// in-memory cache for accessInfo (key: telegramUserID)
var accessCache sync.Map // map[string]*accessInfo

func sessionAction(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, action string, isNewUser bool) {
	now := time.Now()
	if session.SessionID == 0 {
		session.SessionID = 1
		session.SessionStart = now
		session.LastActionAt = now
		session.LastAction = action
		session.Actions = []string{action}
		logAction(bot, chatID, "", action, isNewUser)
		return
	}

	if now.Sub(session.LastActionAt) > 10*time.Minute {
		// start new session
		session.SessionID++
		session.SessionStart = now
		session.Actions = nil
	}
	session.LastAction = action
	session.LastActionAt = now
	session.Actions = append(session.Actions, action)
	logAction(bot, chatID, "", action, isNewUser)
}

func flushSessionLog(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, wasNew bool) {
	if session.SessionStart.IsZero() || session.LastActionAt.IsZero() {
		return
	}
	dur := session.LastActionAt.Sub(session.SessionStart).Round(time.Minute)
	if dur < time.Minute {
		dur = time.Minute
	}
	_ = dur // legacy; no-op
}

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
	ConfirmReferralAndRewardReferrer(newUserID string, rewardDays int64, at time.Time) (string, bool, error)
	GetReferralsCount(userID string) int
	IsPaymentApplied(userID, paymentID string) (bool, error)
	MarkPaymentApplied(userID, paymentID, provider, planID string, at time.Time) (bool, error)
	SetLinkToken(userID, token string) error
	GetUserByLinkToken(token string) (string, error)
	ClearLinkToken(userID string) error
	SetLinkedTo(userID, linkedTo string) error
	GetLinkedTo(userID string) (string, error)
	GetLinkedVKUsers(tgUserID string) ([]string, error)
	SetAutopay(userID, methodID, planID string) error
	DisableAutopay(userID string) error
	ClearAutopay(userID string) error
	GetAutopay(userID string) (methodID, planID string, enabled bool, err error)
	GetUsersWithAutopay() ([]AutopayUser, error)
}

type AutopayUser = struct {
	UserID   string
	MethodID string
	PlanID   string
}

var ratePlans = []RatePlan{
	{ID: "30d", Title: "30 дней", Amount: 99, Days: 30},
	{ID: "60d", Title: "60 дней", Amount: 169, Days: 60},
	{ID: "90d", Title: "90 дней", Amount: 249, Days: 90},
	{ID: "365d", Title: "365 дней", Amount: 949, Days: 365},
}

var ratePlanByID = func() map[string]RatePlan {
	m := make(map[string]RatePlan)
	for _, p := range ratePlans {
		m[p.ID] = p
	}
	return m
}()

type UserSession struct {
	MessageID            int
	State                SessionState
	ContentType          string
	PendingPlanID        string
	PendingSaveCard      bool
	LastAccess           string
	LastLink             string
	CertFileName         string
	CertFileBytes        []byte
	LastAction           string
	LastActionAt         time.Time
	SessionID            int
	SessionStart         time.Time
	Actions              []string
	AutopayProposalMsgID int
}

type logSession struct {
	MsgID   int
	Start   time.Time
	Last    time.Time
	Actions []string
	IsNew   bool
	Sending bool
	Dirty   bool
}

func minutesLabel(n int) string {
	if n <= 1 {
		return "1 мин"
	}
	return fmt.Sprintf("%d мин", n)
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
	testMode       bool
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
	// В тестовом режиме возвращаем заглушку
	if testMode {
		fakeExpiry := time.Now().Add(30 * 24 * time.Hour)
		fakeClient := &xray.Client{
			ID:         "test-uuid-" + telegramUser,
			Email:      email,
			Enable:     true,
			ExpiryTime: fakeExpiry.UnixMilli(),
			SubID:      "test-sub-" + telegramUser,
			TgID:       telegramUser,
		}
		return &accessInfo{
			client:   fakeClient,
			expireAt: fakeExpiry,
			daysLeft: 30,
			link:     "vless://test-key-for-" + telegramUser + "@example.com:443",
		}, nil
	}

	if cfg == nil || cfg.client == nil {
		return nil, fmt.Errorf("xray not configured")
	}

	// Кеш: при addDays==0 отдаём из памяти если есть
	if addDays == 0 {
		if cached, ok := accessCache.Load(telegramUser); ok {
			return cached.(*accessInfo), nil
		}
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
		accessCache.Store(telegramUser, info)
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

	result := &accessInfo{
		client:   primaryClient,
		expireAt: expireAt,
		daysLeft: daysLeft,
		link:     link,
	}
	accessCache.Store(telegramUser, result)
	return result, nil
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
						text = fmt.Sprintf("⏰ ваш доступ к neuravpn закончился.\nдействовал до: %s\nпродлите в разделе «оплата» чтобы пользоваться VPN без ограничений.", expStr)
					} else {
						text = fmt.Sprintf("⏰ ваш доступ к neuravpn заканчивается через %d дн.\nдействует до: %s\nпродлите в разделе «оплата» чтобы пользоваться VPN без ограничений.", daysLeft, expStr)
					}

					msg := tgbotapi.NewMessage(userID, text)
					_, _ = bot.Send(msg)
				}
			}()

			<-ticker.C
		}
	}()
}

// ────────────────────────────────────────────────────────────
// Autopay loop — автоматическое списание за 1 день до окончания
// ────────────────────────────────────────────────────────────

var (
	autopayNotifiedMu sync.Mutex
	autopayNotified   = make(map[string]string) // userID → expiry string (чтобы не слать повторно)

	// TEST_MODE: запоминаем время включения автоплатежа для каждого юзера
	autopayTestTimeMu sync.Mutex
	autopayTestTime   = make(map[string]time.Time) // userID → время включения
)

func startAutopayLoop(bot *tgbotapi.BotAPI, cfg *xraySettings) {
	go func() {
		interval := 1 * time.Hour
		if testMode {
			interval = 1 * time.Minute
			log.Println("🧪 autopay loop: test mode, interval=1m, charge threshold=5m")
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			processAutopayTick(bot, cfg)
			<-ticker.C
		}
	}()
}

func processAutopayTick(bot *tgbotapi.BotAPI, cfg *xraySettings) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("autopay panic recovered: %v", r)
		}
	}()

	users, err := userStore.GetUsersWithAutopay()
	if err != nil {
		log.Printf("autopay GetUsersWithAutopay: %v", err)
		return
	}

	now := time.Now().UTC()

	for _, u := range users {
		plan, ok := ratePlanByID[u.PlanID]
		if !ok {
			log.Printf("autopay unknown plan %q for user %s", u.PlanID, u.UserID)
			continue
		}

		info, err := ensureXrayAccess(cfg, u.UserID, fallbackEmail(u.UserID), 0, false)
		if err != nil || info == nil {
			continue
		}

		remain := info.expireAt.Sub(now)
		daysLeft := remain.Hours() / 24

		if testMode {
			// В тест-режиме: списание через 5 минут после включения автоплатежа
			autopayTestTimeMu.Lock()
			enabledAt, exists := autopayTestTime[u.UserID]
			if !exists {
				autopayTestTime[u.UserID] = now
				enabledAt = now
			}
			autopayTestTimeMu.Unlock()
			if now.Before(enabledAt.Add(5 * time.Minute)) {
				left := enabledAt.Add(5 * time.Minute).Sub(now).Round(time.Second)
				log.Printf("autopay test: user=%s ждём ещё %s до списания", u.UserID, left)
				continue
			}
			log.Printf("autopay test: user=%s 5 мин прошло → списание", u.UserID)
		} else {
			// Уведомление за 3 дня
			if daysLeft > 1 && daysLeft <= 3 {
				autopayNotifyUpcoming(bot, u, plan, info.daysLeft, info.expireAt)
				continue
			}

			// Списание: осталось ≤ 1 дня
			if daysLeft > 1 {
				continue
			}
		}

		chatID, parseErr := strconv.ParseInt(u.UserID, 10, 64)
		if parseErr != nil {
			continue
		}

		email := fallbackEmail(u.UserID)
		meta := map[string]interface{}{
			"chat_id":   chatID,
			"plan_id":   plan.ID,
			"plan_days": plan.Days,
			"autopay":   true,
		}

		payment, err := yookassaClient.CreateAutoPayment(u.MethodID, plan.Amount, "Автопродление "+plan.Title, email, meta)
		if err != nil {
			log.Printf("autopay charge failed user=%s plan=%s: %v", u.UserID, plan.ID, err)
			_ = userStore.DisableAutopay(u.UserID)

			failText := fmt.Sprintf("⚠️ не удалось автоматически продлить подписку (%s, %.0f ₽).\nавтопродление отключено. продлите вручную в разделе «оплата».", plan.Title, plan.Amount)
			failMsg := tgbotapi.NewMessage(chatID, failText)
			_, _ = bot.Send(failMsg)

			alertText := fmt.Sprintf("⚠️ autopay failed user=%s plan=%s: %v", u.UserID, plan.ID, err)
			logMsg := tgbotapi.NewMessage(logChatID, alertText)
			_, _ = bot.Send(logMsg)
			continue
		}

		if payment.Status != "succeeded" && !payment.Paid {
			log.Printf("autopay payment not succeeded user=%s status=%s", u.UserID, payment.Status)
			_ = userStore.DisableAutopay(u.UserID)

			failText := fmt.Sprintf("⚠️ автопродление не прошло (статус: %s).\nавтопродление отключено. продлите вручную в разделе «оплата».", payment.Status)
			failMsg := tgbotapi.NewMessage(chatID, failText)
			_, _ = bot.Send(failMsg)
			continue
		}

		paymentKey := buildAppliedPaymentKey("yookassa_auto", payment.ID)
		_, _ = userStore.MarkPaymentApplied(u.UserID, paymentKey, "yookassa_auto", plan.ID, time.Now())

		// Начислить дни
		session := getSession(chatID)
		if err := issuePlanAccess(bot, chatID, session, plan, cfg, u.UserID, chatID); err != nil {
			log.Printf("autopay issuePlanAccess failed user=%s: %v", u.UserID, err)
			alertText := fmt.Sprintf("⚠️ autopay charged but access failed user=%s plan=%s: %v", u.UserID, plan.ID, err)
			logMsg := tgbotapi.NewMessage(logChatID, alertText)
			_, _ = bot.Send(logMsg)
			continue
		}

		successText := fmt.Sprintf("<tg-emoji emoji-id=\"5344015205531686528\">💰</tg-emoji> подписка автоматически продлена на %d дней (%s, %.0f ₽).", plan.Days, plan.Title, plan.Amount)
		successMsg := tgbotapi.NewMessage(chatID, successText)
		successMsg.ParseMode = "HTML"
		_, _ = bot.Send(successMsg)

		logText := fmt.Sprintf("💰 autopay user=%s plan=%s amount=%.0f ₽", u.UserID, plan.Title, plan.Amount)
		logMsg := tgbotapi.NewMessage(logChatID, logText)
		_, _ = bot.Send(logMsg)

		// Сбросить кеш уведомлений для этого юзера
		autopayNotifiedMu.Lock()
		delete(autopayNotified, u.UserID)
		autopayNotifiedMu.Unlock()

		// TEST_MODE: сбросить таймер чтобы следующее списание тоже ждало 5 мин
		if testMode {
			autopayTestTimeMu.Lock()
			autopayTestTime[u.UserID] = time.Now().UTC()
			autopayTestTimeMu.Unlock()
		}
	}
}

func autopayNotifyUpcoming(bot *tgbotapi.BotAPI, u AutopayUser, plan RatePlan, daysLeft int64, expiry time.Time) {
	expKey := expiry.Format("2006-01-02")

	autopayNotifiedMu.Lock()
	prev := autopayNotified[u.UserID]
	autopayNotifiedMu.Unlock()

	if prev == expKey {
		return // уже уведомляли для этого expiry
	}

	chatID, err := strconv.ParseInt(u.UserID, 10, 64)
	if err != nil {
		return
	}

	text := fmt.Sprintf(
		"ℹ️ через %d дн. мы автоматически продлим вашу подписку за <b>%.0f ₽</b> (тариф <b>%s</b>).\nесли хотите отменить — нажмите кнопку ниже.",
		daysLeft, plan.Amount, plan.Title,
	)
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("❌ отключить автопродление", "disable_autopay"),
		),
	)
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "HTML"
	msg.ReplyMarkup = kb
	_, _ = bot.Send(msg)

	autopayNotifiedMu.Lock()
	autopayNotified[u.UserID] = expKey
	autopayNotifiedMu.Unlock()
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
		keyLine = fmt.Sprintf("<code>%s</code>", html.EscapeString(subURL))
	} else if strings.TrimSpace(info.link) != "" {
		keyLine = fmt.Sprintf("<code>%s</code>", html.EscapeString(info.link))
	}

	text := fmt.Sprintf(`<tg-emoji emoji-id="5346325906526868503">🔌</tg-emoji> подключить neuravpn

<b>ваш ключ:</b>
%s
<tg-emoji emoji-id="5264948349420739524">✏️</tg-emoji> нажмите чтобы скопировать

перейдите в раздел «инструкции» — мы подробно объясним, что и куда нужно вставить.

оставшееся время / действует до:
%s
`, keyLine, combined)
	session.LastAccess = text
	session.LastLink = info.link
	kbRaw := rawInlineKeyboardMarkup{
		InlineKeyboard: [][]rawInlineKeyboardButton{
			{rawCallbackButton("инструкции", "nav_instructions", "", "5264991913274019640")},
			{rawCallbackButton("QR-код", "nav_qrcode", "", "")},
			{
				rawCallbackButton("профиль", "nav_status", "", "5343693752999383705"),
				rawCallbackButton("меню", "nav_menu", "", "5264852846527941278"),
			},
		},
	}
	if err := updateSessionTextRaw(bot, chatID, session, stateMenu, text, "HTML", kbRaw); err == nil {
		return nil
	}

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

type rawInlineKeyboardButton = rawkbd.Button

type rawInlineKeyboardMarkup = rawkbd.Markup

func rawCallbackButton(text, callbackData, style, iconCustomEmojiID string) rawInlineKeyboardButton {
	return rawkbd.CallbackButton(text, callbackData, style, iconCustomEmojiID)
}

func rawURLButton(text, url, iconCustomEmojiID string) rawInlineKeyboardButton {
	return rawkbd.URLButton(text, url, iconCustomEmojiID)
}

func sendMessageRaw(bot *tgbotapi.BotAPI, chatID int64, text string, parseMode string, replyMarkup interface{}) (int, error) {
	params := tgbotapi.Params{}
	params.AddNonZero64("chat_id", chatID)
	params["text"] = text
	if parseMode != "" {
		params["parse_mode"] = parseMode
	}
	params.AddBool("disable_web_page_preview", true)
	if replyMarkup != nil {
		if err := params.AddInterface("reply_markup", replyMarkup); err != nil {
			return 0, err
		}
	}

	resp, err := bot.MakeRequest("sendMessage", params)
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fmt.Errorf("telegram sendMessage error %d: %s", resp.ErrorCode, resp.Description)
	}

	var sent struct {
		MessageID int `json:"message_id"`
	}
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		return 0, err
	}
	if sent.MessageID == 0 {
		return 0, errors.New("telegram sendMessage returned empty message_id")
	}

	return sent.MessageID, nil
}

func editMessageMediaRaw(bot *tgbotapi.BotAPI, chatID int64, messageID int, photoBytes []byte, fileName string, caption string, parseMode string, replyMarkup interface{}) error {
	mediaObj := map[string]string{
		"type":  "photo",
		"media": "attach://file-0",
	}
	if caption != "" {
		mediaObj["caption"] = caption
	}
	if parseMode != "" {
		mediaObj["parse_mode"] = parseMode
	}
	mediaJSON, err := json.Marshal(mediaObj)
	if err != nil {
		return err
	}

	params := tgbotapi.Params{}
	params.AddNonZero64("chat_id", chatID)
	params.AddNonZero("message_id", messageID)
	params["media"] = string(mediaJSON)
	if replyMarkup != nil {
		if err := params.AddInterface("reply_markup", replyMarkup); err != nil {
			return err
		}
	}

	files := []tgbotapi.RequestFile{
		{
			Name: "file-0",
			Data: tgbotapi.FileBytes{Name: fileName, Bytes: photoBytes},
		},
	}

	resp, err := bot.UploadFiles("editMessageMedia", params, files)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("telegram editMessageMedia error %d: %s", resp.ErrorCode, resp.Description)
	}
	return nil
}

func editMessageTextRaw(bot *tgbotapi.BotAPI, chatID int64, messageID int, text string, parseMode string, replyMarkup interface{}) error {
	params := tgbotapi.Params{}
	params.AddNonZero64("chat_id", chatID)
	params.AddNonZero("message_id", messageID)
	params["text"] = text
	if parseMode != "" {
		params["parse_mode"] = parseMode
	}
	params.AddBool("disable_web_page_preview", true)
	if replyMarkup != nil {
		if err := params.AddInterface("reply_markup", replyMarkup); err != nil {
			return err
		}
	}

	resp, err := bot.MakeRequest("editMessageText", params)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("telegram editMessageText error %d: %s", resp.ErrorCode, resp.Description)
	}

	return nil
}

func updateSessionTextRaw(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, state SessionState, text string, parseMode string, keyboard rawInlineKeyboardMarkup) error {
	if session.MessageID != 0 {
		err := editMessageTextRaw(bot, chatID, session.MessageID, text, parseMode, keyboard)
		if err == nil {
			instruct.ResetState(chatID)
			session.State = state
			session.ContentType = "text"
			return nil
		}
	}

	if session.MessageID != 0 {
		_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, session.MessageID))
	}

	instruct.ResetState(chatID)
	sentMessageID, err := sendMessageRaw(bot, chatID, text, parseMode, keyboard)
	if err != nil {
		return err
	}

	session.MessageID = sentMessageID
	session.State = state
	session.ContentType = "text"
	return nil
}

func mainMenuInlineKeyboardRaw() rawInlineKeyboardMarkup {
	return rawInlineKeyboardMarkup{
		InlineKeyboard: [][]rawInlineKeyboardButton{
			{
				rawCallbackButton("подключить VPN", "nav_get_vpn", "", "5346325906526868503"),
				rawCallbackButton("профиль/оплата", "nav_status", "", "5343693752999383705"),
			},
			{
				rawCallbackButton("+15 дней", "nav_referral", "", "5345823764720426390"),
				rawCallbackButton("поддержка", "nav_support", "", "5346123042336573193"),
			},
		},
	}
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
	if base == "" {
		return "Добро пожаловать! Используйте меню ниже, чтобы подключить VPN."
	}
	return base
}

func showMainMenu(bot *tgbotapi.BotAPI, chatID int64, session *UserSession) error {
	text := composeMenuText()
	if err := updateSessionTextRaw(bot, chatID, session, stateMenu, text, "HTML", mainMenuInlineKeyboardRaw()); err == nil {
		return nil
	}
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

func sendReferralSubscriptionPrompt(bot *tgbotapi.BotAPI, chatID int64) {
	text := fmt.Sprintf("чтобы пригласившему начислилось +%d дней, подпишись на наш канал и нажми «проверить».", referralBonusDays)
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

func formatUserLabel(username string, userID int64) string {
	if strings.TrimSpace(username) != "" {
		return "@" + username
	}
	return fmt.Sprintf("ID:%d", userID)
}

func finalizeReferralAfterSubscription(bot *tgbotapi.BotAPI, newUserID int64, newUsername string, xrCfg *xraySettings) (bool, error) {
	newUserIDStr := strconv.FormatInt(newUserID, 10)
	referrerID, granted, err := userStore.ConfirmReferralAndRewardReferrer(newUserIDStr, int64(referralBonusDays), time.Now())
	if err != nil {
		return false, err
	}
	if !granted || strings.TrimSpace(referrerID) == "" {
		return false, nil
	}

	_, _ = ensureXrayAccess(xrCfg, referrerID, fallbackEmail(referrerID), int64(referralBonusDays), true)

	newUserLabel := formatUserLabel(newUsername, newUserID)

	if refChatID, err := strconv.ParseInt(referrerID, 10, 64); err == nil {
		refDays, _ := userStore.GetDays(referrerID)
		refCount := userStore.GetReferralsCount(referrerID)
		refMsg := fmt.Sprintf("🎉 <b>%s подтвердил подписку по вашей реферальной ссылке!</b>\n\n🎁 <b>вам начислено: +%d дней</b>\n👥 <b>всего рефералов:</b> %d\n⏱ <b>баланс:</b> %d дн.", newUserLabel, referralBonusDays, refCount, refDays)
		nmsg := tgbotapi.NewMessage(refChatID, refMsg)
		nmsg.ParseMode = "HTML"
		_, _ = bot.Send(nmsg)
	}

	adminMsg := fmt.Sprintf("✅ <b>%s</b> (ID:%s) подписался по рефералке пользователя <code>%s</code> (ID:%s). Пригласившему начислено +%d дней", newUserLabel, newUserIDStr, referrerID, referrerID, referralBonusDays)
	amsg := tgbotapi.NewMessage(logChatID, adminMsg)
	amsg.ParseMode = "HTML"
	_, _ = bot.Send(amsg)

	return true, nil
}

func rateKeyboardRaw() rawInlineKeyboardMarkup {
	var rows [][]rawInlineKeyboardButton
	var row []rawInlineKeyboardButton
	for _, p := range ratePlans {
		label := fmt.Sprintf("%d дней", p.Days)
		row = append(row, rawCallbackButton(label, "rate_"+p.ID, "", ""))
		if len(row) == 2 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	rows = append(rows, []rawInlineKeyboardButton{
		rawCallbackButton("назад", "nav_status", "", "5264852846527941278"),
	})
	return rawInlineKeyboardMarkup{InlineKeyboard: rows}
}

const (
	starsCurrency      = "XTR"
	starsPayloadPrefix = "stars:"
)

func starsAmountForPlan(plan RatePlan) int {
	n := int(math.Round(plan.Amount * 0.9))
	if n < 1 {
		n = 1
	}
	return n
}

func choosePayKeyboardRaw(plan RatePlan) rawInlineKeyboardMarkup {
	stars := starsAmountForPlan(plan)
	return rawInlineKeyboardMarkup{InlineKeyboard: [][]rawInlineKeyboardButton{
		{rawCallbackButton(fmt.Sprintf("⭐ звёздами (%d ⭐)", stars), "pay_stars_"+plan.ID, "", "")},
		{rawCallbackButton(fmt.Sprintf("💳 картой с автопродлением (%.0f ₽)", plan.Amount), "pay_card_"+plan.ID, "", "")},
		{rawCallbackButton(fmt.Sprintf("💲 любым способом (%.0f ₽)", plan.Amount), "pay_any_"+plan.ID, "", "")},
		{
			rawCallbackButton("назад", "nav_topup", "", "5264852846527941278"),
			rawCallbackButton("меню", "nav_menu", "", "5346299917679757635"),
		},
	}}
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

	// Включаем тестовый режим по переменной окружения
	testMode = strings.ToLower(strings.TrimSpace(os.Getenv("TEST_MODE"))) == "true"
	if testMode {
		log.Println("🧪 TEST MODE ENABLED - using mock data")
	}

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
	if !testMode {
		if err := xClient.LoginToServer(); err != nil {
			log.Fatalf("login to xray failed: %v", err)
		}
	} else {
		log.Println("🧪 Skipping xray login in test mode")
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
	instruct.ValidateCustomEmojiIDs(bot)

	loadExpiryReminderState()
	startExpiryReminder(bot, xrayCfg)
	startAutopayLoop(bot, xrayCfg)

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
func handleAddDays(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	chatID := msg.Chat.ID

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

	// /add <userID> <days>
	args := strings.Fields(msg.CommandArguments())
	if len(args) != 2 {
		m := tgbotapi.NewMessage(chatID, "Использование: <code>/add userID days</code>\nПример: <code>/add 123456789 30</code>")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	targetUserID := strings.TrimSpace(args[0])
	if _, err := strconv.ParseInt(targetUserID, 10, 64); err != nil {
		m := tgbotapi.NewMessage(chatID, "❌ Неверный userID: "+targetUserID)
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	days, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || days <= 0 {
		m := tgbotapi.NewMessage(chatID, "❌ Количество дней должно быть положительным числом")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	info, err := ensureXrayAccess(xrCfg, targetUserID, fallbackEmail(targetUserID), days, true)
	if err != nil {
		m := tgbotapi.NewMessage(chatID, "❌ Ошибка: "+err.Error())
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	text := fmt.Sprintf("✅ Пользователю <code>%s</code> добавлено <b>%d</b> дн.\nОсталось дней: <b>%d</b>", targetUserID, days, info.daysLeft)
	m := tgbotapi.NewMessage(chatID, text)
	m.ParseMode = "HTML"
	_, _ = bot.Send(m)

	logAction(bot, msg.From.ID, msg.From.UserName, fmt.Sprintf("/add %s %d дн.", targetUserID, days), false)
}

func handleRemoveDays(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	chatID := msg.Chat.ID

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

	args := strings.Fields(msg.CommandArguments())
	if len(args) != 2 {
		m := tgbotapi.NewMessage(chatID, "Использование: <code>/remove userID days</code>\nПример: <code>/remove 123456789 7</code>")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	targetUserID := strings.TrimSpace(args[0])
	if _, err := strconv.ParseInt(targetUserID, 10, 64); err != nil {
		m := tgbotapi.NewMessage(chatID, "❌ Неверный userID: "+targetUserID)
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	days, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || days <= 0 {
		m := tgbotapi.NewMessage(chatID, "❌ Количество дней должно быть положительным числом")
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	info, err := ensureXrayAccess(xrCfg, targetUserID, fallbackEmail(targetUserID), -days, true)
	if err != nil {
		m := tgbotapi.NewMessage(chatID, "❌ Ошибка: "+err.Error())
		m.ParseMode = "HTML"
		_, _ = bot.Send(m)
		return
	}

	text := fmt.Sprintf("✅ У пользователя <code>%s</code> убрано <b>%d</b> дн.\nОсталось дней: <b>%d</b>", targetUserID, days, info.daysLeft)
	m := tgbotapi.NewMessage(chatID, text)
	m.ParseMode = "HTML"
	_, _ = bot.Send(m)

	logAction(bot, msg.From.ID, msg.From.UserName, fmt.Sprintf("/remove %s %d дн.", targetUserID, days), false)
}

func handleSyncInbounds(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	handleSyncInboundsInternal(bot, msg, xrCfg, false)
}

func handleSyncActiveInbounds(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	handleSyncInboundsInternal(bot, msg, xrCfg, true)
}

func handleSyncInboundsInternal(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings, activeOnly bool) {
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
	skippedInactive := 0
	for _, uid := range userIDs {
		if activeOnly {
			days, err := userStore.GetDays(uid)
			if err != nil || days <= 0 {
				skippedInactive++
				continue
			}
		}

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
	if activeOnly {
		text += fmt.Sprintf(", пропущено неактивных: %d", skippedInactive)
	}
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

	// Логируем slash-команды так же, как действия с кнопок.
	// /start логируется отдельно внутри handleStart с более детальным контекстом.
	if msg.IsCommand() {
		if action := getCommandActionName(msg.Command()); action != "" {
			logAction(bot, msg.From.ID, msg.From.UserName, action, false)
		}
	}

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

	// /notify_sleep — рассылка пользователям с 0 дней больше недели
	if msg.IsCommand() && msg.Command() == "notify_sleep" {
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

		go func() {
			var sleepIDs []string
			threshold := time.Now().Add(-7 * 24 * time.Hour)

			if pg, ok := userStore.(interface {
				GetSleepingUsers(since time.Time) ([]string, error)
			}); ok {
				var err error
				sleepIDs, err = pg.GetSleepingUsers(threshold)
				if err != nil {
					m := tgbotapi.NewMessage(chatID, "Ошибка: "+err.Error())
					_, _ = bot.Send(m)
					return
				}
			} else if sq, ok := userStore.(interface {
				GetAllUsers() map[string]sqlite.UserData
			}); ok {
				for id, ud := range sq.GetAllUsers() {
					if ud.Days > 0 {
						continue
					}
					if ud.LastDeduct == "" {
						continue
					}
					t, err := time.Parse(time.RFC3339, ud.LastDeduct)
					if err != nil {
						continue
					}
					if t.Before(threshold) {
						sleepIDs = append(sleepIDs, id)
					}
				}
			} else {
				m := tgbotapi.NewMessage(chatID, "userStore не поддерживает эту операцию")
				_, _ = bot.Send(m)
				return
			}

			if len(sleepIDs) == 0 {
				m := tgbotapi.NewMessage(chatID, "Нет спящих пользователей (0 дней > 7 дней)")
				_, _ = bot.Send(m)
				return
			}

			text := "давно не пользовались VPN\n\nесли всё ещё актуально можете вернуться в любой момент"
			kb := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("оплатить", "nav_topup"),
				),
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("+15 дней", "nav_referral"),
				),
			)

			count := 0
			for _, uid := range sleepIDs {
				id, err := strconv.ParseInt(uid, 10, 64)
				if err != nil {
					continue
				}
				m := tgbotapi.NewMessage(id, text)
				m.ReplyMarkup = kb
				if _, err := bot.Send(m); err == nil {
					count++
				}
				time.Sleep(30 * time.Millisecond)
			}
			result := tgbotapi.NewMessage(chatID, fmt.Sprintf("Рассылка спящим завершена. Доставлено: %d из %d", count, len(sleepIDs)))
			_, _ = bot.Send(result)
		}()
		_ = updateSessionText(bot, chatID, session, stateMenu, "Рассылка спящим запущена...", "HTML", mainMenuInlineKeyboard())
		return
	}

	// /notify_sleep_test — превью сообщения notify_sleep самому себе
	if msg.IsCommand() && msg.Command() == "notify_sleep_test" {
		text := "давно не пользовались VPN\n\nесли всё ещё актуально можете вернуться в любой момент"
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("оплатить", "nav_topup"),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("+15 дней", "nav_referral"),
			),
		)
		m := tgbotapi.NewMessage(chatID, text)
		m.ReplyMarkup = kb
		_, _ = bot.Send(m)
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

		userIDStr := strconv.FormatInt(msg.From.ID, 10)
		paymentID := resolveStarsPaymentID(msg.SuccessfulPayment)
		if paymentID == "" {
			paymentID = strings.TrimSpace(msg.SuccessfulPayment.InvoicePayload)
		}
		if paymentID == "" {
			paymentID = fmt.Sprintf("msg:%d", msg.MessageID)
		}
		paymentKey := buildAppliedPaymentKey("stars", paymentID)
		alreadyApplied, err := userStore.IsPaymentApplied(userIDStr, paymentKey)
		if err != nil {
			log.Printf("stars IsPaymentApplied error: %v", err)
			sendPaymentAlert(bot, "payment apply check failed", msg.From.ID, msg.From.UserName, paymentKey, plan.ID, err.Error())
			return
		}
		if alreadyApplied {
			if canProceedKey(msg.From.ID, "pay_skip_"+paymentKey, 5*time.Minute) {
				sendPaymentAlert(bot, "payment apply skipped (already applied)", msg.From.ID, msg.From.UserName, paymentKey, plan.ID, "duplicate successful payment update")
			}
			return
		}

		if err := handleSuccessfulPayment(bot, msg, xrCfg, plan, session); err != nil {
			log.Printf("handleSuccessfulPayment error: %v", err)
			sendPaymentAlert(bot, "payment succeeded but access failed", msg.From.ID, msg.From.UserName, paymentKey, plan.ID, err.Error())
			return
		}

		marked, err := userStore.MarkPaymentApplied(userIDStr, paymentKey, "stars", plan.ID, time.Now())
		if err != nil {
			log.Printf("stars MarkPaymentApplied error: %v", err)
			sendPaymentAlert(bot, "access issued but mark applied failed", msg.From.ID, msg.From.UserName, paymentKey, plan.ID, err.Error())
			return
		}
		if !marked && canProceedKey(msg.From.ID, "pay_skip_mark_"+paymentKey, 5*time.Minute) {
			sendPaymentAlert(bot, "payment apply skipped (already applied)", msg.From.ID, msg.From.UserName, paymentKey, plan.ID, "mark returned duplicate")
		}
		return
	}

	if msg.IsCommand() {
		// For command-driven navigation we always send a new message,
		// while callback-button navigation keeps edit-in-place behavior.
		session.MessageID = 0
		switch msg.Command() {
		case "start":
			handleStart(bot, msg, session, xrCfg)
		case "adlink":
			handleAdLink(bot, msg)
		case "adcheck":
			handleAdCheck(bot, msg)
		case "add":
			handleAddDays(bot, msg, xrCfg)
		case "remove":
			handleRemoveDays(bot, msg, xrCfg)
		case "sync_inbounds":
			handleSyncInbounds(bot, msg, xrCfg)
		case "sync_active_inbounds":
			handleSyncActiveInbounds(bot, msg, xrCfg)
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
			_ = updateSessionTextRaw(bot, chatID, session, stateTopUp, "Тариф не найден, выбери заново.", "HTML", rateKeyboardRaw())
			return
		}
		if err := startPaymentForPlan(bot, chatID, session, plan, session.PendingSaveCard); err != nil {
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
	isNew := userStore.IsNewUser(userID)
	args := strings.TrimSpace(msg.CommandArguments())
	args = strings.TrimPrefix(args, "=")
	if fields := strings.Fields(args); len(fields) > 0 {
		args = fields[0]
	} else {
		args = ""
	}
	referrerID := ""
	if args != "" && strings.HasPrefix(args, "ref_") {
		referrerID = strings.TrimPrefix(args, "ref_")
		if refChatID, err := strconv.ParseInt(referrerID, 10, 64); err == nil && refChatID > 0 {
			referrerID = strconv.FormatInt(refChatID, 10)
		} else {
			referrerID = ""
		}
	}

	startAction := "start"
	if isNew {
		startAction = "новый пользователь"
		if referrerID != "" && referrerID != userID {
			startAction = "новый пользователь по рефералке"
		}
	}
	logAction(bot, msg.From.ID, msg.From.UserName, startAction, isNew)

	if adTag := extractAdTag(msg); adTag != "" {
		adStats.record(adTag, userID)
	}

	if referrerID != "" && referrerID != userID {
		if err := userStore.RecordReferral(userID, referrerID); err == nil {
			if ok, _ := userStore.ClaimStartBonus(userID, "referral", time.Now()); ok {
				_ = userStore.AddDays(userID, 7)
				_, _ = ensureXrayAccess(xrayCfg, userID, fallbackEmail(userID), 7, true)
			}

			subscribed, subErr := isSubscribedToChannel(bot, msg.From.ID)
			if subErr != nil {
				log.Printf("subscription check on start failed: %v", subErr)
				sendReferralSubscriptionPrompt(bot, chatID)
			} else if subscribed {
				if _, err := finalizeReferralAfterSubscription(bot, msg.From.ID, msg.From.UserName, xrayCfg); err != nil {
					log.Printf("finalize referral on start failed: %v", err)
				}
			} else {
				sendReferralSubscriptionPrompt(bot, chatID)
			}
		} else {
			log.Printf("referral record failed: user=%s ref=%s err=%v", userID, referrerID, err)
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
		m := tgbotapi.NewMessage(chatID, "использование: /adlink <канал/@канал/https://t.me/...> [ид_поста]\nпример: /adlink @mychannel 123 или /adlink https://t.me/mychannel/45")
		_, _ = bot.Send(m)
		return
	}
	channel, postID := parseAdInput(args[0])
	if channel == "" {
		m := tgbotapi.NewMessage(chatID, "укажи канал, например @mychannel или ссылку https://t.me/mychannel")
		_, _ = bot.Send(m)
		return
	}
	if len(args) > 1 {
		postID = args[1]
	}
	if postID == "" {
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
		m := tgbotapi.NewMessage(chatID, "использование: /adcheck <канал|@канал|ссылка>\nпример: /adcheck @mychannel")
		_, _ = bot.Send(m)
		return
	}
	channel, _ := parseAdInput(args[0])
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
	if actionName != "" &&
		!(strings.HasPrefix(data, "win_prev_") || strings.HasPrefix(data, "win_next_") ||
			strings.HasPrefix(data, "android_prev_") || strings.HasPrefix(data, "android_next_") ||
			strings.HasPrefix(data, "ios_prev_") || strings.HasPrefix(data, "ios_next_") ||
			strings.HasPrefix(data, "macos_prev_") || strings.HasPrefix(data, "macos_next_") ||
			strings.HasPrefix(data, "chregion_prev_") || strings.HasPrefix(data, "chregion_next_") ||
			strings.HasSuffix(data, "_current") || data == "copy_key" || data == "nav_menu") {
		logAction(bot, userID, username, actionName, false)
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
	case data == "link_vk":
		handleLinkVK(bot, cq, session)
		return
	case data == "nav_qrcode":
		handleQRCode(bot, cq, session, xrCfg)
		return
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
	case data == "change_region_ios":
		if err := startInstructionFlow(bot, chatID, session, xrCfg, instruct.ChangeRegionIOS, 0); err != nil {
			log.Printf("change_region_ios instruction error: %v", err)
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
	case strings.HasPrefix(data, "chregion_prev_"):
		parts := strings.Split(data, "chregion_prev_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionChangeRegionIOS(chatID, bot, n-1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("chregion prev step error: %v", err)
					ackText = "Не удалось обновить шаг"
				}
			}
		}
	case strings.HasPrefix(data, "chregion_next_"):
		parts := strings.Split(data, "chregion_next_")
		if len(parts) == 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				if msgID, err := instruct.InstructionChangeRegionIOS(chatID, bot, n+1); err == nil && msgID != 0 {
					session.MessageID = msgID
					session.State = stateInstructions
				} else if err != nil {
					log.Printf("chregion next step error: %v", err)
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
			"<tg-emoji emoji-id=\"5344015205531686528\">💰</tg-emoji> покупка доступа\n\nсрок: %d дней\nцена: %.0f ₽ или %d ⭐\n\nнажми «оплатить ⭐».",
			p.Days, p.Amount, stars,
		)
		kbRaw := rawInlineKeyboardMarkup{InlineKeyboard: [][]rawInlineKeyboardButton{
			{rawURLButton("оплатить ⭐", link, "")},
			{
				rawCallbackButton("назад", "nav_topup", "", "5264852846527941278"),
				rawCallbackButton("меню", "nav_menu", "", "5346299917679757635"),
			},
		}}
		_ = updateSessionTextRaw(bot, chatID, session, stateChoosePay, text, "HTML", kbRaw)
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
		session.PendingSaveCard = true

		userID := strconv.FormatInt(cq.From.ID, 10)
		if email, _ := userStore.GetEmail(userID); strings.TrimSpace(email) == "" {
			text := "📧 Для оплаты картой нужен e-mail для чека.\nОтправь e-mail следующим сообщением (пример: name@example.com).\n\n" +
				"<b>Продолжи, введя e-mail.</b>"
			kbRaw := rawInlineKeyboardMarkup{InlineKeyboard: [][]rawInlineKeyboardButton{
				{
					rawCallbackButton("назад", "nav_topup", "", "5264852846527941278"),
					rawCallbackButton("меню", "nav_menu", "", "5346299917679757635"),
				},
			}}
			_ = updateSessionTextRaw(bot, chatID, session, stateCollectEmail, text, "HTML", kbRaw)
			ackCallback(bot, cq, "пришли e-mail")
			return
		}

		if err := startPaymentForPlan(bot, chatID, session, p, true); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось создать платёж.", "", mainMenuInlineKeyboard())
			ackCallback(bot, cq, "ошибка оплаты")
			return
		}
		ackCallback(bot, cq, "счёт создан")
		return

	case strings.HasPrefix(data, "pay_any_"):
		id := strings.TrimPrefix(data, "pay_any_")
		p, ok := ratePlanByID[id]
		if !ok {
			ackCallback(bot, cq, "тариф не найден")
			return
		}
		session.PendingPlanID = p.ID
		session.PendingSaveCard = false

		userID := strconv.FormatInt(cq.From.ID, 10)
		if email, _ := userStore.GetEmail(userID); strings.TrimSpace(email) == "" {
			text := "📧 Для оплаты нужен e-mail для чека.\nОтправь e-mail следующим сообщением (пример: name@example.com).\n\n" +
				"<b>Продолжи, введя e-mail.</b>"
			kbRaw := rawInlineKeyboardMarkup{InlineKeyboard: [][]rawInlineKeyboardButton{
				{
					rawCallbackButton("назад", "nav_topup", "", "5264852846527941278"),
					rawCallbackButton("меню", "nav_menu", "", "5346299917679757635"),
				},
			}}
			_ = updateSessionTextRaw(bot, chatID, session, stateCollectEmail, text, "HTML", kbRaw)
			ackCallback(bot, cq, "пришли e-mail")
			return
		}

		if err := startPaymentForPlan(bot, chatID, session, p, false); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось создать платёж.", "", mainMenuInlineKeyboard())
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
			return
		}
	case data == "check_payment":
		handleCheckPayment(bot, cq, session, xrCfg)

	case data == "enable_autopay":
		userIDStr := strconv.FormatInt(int64(cq.From.ID), 10)
		if session.AutopayProposalMsgID > 0 {
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, session.AutopayProposalMsgID))
			session.AutopayProposalMsgID = 0
		}
		// Включаем автопродление (работает и из proposal, и из профиля)
		methodID, planID, _, _ := userStore.GetAutopay(userIDStr)
		if methodID != "" && planID != "" {
			if err := userStore.SetAutopay(userIDStr, methodID, planID); err != nil {
				log.Printf("enable_autopay error user=%s: %v", userIDStr, err)
			}
		}
		ackText = "автопродление включено ✅"
		handleStatus(bot, cq, session, xrCfg)

	case data == "skip_autopay":
		userIDStr := strconv.FormatInt(int64(cq.From.ID), 10)
		_ = userStore.DisableAutopay(userIDStr)
		if session.AutopayProposalMsgID > 0 {
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, session.AutopayProposalMsgID))
			session.AutopayProposalMsgID = 0
		}
		ackText = "автопродление отключено"

	case data == "disable_autopay":
		userIDStr := strconv.FormatInt(int64(cq.From.ID), 10)
		if err := userStore.DisableAutopay(userIDStr); err != nil {
			log.Printf("disable_autopay error user=%s: %v", userIDStr, err)
		}
		ackText = "автопродление отключено ❌"
		handleStatus(bot, cq, session, xrCfg)

	case data == "unbind_card":
		// Показываем подтверждение: редактируем текущее сообщение
		confirmText := "вы отвязываете карту. автопродление отключится, и для его повторного подключения потребуется новая оплата."
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("отвязать", "unbind_card_confirm"),
				tgbotapi.NewInlineKeyboardButtonData("нет", "unbind_card_cancel"),
			),
		)
		edit := tgbotapi.NewEditMessageText(chatID, cq.Message.MessageID, confirmText)
		edit.ReplyMarkup = &kb
		_, _ = bot.Send(edit)

	case data == "unbind_card_confirm":
		userIDStr := strconv.FormatInt(int64(cq.From.ID), 10)
		if err := userStore.ClearAutopay(userIDStr); err != nil {
			log.Printf("unbind_card error user=%s: %v", userIDStr, err)
		}
		ackText = "карта отвязана ✅"
		handleStatus(bot, cq, session, xrCfg)

	case data == "unbind_card_cancel":
		handleStatus(bot, cq, session, xrCfg)

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

func parseAdInput(raw string) (channel, post string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	raw = strings.TrimPrefix(raw, "@")
	if strings.HasPrefix(raw, "https://") || strings.HasPrefix(raw, "http://") {
		if u, err := url.Parse(raw); err == nil {
			// path like /channel or /channel/post
			parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
			if len(parts) > 0 {
				channel = parts[0]
			}
			if len(parts) > 1 {
				post = parts[1]
			}
			return channel, post
		}
	}
	parts := strings.Split(raw, "/")
	channel = parts[0]
	if len(parts) > 1 {
		post = parts[1]
	}
	return channel, post
}

func randomSlug(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(letters[rand.Intn(len(letters))])
	}
	return b.String()
}

func extractAdTag(msg *tgbotapi.Message) string {
	// Primary: command arguments
	args := strings.TrimSpace(msg.CommandArguments())
	args = strings.TrimPrefix(args, "=")
	if strings.HasPrefix(args, "ad_") {
		return strings.TrimPrefix(args, "ad_")
	}

	// Fallback: parse from full text (/start=ad_xxx or start=ad_xxx)
	text := strings.TrimSpace(msg.Text)
	text = strings.TrimPrefix(text, "/")
	text = strings.TrimPrefix(text, "start")
	text = strings.TrimLeft(text, " =")
	if strings.HasPrefix(text, "ad_") {
		return strings.TrimPrefix(text, "ad_")
	}

	// Fallback: search "start=ad_"
	if idx := strings.Index(msg.Text, "start=ad_"); idx != -1 {
		val := msg.Text[idx+len("start="):]
		val = strings.Fields(val)[0]
		if strings.HasPrefix(val, "ad_") {
			return strings.TrimPrefix(val, "ad_")
		}
	}
	return ""
}

func stripHTML(s string) string {
	var b strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			continue
		}
		if !inTag {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func logAction(bot *tgbotapi.BotAPI, userID int64, username, action string, isNew bool) {
	now := time.Now()
	logSessionMu.Lock()
	ls := logSessions[userID]
	if ls == nil || now.Sub(ls.Last) > 10*time.Minute {
		ls = &logSession{
			Start:   now,
			Last:    now,
			Actions: []string{},
			IsNew:   isNew,
		}
		logSessions[userID] = ls
	}
	if isNew {
		ls.IsNew = true
	}
	ls.Last = now
	if strings.TrimSpace(action) != "" && (len(ls.Actions) == 0 || ls.Actions[len(ls.Actions)-1] != action) {
		ls.Actions = append(ls.Actions, action)
	}
	logSessionMu.Unlock()

	userLink := fmt.Sprintf(`<a href="tg://user?id=%d">ID:%d</a>`, userID, userID)
	if username == "" {
		if u, err := bot.GetChat(tgbotapi.ChatInfoConfig{ChatConfig: tgbotapi.ChatConfig{ChatID: userID}}); err == nil && u.UserName != "" {
			username = u.UserName
		}
	}
	if username != "" {
		userLink = fmt.Sprintf(`<a href="https://t.me/%s">@%s</a> (ID:%d)`, username, username, userID)
	}

	for {
		logSessionMu.Lock()
		ls = logSessions[userID]
		if ls == nil {
			logSessionMu.Unlock()
			return
		}

		if ls.Sending {
			ls.Dirty = true
			logSessionMu.Unlock()
			return
		}

		dur := ls.Last.Sub(ls.Start).Round(time.Minute)
		mins := int(math.Round(dur.Minutes()))
		if mins < 1 {
			mins = 1
		}
		newMark := ""
		if ls.IsNew {
			newMark = " НОВЫЙ ПОЛЬЗОВАТЕЛЬ"
		}
		actions := "—"
		if len(ls.Actions) > 0 {
			actions = strings.Join(ls.Actions, " → ")
		}
		text := fmt.Sprintf("👤 %s%s\n🕒 %s–%s · сессия %s\n🔗 действия: %s", userLink, newMark, ls.Start.Format("15:04"), ls.Last.Format("15:04"), minutesLabel(mins), actions)
		msgID := ls.MsgID
		ls.Sending = true
		logSessionMu.Unlock()

		newMsgID := 0
		if msgID == 0 {
			if testMode {
				// В тестовом режиме просто логируем без отправки
				log.Printf("[TEST MODE] log action: %s", text)
			} else {
				msg := tgbotapi.NewMessage(logChatID, text)
				msg.ParseMode = "HTML"
				msg.DisableWebPagePreview = true
				if sent, err := bot.Send(msg); err == nil {
					newMsgID = sent.MessageID
				} else {
					log.Printf("log action send failed: %v", err)
				}
			}
		} else {
			if testMode {
				// В тестовом режиме просто логируем
				log.Printf("[TEST MODE] log action update: %s", text)
			} else {
				edit := tgbotapi.NewEditMessageText(logChatID, msgID, text)
				edit.ParseMode = "HTML"
				edit.DisableWebPagePreview = true
				if _, err := bot.Send(edit); err != nil {
					if strings.Contains(strings.ToLower(err.Error()), "message is not modified") {
						// no-op: same text already in log message
					} else {
						msg := tgbotapi.NewMessage(logChatID, text)
						msg.ParseMode = "HTML"
						msg.DisableWebPagePreview = true
						if sent, err2 := bot.Send(msg); err2 == nil {
							newMsgID = sent.MessageID
						} else {
							log.Printf("log action edit failed: %v; fallback send failed: %v", err, err2)
						}
					}
				}
			}
		}

		logSessionMu.Lock()
		ls = logSessions[userID]
		if ls == nil {
			logSessionMu.Unlock()
			return
		}
		if newMsgID != 0 {
			ls.MsgID = newMsgID
		}
		ls.Sending = false
		if ls.Dirty {
			ls.Dirty = false
			logSessionMu.Unlock()
			continue
		}
		logSessionMu.Unlock()
		return
	}
}

func handleTopUp(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = ""
	var builder strings.Builder
	builder.WriteString("<tg-emoji emoji-id=\"5344015205531686528\">💰</tg-emoji> покупка доступа\nчем больше период — тем выгоднее!\n\nвыберите период ниже.\nоплата ⭐ звездами - <b>скидка 10%.</b>\n\nтарифы:\n")
	for _, plan := range ratePlans {
		stars := starsAmountForPlan(plan)
		builder.WriteString(fmt.Sprintf("• %d дней — %.0f ₽ или %d⭐\n", plan.Days, plan.Amount, stars))
	}
	header := strings.TrimSuffix(builder.String(), "\n")
	_ = updateSessionTextRaw(bot, chatID, session, stateTopUp, header, "HTML", rateKeyboardRaw())
}
func handleRateSelection(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, plan RatePlan) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = plan.ID

	stars := starsAmountForPlan(plan)
	text := fmt.Sprintf(
		"<tg-emoji emoji-id=\"5344015205531686528\">💰</tg-emoji> покупка доступа\n\nсрок: %d дней\nцена: %.0f ₽ или %d ⭐\n\nвыберите способ оплаты:",
		plan.Days, plan.Amount, stars,
	)
	_ = updateSessionTextRaw(bot, chatID, session, stateChoosePay, text, "HTML", choosePayKeyboardRaw(plan))
	ackCallback(bot, cq, "выберите способ оплаты")
}
func startPaymentForPlan(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, plan RatePlan, saveCard bool) error {
	metadata := map[string]interface{}{
		"plan_id":     plan.ID,
		"plan_title":  plan.Title,
		"plan_days":   plan.Days,
		"plan_amount": plan.Amount,
	}

	email, _ := userStore.GetEmail(strconv.FormatInt(chatID, 10))
	newID, _, err := yookassaClient.SendVPNPayment(bot, chatID, session.MessageID, plan.Amount, plan.Title, metadata, email, saveCard)
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
	userID := int64(cq.From.ID)
	userIDStr := strconv.FormatInt(userID, 10)

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

	meta := payment.Metadata
	plan := resolvePlanFromMetadata(meta, session)
	if plan.Title == "" {
		ackCallback(bot, cq, "Тариф в платеже не найден")
		return
	}

	paymentKey := buildAppliedPaymentKey("yookassa", strings.TrimSpace(payment.ID))
	if paymentKey == "" {
		log.Printf("empty yookassa payment key for chat=%d", chatID)
		ackCallback(bot, cq, "Ошибка проверки платежа, попробуй ещё раз")
		return
	}

	alreadyApplied, err := userStore.IsPaymentApplied(userIDStr, paymentKey)
	if err != nil {
		log.Printf("yookassa IsPaymentApplied error: %v", err)
		sendPaymentAlert(bot, "payment apply check failed", userID, cq.From.UserName, paymentKey, plan.ID, err.Error())
		ackCallback(bot, cq, "Ошибка проверки платежа")
		return
	}
	if alreadyApplied {
		yookassaClient.ClearPayments(chatID)
		if canProceedKey(userID, "pay_skip_"+paymentKey, 5*time.Minute) {
			sendPaymentAlert(bot, "payment apply skipped (already applied)", userID, cq.From.UserName, paymentKey, plan.ID, "user retried check after applied")
		}
		ackCallback(bot, cq, "Платёж уже обработан")
		return
	}

	fake := &tgbotapi.Message{Chat: cq.Message.Chat, From: cq.From}
	if err := handleSuccessfulPayment(bot, fake, xrCfg, plan, session); err != nil {
		log.Printf("handleSuccessfulPayment error: %v", err)
		sendPaymentAlert(bot, "payment succeeded but access failed", userID, cq.From.UserName, paymentKey, plan.ID, err.Error())
		ackCallback(bot, cq, "Оплата получена, но доступ пока не выдался. Нажми «Я оплатил» ещё раз через минуту или напиши в поддержку.")
		return
	}

	marked, err := userStore.MarkPaymentApplied(userIDStr, paymentKey, "yookassa", plan.ID, time.Now())
	if err != nil {
		log.Printf("yookassa MarkPaymentApplied error: %v", err)
		sendPaymentAlert(bot, "access issued but mark applied failed", userID, cq.From.UserName, paymentKey, plan.ID, err.Error())
		ackCallback(bot, cq, "Доступ выдан, но возникла ошибка фиксации платежа. Мы уже разбираемся.")
		return
	}
	if !marked {
		yookassaClient.ClearPayments(chatID)
		if canProceedKey(userID, "pay_skip_mark_"+paymentKey, 5*time.Minute) {
			sendPaymentAlert(bot, "payment apply skipped (already applied)", userID, cq.From.UserName, paymentKey, plan.ID, "mark returned duplicate")
		}
		ackCallback(bot, cq, "Платёж уже обработан")
		return
	}

	yookassaClient.ClearPayments(chatID)
	ackCallback(bot, cq, fmt.Sprintf("Платеж за %s подтверждён", plan.Title))

	// autopay: если карта сохранена, предложить автопродление
	if pm := payment.PaymentMethod; pm != nil && pm.Saved && pm.ID != "" {
		if err := userStore.SetAutopay(userIDStr, pm.ID, plan.ID); err != nil {
			log.Printf("SetAutopay error user=%s: %v", userIDStr, err)
		} else {
			// Сохраняем карту, но НЕ включаем — пользователь сам решит
			_ = userStore.DisableAutopay(userIDStr)
			autopayText := fmt.Sprintf(
				"чтобы не потерять <b>доступ</b> к VPN и оставаться на <b>связи</b> предлагаем включить автопродление\nпри окончании подписки автоматически спишется <b>%.0f ₽</b> за тариф <b>%s</b>.",
				plan.Amount, plan.Title,
			)
			kb := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("включить", "enable_autopay"),
					tgbotapi.NewInlineKeyboardButtonData("нет, спасибо", "skip_autopay"),
				),
			)
			msg := tgbotapi.NewMessage(chatID, autopayText)
			msg.ParseMode = "HTML"
			msg.ReplyMarkup = kb
			if sent, err := bot.Send(msg); err == nil {
				session.AutopayProposalMsgID = sent.MessageID
			}
		}
	}
}

func handleClaimSubscriptionBonus(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)
	userIDStr := strconv.FormatInt(userID, 10)

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

	refRewardGranted := false
	if granted, err := finalizeReferralAfterSubscription(bot, userID, cq.From.UserName, xrCfg); err != nil {
		log.Printf("finalize referral on claim_sub_bonus failed: %v", err)
	} else {
		refRewardGranted = granted
	}

	if claimed, err := userStore.IsStartBonusClaimed(userIDStr); err == nil && claimed {
		if refRewardGranted {
			ackCallback(bot, cq, "пригласившему начислено +15 дней")
		} else {
			ackCallback(bot, cq, "бонус уже получен")
		}
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
	if refRewardGranted {
		ackCallback(bot, cq, "бонус выдан, пригласившему +15 дней")
		return
	}
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

func handleQRCode(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)
	userIDStr := strconv.FormatInt(userID, 10)

	info, err := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), 0, true)
	if err != nil || info == nil || info.client == nil {
		ackCallback(bot, cq, "не удалось получить ключ")
		return
	}

	link := generateSubscriptionURL(xrCfg, info.client)
	if strings.TrimSpace(link) == "" {
		link = info.link
	}
	if strings.TrimSpace(link) == "" {
		ackCallback(bot, cq, "ключ недоступен")
		return
	}

	png, err := qrcode.Encode(link, qrcode.Medium, 512)
	if err != nil {
		log.Printf("qrcode encode error: %v", err)
		ackCallback(bot, cq, "не удалось создать QR-код")
		return
	}

	caption := fmt.Sprintf("<b>qr-код вашего ключа</b>\n\n<b>ключ:</b>\n<code>%s</code>", html.EscapeString(link))

	kbRaw := rawInlineKeyboardMarkup{InlineKeyboard: [][]rawInlineKeyboardButton{
		{
			rawCallbackButton("назад", "nav_get_vpn", "", "5264852846527941278"),
			rawCallbackButton("меню", "nav_menu", "", "5346299917679757635"),
		},
	}}

	if err := editMessageMediaRaw(bot, chatID, session.MessageID, png, "qr.png", caption, "HTML", kbRaw); err != nil {
		log.Printf("editMessageMedia error: %v", err)
		return
	}

	session.State = stateGetVPN
	session.ContentType = "photo"
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
		"<tg-emoji emoji-id=\"5343693752999383705\">👤</tg-emoji> профиль\n• id: <code>%d</code>\n• mail: %s\n• рефералы: %d (дней: %d)",
		userID, emailEsc, refCount, refBonus,
	)

	// Show linked VK account if any
	linkedVK, _ := userStore.GetLinkedVKUsers(userIDStr)
	if len(linkedVK) > 0 {
		header += fmt.Sprintf("\n• вк: привязан (%s)", html.EscapeString(linkedVK[0]))
	}

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
			"\n\nу вас есть доступ к neuravpn 🟢\nон активен ещё <b>%d</b> дней\nдо <code>%s</code>\n\nесли хотите продлить доступ - переходите в раздел «оплата»\nтам все очень дешево!",
			days, expStr,
		)
	} else {
		accessBlock = "\n\nу вас нет доступа к neuravpn 🔴\n\nесли хотите продлить доступ - переходите в раздел «оплата»\nтам все очень дешево!"
	}

	profileText := header + accessBlock

	// Autopay status
	apMethodID, apPlanID, apEnabled, _ := userStore.GetAutopay(userIDStr)
	if apEnabled {
		if apPlan, ok := ratePlanByID[apPlanID]; ok {
			profileText += fmt.Sprintf("\n\n<tg-emoji emoji-id=\"5345823764720426390\">🔄️</tg-emoji> автопродление: включено (%s, %.0f ₽)", apPlan.Title, apPlan.Amount)
		} else {
			profileText += "\n\nавтопродление: вкл ✅"
		}
	}

	kbRaw := rawInlineKeyboardMarkup{
		InlineKeyboard: [][]rawInlineKeyboardButton{
			{rawCallbackButton("оплата", "nav_topup", "", "5344015205531686528")},
			{rawCallbackButton("e-mail", "edit_email", "", "5264870816671113060")},
		},
	}
	if len(linkedVK) == 0 {
		kbRaw.InlineKeyboard = append(kbRaw.InlineKeyboard,
			[]rawInlineKeyboardButton{rawCallbackButton("🔗 связать с ВК", "link_vk", "", "")},
		)
	}
	if apEnabled {
		kbRaw.InlineKeyboard = append(kbRaw.InlineKeyboard,
			[]rawInlineKeyboardButton{rawCallbackButton("отключить автопродление", "disable_autopay", "", "5264863854529124844")},
		)
	} else if apMethodID != "" && apPlanID != "" {
		kbRaw.InlineKeyboard = append(kbRaw.InlineKeyboard,
			[]rawInlineKeyboardButton{rawCallbackButton("включить автопродление", "enable_autopay", "", "5345823764720426390")},
		)
	}
	if apMethodID != "" {
		kbRaw.InlineKeyboard = append(kbRaw.InlineKeyboard,
			[]rawInlineKeyboardButton{rawCallbackButton("отвязать карту", "unbind_card", "", "5264863854529124844")},
		)
	}
	kbRaw.InlineKeyboard = append(kbRaw.InlineKeyboard,
		[]rawInlineKeyboardButton{rawCallbackButton("меню", "nav_menu", "", "5264852846527941278")},
	)
	if err := updateSessionTextRaw(bot, chatID, session, stateStatus, profileText, "HTML", kbRaw); err == nil {
		return
	}

	kbRows := [][]tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("💰 оплата", "nav_topup")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("✏️ e-mail", "edit_email")),
	}
	if len(linkedVK) == 0 {
		kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("🔗 связать с ВК", "link_vk")))
	}
	if apEnabled {
		kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("❌ отключить автопродление", "disable_autopay")))
	} else if apMethodID != "" && apPlanID != "" {
		kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("🔄 включить автопродление", "enable_autopay")))
	}
	if apMethodID != "" {
		kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("🗑 отвязать карту", "unbind_card")))
	}
	kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("⬅️ меню", "nav_menu")))
	kb := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: kbRows}

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
	text := "<tg-emoji emoji-id=\"5264870816671113060\">✏️</tg-emoji> отправь новый e-mail сообщением."
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("назад", "nav_status"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateEditEmail, text, "HTML", kb)
	ackCallback(bot, cq, "жду e-mail")
}

func handleLinkVK(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	userIDStr := strconv.FormatInt(cq.From.ID, 10)

	// Check if already linked
	linked, _ := userStore.GetLinkedVKUsers(userIDStr)
	if len(linked) > 0 {
		text := fmt.Sprintf("ℹ️ аккаунт уже привязан к вк: <code>%s</code>", html.EscapeString(linked[0]))
		_ = updateSessionText(bot, chatID, session, stateStatus, text, "HTML",
			tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_status")),
			))
		ackCallback(bot, cq, "уже привязан")
		return
	}

	token := randomSlug(16)
	if err := userStore.SetLinkToken(userIDStr, token); err != nil {
		log.Printf("link token error: %v", err)
		ackCallback(bot, cq, "ошибка, попробуйте позже")
		return
	}

	code := "link_" + token
	text := fmt.Sprintf(
		"🔗 <b>привязка ВК</b>\n\n"+
			"отправьте это сообщение нашему VK-боту:\n\n"+
			"<code>%s</code>\n\n"+
			"после привязки подписка и данные станут общими.\n\n"+
			"⚠️ токен одноразовый.",
		html.EscapeString(code),
	)

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("🔗 перейти в ВК", "https://vk.com/neuravpn"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", "nav_status"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
	ackCallback(bot, cq, "")
}

func handleInstructionsMenu(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	instruct.ResetState(chatID)
	text := "<tg-emoji emoji-id=\"5264991913274019640\">🛠️</tg-emoji> инструкции\nвыбери платформу:"
	kbRaw := rawInlineKeyboardMarkup{
		InlineKeyboard: [][]rawInlineKeyboardButton{
			{
				rawCallbackButton("windows", "windows", "", ""),
				rawCallbackButton("android", "android", "", ""),
			},
			{
				rawCallbackButton("ios", "ios", "", ""),
				rawCallbackButton("macos", "macos", "", ""),
			},
			{
				rawCallbackButton("смена региона ios", "change_region_ios", "", ""),
			},
			{
				rawCallbackButton("меню", "nav_menu", "", "5264852846527941278"),
			},
		},
	}
	if err := updateSessionTextRaw(bot, chatID, session, stateInstructions, text, "HTML", kbRaw); err != nil {
		log.Printf("handleInstructionsMenu raw keyboard error: %v", err)
	}
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
	case instruct.ChangeRegionIOS:
		msgID, err = instruct.InstructionChangeRegionIOS(chatID, bot, step)
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
	userLink := fmt.Sprintf(`<a href="tg://user?id=%d">ID:%d</a>`, userID, userID)
	if msg.From.UserName != "" {
		userLink = fmt.Sprintf(`<a href="https://t.me/%s">@%s</a> (ID:%d)`, msg.From.UserName, msg.From.UserName, userID)
	}
	payText := fmt.Sprintf("💰 %s оплатил %s за %.0f ₽ 🎉", userLink, plan.Title, plan.Amount)
	m := tgbotapi.NewMessage(logChatID, payText)
	m.ParseMode = "HTML"
	m.DisableWebPagePreview = true
	_, _ = bot.Send(m)
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

func buildAppliedPaymentKey(provider, paymentID string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	paymentID = strings.TrimSpace(paymentID)
	if provider == "" || paymentID == "" {
		return ""
	}
	return provider + ":" + paymentID
}

func resolveStarsPaymentID(p *tgbotapi.SuccessfulPayment) string {
	if p == nil {
		return ""
	}
	if id := strings.TrimSpace(p.TelegramPaymentChargeID); id != "" {
		return id
	}
	return strings.TrimSpace(p.ProviderPaymentChargeID)
}

func sendPaymentAlert(bot *tgbotapi.BotAPI, event string, userID int64, username, paymentKey, planID, details string) {
	if bot == nil {
		return
	}

	userLink := fmt.Sprintf(`<a href="tg://user?id=%d">ID:%d</a>`, userID, userID)
	if strings.TrimSpace(username) != "" {
		userLink = fmt.Sprintf(`<a href="https://t.me/%s">@%s</a> (ID:%d)`, html.EscapeString(username), html.EscapeString(username), userID)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("⚠️ <b>%s</b>\n", html.EscapeString(event)))
	b.WriteString(fmt.Sprintf("user: %s\n", userLink))
	if strings.TrimSpace(paymentKey) != "" {
		b.WriteString(fmt.Sprintf("payment: <code>%s</code>\n", html.EscapeString(paymentKey)))
	}
	if strings.TrimSpace(planID) != "" {
		b.WriteString(fmt.Sprintf("plan: <code>%s</code>\n", html.EscapeString(planID)))
	}
	if strings.TrimSpace(details) != "" {
		b.WriteString(fmt.Sprintf("details: <code>%s</code>", html.EscapeString(details)))
	}

	msg := tgbotapi.NewMessage(logChatID, b.String())
	msg.ParseMode = "HTML"
	msg.DisableWebPagePreview = true
	_, _ = bot.Send(msg)
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
		id := strings.TrimPrefix(data, "rate_")
		if p, ok := ratePlanByID[id]; ok {
			return fmt.Sprintf("выбор суммы: %s", p.Title)
		}
		return "выбор суммы"
	}
	if strings.HasPrefix(data, "win_prev_") || strings.HasPrefix(data, "android_prev_") || strings.HasPrefix(data, "ios_prev_") || strings.HasPrefix(data, "macos_prev_") || strings.HasPrefix(data, "chregion_prev_") {
		return "инструкция: назад"
	}
	if strings.HasPrefix(data, "win_next_") || strings.HasPrefix(data, "android_next_") || strings.HasPrefix(data, "ios_next_") || strings.HasPrefix(data, "macos_next_") || strings.HasPrefix(data, "chregion_next_") {
		return "инструкция: дальше"
	}

	if name, ok := actionMap[data]; ok {
		return name
	}

	return data
}

func getCommandActionName(command string) string {
	cmd := strings.ToLower(strings.TrimSpace(command))
	switch cmd {
	case "start":
		return ""
	case "topup", "пополнить", "пополнить_баланс":
		return getActionName("nav_topup")
	case "getvpn", "vpn", "подключить", "получитьvpn":
		return getActionName("nav_get_vpn")
	case "status", "profile", "профиль":
		return getActionName("nav_status")
	case "instructions", "инструкции":
		return getActionName("nav_instructions")
	case "referral", "рефералы":
		return getActionName("nav_referral")
	case "support", "поддержка":
		return getActionName("nav_support")
	default:
		if cmd == "" {
			return ""
		}
		return "/" + cmd
	}
}

func notifyAdmins(bot *tgbotapi.BotAPI, userID int64, username, action string) {
	logAction(bot, userID, username, action, false)
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
		"<tg-emoji emoji-id=\"5345823764720426390\">🎁</tg-emoji> +15 дней к доступу\n\n"+
			"кстати, у нас есть реферальная программа.\nприводишь друга → он подписывается на канал → получаешь +15 дней доступа.\n\n"+
			"🔗 твоя ссылка\n<code>%s</code>\n\n"+
			"пришло друзей: %d\nнакопленный бонус: %d дней.",
		link, count, bonus,
	)
	share := shareURL
	kbRaw := rawInlineKeyboardMarkup{
		InlineKeyboard: [][]rawInlineKeyboardButton{
			{
				{
					Text:              "поделиться ссылкой",
					URL:               &share,
					IconCustomEmojiID: "5345823764720426390",
				},
			},
			{
				rawCallbackButton("меню", "nav_menu", "", "5264852846527941278"),
			},
		},
	}
	if err := updateSessionTextRaw(bot, chatID, session, stateMenu, text, "HTML", kbRaw); err != nil {
		log.Printf("handleReferral raw keyboard error: %v", err)
	}
}
func handleSupport(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "<tg-emoji emoji-id=\"5346123042336573193\">📞</tg-emoji> поддержка\n\nесть вопросы или предложения? пиши: @neuravpn_support\nответим лично, никаких почтовых ящиков."
	kbRaw := rawInlineKeyboardMarkup{
		InlineKeyboard: [][]rawInlineKeyboardButton{
			{
				rawCallbackButton("меню", "nav_menu", "", "5264852846527941278"),
			},
		},
	}
	if err := updateSessionTextRaw(bot, chatID, session, stateMenu, text, "HTML", kbRaw); err == nil {
		ackCallback(bot, cq, "поддержка")
		return
	}
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
	ackCallback(bot, cq, "поддержка")
}
