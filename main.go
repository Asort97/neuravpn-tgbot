package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
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
	"github.com/Asort97/vpnBot/clients/vkbot"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	"github.com/SevereCloud/vksdk/v3/api"
	"github.com/SevereCloud/vksdk/v3/events"
	longpoll "github.com/SevereCloud/vksdk/v3/longpoll-bot"
)

// ────────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────────

const (
	startTrialDays    = 7
	channelBonusDays  = 7
	referralBonusDays = 15
)

const startText = `👋 добро пожаловать!

этот бот поможет подключить neuravpn с понятными инструкциями для любой платформы.

перед покупкой основного тарифа мы предлагаем пробный период - 7 дней.
попробуйте. мы не заставляем.

гарантируем стабильный и бесперебойный доступ ко всем заблокированным ресурсам
без ограничения исходной скорости вашего интернета.
можете проверить.`

// ────────────────────────────────────────────────────────────────
// Runtime settings (env-overridable)
// ────────────────────────────────────────────────────────────────

var (
	vkGroupURL   string // e.g. "https://vk.com/neuravpn"
	vkGroupID    int    // numeric VK group ID
	adStats      = newAdStatsStore(resolveAdStatsPath())
	logSessionMu sync.Mutex
	logSessions  = make(map[int64]*logSession)
)

func init() { rand.Seed(time.Now().UnixNano()) }

func resolveAdStatsPath() string {
	if p := strings.TrimSpace(os.Getenv("AD_STATS_PATH")); p != "" {
		return p
	}
	exe, err := os.Executable()
	if err != nil {
		return filepath.Join("database", "ad_stats.json")
	}
	return filepath.Join(filepath.Dir(exe), "database", "ad_stats.json")
}

// ────────────────────────────────────────────────────────────────
// Throttling
// ────────────────────────────────────────────────────────────────

var lastActionKey = make(map[int64]map[string]time.Time)

func canProceedKey(userID int64, key string, interval time.Duration) bool {
	now := time.Now()
	if lastActionKey[userID] == nil {
		lastActionKey[userID] = make(map[string]time.Time)
	}
	if t, ok := lastActionKey[userID][key]; ok && now.Sub(t) < interval {
		return false
	}
	lastActionKey[userID][key] = now
	return true
}

// ────────────────────────────────────────────────────────────────
// Session types
// ────────────────────────────────────────────────────────────────

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

type UserSession struct {
	MessageID     int
	State         SessionState
	ContentType   string
	PendingPlanID string
	LastAccess    string
	LastLink      string
	LastAction    string
	LastActionAt  time.Time
	SessionID     int
	SessionStart  time.Time
	Actions       []string
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

type RatePlan struct {
	ID     string
	Title  string
	Amount float64
	Days   int
}

// ────────────────────────────────────────────────────────────────
// Ad stats
// ────────────────────────────────────────────────────────────────

type adStatsStore struct {
	mu   sync.RWMutex
	path string
	data map[string]map[string]bool
}

func newAdStatsStore(path string) *adStatsStore {
	return &adStatsStore{path: path, data: make(map[string]map[string]bool)}
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

func (s *adStatsStore) record(tag, userID string) (int, bool) {
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

// ────────────────────────────────────────────────────────────────
// DataStore interface (shared with Telegram bot via same DB)
// ────────────────────────────────────────────────────────────────

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
}

// ────────────────────────────────────────────────────────────────
// Rate plans
// ────────────────────────────────────────────────────────────────

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

// ────────────────────────────────────────────────────────────────
// Xray / access types
// ────────────────────────────────────────────────────────────────

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

// ────────────────────────────────────────────────────────────────
// Global state
// ────────────────────────────────────────────────────────────────

var (
	yookassaClient *yookassa.YooKassaClient
	userStore      DataStore
	xrayCfg        *xraySettings
	oldXrayCfg     *xraySettings
	privacyURL     string
	adminIDs       []int64
	logPeerID      int // VK peer_id for admin log messages (could be a chat or user)
	userSessions   = make(map[int64]*UserSession)
	testMode       bool
	tgBotName      string // e.g. "neuravpn_bot"
)

var (
	expiryReminderMu    sync.Mutex
	expiryReminderState = make(map[int64]map[string]string)
)

const expiryReminderStatePath = "database/reminder_state.json"

// ────────────────────────────────────────────────────────────────
// VK user ID helpers — DB stores VK users as "vk_<numericID>"
// ────────────────────────────────────────────────────────────────

func vkUserIDStr(peerID int) string {
	return fmt.Sprintf("vk_%d", peerID)
}

func parseVKUserID(dbID string) (int, bool) {
	dbID = strings.TrimSpace(dbID)
	if strings.HasPrefix(dbID, "vk_") {
		if id, err := strconv.Atoi(strings.TrimPrefix(dbID, "vk_")); err == nil {
			return id, true
		}
	}
	if id, err := strconv.Atoi(dbID); err == nil {
		return id, true
	}
	return 0, false
}

// ────────────────────────────────────────────────────────────────
// Linked-account resolution: VK peer → actual DB user ID
// ────────────────────────────────────────────────────────────────

var (
	linkedAccountsMu sync.RWMutex
	linkedAccounts   = make(map[int]string)
)

// resolvedUserID returns the DB user ID to use for all operations.
// If the VK user is linked to a TG user, returns the TG user ID.
func resolvedUserID(peerID int) string {
	linkedAccountsMu.RLock()
	if resolved, ok := linkedAccounts[peerID]; ok {
		linkedAccountsMu.RUnlock()
		return resolved
	}
	linkedAccountsMu.RUnlock()

	vkID := vkUserIDStr(peerID)
	if userStore == nil {
		return vkID
	}
	linked, err := userStore.GetLinkedTo(vkID)
	if err == nil && strings.TrimSpace(linked) != "" {
		linkedAccountsMu.Lock()
		linkedAccounts[peerID] = linked
		linkedAccountsMu.Unlock()
		return linked
	}
	return vkID
}

// ────────────────────────────────────────────────────────────────
// Session management
// ────────────────────────────────────────────────────────────────

func getSession(peerID int) *UserSession {
	key := int64(peerID)
	if s, ok := userSessions[key]; ok {
		return s
	}
	s := &UserSession{}
	userSessions[key] = s
	return s
}

func sessionAction(bot *vkbot.Bot, peerID int, session *UserSession, action string, isNewUser bool) {
	now := time.Now()
	if session.SessionID == 0 {
		session.SessionID = 1
		session.SessionStart = now
		session.LastActionAt = now
		session.LastAction = action
		session.Actions = []string{action}
		logAction(bot, peerID, action, isNewUser)
		return
	}
	if now.Sub(session.LastActionAt) > 10*time.Minute {
		session.SessionID++
		session.SessionStart = now
		session.Actions = nil
	}
	session.LastAction = action
	session.LastActionAt = now
	session.Actions = append(session.Actions, action)
	logAction(bot, peerID, action, isNewUser)
}

// ────────────────────────────────────────────────────────────────
// Formatting helpers
// ────────────────────────────────────────────────────────────────

func minutesLabel(n int) string {
	if n <= 1 {
		return "1 мин"
	}
	return fmt.Sprintf("%d мин", n)
}

func formatExpiryUTC(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("02.01.2006 15:04 UTC")
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

func formatUserLabel(peerID int) string {
	return fmt.Sprintf("https://vk.com/id%d", peerID)
}

// ────────────────────────────────────────────────────────────────
// VK message helpers
// ────────────────────────────────────────────────────────────────

func updateSessionText(bot *vkbot.Bot, peerID int, session *UserSession, state SessionState, text string, kb rawkbd.Markup) error {
	vkKb := kb.ToVKKeyboard()
	if session.MessageID != 0 {
		if err := bot.EditMessage(peerID, session.MessageID, text, vkKb); err == nil {
			instruct.ResetState(int64(peerID))
			session.State = state
			session.ContentType = "text"
			return nil
		}
	}
	return replaceSessionWithText(bot, peerID, session, state, text, kb)
}

func replaceSessionWithText(bot *vkbot.Bot, peerID int, session *UserSession, state SessionState, text string, kb rawkbd.Markup) error {
	if session.MessageID != 0 {
		_ = bot.DeleteMessage(peerID, []int{session.MessageID})
	}
	instruct.ResetState(int64(peerID))
	vkKb := kb.ToVKKeyboard()
	msgID, err := bot.SendMessage(peerID, text, vkKb)
	if err != nil {
		return err
	}
	session.MessageID = msgID
	session.State = state
	session.ContentType = "text"
	return nil
}

// ────────────────────────────────────────────────────────────────
// Keyboard builders
// ────────────────────────────────────────────────────────────────

func mainMenuKeyboard() rawkbd.Markup {
	return rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{
				rawkbd.CallbackButton("🔌 подключить VPN", "nav_get_vpn", "", ""),
				rawkbd.CallbackButton("👤 профиль/оплата", "nav_status", "", ""),
			},
			{
				rawkbd.CallbackButton("🎁 +15 дней", "nav_referral", "", ""),
				rawkbd.CallbackButton("📞 поддержка", "nav_support", "", ""),
			},
		},
	}
}

func rateKeyboard() rawkbd.Markup {
	var rows [][]rawkbd.Button
	var row []rawkbd.Button
	for _, p := range ratePlans {
		label := fmt.Sprintf("%d дней — %.0f ₽", p.Days, p.Amount)
		row = append(row, rawkbd.CallbackButton(label, "rate_"+p.ID, "", ""))
		if len(row) == 2 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	rows = append(rows, []rawkbd.Button{
		rawkbd.CallbackButton("⬅️ назад", "nav_status", "", ""),
	})
	return rawkbd.Markup{Buttons: rows}
}

// ────────────────────────────────────────────────────────────────
// VK group membership check (replaces TG channel subscription)
// ────────────────────────────────────────────────────────────────

func isGroupMember(bot *vkbot.Bot, userID int) (bool, error) {
	return bot.IsGroupMember(userID)
}

// ────────────────────────────────────────────────────────────────
// Xray access
// ────────────────────────────────────────────────────────────────

func ensureXrayAccess(cfg *xraySettings, userIDStr string, email string, addDays int64, createIfMissing bool) (*accessInfo, error) {
	if testMode {
		fakeExpiry := time.Now().Add(30 * 24 * time.Hour)
		fakeClient := &xray.Client{
			ID:         "test-uuid-" + userIDStr,
			Email:      email,
			Enable:     true,
			ExpiryTime: fakeExpiry.UnixMilli(),
			SubID:      "test-sub-" + userIDStr,
			TgID:       userIDStr,
		}
		return &accessInfo{
			client:   fakeClient,
			expireAt: fakeExpiry,
			daysLeft: 30,
			link:     "vless://test-key-for-" + userIDStr + "@example.com:443",
		}, nil
	}

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
		if len(inboundIDs) == 0 && cfg.inboundID > 0 {
			inboundIDs = append(inboundIDs, cfg.inboundID)
		}
	}
	if len(inboundIDs) == 0 {
		return nil, fmt.Errorf("no inbounds available to ensure client")
	}

	if !createIfMissing && addDays == 0 {
		c, err := cfg.client.GetClientByTelegram(inboundIDs[0], userIDStr)
		if err != nil {
			return nil, err
		}
		if c == nil {
			return nil, nil
		}
		if strings.TrimSpace(c.Email) == "" {
			c.Email = userIDStr
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
		_ = userStore.SetDays(userIDStr, info.daysLeft)
		return info, nil
	}

	subID, _ := userStore.EnsureSubscriptionID(userIDStr)
	primaryClient, expireAt, err := cfg.client.EnsureClientAcrossInbounds(inboundIDs, userIDStr, email, addDays, subID)
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
	_ = userStore.SetDays(userIDStr, daysLeft)

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

func generateSubscriptionURL(cfg *xraySettings, c *xray.Client) string {
	if cfg == nil || c == nil {
		return ""
	}
	base := cfg.subBaseURL
	if strings.TrimSpace(base) == "" {
		base = "https://sub.staticdeliverycdn.com:2096"
	}
	subID := strings.TrimSpace(c.SubID)
	if subID == "" {
		subID = "sub" + strings.TrimSpace(c.TgID)
	}
	if !strings.HasPrefix(base, "http") {
		base = "https://" + base
	}
	return fmt.Sprintf("%s/s-39fj3r9f3j/%s", strings.TrimRight(base, "/"), subID)
}

// ────────────────────────────────────────────────────────────────
// Expiry reminder
// ────────────────────────────────────────────────────────────────

func collectExpiryByVKUser(cfg *xraySettings) (map[int]time.Time, error) {
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
		return nil, fmt.Errorf("no inbounds for reminder")
	}

	// Collect expiry for ALL clients (both VK and TG) by their TgID.
	allExpiry := make(map[string]time.Time)
	var vkPeers []int
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
			exp := time.UnixMilli(c.ExpiryTime)
			if existing, has := allExpiry[tgID]; !has || exp.After(existing) {
				allExpiry[tgID] = exp
			}
			if peerID, ok := parseVKUserID(tgID); ok {
				vkPeers = append(vkPeers, peerID)
			}
		}
	}

	// For linked VK users, use the TG client's expiry instead.
	result := make(map[int]time.Time)
	for _, peerID := range vkPeers {
		resolved := resolvedUserID(peerID)
		vkID := vkUserIDStr(peerID)
		if resolved != vkID {
			if tgExp, ok := allExpiry[resolved]; ok {
				result[peerID] = tgExp
				continue
			}
		}
		if exp, ok := allExpiry[vkID]; ok {
			result[peerID] = exp
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

func startExpiryReminder(bot *vkbot.Bot, cfg *xraySettings) {
	go func() {
		ticker := time.NewTicker(12 * time.Hour)
		defer ticker.Stop()
		for {
			func() {
				expiries, err := collectExpiryByVKUser(cfg)
				if err != nil {
					log.Printf("expiry reminder: %v", err)
					return
				}
				now := time.Now().UTC()
				for peerID, exp := range expiries {
					remain := exp.Sub(now)
					daysLeft := int64(0)
					if remain > 0 {
						daysLeft = int64(remain.Hours()/24 + 0.999)
						clearExpiryReminderStage(int64(peerID), "expired")
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
					if !shouldSendExpiryReminder(int64(peerID), key, exp) {
						continue
					}
					expStr := formatExpiryUTC(exp)
					text := ""
					if daysLeft <= 0 {
						text = fmt.Sprintf("⏰ ваш доступ к neuravpn закончился.\nдействовал до: %s\nпродлите в разделе «оплата».", expStr)
					} else {
						text = fmt.Sprintf("⏰ ваш доступ к neuravpn заканчивается через %d дн.\nдействует до: %s\nпродлите в разделе «оплата».", daysLeft, expStr)
					}
					_, _ = bot.SendMessage(peerID, text, nil)
				}
			}()
			<-ticker.C
		}
	}()
}

// ────────────────────────────────────────────────────────────────
// Send access info to user
// ────────────────────────────────────────────────────────────────

func sendAccess(info *accessInfo, userIDStr string, peerID int, addedDays int, cfg *xraySettings, bot *vkbot.Bot, session *UserSession) error {
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
		keyLine = subURL
	} else if strings.TrimSpace(info.link) != "" {
		keyLine = info.link
	}

	text := fmt.Sprintf(`🔌 подключить neuravpn

ваш ключ:
%s
✏️ скопируйте ключ выше

перейдите в раздел «инструкции» — мы подробно объясним, что и куда нужно вставить.

оставшееся время / действует до:
%s`, keyLine, combined)
	if addedDays > 0 {
		text += fmt.Sprintf("\n\n✨ Начислено: +%d дней", addedDays)
	}

	session.LastAccess = text
	session.LastLink = info.link

	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{rawkbd.CallbackButton("🛠 инструкции", "nav_instructions", "", "")},
			{
				rawkbd.CallbackButton("👤 профиль", "nav_status", "", ""),
				rawkbd.CallbackButton("🏠 меню", "nav_menu", "", ""),
			},
		},
	}
	return updateSessionText(bot, peerID, session, stateMenu, text, kb)
}

func issuePlanAccess(bot *vkbot.Bot, peerID int, session *UserSession, plan RatePlan, cfg *xraySettings, userIDStr string) error {
	info, err := ensureXrayAccess(cfg, userIDStr, fallbackEmail(userIDStr), int64(plan.Days), true)
	if err != nil {
		return err
	}
	return sendAccess(info, userIDStr, peerID, plan.Days, cfg, bot, session)
}

// ────────────────────────────────────────────────────────────────
// Payment
// ────────────────────────────────────────────────────────────────

func startPaymentForPlan(bot *vkbot.Bot, peerID int, session *UserSession, plan RatePlan) error {
	metadata := map[string]interface{}{
		"plan_id":     plan.ID,
		"plan_title":  plan.Title,
		"plan_days":   plan.Days,
		"plan_amount": plan.Amount,
	}
	email, _ := userStore.GetEmail(resolvedUserID(peerID))
	confirmationURL, err := yookassaClient.CreatePaymentURL(int64(peerID), plan.Amount, plan.Title, metadata, email)
	if err != nil {
		return err
	}

	text := fmt.Sprintf("💳 %s\n\n💰 Сумма к оплате: %.0f ₽\n📝 Описание: %s\n\nНажмите «Оплатить», чтобы продолжить.", plan.Title, plan.Amount, plan.Title)

	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{rawkbd.URLButton("💳 Оплатить", confirmationURL, "")},
			{rawkbd.CallbackButton("✅ Я оплатил", "check_payment", "", "")},
			{rawkbd.CallbackButton("⬅️ Назад в меню", "nav_menu", "", "")},
		},
	}

	session.PendingPlanID = plan.ID
	instruct.ResetState(int64(peerID))
	return replaceSessionWithText(bot, peerID, session, stateTopUp, text, kb)
}

func handleCheckPayment(bot *vkbot.Bot, peerID int, userID int, eventID string, session *UserSession, xrCfg *xraySettings) {
	userIDStr := resolvedUserID(userID)

	payment, ok, err := yookassaClient.FindSucceededPayment(int64(peerID))
	if err != nil {
		log.Printf("FindSucceededPayment error: %v", err)
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Ошибка проверки платежа")
		return
	}
	if !ok || payment == nil {
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Платёж не найден, попробуй позже (5-10 сек)")
		return
	}

	meta := payment.Metadata
	plan := resolvePlanFromMetadata(meta, session)
	if plan.Title == "" {
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Тариф в платеже не найден")
		return
	}

	paymentKey := buildAppliedPaymentKey("yookassa", strings.TrimSpace(payment.ID))
	if paymentKey == "" {
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Ошибка проверки платежа")
		return
	}

	alreadyApplied, err := userStore.IsPaymentApplied(userIDStr, paymentKey)
	if err != nil {
		log.Printf("yookassa IsPaymentApplied error: %v", err)
		sendPaymentAlert(bot, "payment apply check failed", peerID, paymentKey, plan.ID, err.Error())
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Ошибка проверки платежа")
		return
	}
	if alreadyApplied {
		yookassaClient.ClearPayments(int64(peerID))
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Платёж уже обработан")
		return
	}

	if err := handleSuccessfulPayment(bot, peerID, userID, xrCfg, plan, session); err != nil {
		log.Printf("handleSuccessfulPayment error: %v", err)
		sendPaymentAlert(bot, "payment succeeded but access failed", peerID, paymentKey, plan.ID, err.Error())
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Оплата получена, но доступ не выдался. Попробуй ещё раз через минуту.")
		return
	}

	marked, err := userStore.MarkPaymentApplied(userIDStr, paymentKey, "yookassa", plan.ID, time.Now())
	if err != nil {
		log.Printf("yookassa MarkPaymentApplied error: %v", err)
	}
	if !marked {
		yookassaClient.ClearPayments(int64(peerID))
		_ = bot.SendEventAnswer(eventID, peerID, userID, "Платёж уже обработан")
		return
	}

	yookassaClient.ClearPayments(int64(peerID))
	_ = bot.SendEventAnswer(eventID, peerID, userID, fmt.Sprintf("Платёж за %s подтверждён", plan.Title))
}

func handleSuccessfulPayment(bot *vkbot.Bot, peerID, userID int, xrCfg *xraySettings, plan RatePlan, session *UserSession) error {
	userIDStr := resolvedUserID(userID)

	waitingText := fmt.Sprintf("готовлю доступ по тарифу %s...", plan.Title)
	_ = updateSessionText(bot, peerID, session, stateTopUp, waitingText, mainMenuKeyboard())

	if err := issuePlanAccess(bot, peerID, session, plan, xrCfg, userIDStr); err != nil {
		return err
	}
	session.PendingPlanID = ""

	// Payment log to admin
	userLink := formatUserLabel(userID)
	payText := fmt.Sprintf("💰 %s (ID:%d) оплатил %s за %.0f ₽ 🎉", userLink, userID, plan.Title, plan.Amount)
	if logPeerID != 0 {
		_, _ = bot.SendMessage(logPeerID, payText, nil)
	}
	return nil
}

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

func sendPaymentAlert(bot *vkbot.Bot, event string, peerID int, paymentKey, planID, details string) {
	if bot == nil || logPeerID == 0 {
		return
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("⚠️ %s\n", event))
	b.WriteString(fmt.Sprintf("user: https://vk.com/id%d (ID:%d)\n", peerID, peerID))
	if paymentKey != "" {
		b.WriteString(fmt.Sprintf("payment: %s\n", paymentKey))
	}
	if planID != "" {
		b.WriteString(fmt.Sprintf("plan: %s\n", planID))
	}
	if details != "" {
		b.WriteString(fmt.Sprintf("details: %s", details))
	}
	_, _ = bot.SendMessage(logPeerID, b.String(), nil)
}

// ────────────────────────────────────────────────────────────────
// Menu
// ────────────────────────────────────────────────────────────────

func composeMenuText() string {
	base := strings.TrimSpace(startText)
	if vkGroupURL != "" {
		base += "\n\nнаша группа: " + vkGroupURL
		base += "\nнаш основной бот в Telegram: " + "t.me/neuravpn_bot"
	}
	return base
}

func showMainMenu(bot *vkbot.Bot, peerID int, session *UserSession) error {
	return updateSessionText(bot, peerID, session, stateMenu, composeMenuText(), mainMenuKeyboard())
}

// ────────────────────────────────────────────────────────────────
// Channel (group) bonus
// ────────────────────────────────────────────────────────────────

func sendGroupBonusOffer(bot *vkbot.Bot, peerID int) {
	text := fmt.Sprintf("кстати, у нас есть группа.\n\nесли подпишешься — добавим +%d дней доступа.", channelBonusDays)
	groupURL := vkGroupURL
	if groupURL == "" {
		groupURL = fmt.Sprintf("https://vk.com/club%d", vkGroupID)
	}
	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{
				rawkbd.URLButton("подписаться", groupURL, ""),
				rawkbd.CallbackButton("проверить", "claim_sub_bonus", "", ""),
			},
		},
	}
	vkKb := kb.ToVKKeyboard()
	_, _ = bot.SendMessage(peerID, text, vkKb)
}

func sendReferralSubscriptionPrompt(bot *vkbot.Bot, peerID int) {
	text := fmt.Sprintf("чтобы пригласившему начислилось +%d дней, подпишись на нашу группу и нажми «проверить».", referralBonusDays)
	groupURL := vkGroupURL
	if groupURL == "" {
		groupURL = fmt.Sprintf("https://vk.com/club%d", vkGroupID)
	}
	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{
				rawkbd.URLButton("подписаться", groupURL, ""),
				rawkbd.CallbackButton("проверить", "claim_sub_bonus", "", ""),
			},
		},
	}
	vkKb := kb.ToVKKeyboard()
	_, _ = bot.SendMessage(peerID, text, vkKb)
}

// ────────────────────────────────────────────────────────────────
// Referral
// ────────────────────────────────────────────────────────────────

func finalizeReferralAfterSubscription(bot *vkbot.Bot, peerID int, xrCfg *xraySettings) (bool, error) {
	userIDStr := resolvedUserID(peerID)
	referrerID, granted, err := userStore.ConfirmReferralAndRewardReferrer(userIDStr, int64(referralBonusDays), time.Now())
	if err != nil {
		return false, err
	}
	if !granted || strings.TrimSpace(referrerID) == "" {
		return false, nil
	}

	_, _ = ensureXrayAccess(xrCfg, referrerID, fallbackEmail(referrerID), int64(referralBonusDays), true)

	newUserLabel := formatUserLabel(peerID)

	if refPeerID, ok := parseVKUserID(referrerID); ok {
		refDays, _ := userStore.GetDays(referrerID)
		refCount := userStore.GetReferralsCount(referrerID)
		refMsg := fmt.Sprintf("🎉 %s подтвердил подписку по вашей реферальной ссылке!\n\n🎁 вам начислено: +%d дней\n👥 всего рефералов: %d\n⏱ баланс: %d дн.", newUserLabel, referralBonusDays, refCount, refDays)
		_, _ = bot.SendMessage(refPeerID, refMsg, nil)
	}

	if logPeerID != 0 {
		adminMsg := fmt.Sprintf("✅ VK ID:%d подписался по рефералке пользователя %s. Пригласившему начислено +%d дней", peerID, referrerID, referralBonusDays)
		_, _ = bot.SendMessage(logPeerID, adminMsg, nil)
	}

	return true, nil
}

// ────────────────────────────────────────────────────────────────
// Handlers
// ────────────────────────────────────────────────────────────────

func handleStart(bot *vkbot.Bot, peerID int, session *UserSession, xrCfg *xraySettings, firstMessage string) {
	userIDStr := resolvedUserID(peerID)
	isNew := userStore.IsNewUser(userIDStr)

	// Parse referral from first message text
	referrerID := ""
	text := strings.TrimSpace(firstMessage)
	if strings.HasPrefix(text, "ref_") {
		refPart := strings.TrimPrefix(text, "ref_")
		if fields := strings.Fields(refPart); len(fields) > 0 {
			refPart = fields[0]
		}
		// referrer could be "vk_12345" or just "12345"
		if _, err := strconv.Atoi(refPart); err == nil {
			referrerID = "vk_" + refPart
		} else if strings.HasPrefix(refPart, "vk_") {
			referrerID = refPart
		}
	}

	startAction := "start"
	if isNew {
		startAction = "новый пользователь"
		if referrerID != "" {
			startAction = "новый пользователь по рефералке"
		}
	}
	logAction(bot, peerID, startAction, isNew)

	// Ad tag from message
	if strings.HasPrefix(text, "ad_") {
		adTag := strings.TrimPrefix(text, "ad_")
		if f := strings.Fields(adTag); len(f) > 0 {
			adStats.record(f[0], userIDStr)
		}
	}

	if referrerID != "" && referrerID != userIDStr {
		if err := userStore.RecordReferral(userIDStr, referrerID); err == nil {
			if ok, _ := userStore.ClaimStartBonus(userIDStr, "referral", time.Now()); ok {
				_ = userStore.AddDays(userIDStr, 7)
				_, _ = ensureXrayAccess(xrayCfg, userIDStr, fallbackEmail(userIDStr), 7, true)
			}

			subscribed, subErr := isGroupMember(bot, peerID)
			if subErr != nil {
				log.Printf("subscription check on start failed: %v", subErr)
				sendReferralSubscriptionPrompt(bot, peerID)
			} else if subscribed {
				if _, err := finalizeReferralAfterSubscription(bot, peerID, xrayCfg); err != nil {
					log.Printf("finalize referral on start failed: %v", err)
				}
			} else {
				sendReferralSubscriptionPrompt(bot, peerID)
			}
		} else {
			log.Printf("referral record failed: user=%s ref=%s err=%v", userIDStr, referrerID, err)
		}
	}

	session.PendingPlanID = ""
	_ = showMainMenu(bot, peerID, session)

	if claimed, err := userStore.IsStartBonusClaimed(userIDStr); err == nil && !claimed {
		sendGroupBonusOffer(bot, peerID)
	}
}

func handleGetVPN(bot *vkbot.Bot, peerID int, session *UserSession, xrCfg *xraySettings) {
	userIDStr := resolvedUserID(peerID)

	info, err := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), 0, true)
	if err != nil {
		log.Printf("ensureXrayAccess error: %v", err)
		_ = updateSessionText(bot, peerID, session, stateGetVPN, "Не удалось получить доступ. Напиши в поддержку.", mainMenuKeyboard())
		return
	}

	if err := sendAccess(info, userIDStr, peerID, 0, xrCfg, bot, session); err != nil {
		log.Printf("sendAccess error: %v", err)
		_ = updateSessionText(bot, peerID, session, stateGetVPN, "Не получилось отправить ссылку.", mainMenuKeyboard())
	}
}

func handleStatus(bot *vkbot.Bot, peerID int, session *UserSession, xrCfg *xraySettings) {
	userIDStr := resolvedUserID(peerID)

	info, _ := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), 0, false)
	days, _ := userStore.GetDays(userIDStr)
	if info != nil && info.daysLeft > 0 {
		days = info.daysLeft
	}

	email, _ := userStore.GetEmail(userIDStr)
	if strings.TrimSpace(email) == "" {
		email = "не указан"
	}
	refCount := userStore.GetReferralsCount(userIDStr)
	refBonus := refCount * referralBonusDays

	vkID := vkUserIDStr(peerID)
	idLine := fmt.Sprintf("vk_%d", peerID)
	if vkID != userIDStr {
		idLine += fmt.Sprintf(" → связан с %s", userIDStr)
	}
	header := fmt.Sprintf("👤 профиль\n• id: %s\n• mail: %s\n• рефералы: %d (дней: %d)", idLine, email, refCount, refBonus)

	var accessBlock string
	if days > 0 {
		expTime := time.Time{}
		if info != nil && !info.expireAt.IsZero() {
			expTime = info.expireAt
		} else {
			expTime = time.Now().UTC().Add(time.Duration(days) * 24 * time.Hour)
		}
		expStr := formatExpiryUTC(expTime)
		accessBlock = fmt.Sprintf("\n\nу вас есть доступ к neuravpn 🟢\nон активен ещё %d дней\nдо %s\n\nесли хотите продлить доступ - переходите в раздел «оплата»\nтам все очень дешево!", days, expStr)
	} else {
		accessBlock = "\n\nу вас нет доступа к neuravpn 🔴\n\nесли хотите продлить доступ - переходите в раздел «оплата»\nтам все очень дешево!"
	}

	profileText := header + accessBlock

	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{rawkbd.CallbackButton("💰 оплата", "nav_topup", "", "")},
			{rawkbd.CallbackButton("✏️ e-mail", "edit_email", "", "")},
			{rawkbd.CallbackButton("⬅️ меню", "nav_menu", "", "")},
		},
	}
	_ = updateSessionText(bot, peerID, session, stateStatus, profileText, kb)
}

func handleTopUp(bot *vkbot.Bot, peerID int, session *UserSession) {
	session.PendingPlanID = ""
	var builder strings.Builder
	builder.WriteString("💰 покупка доступа\nчем больше период — тем выгоднее!\n\nвыберите период ниже.\n\nтарифы:\n")
	for _, plan := range ratePlans {
		builder.WriteString(fmt.Sprintf("• %d дней — %.0f ₽\n", plan.Days, plan.Amount))
	}
	header := strings.TrimSuffix(builder.String(), "\n")
	_ = updateSessionText(bot, peerID, session, stateTopUp, header, rateKeyboard())
}

func handleRateSelection(bot *vkbot.Bot, peerID int, eventID string, session *UserSession, plan RatePlan) {
	session.PendingPlanID = plan.ID
	userIDStr := resolvedUserID(peerID)

	// Check if email is on file
	if email, _ := userStore.GetEmail(userIDStr); strings.TrimSpace(email) == "" {
		text := "📧 Для оплаты картой нужен e-mail для чека.\nОтправь e-mail следующим сообщением (пример: name@example.com)."
		kb := rawkbd.Markup{
			Buttons: [][]rawkbd.Button{
				{
					rawkbd.CallbackButton("⬅️ назад", "nav_topup", "", ""),
					rawkbd.CallbackButton("🏠 меню", "nav_menu", "", ""),
				},
			},
		}
		_ = updateSessionText(bot, peerID, session, stateCollectEmail, text, kb)
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "пришли e-mail")
		return
	}

	if err := startPaymentForPlan(bot, peerID, session, plan); err != nil {
		log.Printf("startPaymentForPlan error: %v", err)
		_ = updateSessionText(bot, peerID, session, stateTopUp, "Не удалось создать платёж.", mainMenuKeyboard())
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "ошибка оплаты")
		return
	}
	_ = bot.SendEventAnswer(eventID, peerID, peerID, "счёт создан")
}

func handleReferral(bot *vkbot.Bot, peerID int, session *UserSession) {
	userIDStr := resolvedUserID(peerID)
	refCode := fmt.Sprintf("ref_vk_%d", peerID)
	count := userStore.GetReferralsCount(userIDStr)
	bonus := count * referralBonusDays

	text := fmt.Sprintf(
		"🎁 +15 дней к доступу\n\n"+
			"кстати, у нас есть реферальная программа.\nприводишь друга → он подписывается на группу → получаешь +15 дней доступа.\n\n"+
			"🔗 твой реферальный код:\n%s\n\n"+
			"попроси друга написать этот код боту первым сообщением.\n\n"+
			"пришло друзей: %d\nнакопленный бонус: %d дней.",
		refCode, count, bonus,
	)
	groupURL := vkGroupURL
	if groupURL == "" {
		groupURL = fmt.Sprintf("https://vk.com/club%d", vkGroupID)
	}
	shareText := url.QueryEscape("подключай vpn, опробовав его бесплатно 7 дней! Напиши боту код " + refCode)
	shareURL := fmt.Sprintf("https://vk.com/share.php?url=%s&title=%s", url.QueryEscape(groupURL), shareText)

	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{rawkbd.URLButton("поделиться ссылкой", shareURL, "")},
			{rawkbd.CallbackButton("🏠 меню", "nav_menu", "", "")},
		},
	}
	_ = updateSessionText(bot, peerID, session, stateMenu, text, kb)
}

func handleSupport(bot *vkbot.Bot, peerID int, session *UserSession) {
	text := "📞 поддержка\n\nесть вопросы или предложения? пиши: @neuravpn_support\nответим лично, никаких почтовых ящиков."
	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{rawkbd.CallbackButton("🏠 меню", "nav_menu", "", "")},
		},
	}
	_ = updateSessionText(bot, peerID, session, stateMenu, text, kb)
}

func handleEditEmail(bot *vkbot.Bot, peerID int, session *UserSession) {
	text := "✏️ отправь новый e-mail сообщением."
	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{rawkbd.CallbackButton("⬅️ назад", "nav_status", "", "")},
		},
	}
	_ = updateSessionText(bot, peerID, session, stateEditEmail, text, kb)
}

func handleLinkAccount(bot *vkbot.Bot, peerID int, fromID int, session *UserSession, token string, xrCfg *xraySettings) {
	vkUserID := vkUserIDStr(peerID)

	// Check if already linked
	if existing, err := userStore.GetLinkedTo(vkUserID); err == nil && strings.TrimSpace(existing) != "" {
		_ = updateSessionText(bot, peerID, session, stateMenu,
			"ℹ️ аккаунт уже привязан к telegram.", mainMenuKeyboard())
		return
	}

	// Find TG user by token
	tgUserID, err := userStore.GetUserByLinkToken(token)
	if err != nil {
		log.Printf("link token lookup failed: %v", err)
		_ = updateSessionText(bot, peerID, session, stateMenu,
			"⚠️ ссылка недействительна или уже использована.\nпопробуйте создать новую ссылку в telegram-боте.", mainMenuKeyboard())
		return
	}

	if tgUserID == vkUserID {
		_ = updateSessionText(bot, peerID, session, stateMenu,
			"ℹ️ этот токен принадлежит текущему аккаунту.", mainMenuKeyboard())
		return
	}

	// Transfer VK days to TG user before linking
	vkDays, _ := userStore.GetDays(vkUserID)
	tgDays, _ := userStore.GetDays(tgUserID)
	if vkDays > 0 {
		if err := userStore.AddDays(tgUserID, vkDays); err != nil {
			log.Printf("link: AddDays to TG user failed: %v", err)
		}
		_ = userStore.SetDays(vkUserID, 0)
	}

	// Set linked_to: VK user → TG user
	if err := userStore.SetLinkedTo(vkUserID, tgUserID); err != nil {
		log.Printf("link: SetLinkedTo failed: %v", err)
		_ = updateSessionText(bot, peerID, session, stateMenu,
			"⚠️ ошибка привязки, попробуйте позже.", mainMenuKeyboard())
		return
	}

	// Update cache
	linkedAccountsMu.Lock()
	linkedAccounts[peerID] = tgUserID
	linkedAccountsMu.Unlock()

	// Clear the token so it can't be reused
	_ = userStore.ClearLinkToken(tgUserID)

	totalDays := tgDays + vkDays

	// Log to admin
	if logPeerID != 0 {
		logText := fmt.Sprintf("🔗 привязка: %s → %s\nvk_days=%d перенесено, итого у TG=%d",
			vkUserID, tgUserID, vkDays, totalDays)
		_, _ = bot.SendMessage(logPeerID, logText, nil)
	}

	resultText := fmt.Sprintf("✅ аккаунт привязан к telegram!\n\n"+
		"• перенесено дней из вк: %d\n"+
		"• итого у аккаунта: %d дней\n\n"+
		"теперь все данные (дни, ключ, почта) общие\nмежду telegram и вк.", vkDays, totalDays)

	_ = updateSessionText(bot, peerID, session, stateMenu, resultText, mainMenuKeyboard())
}

func handleInstructionsMenu(bot *vkbot.Bot, peerID int, session *UserSession) {
	instruct.ResetState(int64(peerID))
	text := "🛠️ инструкции\nвыбери платформу:"
	kb := rawkbd.Markup{
		Buttons: [][]rawkbd.Button{
			{
				rawkbd.CallbackButton("🖥️ Windows", "windows", "", ""),
				rawkbd.CallbackButton("📱 Android", "android", "", ""),
			},
			{
				rawkbd.CallbackButton("🍏 iOS", "ios", "", ""),
				rawkbd.CallbackButton("💻 MacOS", "macos", "", ""),
			},
			{rawkbd.CallbackButton("🏠 меню", "nav_menu", "", "")},
		},
	}
	_ = updateSessionText(bot, peerID, session, stateInstructions, text, kb)
}

func startInstructionFlow(bot *vkbot.Bot, peerID int, session *UserSession, xrCfg *xraySettings, platform instruct.InstructType, step int) error {
	prevMessageID := session.MessageID
	instruct.ResetState(int64(peerID))

	userIDStr := resolvedUserID(peerID)
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
	instruct.SetInstructionKey(int64(peerID), key)

	var msgID int
	var err error

	switch platform {
	case instruct.Windows:
		msgID, err = instruct.InstructionWindows(int64(peerID), bot, step)
	case instruct.Android:
		msgID, err = instruct.InstructionAndroid(int64(peerID), bot, step)
	case instruct.IOS:
		msgID, err = instruct.InstructionIos(int64(peerID), bot, step)
	case instruct.MacOS:
		msgID, err = instruct.InstructionMacOS(int64(peerID), bot, step)
	default:
		return fmt.Errorf("unsupported instruction platform: %v", platform)
	}
	if err != nil {
		return err
	}

	if prevMessageID != 0 {
		_ = bot.DeleteMessage(peerID, []int{prevMessageID})
	}
	session.MessageID = msgID
	session.State = stateInstructions
	session.ContentType = "photo"
	return nil
}

func handleClaimSubscriptionBonus(bot *vkbot.Bot, peerID int, eventID string, session *UserSession, xrCfg *xraySettings) {
	userIDStr := resolvedUserID(peerID)

	ok, err := isGroupMember(bot, peerID)
	if err != nil {
		log.Printf("subscription check failed: %v", err)
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "не удалось проверить подписку")
		return
	}
	if !ok {
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "сначала подпишитесь на группу")
		return
	}

	refRewardGranted := false
	if granted, err := finalizeReferralAfterSubscription(bot, peerID, xrCfg); err != nil {
		log.Printf("finalize referral on claim_sub_bonus failed: %v", err)
	} else {
		refRewardGranted = granted
	}

	if claimed, err := userStore.IsStartBonusClaimed(userIDStr); err == nil && claimed {
		if refRewardGranted {
			_ = bot.SendEventAnswer(eventID, peerID, peerID, "пригласившему начислено +15 дней")
		} else {
			_ = bot.SendEventAnswer(eventID, peerID, peerID, "бонус уже получен")
		}
		return
	}

	if ok, err := userStore.ClaimStartBonus(userIDStr, "channel", time.Now()); err != nil {
		log.Printf("claim start bonus failed: %v", err)
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "не удалось начислить бонус")
		return
	} else if !ok {
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "бонус уже получен")
		return
	}

	if err := userStore.AddDays(userIDStr, int64(channelBonusDays)); err != nil {
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "не удалось начислить дни")
		return
	}

	info, err := ensureXrayAccess(xrCfg, userIDStr, fallbackEmail(userIDStr), int64(channelBonusDays), true)
	if err != nil {
		log.Printf("ensureXrayAccess bonus error: %v", err)
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "не удалось выдать доступ")
		return
	}

	_ = sendAccess(info, userIDStr, peerID, channelBonusDays, xrCfg, bot, session)
	if refRewardGranted {
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "бонус выдан, пригласившему +15 дней")
	} else {
		_ = bot.SendEventAnswer(eventID, peerID, peerID, "бонус выдан")
	}
}

// ────────────────────────────────────────────────────────────────
// Admin commands (text-based)
// ────────────────────────────────────────────────────────────────

func isAdmin(userID int) bool {
	for _, id := range adminIDs {
		if id == int64(userID) {
			return true
		}
	}
	return false
}

func handleAdminAdd(bot *vkbot.Bot, peerID int, args string, xrCfg *xraySettings) {
	parts := strings.Fields(args)
	if len(parts) != 2 {
		_, _ = bot.SendMessage(peerID, "Использование: /add userID days\nПример: /add vk_123456789 30", nil)
		return
	}
	targetUserID := strings.TrimSpace(parts[0])
	days, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || days <= 0 {
		_, _ = bot.SendMessage(peerID, "❌ Количество дней должно быть положительным числом", nil)
		return
	}

	info, err := ensureXrayAccess(xrCfg, targetUserID, fallbackEmail(targetUserID), days, true)
	if err != nil {
		_, _ = bot.SendMessage(peerID, "❌ Ошибка: "+err.Error(), nil)
		return
	}
	text := fmt.Sprintf("✅ Пользователю %s добавлено %d дн.\nОсталось дней: %d", targetUserID, days, info.daysLeft)
	_, _ = bot.SendMessage(peerID, text, nil)
}

func handleAdminNotify(bot *vkbot.Bot, peerID int, broadcastText string) {
	if strings.TrimSpace(broadcastText) == "" {
		_, _ = bot.SendMessage(peerID, "Укажите текст для рассылки: /notify <текст>", nil)
		return
	}

	go func() {
		var userIDs []string
		var gerr error
		if pg, ok := userStore.(interface{ GetAllUserIDs() ([]string, error) }); ok {
			userIDs, gerr = pg.GetAllUserIDs()
		} else if sq, ok := userStore.(interface {
			GetAllUsers() map[string]sqlite.UserData
		}); ok {
			for id := range sq.GetAllUsers() {
				userIDs = append(userIDs, id)
			}
		} else {
			gerr = fmt.Errorf("userStore не поддерживает массовое получение id")
		}
		if gerr != nil {
			_, _ = bot.SendMessage(peerID, "Ошибка получения пользователей: "+gerr.Error(), nil)
			return
		}
		count := 0
		for _, uid := range userIDs {
			target, ok := parseVKUserID(uid)
			if !ok {
				continue
			}
			if _, err := bot.SendMessage(target, broadcastText, nil); err == nil {
				count++
			}
			time.Sleep(50 * time.Millisecond)
		}
		_, _ = bot.SendMessage(peerID, fmt.Sprintf("Рассылка завершена. Доставлено: %d", count), nil)
	}()
	_, _ = bot.SendMessage(peerID, "Рассылка запущена", nil)
}

func handleAdminSyncInbounds(bot *vkbot.Bot, peerID int, xrCfg *xraySettings, activeOnly bool) {
	inboundIDs := xrCfg.inboundIDs
	if len(inboundIDs) == 0 {
		inbounds, err := xrCfg.client.GetAllInbounds()
		if err != nil {
			_, _ = bot.SendMessage(peerID, "ошибка загрузки инбаундов: "+err.Error(), nil)
			return
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				inboundIDs = append(inboundIDs, ib.ID)
			}
		}
	}
	if len(inboundIDs) == 0 {
		_, _ = bot.SendMessage(peerID, "нет доступных инбаундов для синхронизации", nil)
		return
	}

	var userIDs []string
	if pg, ok := userStore.(interface{ GetAllUserIDs() ([]string, error) }); ok {
		ids, err := pg.GetAllUserIDs()
		if err != nil {
			_, _ = bot.SendMessage(peerID, "ошибка получения пользователей: "+err.Error(), nil)
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
		_, _ = bot.SendMessage(peerID, "пользователи не найдены в хранилище", nil)
		return
	}

	updated, failed, skippedInactive := 0, 0, 0
	for _, uid := range userIDs {
		if activeOnly {
			days, err := userStore.GetDays(uid)
			if err != nil || days <= 0 {
				skippedInactive++
				continue
			}
		}
		email := fallbackEmail(uid)
		subID, _ := userStore.EnsureSubscriptionID(uid)
		c, _, err := xrCfg.client.EnsureClientAcrossInbounds(inboundIDs, uid, email, 0, subID)
		if err != nil {
			failed++
			continue
		}
		if c != nil {
			updated++
		}
		time.Sleep(20 * time.Millisecond)
	}

	text := fmt.Sprintf("Синхронизация завершена. Обновлено: %d, ошибок: %d", updated, failed)
	if activeOnly {
		text += fmt.Sprintf(", пропущено неактивных: %d", skippedInactive)
	}
	_, _ = bot.SendMessage(peerID, text, nil)
}

func handleAdminMigrateUsers(bot *vkbot.Bot, peerID int, xrCfg *xraySettings) {
	inboundIDs := xrCfg.inboundIDs
	if len(inboundIDs) == 0 {
		inbounds, err := xrCfg.client.GetAllInbounds()
		if err != nil {
			_, _ = bot.SendMessage(peerID, "❌ Ошибка загрузки инбаундов: "+err.Error(), nil)
			return
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				inboundIDs = append(inboundIDs, ib.ID)
			}
		}
	}
	if len(inboundIDs) == 0 {
		_, _ = bot.SendMessage(peerID, "❌ Нет доступных инбаундов для миграции", nil)
		return
	}

	var userIDs []string
	if pg, ok := userStore.(interface{ GetAllUserIDs() ([]string, error) }); ok {
		ids, err := pg.GetAllUserIDs()
		if err != nil {
			_, _ = bot.SendMessage(peerID, "❌ Ошибка получения пользователей: "+err.Error(), nil)
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
		_, _ = bot.SendMessage(peerID, "❌ Пользователи не найдены", nil)
		return
	}

	migrated, failed, skipped := 0, 0, 0
	for _, uid := range userIDs {
		days, err := userStore.GetDays(uid)
		if err != nil || days <= 0 {
			skipped++
			continue
		}
		email := fallbackEmail(uid)
		subID, _ := userStore.EnsureSubscriptionID(uid)
		_, _, err = xrCfg.client.EnsureClientAcrossInbounds(inboundIDs, uid, email, days, subID)
		if err != nil {
			failed++
			continue
		}
		migrated++
		time.Sleep(50 * time.Millisecond)
	}

	text := fmt.Sprintf("✅ Миграция завершена!\n\nВсего: %d\n✅ Мигрировано: %d\n⏭ Пропущено: %d\n❌ Ошибок: %d", len(userIDs), migrated, skipped, failed)
	_, _ = bot.SendMessage(peerID, text, nil)
}

func handleAdminMigrateExpiryFromOld(bot *vkbot.Bot, peerID int) {
	if oldXrayCfg == nil || oldXrayCfg.client == nil {
		_, _ = bot.SendMessage(peerID, "❌ Старый Xray сервер не настроен", nil)
		return
	}

	oldInboundIDs := oldXrayCfg.inboundIDs
	if len(oldInboundIDs) == 0 {
		inbounds, err := oldXrayCfg.client.GetAllInbounds()
		if err != nil {
			_, _ = bot.SendMessage(peerID, "❌ Ошибка загрузки инбаундов старого сервера: "+err.Error(), nil)
			return
		}
		for _, ib := range inbounds {
			if ib.Enable && strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
				oldInboundIDs = append(oldInboundIDs, ib.ID)
			}
		}
	}
	if len(oldInboundIDs) == 0 {
		_, _ = bot.SendMessage(peerID, "❌ Не найдены инбаунды на старом сервере", nil)
		return
	}

	type oldClientInfo struct {
		tgID       string
		expiryTime int64
	}
	oldClientsMap := make(map[string]oldClientInfo)
	for _, inboundID := range oldInboundIDs {
		clients, err := oldXrayCfg.client.GetInboundById(inboundID)
		if err != nil {
			continue
		}
		for _, c := range clients {
			tgID := strings.TrimSpace(c.TgID)
			if tgID == "" {
				continue
			}
			if old, exists := oldClientsMap[tgID]; !exists || c.ExpiryTime > old.expiryTime {
				oldClientsMap[tgID] = oldClientInfo{tgID: tgID, expiryTime: c.ExpiryTime}
			}
		}
	}

	if len(oldClientsMap) == 0 {
		_, _ = bot.SendMessage(peerID, "❌ Клиенты не найдены на старом сервере", nil)
		return
	}

	updated, skipped, failed := 0, 0, 0
	for _, oldClient := range oldClientsMap {
		expireAt := time.UnixMilli(oldClient.expiryTime)
		remain := time.Until(expireAt)
		daysLeft := int64(0)
		if remain > 0 {
			daysLeft = int64(remain.Hours()/24 + 0.999)
		}
		if daysLeft <= 0 {
			skipped++
			continue
		}
		if err := userStore.SetDays(oldClient.tgID, daysLeft); err != nil {
			failed++
			continue
		}

		newInboundIDs := xrayCfg.inboundIDs
		if len(newInboundIDs) == 0 && xrayCfg.inboundID > 0 {
			newInboundIDs = []int{xrayCfg.inboundID}
		}
		setExpireAt := time.Now().Add(time.Duration(daysLeft) * 24 * time.Hour)
		for _, inboundID := range newInboundIDs {
			c, err := xrayCfg.client.GetClientByTelegram(inboundID, oldClient.tgID)
			if err != nil || c == nil {
				continue
			}
			c.ExpiryTime = setExpireAt.UnixMilli()
			_ = xrayCfg.client.UpdateClient(inboundID, *c)
		}
		updated++
		time.Sleep(20 * time.Millisecond)
	}

	text := fmt.Sprintf("✅ Миграция сроков завершена!\n\nВсего: %d\n✅ Обновлено: %d\n⏭ Истекло: %d\n❌ Ошибок: %d", len(oldClientsMap), updated, skipped, failed)
	_, _ = bot.SendMessage(peerID, text, nil)
}

func handleAdLink(bot *vkbot.Bot, peerID int, args string) {
	parts := strings.Fields(args)
	if len(parts) < 1 {
		_, _ = bot.SendMessage(peerID, "использование: /adlink <канал> [ид_поста]", nil)
		return
	}
	channel, postID := parseAdInput(parts[0])
	if channel == "" {
		_, _ = bot.SendMessage(peerID, "укажи канал, например @mychannel", nil)
		return
	}
	if len(parts) > 1 {
		postID = parts[1]
	}
	if postID == "" {
		postID = randomSlug(8)
	}
	tag := fmt.Sprintf("%s/%s", channel, postID)
	startParam := "ad_" + tag
	text := fmt.Sprintf("Рекламная ссылка:\nканал: @%s\nпост: %s\nкод для бота: %s", channel, postID, startParam)
	_, _ = bot.SendMessage(peerID, text, nil)
}

func handleAdCheck(bot *vkbot.Bot, peerID int, args string) {
	parts := strings.Fields(args)
	if len(parts) < 1 {
		_, _ = bot.SendMessage(peerID, "использование: /adcheck <канал>", nil)
		return
	}
	channel, _ := parseAdInput(parts[0])
	stats := adStats.statsForChannel(channel)
	if len(stats) == 0 {
		_, _ = bot.SendMessage(peerID, fmt.Sprintf("нет данных по каналу @%s", channel), nil)
		return
	}
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
		b.WriteString(fmt.Sprintf("• пост %s — %d переходов\n", post, it.count))
	}
	_, _ = bot.SendMessage(peerID, b.String(), nil)
}

// ────────────────────────────────────────────────────────────────
// Log actions (admin notification)
// ────────────────────────────────────────────────────────────────

func logAction(bot *vkbot.Bot, peerID int, action string, isNew bool) {
	now := time.Now()
	uid := int64(peerID)
	logSessionMu.Lock()
	ls := logSessions[uid]
	if ls == nil || now.Sub(ls.Last) > 10*time.Minute {
		ls = &logSession{Start: now, Last: now, Actions: []string{}, IsNew: isNew}
		logSessions[uid] = ls
	}
	if isNew {
		ls.IsNew = true
	}
	ls.Last = now
	if strings.TrimSpace(action) != "" && (len(ls.Actions) == 0 || ls.Actions[len(ls.Actions)-1] != action) {
		ls.Actions = append(ls.Actions, action)
	}
	logSessionMu.Unlock()

	if logPeerID == 0 {
		return
	}

	userLink := fmt.Sprintf("https://vk.com/id%d (ID:%d)", peerID, peerID)

	for {
		logSessionMu.Lock()
		ls = logSessions[uid]
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
		mins := int(dur.Minutes())
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
				log.Printf("[TEST MODE] log action: %s", text)
			} else {
				if sent, err := bot.SendMessage(logPeerID, text, nil); err == nil {
					newMsgID = sent
				} else {
					log.Printf("log action send failed: %v", err)
				}
			}
		} else {
			if testMode {
				log.Printf("[TEST MODE] log action update: %s", text)
			} else {
				if err := bot.EditMessage(logPeerID, msgID, text, nil); err != nil {
					if sent, err2 := bot.SendMessage(logPeerID, text, nil); err2 == nil {
						newMsgID = sent
					} else {
						log.Printf("log action edit failed: %v; fallback send failed: %v", err, err2)
					}
				}
			}
		}

		logSessionMu.Lock()
		ls = logSessions[uid]
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

// ────────────────────────────────────────────────────────────────
// Utility
// ────────────────────────────────────────────────────────────────

func parseAdInput(raw string) (channel, post string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	raw = strings.TrimPrefix(raw, "@")
	if strings.HasPrefix(raw, "https://") || strings.HasPrefix(raw, "http://") {
		if u, err := url.Parse(raw); err == nil {
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
	if name, ok := actionMap[data]; ok {
		return name
	}
	return data
}

// ────────────────────────────────────────────────────────────────
// VK Event (callback button) handler
// ────────────────────────────────────────────────────────────────

func handleEvent(bot *vkbot.Bot, obj events.MessageEventObject, xrCfg *xraySettings) {
	peerID := obj.PeerID
	userID := obj.UserID
	eventID := obj.EventID

	// Parse callback data from payload
	var payload struct {
		Cmd string `json:"cmd"`
	}
	if err := json.Unmarshal(obj.Payload, &payload); err != nil {
		log.Printf("event payload parse error: %v", err)
		return
	}
	data := payload.Cmd
	if data == "" {
		return
	}

	session := getSession(peerID)

	// Log action (skip navigation/instruction steps)
	actionName := getActionName(data)
	if actionName != "" &&
		!(strings.HasPrefix(data, "win_prev_") || strings.HasPrefix(data, "win_next_") ||
			strings.HasPrefix(data, "android_prev_") || strings.HasPrefix(data, "android_next_") ||
			strings.HasPrefix(data, "ios_prev_") || strings.HasPrefix(data, "ios_next_") ||
			strings.HasPrefix(data, "macos_prev_") || strings.HasPrefix(data, "macos_next_") ||
			strings.HasSuffix(data, "_current") || data == "nav_menu") {
		logAction(bot, peerID, actionName, false)
	}

	ackText := ""

	switch {
	case data == "nav_menu":
		_ = showMainMenu(bot, peerID, session)
	case data == "nav_get_vpn":
		handleGetVPN(bot, peerID, session, xrCfg)
	case data == "nav_topup":
		handleTopUp(bot, peerID, session)
	case data == "nav_status":
		handleStatus(bot, peerID, session, xrCfg)
	case data == "nav_referral":
		handleReferral(bot, peerID, session)
	case data == "nav_support":
		handleSupport(bot, peerID, session)
	case data == "edit_email":
		handleEditEmail(bot, peerID, session)
	case data == "nav_instructions":
		handleInstructionsMenu(bot, peerID, session)
	case data == "claim_sub_bonus":
		handleClaimSubscriptionBonus(bot, peerID, eventID, session, xrCfg)
		return
	case data == "windows":
		if err := startInstructionFlow(bot, peerID, session, xrCfg, instruct.Windows, 0); err != nil {
			log.Printf("windows instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "android":
		if err := startInstructionFlow(bot, peerID, session, xrCfg, instruct.Android, 0); err != nil {
			log.Printf("android instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "ios":
		if err := startInstructionFlow(bot, peerID, session, xrCfg, instruct.IOS, 0); err != nil {
			log.Printf("ios instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "macos":
		if err := startInstructionFlow(bot, peerID, session, xrCfg, instruct.MacOS, 0); err != nil {
			log.Printf("macos instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case strings.HasPrefix(data, "win_prev_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "win_prev_")); err == nil {
			if msgID, err := instruct.InstructionWindows(int64(peerID), bot, n-1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "win_next_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "win_next_")); err == nil {
			if msgID, err := instruct.InstructionWindows(int64(peerID), bot, n+1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "android_prev_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "android_prev_")); err == nil {
			if msgID, err := instruct.InstructionAndroid(int64(peerID), bot, n-1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "android_next_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "android_next_")); err == nil {
			if msgID, err := instruct.InstructionAndroid(int64(peerID), bot, n+1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "ios_prev_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "ios_prev_")); err == nil {
			if msgID, err := instruct.InstructionIos(int64(peerID), bot, n-1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "ios_next_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "ios_next_")); err == nil {
			if msgID, err := instruct.InstructionIos(int64(peerID), bot, n+1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "macos_prev_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "macos_prev_")); err == nil {
			if msgID, err := instruct.InstructionMacOS(int64(peerID), bot, n-1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "macos_next_"):
		if n, err := strconv.Atoi(strings.TrimPrefix(data, "macos_next_")); err == nil {
			if msgID, err := instruct.InstructionMacOS(int64(peerID), bot, n+1); err == nil && msgID != 0 {
				session.MessageID = msgID
				session.State = stateInstructions
			}
		}
	case strings.HasPrefix(data, "rate_"):
		id := strings.TrimPrefix(data, "rate_")
		if p, ok := ratePlanByID[id]; ok {
			handleRateSelection(bot, peerID, eventID, session, p)
			return
		}
	case data == "check_payment":
		handleCheckPayment(bot, peerID, userID, eventID, session, xrCfg)
		return
	}

	_ = bot.SendEventAnswer(eventID, peerID, userID, ackText)
}

// ────────────────────────────────────────────────────────────────
// VK Message handler
// ────────────────────────────────────────────────────────────────

func handleMessage(bot *vkbot.Bot, msg events.MessageNewObject, xrCfg *xraySettings) {
	peerID := msg.Message.PeerID
	fromID := msg.Message.FromID
	text := strings.TrimSpace(msg.Message.Text)
	session := getSession(peerID)

	// Admin commands (prefix with /)
	if strings.HasPrefix(text, "/") && isAdmin(fromID) {
		parts := strings.SplitN(text, " ", 2)
		cmd := strings.TrimPrefix(parts[0], "/")
		args := ""
		if len(parts) > 1 {
			args = parts[1]
		}
		switch cmd {
		case "add":
			handleAdminAdd(bot, peerID, args, xrCfg)
			return
		case "notify":
			handleAdminNotify(bot, peerID, args)
			return
		case "sync_inbounds":
			handleAdminSyncInbounds(bot, peerID, xrCfg, false)
			return
		case "sync_active_inbounds":
			handleAdminSyncInbounds(bot, peerID, xrCfg, true)
			return
		case "migrate_users":
			handleAdminMigrateUsers(bot, peerID, xrCfg)
			return
		case "migrate_expiry_from_old":
			handleAdminMigrateExpiryFromOld(bot, peerID)
			return
		case "adlink":
			handleAdLink(bot, peerID, args)
			return
		case "adcheck":
			handleAdCheck(bot, peerID, args)
			return
		}
	}

	// State: collect email
	if session.State == stateCollectEmail {
		userIDStr := resolvedUserID(fromID)
		addr, err := mail.ParseAddress(text)
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(bot, peerID, session, stateCollectEmail, "Неверный e-mail, пример: name@example.com", mainMenuKeyboard())
			return
		}
		_ = userStore.SetEmail(userIDStr, addr.Address)
		_ = userStore.AcceptPrivacy(userIDStr, time.Now())

		plan, ok := ratePlanByID[session.PendingPlanID]
		if !ok {
			_ = updateSessionText(bot, peerID, session, stateTopUp, "Тариф не найден, выбери заново.", rateKeyboard())
			return
		}
		if err := startPaymentForPlan(bot, peerID, session, plan); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, peerID, session, stateTopUp, "Не удалось создать платёж.", mainMenuKeyboard())
		}
		return
	}

	// State: edit email
	if session.State == stateEditEmail {
		userIDStr := resolvedUserID(fromID)
		addr, err := mail.ParseAddress(text)
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(bot, peerID, session, stateEditEmail, "Неверный e-mail.", mainMenuKeyboard())
			return
		}
		_ = userStore.SetEmail(userIDStr, addr.Address)
		handleStatus(bot, peerID, session, xrCfg)
		return
	}

	// link_<token> from Telegram link flow
	if strings.HasPrefix(strings.ToLower(text), "link_") {
		token := strings.TrimPrefix(text, "link_")
		token = strings.TrimPrefix(token, "Link_")
		token = strings.TrimSpace(token)
		if len(token) >= 8 {
			handleLinkAccount(bot, peerID, fromID, session, token, xrCfg)
			return
		}
	}

	// Referral code (first message): ref_vk_12345 or ref_12345
	if strings.HasPrefix(strings.ToLower(text), "ref_") {
		session.MessageID = 0
		handleStart(bot, peerID, session, xrCfg, text)
		return
	}

	// Ad tag
	if strings.HasPrefix(strings.ToLower(text), "ad_") {
		userIDStr := resolvedUserID(fromID)
		adTag := strings.TrimPrefix(text, "ad_")
		if f := strings.Fields(adTag); len(f) > 0 {
			adStats.record(f[0], userIDStr)
		}
	}

	// Default: show main menu (equivalent to /start)
	session.MessageID = 0
	handleStart(bot, peerID, session, xrCfg, text)
}

// ────────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────────

func main() {
	yookassaApiKey := os.Getenv("YOOKASSA_API_KEY")
	yookassaStoreID := os.Getenv("YOOKASSA_STORE_ID")
	vkToken := os.Getenv("VK_BOT_TOKEN")
	privacyURL = os.Getenv("PRIVACY_URL")
	dbDSN := strings.TrimSpace(os.Getenv("DB_DSN"))

	testMode = strings.ToLower(strings.TrimSpace(os.Getenv("TEST_MODE"))) == "true"
	if testMode {
		log.Println("🧪 TEST MODE ENABLED - using mock data")
	}
	tgBotName = strings.TrimSpace(os.Getenv("TG_BOT_NAME"))

	// VK group settings
	if v := strings.TrimSpace(os.Getenv("VK_GROUP_ID")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			vkGroupID = id
		}
	}
	vkGroupURL = strings.TrimSpace(os.Getenv("VK_GROUP_URL"))
	if vkGroupURL == "" && vkGroupID > 0 {
		vkGroupURL = fmt.Sprintf("https://vk.com/club%d", vkGroupID)
	}

	// Log peer ID (VK peer for admin logs)
	if v := strings.TrimSpace(os.Getenv("VK_LOG_PEER_ID")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			logPeerID = id
		}
	}

	// Admin IDs
	adminIDsStr := os.Getenv("ADMIN_IDS")
	if adminIDsStr != "" {
		for _, idStr := range strings.Split(adminIDsStr, ",") {
			idStr = strings.TrimSpace(idStr)
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				adminIDs = append(adminIDs, id)
			}
		}
	}
	adminIDs = append(adminIDs, 7968465778)

	// Xray setup
	xrayUser := os.Getenv("XRAY_USERNAME")
	xrayPass := os.Getenv("XRAY_PASSWORD")
	xrayHost := os.Getenv("XRAY_HOST")
	xrayPort := os.Getenv("XRAY_PORT")
	xrayBasePath := os.Getenv("XRAY_WEB_BASE_PATH")
	inboundID, _ := strconv.Atoi(os.Getenv("XRAY_INBOUND_ID"))
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

	// Old Xray for migration
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
				inboundIDs: oldInboundIDs,
			}
			log.Println("✅ Old Xray server connected for migration")
		}
	}

	// YooKassa
	yookassaClient = yookassa.New(yookassaStoreID, yookassaApiKey)
	if vkGroupURL != "" {
		yookassaClient.SetReturnURL(vkGroupURL)
	}

	// Storage
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

	// VK API init
	vk := api.NewVK(vkToken)
	if vkGroupID == 0 {
		log.Fatal("VK_GROUP_ID is required")
	}

	bot := vkbot.New(vk, vkGroupID)

	instruct.ValidateCustomEmojiIDs(bot)

	loadExpiryReminderState()
	startExpiryReminder(bot, xrayCfg)

	// Periodic Xray re-login
	go func() {
		for {
			time.Sleep(1 * time.Hour)
			if err := xClient.LoginToServer(); err != nil {
				log.Printf("[XRAY] re-login failed: %v", err)
				if logPeerID != 0 {
					_, _ = bot.SendMessage(logPeerID, "⚠️ Релогин к Xray завершился ошибкой", nil)
				}
			} else {
				log.Printf("[XRAY] re-login success")
			}
		}
	}()

	// VK Long Poll
	lp, err := longpoll.NewLongPoll(vk, vkGroupID)
	if err != nil {
		log.Fatalf("VK long poll init error: %v", err)
	}

	lp.MessageNew(func(ctx context.Context, obj events.MessageNewObject) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("❌ Panic в обработчике message: %v", r)
			}
		}()
		handleMessage(bot, obj, xrayCfg)
	})

	lp.MessageEvent(func(ctx context.Context, obj events.MessageEventObject) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("❌ Panic в обработчике event: %v", r)
			}
		}()
		handleEvent(bot, obj, xrayCfg)
	})

	log.Println("🚀 VK Бот запущен")

	if err := lp.Run(); err != nil {
		log.Fatalf("VK long poll error: %v", err)
	}
}
