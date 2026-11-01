package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	"github.com/Asort97/vpnBot/clients/postgres"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
)

const reissuePilotID = "623290294"

// A request started before rotation must not return newly issued credentials.
func writeAuthorizedSubscriptionEntry(w http.ResponseWriter, entry mergedSubscriptionCacheEntry, cacheState, userID, token string) {
	if userID == reissuePilotID && !isValidMergedSubscriptionToken(userID, token) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	writeMergedSubscriptionEntry(w, entry, cacheState)
}

var reissuePanelLocks sync.Map

func lockReissuePanel(userID string, cfg *xraySettings) func() {
	if userID != reissuePilotID || cfg == nil || cfg.client == nil {
		return func() {}
	}
	v, _ := reissuePanelLocks.LoadOrStore(deletionPanelID(cfg), &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

func reissuePublicState(userID string) (map[string]any, error) {
	if userID != reissuePilotID {
		return map[string]any{"allowed": false}, nil
	}
	r, err := currentReissue(userID)
	if err != nil {
		return nil, err
	}
	generation := r.Generation
	if generation == "" {
		generation = "initial"
	}
	return map[string]any{"allowed": true, "generation": generation, "pending": r.Generation != "" && !r.Active, "ready": r.Active}, nil
}

type reissueStore interface {
	GetSubscriptionReissue(context.Context, string) (postgres.SubscriptionReissue, error)
	BeginSubscriptionReissue(context.Context, string, string, map[string]bool) (postgres.SubscriptionReissue, error)
	ClaimSubscriptionReissues(context.Context, string) ([]postgres.SubscriptionReissue, error)
	SaveReissueBeforeState(context.Context, postgres.SubscriptionReissue, []byte) error
	CompleteSubscriptionReissue(context.Context, postgres.SubscriptionReissue) error
	SubscriptionReissuePanelReady(context.Context, string, string) (bool, error)
}

func currentReissue(userID string) (postgres.SubscriptionReissue, error) {
	if userID != reissuePilotID {
		return postgres.SubscriptionReissue{}, nil
	}
	s, ok := userStore.(reissueStore)
	if !ok {
		return postgres.SubscriptionReissue{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.GetSubscriptionReissue(ctx, userID)
}

func reissueKey(generation, panel, email string) string {
	h := hmac.New(sha256.New, []byte(mergedSubSecret))
	h.Write([]byte("reissue-key-v1\n" + generation + "\n" + panel + "\n" + email))
	b := h.Sum(nil)[:16]
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	key, _ := uuid.FromBytes(b)
	return key.String()
}

func reissuePanelReady(userID string, cfg *xraySettings) error {
	if userID != reissuePilotID {
		return nil
	}
	s, ok := userStore.(reissueStore)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ready, err := s.SubscriptionReissuePanelReady(ctx, userID, deletionPanelID(cfg))
	if err != nil {
		return err
	}
	if !ready {
		return errors.New("subscription reissue pending")
	}
	return nil
}

func requestSubscriptionReissue(userID, expected string) (postgres.SubscriptionReissue, error) {
	if userID != reissuePilotID {
		return postgres.SubscriptionReissue{}, errors.New("reissue pilot only")
	}
	unlock := lockReissuePanel(userID, xrayCfg)
	defer unlock()
	s, ok := userStore.(reissueStore)
	if !ok || xrayCfg == nil || mergedSubSecret == "" {
		return postgres.SubscriptionReissue{}, errors.New("reissue unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	panels := map[string]bool{}
	for _, cfg := range deviceProviderConfigs(xrayCfg) {
		panels[deletionPanelID(cfg)] = deletionPanelID(cfg) == deletionPanelID(xrayCfg)
	}
	r, err := s.BeginSubscriptionReissue(ctx, userID, expected, panels)
	if err == nil {
		clearReissueCaches(userID)
	}
	return r, err
}

func clearReissueCaches(userID string) {
	accessCache.Delete(userID)
	invalidateMergedSubscriptionCache(userID)
	deviceSnapshots.Lock()
	for k := range deviceSnapshots.values {
		if strings.HasPrefix(k, userID+":") {
			delete(deviceSnapshots.values, k)
		}
	}
	deviceSnapshots.Unlock()
}

func executeSubscriptionReissue(ctx context.Context, cfg *xraySettings, r postgres.SubscriptionReissue, s reissueStore) error {
	unlock := lockReissuePanel(r.UserID, cfg)
	defer unlock()
	before, err := cfg.client.ReissueRecords(ctx, r.UserID)
	if err != nil {
		return err
	}
	if r.Primary && len(before) == 0 {
		return errors.New("no primary subscription to reissue")
	}
	data, err := json.Marshal(before)
	if err != nil {
		return err
	}
	if err = s.SaveReissueBeforeState(ctx, r, data); err != nil {
		return err
	}
	for _, record := range before {
		var email string
		_ = json.Unmarshal(record.Client["email"], &email)
		key := reissueKey(r.Generation, r.Panel, email)
		if err = cfg.client.ApplyReissue(ctx, record, key, r.SubID); err != nil {
			return err
		}
	}
	after, err := cfg.client.ReissueRecords(ctx, r.UserID)
	if err != nil {
		return err
	}
	if len(before) != len(after) {
		return errors.New("reissue record count changed")
	}
	for _, record := range before {
		var email string
		_ = json.Unmarshal(record.Client["email"], &email)
		found := false
		for _, updated := range after {
			var other string
			_ = json.Unmarshal(updated.Client["email"], &other)
			if email == other {
				found = true
				if err = xray.VerifyReissue(record, updated, reissueKey(r.Generation, r.Panel, email), r.SubID); err != nil {
					return err
				}
				break
			}
		}
		if !found {
			return errors.New("reissued client missing")
		}
	}
	if err = s.CompleteSubscriptionReissue(ctx, r); err != nil {
		return err
	}
	clearReissueCaches(r.UserID)
	return nil
}

func startSubscriptionReissueWorkers() {
	s, ok := userStore.(reissueStore)
	if !ok {
		return
	}
	for _, cfg := range deviceProviderConfigs(xrayCfg) {
		go func(cfg *xraySettings) {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for range ticker.C {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				jobs, err := s.ClaimSubscriptionReissues(ctx, deletionPanelID(cfg))
				cancel()
				if err != nil {
					continue
				}
				for _, r := range jobs {
					if r.UserID != reissuePilotID {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					err = executeSubscriptionReissue(ctx, cfg, r, s)
					cancel()
					if err != nil {
						log.Printf("[reissue] retry scheduled user=%s panel=%s", r.UserID, r.Panel[:12])
					}
				}
			}
		}(cfg)
	}
}

func handleReissueCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery) {
	if cq == nil || cq.From == nil || cq.Message == nil {
		return
	}
	userID := strconv.FormatInt(cq.From.ID, 10)
	if userID != reissuePilotID || cq.Message.Chat.ID != cq.From.ID {
		_, _ = bot.Request(tgbotapi.NewCallback(cq.ID, "Недоступно"))
		return
	}
	_, _ = bot.Request(tgbotapi.NewCallback(cq.ID, ""))
	r, err := currentReissue(userID)
	if err != nil {
		_, _ = bot.Send(tgbotapi.NewMessage(cq.Message.Chat.ID, "Не удалось проверить подписку. Повтори позже."))
		return
	}
	generation := r.Generation
	if generation == "" {
		generation = "initial"
	}
	text := "Перевыпустить подписку?\n\nСтарые ссылки и ключи перестанут работать. На всех своих устройствах нужно будет добавить новую ссылку. Оплаченные дни и лимит сохранятся."
	keyboard := tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Перевыпустить", "reissue:confirm:"+generation)), tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Отмена", "nav_devices")))
	if strings.HasPrefix(cq.Data, "reissue:confirm:") {
		expected := strings.TrimPrefix(cq.Data, "reissue:confirm:")
		if expected == "initial" {
			expected = ""
		}
		r, err = requestSubscriptionReissue(userID, expected)
		if err != nil {
			text = "Перевыпуск пока недоступен. Повтори позже."
		} else {
			text = "Перевыпуск запущен. Новая ссылка появится в разделе «Подключить VPN» после завершения."
		}
		keyboard = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Проверить", "reissue:status")))
	} else if r.Generation != "" && !r.Active {
		text = "Подготавливаем новые ключи. Оплаченные дни сохраняются."
		keyboard = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Проверить", "reissue:status")))
	} else if cq.Data == "reissue:status" && r.Active {
		text = "Новая ссылка готова. Добавь её в приложение через «Подключить VPN»."
		keyboard = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Подключить VPN", "nav_get_vpn")))
	}
	msg := tgbotapi.NewEditMessageTextAndMarkup(cq.Message.Chat.ID, cq.Message.MessageID, text, keyboard)
	_, _ = bot.Send(msg)
}
