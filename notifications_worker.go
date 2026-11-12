package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	"github.com/Asort97/vpnBot/clients/lifecycle"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func window(age, start, width time.Duration) bool { return age >= start && age < start+width }

// Bounded windows avoid sending months of missed campaigns on first deployment.
func lifecycleKinds(u lifecycle.User, entered time.Time, referred bool, issued, expiry, now time.Time) []string {
	var kinds []string
	if !u.Bonus && !u.Paid && !u.Referred && !referred && !entered.IsZero() {
		for _, d := range []int{1, 3, 7} {
			if window(now.Sub(entered), time.Duration(d)*24*time.Hour, 24*time.Hour) {
				kinds = append(kinds, fmt.Sprintf("unclaimed_%d", d))
			}
		}
	}
	if !expiry.IsZero() && !u.Autopay {
		left := expiry.Sub(now)
		if left > 0 && left <= 24*time.Hour {
			kinds = append(kinds, "d1")
		} else if left > 48*time.Hour && left <= 72*time.Hour && (!u.Bonus || u.Paid) {
			kinds = append(kinds, "d3")
		}
		for _, d := range []int{0, 3, 7, 30} {
			if window(-left, time.Duration(d)*24*time.Hour, 24*time.Hour) {
				kind := "expired"
				if d > 0 {
					kind = fmt.Sprintf("expired_d%d", d)
				}
				kinds = append(kinds, kind)
			}
		}
	}
	if !issued.IsZero() && (expiry.IsZero() || expiry.After(now)) {
		if window(now.Sub(issued), 20*time.Minute, 24*time.Hour-20*time.Minute) {
			kinds = append(kinds, "help_20m")
		}
		if window(now.Sub(issued), 24*time.Hour, 24*time.Hour) {
			kinds = append(kinds, "help_24h")
		}
	}
	return kinds
}

func noticeOnboarding(id string) *onboardingInstructionFollowup {
	onboardingMu.Lock()
	defer onboardingMu.Unlock()
	f := onboardingFollowups[id]
	if f == nil {
		return nil
	}
	copy := *f
	return &copy
}
func noticeConnectionKnown(cfg *xraySettings, id string) (connected, known bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	configs := []*xraySettings{cfg}
	configs = append(configs, mergedProviderConfigsForSubscription()...)
	seen := map[*xraySettings]bool{}
	var total int64
	known = true
	for _, c := range configs {
		if c == nil || c.client == nil || seen[c] {
			continue
		}
		seen[c] = true
		scoped, err := c.client.WithContext(ctx)
		if err != nil {
			known = false
			continue
		}
		ids := c.inboundIDs
		if len(ids) == 0 && c.inboundID > 0 {
			ids = []int{c.inboundID}
		}
		clients, err := scoped.GetSubscriptionClients(ctx, id, ids)
		if err != nil {
			known = false
			continue
		}
		emails := map[string]bool{}
		for _, client := range clients {
			if emails[client.Email] {
				continue
			}
			emails[client.Email] = true
			traffic, e := scoped.GetClientTrafficByEmail(client.Email)
			if e != nil || traffic == nil {
				known = false
				continue
			}
			if traffic.Up > 0 {
				total += traffic.Up
			}
			if traffic.Down > 0 {
				total += traffic.Down
			}
			if total > onboardingTrafficThresholdBytes {
				return true, true
			}
		}
	}
	return false, known && len(seen) > 0
}

func legacyNoticeSent(id, stage string, expiry time.Time) bool {
	n, _ := strconv.ParseInt(id, 10, 64)
	expiryReminderMu.Lock()
	defer expiryReminderMu.Unlock()
	return expiryReminderState[n] != nil && expiryReminderState[n][stage] == expiry.UTC().Format(time.RFC3339Nano)
}

func sendNoticeCandidate(bot *tgbotapi.BotAPI, cfg *xraySettings, id, kind string, expiry time.Time) {
	u, err := notificationFacts(id)
	if err != nil {
		return
	}
	key := kind
	if strings.HasPrefix(kind, "expired") || kind == "d1" || kind == "d3" {
		key = kind + ":" + expiry.UTC().Format(time.RFC3339Nano)
		if legacyNoticeSent(id, kind, expiry) {
			return
		}
	}
	if kind == "d1" && u.Bonus && !u.Paid {
		connected, known := noticeConnectionKnown(cfg, id)
		if !known {
			return
		}
		kind = "trial_d1"
		if !connected {
			kind = "trial_help"
		}
	}
	if kind == "help_20m" {
		if f := noticeOnboarding(id); f != nil && (f.Sent || f.InstructionOpened || f.Connected) {
			return
		}
	}
	s := noticeTemplate(kind, key)
	sendLifecycleNotice(bot, id, s, func() bool {
		fresh, err := notificationFacts(id)
		if err != nil {
			return false
		}
		now := time.Now().UTC()
		if strings.HasPrefix(kind, "unclaimed_") {
			return !fresh.Bonus && !fresh.Paid && !fresh.Referred && !notices.snapshot(id).Referred
		}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		client, err := readPrimarySubscriptionClient(ctx, cfg, id)
		if err != nil || client == nil {
			return false
		}
		status := subscriptionStatusForClient(client, now)
		if strings.HasPrefix(kind, "expired") {
			return !fresh.Autopay && !status.Active && status.ExpiresAt != nil && status.ExpiresAt.Equal(expiry) && !fresh.LastPaidAt.After(expiry)
		}
		if kind == "d1" || kind == "d3" || strings.HasPrefix(kind, "trial_") {
			if fresh.Autopay || !status.Active || status.ExpiresAt == nil || !status.ExpiresAt.Equal(expiry) {
				return false
			}
			if strings.HasPrefix(kind, "trial_") && (!fresh.Bonus || fresh.Paid) {
				return false
			}
		}
		if strings.HasPrefix(kind, "help_") || strings.HasPrefix(kind, "trial_") {
			if !status.Active {
				return false
			}
			connected, known := noticeConnectionKnown(cfg, id)
			if !known {
				return false
			}
			if kind == "trial_d1" {
				return connected
			}
			if connected {
				return false
			}
			// Do not repeat help at trial expiry immediately after the 24-hour help.
			if kind == "trial_help" {
				for _, r := range notices.snapshot(id).Records {
					if strings.HasPrefix(r.Kind, "help_") && r.Status == "sent" && now.Sub(r.At) < 24*time.Hour {
						return false
					}
				}
			}
		}
		return true
	})
}

func processNoticePayments(bot *tgbotapi.BotAPI) {
	if notices.load() != nil || yookassaClient == nil {
		return
	}
	notices.mu.Lock()
	bills := map[string]noticeBill{}
	for id, u := range notices.state.Users {
		if u.Bill != nil && !u.Blocked {
			bills[id] = *u.Bill
		}
	}
	notices.mu.Unlock()
	for id, b := range bills {
		if _, ok := ratePlanByID[b.Plan]; !ok {
			continue
		}
		age := time.Since(b.CreatedAt)
		kind := ""
		if window(age, 5*time.Minute, 55*time.Minute) {
			kind = "payment_5m"
		}
		if window(age, 24*time.Hour, 24*time.Hour) {
			kind = "payment_24h"
		}
		if kind == "" {
			continue
		}
		sendLifecycleNotice(bot, id, noticeTemplate(kind, kind+":"+b.ID), func() bool {
			current := currentNoticeBill(id)
			if current == nil || current.ID != b.ID {
				return false
			}
			u, err := notificationFacts(id)
			if err != nil || u.Autopay || !u.LastPaidAt.Before(b.CreatedAt) {
				return false
			}
			applied, err := userStore.IsPaymentApplied(id, buildAppliedPaymentKey("yookassa", b.ID))
			if err != nil || applied {
				return false
			}
			payment, err := yookassaClient.GetYooKassaPaymentStatus(b.ID)
			current = currentNoticeBill(id)
			return err == nil && current != nil && current.ID == b.ID && exactUnpaid(payment, &b, id)
		})
	}
}

// One authoritative list tolerates deleted configured inbounds without hiding API errors.
func notificationExpirySnapshot(cfg *xraySettings) (map[int64]time.Time, error) {
	if cfg == nil || cfg.client == nil {
		return nil, fmt.Errorf("primary panel unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	scoped, err := cfg.client.WithContext(ctx)
	if err != nil {
		return nil, err
	}
	inbounds, err := scoped.GetAllInbounds()
	if err != nil {
		return nil, err
	}
	ids := cfg.inboundIDs
	if len(ids) == 0 && cfg.inboundID > 0 {
		ids = []int{cfg.inboundID}
	}
	allowed := map[int]bool{}
	for _, id := range ids {
		allowed[id] = true
	}
	result := map[int64]time.Time{}
	matched := false
	for _, inbound := range inbounds {
		if len(allowed) > 0 {
			if !allowed[inbound.ID] {
				continue
			}
		} else if !inbound.Enable || !strings.EqualFold(inbound.Protocol, "vless") {
			continue
		}
		matched = true
		var settings xray.InboundSettings
		if err := json.Unmarshal([]byte(inbound.Settings), &settings); err != nil {
			return nil, fmt.Errorf("invalid notification inbound settings: %d", inbound.ID)
		}
		for _, client := range settings.Clients {
			id, err := strconv.ParseInt(strings.TrimSpace(client.TgID), 10, 64)
			if err != nil || id <= 0 || client.ExpiryTime <= 0 {
				continue
			}
			expiry := time.UnixMilli(client.ExpiryTime)
			if expiry.After(result[id]) {
				result[id] = expiry
			}
		}
	}
	if !matched {
		return nil, fmt.Errorf("no configured notification inbounds exist")
	}
	return result, nil
}

func processLifecycleNotices(bot *tgbotapi.BotAPI, cfg *xraySettings) {
	store, ok := userStore.(lifecycle.Store)
	if !ok || notices.load() != nil {
		return
	}
	users, err := store.NotificationUsers()
	if err != nil {
		log.Printf("notification users: %v", err)
		return
	}
	expiries, expiryErr := notificationExpirySnapshot(cfg)
	if expiryErr != nil {
		log.Printf("notification expiry snapshot unavailable")
	}
	now := time.Now().UTC()
	for _, u := range users {
		n, err := strconv.ParseInt(u.ID, 10, 64)
		if err != nil || n <= 0 {
			continue
		}
		notices.mu.Lock()
		local := notices.user(u.ID)
		entered := local.EnteredAt
		referred := local.Referred
		blocked := local.Blocked
		incident, active, recovered := notices.state.IncidentID, notices.state.IncidentActive, notices.state.RecoveredAt
		incidentRecord := local.Records["incident:"+incident]
		gotIncident := incidentRecord != nil && incidentRecord.Status == "sent"
		notices.mu.Unlock()
		if blocked {
			continue
		}
		if active && (u.Bonus || u.Paid || !expiries[n].IsZero()) {
			sendLifecycleNotice(bot, u.ID, noticeTemplate("incident", "incident:"+incident), func() bool { return notices.incidentMatches(incident, true) })
		} else if !active && gotIncident && !recovered.IsZero() && now.Sub(recovered) < 7*24*time.Hour {
			sendLifecycleNotice(bot, u.ID, noticeTemplate("recovered", "recovered:"+incident), func() bool { return notices.incidentMatches(incident, false) })
		}
		if active {
			continue
		}
		// Old accounts use the persisted registration date, never rollout time.
		if entered.IsZero() {
			entered = u.CreatedAt
		}
		issued := time.Time{}
		if f := noticeOnboarding(u.ID); f != nil {
			issued = f.IssuedAt
		} else if u.Bonus {
			issued = u.BonusAt
		}
		for _, kind := range lifecycleKinds(u, entered, referred, issued, expiries[n], now) {
			if expiryErr != nil && !strings.HasPrefix(kind, "unclaimed_") {
				continue
			}
			sendNoticeCandidate(bot, cfg, u.ID, kind, expiries[n])
		}
		// Connection/payment outcomes are observations, not claims that a campaign caused them.
		observeNoticeOutcomes(bot, cfg, u)
	}
}

func observeNoticeOutcomes(bot *tgbotapi.BotAPI, cfg *xraySettings, u lifecycle.User) {
	notices.mu.Lock()
	local := notices.user(u.ID)
	tracked := false
	firstSent := time.Time{}
	for _, r := range local.Records {
		if r.Status == "sent" {
			tracked = true
			if firstSent.IsZero() || r.At.Before(firstSent) {
				firstSent = r.At
			}
		}
	}
	needConnection := tracked && local.ConnectedAt.IsZero() && (u.Bonus || u.Paid)
	paidNew := tracked && u.Paid && u.LastPaidAt.After(local.PaidAt) && !u.LastPaidAt.Before(firstSent)
	notices.mu.Unlock()
	if paidNew {
		if notices.change(u.ID, func(n *noticeUser) { n.PaidAt = u.LastPaidAt }) == nil {
			id, _ := strconv.ParseInt(u.ID, 10, 64)
			sendUserNotificationAdminLog(bot, id, "", "событие: оплата после уведомлений")
		}
	}
	if needConnection {
		connected, known := noticeConnectionKnown(cfg, u.ID)
		if known && connected {
			if notices.change(u.ID, func(n *noticeUser) { n.ConnectedAt = time.Now().UTC() }) == nil {
				id, _ := strconv.ParseInt(u.ID, 10, 64)
				sendUserNotificationAdminLog(bot, id, "", "событие: подключение подтверждено трафиком")
			}
		}
	}
}

func startLifecycleNotices(bot *tgbotapi.BotAPI, cfg *xraySettings) {
	if err := notices.load(); err != nil {
		log.Printf("notification worker disabled: %v", err)
		return
	}
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			runNoticeTick(func() {
				processNoticePendingAutopay(bot)
				processNoticePayments(bot)
				processNoticeAutopayFailures(bot)
				processNoticeOnboarding(bot, cfg)
			})
			<-ticker.C
		}
	}()
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for {
			runNoticeTick(func() { processLifecycleNotices(bot, cfg) })
			select {
			case <-ticker.C:
			case <-noticeWake:
			}
		}
	}()
}

func runNoticeTick(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("notification worker panic: %v", r)
		}
	}()
	fn()
}

func processNoticeOnboarding(bot *tgbotapi.BotAPI, cfg *xraySettings) {
	now := time.Now()
	due := map[string]string{}
	onboardingMu.Lock()
	for id, f := range onboardingFollowups {
		if f == nil || f.IssuedAt.IsZero() {
			continue
		}
		age := now.Sub(f.IssuedAt)
		if window(age, 20*time.Minute, 24*time.Hour-20*time.Minute) {
			due[id] = "help_20m"
		}
		if window(age, 24*time.Hour, 24*time.Hour) {
			due[id] = "help_24h"
		}
	}
	onboardingMu.Unlock()
	for id, kind := range due {
		sendNoticeCandidate(bot, cfg, id, kind, time.Time{})
	}
}

var noticeWake = make(chan struct{}, 1)

func sendAutopayFailureNotice(bot *tgbotapi.BotAPI, id string, expiry time.Time) {
	if err := notices.change(id, func(u *noticeUser) { u.AutopayFailureExpiry = expiry; u.AutopayFailureAt = time.Now().UTC() }); err != nil {
		log.Printf("autopay failure queue: %v", err)
		return
	}
	processNoticeAutopayFailures(bot)
}

func processNoticeAutopayFailures(bot *tgbotapi.BotAPI) {
	notices.mu.Lock()
	pending := map[string]noticeUser{}
	for id, u := range notices.state.Users {
		if !u.AutopayFailureAt.IsZero() && time.Since(u.AutopayFailureAt) < 7*24*time.Hour {
			pending[id] = noticeUser{AutopayFailureExpiry: u.AutopayFailureExpiry, AutopayFailureAt: u.AutopayFailureAt}
		}
	}
	notices.mu.Unlock()
	for id, queued := range pending {
		sendLifecycleNotice(bot, id, noticeTemplate("autopay_failed", "autopay_failed:"+queued.AutopayFailureExpiry.UTC().Format(time.RFC3339Nano)), func() bool {
			u, e := notificationFacts(id)
			return e == nil && !u.Autopay && u.LastPaidAt.Before(queued.AutopayFailureAt)
		})
	}
}
