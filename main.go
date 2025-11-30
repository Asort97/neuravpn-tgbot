package main

import (
	"context"
	"fmt"
	"html"
	"log"
	"net/mail"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	instruct "github.com/Asort97/vpnBot/clients/instruction"
	pgstore "github.com/Asort97/vpnBot/clients/postgres"
	sqlite "github.com/Asort97/vpnBot/clients/sqLite"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const startText = `Привет! <b>Добро пожаловать в HappyCat VPN</b> 😺🔒

Здесь ты можешь:
• Получить или продлить доступ к VPN
• Оплатить дни и сразу активировать
• Узнать статус и остаток
• Пригласить друзей и получать бонусы
• Открыть подробные инструкции
• Связаться с поддержкой 24/7

<a href="https://t.me/happycatvpn">Наш новостной канал</a> 📰

Выбирай нужный раздел ниже 👇`

// throttling map (keyed by user id and action key)
var lastActionKey = make(map[int64]map[string]time.Time)

type SessionState string

const (
	stateMenu         SessionState = "menu"
	stateGetVPN       SessionState = "get_vpn"
	stateTopUp        SessionState = "top_up"
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

type DataStore interface {
	AddDays(userID string, days int64) error
	GetDays(userID string) (int64, error)
	SetDays(userID string, days int64) error
	GetEmail(userID string) (string, error)
	SetEmail(userID, email string) error
	AcceptPrivacy(userID string, at time.Time) error
	IsNewUser(userID string) bool
	RecordReferral(newUserID, referrerID string) error
	GetReferralsCount(userID string) int
}

var ratePlans = []RatePlan{
	{ID: "15d", Title: "15 дней", Amount: 25, Days: 15},
	{ID: "30d", Title: "30 дней", Amount: 50, Days: 30},
	{ID: "60d", Title: "60 дней", Amount: 100, Days: 60},
	{ID: "120d", Title: "120 дней", Amount: 200, Days: 120},
	{ID: "240d", Title: "240 дней", Amount: 300, Days: 240},
	{ID: "365d", Title: "365 дней", Amount: 400, Days: 365},
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
	serverAddress string
	serverPort    int
	serverName    string
	publicKey     string
	shortID       string
	spiderX       string
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
	privacyURL     string
	adminIDs       []int64
	userSessions   = make(map[int64]*UserSession)
)

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

func ensureXrayAccess(cfg *xraySettings, telegramUser string, email string, addDays int64, createIfMissing bool) (*accessInfo, error) {
	if cfg == nil || cfg.client == nil {
		return nil, fmt.Errorf("xray not configured")
	}

	client, err := cfg.client.GetClientByTelegram(cfg.inboundID, telegramUser)
	if err != nil {
		return nil, err
	}

	if client == nil {
		if !createIfMissing && addDays == 0 {
			return nil, nil
		}
		client = &xray.Client{
			Enable:  true,
			Flow:    "xtls-rprx-vision",
			LimitIP: 0,
			TotalGB: 0,
			TgID:    telegramUser,
			Email:   telegramUser,
			Comment: "tg:" + telegramUser,
		}
	} else {
		if strings.TrimSpace(client.Email) == "" || client.Email != telegramUser {
			client.Email = telegramUser
		}
		if strings.TrimSpace(client.TgID) == "" {
			client.TgID = telegramUser
		}
		if client.Comment == "" {
			client.Comment = "tg:" + telegramUser
		}
		client.Enable = true
	}

	expireAt, err := cfg.client.EnsureExpiry(cfg.inboundID, client, addDays)
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
		link = cfg.client.GenerateVLESSLink(client, cfg.serverAddress, cfg.serverPort, cfg.serverName, cfg.publicKey, cfg.shortID, cfg.spiderX)
	}

	return &accessInfo{
		client:   client,
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

func sendAccess(info *accessInfo, telegramUserID string, chatID int64, addedDays int, userID int64, cfg *xraySettings, bot *tgbotapi.BotAPI, session *UserSession) error {
	if info == nil || info.client == nil {
		return fmt.Errorf("no access info")
	}

	exp := "-"
	if !info.expireAt.IsZero() {
		exp = info.expireAt.UTC().Format("02.01.2006 15:04 MST")
	}

	linkLine := "попробуй ещё раз получить ссылку"
	if strings.TrimSpace(info.link) != "" {
		linkLine = fmt.Sprintf("<code>%s</code>", info.link)
	}

	text := fmt.Sprintf(
		"🔐 <b>Доступ готов!</b>\n🔗 Ссылка: %s\n📆 Осталось дней: %d\n⏳ Действует до: %s\n🆔 ID: <code>%d</code>",
		linkLine, info.daysLeft, exp, userID,
	)
	if addedDays > 0 {
		text += fmt.Sprintf("\n🎁 Начислено: +%d дн.", addedDays)
	}

	session.LastAccess = text
	session.LastLink = info.link

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📚 Инструкции", "nav_instructions"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🏠 Меню", "nav_menu"),
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
			tgbotapi.NewInlineKeyboardButtonData("🔐 Подключить VPN", "nav_get_vpn"),
			tgbotapi.NewInlineKeyboardButtonData("💰 Пополнить баланс", "nav_topup"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("👤 Профиль", "nav_status"),
			tgbotapi.NewInlineKeyboardButtonData("🎁 +15 дней", "nav_referral"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📚 Инструкции", "nav_instructions"),
			tgbotapi.NewInlineKeyboardButtonData("📞 Поддержка", "nav_support"),
		),
	)
}

func composeMenuText() string {
	trimmed := strings.TrimSpace(startText)
	if trimmed == "" {
		return "Выберите действие в меню ниже."
	}
	return trimmed
}

func rateKeyboard() tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	var row []tgbotapi.InlineKeyboardButton
	for _, p := range ratePlans {
		label := fmt.Sprintf("💸 %.0f₽ · %d дн.", p.Amount, p.Days)
		row = append(row, tgbotapi.NewInlineKeyboardButtonData(label, "rate_"+p.ID))
		if len(row) == 3 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", "nav_menu"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

func main() {
	yookassaApiKey := os.Getenv("YOOKASSA_API_KEY")
	yookassaStoreID := os.Getenv("YOOKASSA_STORE_ID")
	botToken := os.Getenv("TG_BOT_TOKEN")
	privacyURL = os.Getenv("PRIVACY_URL")
	dbDSN := strings.TrimSpace(os.Getenv("DB_DSN"))

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

	xrayUser := os.Getenv("XRAY_USERNAME")
	xrayPass := os.Getenv("XRAY_PASSWORD")
	xrayHost := os.Getenv("XRAY_HOST")
	xrayPort := os.Getenv("XRAY_PORT")
	xrayBasePath := os.Getenv("XRAY_WEB_BASE_PATH")
	inboundID, _ := strconv.Atoi(os.Getenv("XRAY_INBOUND_ID"))
	serverPort, _ := strconv.Atoi(os.Getenv("XRAY_SERVER_PORT"))

	xClient := xray.New(xrayUser, xrayPass, xrayHost, xrayPort, xrayBasePath)
	if err := xClient.LoginToServer(); err != nil {
		log.Fatalf("login to xray failed: %v", err)
	}

	xrayCfg = &xraySettings{
		client:        xClient,
		inboundID:     inboundID,
		serverAddress: os.Getenv("XRAY_SERVER_ADDRESS"),
		serverPort:    serverPort,
		serverName:    os.Getenv("XRAY_SERVER_NAME"),
		publicKey:     os.Getenv("XRAY_PUBLIC_KEY"),
		shortID:       os.Getenv("XRAY_SHORT_ID"),
		spiderX:       os.Getenv("XRAY_SPIDER_X"),
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

	// Профилактический re-login к XRAY раз в час
	go func() {
		for {
			time.Sleep(1 * time.Hour)
			if err := xClient.LoginToServer(); err != nil {
				sendMessageToAdmin("Релогин завершился ошибкой...", "bot", bot, 1234)
				log.Printf("[XRAY] re-login failed: %v", err)
			} else {
				sendMessageToAdmin("Релогин прошел успешно!", "bot", bot, 1234)
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
		text := strings.TrimSpace(msg.CommandArguments())
		if text == "" {
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
				m := tgbotapi.NewMessage(id, text)
				m.ParseMode = "HTML"
				_, err = bot.Send(m)
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
		plan, ok := ratePlanByID[session.PendingPlanID]
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
			_ = updateSessionText(bot, chatID, session, stateMenu, composeMenuText(), "HTML", mainMenuInlineKeyboard())
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

	isNew := userStore.IsNewUser(userID)
	if isNew {
		_ = userStore.AddDays(userID, 7)
		_, _ = ensureXrayAccess(xrayCfg, userID, fallbackEmail(userID), 7, true)
		if referrerID != "" && referrerID != userID {
			if err := userStore.RecordReferral(userID, referrerID); err == nil {
				_ = userStore.AddDays(referrerID, 15)
				_, _ = ensureXrayAccess(xrayCfg, referrerID, fallbackEmail(referrerID), 15, true)
				if refChatID, err := strconv.ParseInt(referrerID, 10, 64); err == nil {
					refDays, _ := userStore.GetDays(referrerID)
					refCount := userStore.GetReferralsCount(referrerID)
					// Красивое уведомление пригласившему
					newUserName := msg.From.UserName
					if newUserName == "" {
						newUserName = fmt.Sprintf("ID:%s", userID)
					} else {
						newUserName = fmt.Sprintf("@%s", newUserName)
					}
					refMsg := fmt.Sprintf("🎉 <b>По вашей реферальной ссылке зарегистрировался %s!</b>\n\n🎁 <b>Вам начислено: +15 дней</b>\n👥 <b>Всего рефералов:</b> %d\n⏱ <b>Баланс:</b> %d дн.", newUserName, refCount, refDays)
					nmsg := tgbotapi.NewMessage(refChatID, refMsg)
					nmsg.ParseMode = "HTML"
					_, _ = bot.Send(nmsg)

					// Уведомление админу
					for _, adminID := range adminIDs {
						adminMsg := fmt.Sprintf("🆕 <b>Реферал:</b> Пользователь <code>%s</code> (ID:%s) перешёл по ссылке пользователя <code>%s</code> (ID:%s)\nПригласившему начислено +15 дней.", newUserName, userID, referrerID, referrerID)
						amsg := tgbotapi.NewMessage(adminID, adminMsg)
						amsg.ParseMode = "HTML"
						_, _ = bot.Send(amsg)
					}
				}
			}
		}
	}

	session.PendingPlanID = ""
	_ = updateSessionText(bot, chatID, session, stateMenu, composeMenuText(), "HTML", mainMenuInlineKeyboard())
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

	// Логирование действия для админов
	username := cq.From.UserName
	userID := int64(cq.From.ID)
	actionName := getActionName(data)
	notifyAdmins(bot, userID, username, actionName)

	switch {
	case data == "nav_menu":
		_ = updateSessionText(bot, chatID, session, stateMenu, composeMenuText(), "HTML", mainMenuInlineKeyboard())
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
	case data == "windows":
		if err := startInstructionFlow(bot, chatID, session, instruct.Windows, 0); err != nil {
			log.Printf("windows instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "android":
		if err := startInstructionFlow(bot, chatID, session, instruct.Android, 0); err != nil {
			log.Printf("android instruction error: %v", err)
			ackText = "Не удалось открыть инструкцию"
		}
	case data == "ios":
		if err := startInstructionFlow(bot, chatID, session, instruct.IOS, 0); err != nil {
			log.Printf("ios instruction error: %v", err)
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
	case strings.HasPrefix(data, "rate_"):
		id := strings.TrimPrefix(data, "rate_")
		if p, ok := ratePlanByID[id]; ok {
			handleRateSelection(bot, cq, session, p)
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

func handleTopUp(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = ""
	var builder strings.Builder
	builder.WriteString("💰 <b>Выбор тарифа</b>\nЧем больше период — тем выгоднее.\n\nДоступные варианты:\n")
	for _, plan := range ratePlans {
		builder.WriteString(fmt.Sprintf("• %.0f₽ — %d дней\n", plan.Amount, plan.Days))
	}
	header := strings.TrimSuffix(builder.String(), "\n")
	_ = updateSessionText(bot, chatID, session, stateTopUp, header, "HTML", rateKeyboard())
}

func handleRateSelection(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, plan RatePlan) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = plan.ID

	userID := strconv.FormatInt(cq.From.ID, 10)
	if email, _ := userStore.GetEmail(userID); strings.TrimSpace(email) == "" {
		text := "📧 Нужен e-mail для счёта. Отправь e-mail сообщением.\n\n" +
			"<b>Продолжая и вводя e-mail, вы соглашаетесь с <a href=\"https://telegra.ph/POLZOVATELSKOE-SOGLASHENIE-PUBLICHNAYA-OFERTA-SERVISA-HAPPY-CAT-VPN-10-27\">пользовательским соглашением</a>.</b>"
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("⬅️ Меню", "nav_menu"),
			),
		)
		_ = updateSessionText(bot, chatID, session, stateCollectEmail, text, "HTML", kb)
		ackCallback(bot, cq, "Отправь e-mail")
		return
	}

	if err := startPaymentForPlan(bot, chatID, session, plan); err != nil {
		log.Printf("startPaymentForPlan error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateTopUp, "❌ Не удалось создать платёж.", "", mainMenuInlineKeyboard())
		ackCallback(bot, cq, "Ошибка платежа")
		return
	}

	ackCallback(bot, cq, fmt.Sprintf("✅ Счёт на %s создан", plan.Title))
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

func handlePreCheckout(bot *tgbotapi.BotAPI, pcq *tgbotapi.PreCheckoutQuery) {
	ans := tgbotapi.PreCheckoutConfig{
		PreCheckoutQueryID: pcq.ID,
		OK:                 true,
	}
	if _, err := bot.Request(ans); err != nil {
		log.Printf("precheckout answer error: %v", err)
	}
}

func handleGetVPN(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)
	telegramUser := fmt.Sprint(userID)

	bonus := int64(0)
	if userStore.IsNewUser(telegramUser) {
		bonus = 7
	}

	info, err := ensureXrayAccess(xrCfg, telegramUser, fallbackEmail(telegramUser), bonus, true)
	if err != nil {
		log.Printf("ensureXrayAccess error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не удалось получить доступ. Напиши в поддержку.", "", mainMenuInlineKeyboard())
		return
	}

	if err := sendAccess(info, telegramUser, chatID, int(bonus), userID, xrCfg, bot, session); err != nil {
		log.Printf("sendAccess error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не получилось отправить ссылку.", "", mainMenuInlineKeyboard())
		return
	}

	sendMessageToAdmin(fmt.Sprintf("user id:%d запросил VPN", cq.From.ID), cq.From.UserName, bot, userID)
}

func handleStatus(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)

	text, err := buildStatusText(xrayCfg, int(userID))
	if err != nil {
		text = "❌ Не удалось получить статус"
	}
	email, _ := userStore.GetEmail(strconv.Itoa(int(userID)))
	if strings.TrimSpace(email) == "" {
		email = "-"
	}
	refCount := userStore.GetReferralsCount(strconv.FormatInt(userID, 10))
	refBonus := refCount * 15
	statusText := fmt.Sprintf(
		"👤 <b>Профиль</b>\n<b>├ 🪪 ID:</b> <code>%d</code>\n<b>├ 📧 Mail:</b> %s\n<b>└ 🎁 Рефералы</b>: %d (дней: %d)\n\n%s",
		userID, email, refCount, refBonus, text,
	)

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✏️ Изменить e-mail", "edit_email"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Меню", "nav_menu"),
		),
	)

	_ = updateSessionText(bot, chatID, session, stateStatus, statusText, "HTML", kb)
}

func buildStatusText(cfg *xraySettings, userID int) (string, error) {
	telegramUser := fmt.Sprint(userID)
	info, _ := ensureXrayAccess(cfg, telegramUser, fallbackEmail(telegramUser), 0, true)
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
		exp = info.expireAt.Format("02.01.2006 15:04")
	}
	linkLine := ""
	if info != nil && strings.TrimSpace(info.link) != "" {
		linkLine = fmt.Sprintf("\n\n<b>🔗 Ключ-подключение</b>\n<code>%s</code>", info.link)
	}
	return fmt.Sprintf("💳 <b>Подписка</b>\n<b>├ %s Статус:</b> %s\n<b>├ ⏱ Остаток:</b> %d дн.\n<b>└ 📅 Действует до:</b> %s%s", statusEmoji, statusText, days, exp, linkLine), nil
}

func handleEditEmail(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "✏️ Отправь новый e-mail сообщением."
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", "nav_status"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateEditEmail, text, "HTML", kb)
	ackCallback(bot, cq, "Жду e-mail")
}

func handleInstructionsMenu(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	instruct.ResetState(chatID)
	text := "📚 <b>Инструкции</b>\nВыбери платформу:"
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🖥 Windows", "windows"),
			tgbotapi.NewInlineKeyboardButtonData("🤖 Android", "android"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🍎 iOS", "ios"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateInstructions, text, "HTML", kb)
}

func startInstructionFlow(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, platform instruct.InstructType, step int) error {
	prevMessageID := session.MessageID
	instruct.ResetState(chatID)

	var (
		msgID int
		err   error
	)

	switch platform {
	case instruct.Windows:
		msgID, err = instruct.InstructionWindows(chatID, bot, step)
	case instruct.Android:
		msgID, err = instruct.InstructionAndroid(chatID, bot, step)
	case instruct.IOS:
		msgID, err = instruct.InstructionIos(chatID, bot, step)
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

	waitingText := fmt.Sprintf("Готовлю доступ по тарифу %s...", plan.Title)
	_ = updateSessionText(bot, chatID, session, stateTopUp, waitingText, "HTML", mainMenuInlineKeyboard())

	if err := issuePlanAccess(bot, chatID, session, plan, xrCfg, telegramUser, userID); err != nil {
		return err
	}

	session.PendingPlanID = ""
	sendMessageToAdmin(fmt.Sprintf("Платёж от %d за %s", msg.From.ID, plan.Title), msg.From.UserName, bot, userID)
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
		userLink = fmt.Sprintf("<a href=\"tg://user?id=%d\">Профиль пользователя</a>", id)
	}
	newText := fmt.Sprintf("%s:\n%s", userLink, html.EscapeString(text))
	msg := tgbotapi.NewMessage(623290294, newText)
	msg.ParseMode = "HTML"

	msg2 := tgbotapi.NewMessage(6365653009, newText)
	msg2.ParseMode = "HTML"
	bot.Send(msg)
	bot.Send(msg2)

}

func getActionName(data string) string {
	actionMap := map[string]string{
		"nav_menu":         "🏠 Главное меню",
		"nav_get_vpn":      "🔐 Получить VPN",
		"nav_topup":        "💰 Пополнить баланс",
		"nav_status":       "👤 Профиль",
		"nav_referral":     "🎁 Рефералы",
		"nav_support":      "📞 Поддержка",
		"nav_instructions": "📚 Инструкции",
		"edit_email":       "✏️ Изменить e-mail",
		"windows":          "🖥 Инструкция Windows",
		"android":          "🤖 Инструкция Android",
		"ios":              "🍎 Инструкция iOS",
		"check_payment":    "💳 Проверка платежа",
	}

	// Префиксы для динамических действий
	if strings.HasPrefix(data, "rate_") {
		return "💸 Выбор тарифа"
	}
	if strings.HasPrefix(data, "win_prev_") || strings.HasPrefix(data, "android_prev_") || strings.HasPrefix(data, "ios_prev_") {
		return "⬅️ Предыдущий шаг инструкции"
	}
	if strings.HasPrefix(data, "win_next_") || strings.HasPrefix(data, "android_next_") || strings.HasPrefix(data, "ios_next_") {
		return "➡️ Следующий шаг инструкции"
	}

	// Если действие найдено в карте
	if name, ok := actionMap[data]; ok {
		return name
	}

	// По умолчанию возвращаем сырое значение
	return data
}

func notifyAdmins(bot *tgbotapi.BotAPI, userID int64, username, action string) {
	if len(adminIDs) == 0 {
		return
	}
	userLink := fmt.Sprintf(`<a href="tg://user?id=%d">ID:%d</a>`, userID, userID)
	if username != "" {
		userLink = fmt.Sprintf(`<a href="https://t.me/%s">@%s</a> (ID:%d)`, username, username, userID)
	}
	text := fmt.Sprintf("📊 Действие: <b>%s</b>\nПользователь: %s", action, userLink)
	for _, adminID := range adminIDs {
		if adminID == userID {
			continue // не уведомляем админа о его же действиях
		}
		msg := tgbotapi.NewMessage(adminID, text)
		msg.ParseMode = "HTML"
		msg.DisableWebPagePreview = true
		_, _ = bot.Send(msg)
	}
}

// Simple referral stats screen
func handleReferral(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	userID := strconv.FormatInt(cq.From.ID, 10)
	link := fmt.Sprintf("https://t.me/%s?start=ref_%s", bot.Self.UserName, userID)
	count := userStore.GetReferralsCount(userID)
	bonus := count * 15
	shareURL := fmt.Sprintf("https://t.me/share/url?url=%s&text=%s", url.QueryEscape(link), url.QueryEscape("Подключайся к HappyCat VPN и получай бонусы!"))

	text := fmt.Sprintf(
		"🎁 <b>Реферальная программа</b>\n\n"+
			"<b>🔗 Твоя ссылка</b>\n<code>%s</code>\n\n"+
			"<b>📊 Статистика</b>\n"+
			"├ 👥 Приглашено: %d\n"+
			"└ 🎉 Бонус: %d дн.\n\n"+
			"<b>⚙️ Как получить +15 дней</b>\n"+
			"• Поделись ссылкой с другом\n"+
			"• Он переходит и активирует VPN\n"+
			"• Ты автоматически получаешь +15 дней",
		link, count, bonus,
	)
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("📤 Поделиться", shareURL),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
	// ackCallback(bot, cq, "Рефералы")
}

// Simple support screen
func handleSupport(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "📞 <b>Поддержка</b>\n\nЕсть вопросы? Пиши: @asortiment97 либо сюда https://t.me/HappyVPNchat\nОтвечаем в течении дня."
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateMenu, text, "HTML", kb)
	ackCallback(bot, cq, "Поддержка")
}
