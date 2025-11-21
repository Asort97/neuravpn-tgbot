package main

import (
	"fmt"
	"log"
	"net/mail"
	"os"
	"strconv"
	"strings"
	"time"

	xray "github.com/Asort97/vpnBot/clients/Xray"
	instruct "github.com/Asort97/vpnBot/clients/instruction"
	sqlite "github.com/Asort97/vpnBot/clients/sqLite"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const startText = "Привет! Это HappyCat VPN. Нажимай кнопки ниже, чтобы получить доступ или пополнить дни."

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
	PendingPlanID string
	LastAccess    string
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
	sqliteClient   *sqlite.Store
	xrayCfg        *xraySettings
	privacyURL     string
	userSessions   = make(map[int64]*UserSession)
)

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
	_ = sqliteClient.SetDays(telegramUser, daysLeft)

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
	if email, err := sqliteClient.GetEmail(userID); err == nil && strings.TrimSpace(email) != "" {
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
		"Твой доступ готов!\nСсылка: %s\nОсталось дней: %d\nДействует до: %s\nID: <code>%d</code>",
		linkLine, info.daysLeft, exp, userID,
	)
	if addedDays > 0 {
		text += fmt.Sprintf("\nНачислено: +%d дн.", addedDays)
	}

	session.LastAccess = text
	instruct.EnableCertButton(chatID, true)

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Ссылка ещё раз", "resend_access"),
			tgbotapi.NewInlineKeyboardButtonData("Инструкции", "nav_instructions"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Меню", "nav_menu"),
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
	return nil
}

func mainMenuKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Получить VPN", "nav_get_vpn"),
			tgbotapi.NewInlineKeyboardButtonData("Пополнить дни", "nav_topup"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Статус", "nav_status"),
			tgbotapi.NewInlineKeyboardButtonData("Инструкции", "nav_instructions"),
		),
	)
}

func rateKeyboard() tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	var row []tgbotapi.InlineKeyboardButton
	for _, p := range ratePlans {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s", p.Title), "rate_"+p.ID))
		if len(row) == 3 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("В меню", "nav_menu"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

func main() {
	yookassaApiKey := os.Getenv("YOOKASSA_API_KEY")
	yookassaStoreID := os.Getenv("YOOKASSA_STORE_ID")
	botToken := os.Getenv("TG_BOT_TOKEN")
	privacyURL = os.Getenv("PRIVACY_URL")

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
	sqliteClient = sqlite.New("database/data.json")

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatalf("bot init error: %v", err)
	}

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.PreCheckoutQuery != nil {
			handlePreCheckout(bot, update.PreCheckoutQuery)
			continue
		}
		if msg := update.Message; msg != nil {
			handleIncomingMessage(bot, msg, xrayCfg)
			continue
		}
		if cq := update.CallbackQuery; cq != nil && cq.Message != nil {
			handleCallback(bot, cq, xrayCfg)
		}
	}
}

func handleIncomingMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings) {
	chatID := msg.Chat.ID
	session := getSession(chatID)

	if msg.SuccessfulPayment != nil {
		plan, ok := ratePlanByID[session.PendingPlanID]
		if !ok {
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Платеж есть, но тариф не ясен. Напиши в поддержку.", "", mainMenuKeyboard())
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
		case "referral":
			handleReferralStats(bot, msg)
		default:
		}
		return
	}

	if session.State == stateCollectEmail {
		userID := strconv.FormatInt(msg.From.ID, 10)
		addr, err := mail.ParseAddress(strings.TrimSpace(msg.Text))
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(bot, chatID, session, stateCollectEmail, "Неверный e-mail, пример: name@example.com", "HTML", mainMenuKeyboard())
			return
		}
		_ = sqliteClient.SetEmail(userID, addr.Address)
		_ = sqliteClient.AcceptPrivacy(userID, time.Now())

		plan, ok := ratePlanByID[session.PendingPlanID]
		if !ok {
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Тариф не найден, выбери заново.", "HTML", rateKeyboard())
			return
		}
		if err := startPaymentForPlan(bot, chatID, session, plan); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось создать платеж.", "", mainMenuKeyboard())
		}
		return
	}

	if session.State == stateEditEmail {
		userID := strconv.FormatInt(msg.From.ID, 10)
		addr, err := mail.ParseAddress(strings.TrimSpace(msg.Text))
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(bot, chatID, session, stateEditEmail, "Неверный e-mail.", "HTML", mainMenuKeyboard())
			return
		}
		_ = sqliteClient.SetEmail(userID, addr.Address)
		handleStatus(bot, &tgbotapi.CallbackQuery{Message: msg}, session, xrCfg)
		return
	}
}

func handleStart(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, session *UserSession, xrCfg *xraySettings) {
	chatID := msg.Chat.ID
	telegramUser := strconv.FormatInt(msg.From.ID, 10)

	if sqliteClient.IsNewUser(telegramUser) {
		_ = sqliteClient.AddDays(telegramUser, 7)
		_, _ = ensureXrayAccess(xrayCfg, telegramUser, fallbackEmail(telegramUser), 7, true)
	}

	session.PendingPlanID = ""
	_ = updateSessionText(bot, chatID, session, stateMenu, startText, "HTML", mainMenuKeyboard())
}

func handleReferralStats(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userID := strconv.FormatInt(msg.From.ID, 10)
	link := fmt.Sprintf("https://t.me/%s?start=ref_%s", bot.Self.UserName, userID)
	count := sqliteClient.GetReferralsCount(userID)
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

	switch {
	case data == "nav_menu":
		_ = updateSessionText(bot, chatID, session, stateMenu, startText, "HTML", mainMenuKeyboard())
	case data == "nav_get_vpn":
		handleGetVPN(bot, cq, session, xrCfg)
	case data == "nav_topup":
		handleTopUp(bot, cq, session)
	case data == "nav_status":
		handleStatus(bot, cq, session, xrCfg)
	case data == "edit_email":
		handleEditEmail(bot, cq, session)
	case data == "nav_instructions":
		handleInstructionsMenu(bot, cq, session)
	case strings.HasPrefix(data, "rate_"):
		id := strings.TrimPrefix(data, "rate_")
		if p, ok := ratePlanByID[id]; ok {
			handleRateSelection(bot, cq, session, p)
			return
		}
	case data == "check_payment":
		handleCheckPayment(bot, cq, session, xrCfg)
	case data == "resend_access":
		if strings.TrimSpace(session.LastAccess) != "" {
			msg := tgbotapi.NewMessage(chatID, session.LastAccess)
			msg.ParseMode = "HTML"
			msg.DisableWebPagePreview = true
			bot.Send(msg)
		} else {
			ackText = "Данных нет, запроси VPN заново"
		}
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
	_ = updateSessionText(bot, chatID, session, stateTopUp, "Выбери тариф:", "HTML", rateKeyboard())
}

func handleRateSelection(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, plan RatePlan) {
	chatID := cq.Message.Chat.ID
	session.PendingPlanID = plan.ID

	userID := strconv.FormatInt(cq.From.ID, 10)
	if email, _ := sqliteClient.GetEmail(userID); strings.TrimSpace(email) == "" {
		text := "Нужен e-mail для счёта. Отправь e-mail сообщением."
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("Меню", "nav_menu"),
			),
		)
		_ = updateSessionText(bot, chatID, session, stateCollectEmail, text, "HTML", kb)
		ackCallback(bot, cq, "Отправь e-mail")
		return
	}

	if err := startPaymentForPlan(bot, chatID, session, plan); err != nil {
		log.Printf("startPaymentForPlan error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось создать платеж.", "", mainMenuKeyboard())
		ackCallback(bot, cq, "Ошибка платежа")
		return
	}

	ackCallback(bot, cq, fmt.Sprintf("Счёт на %s создан", plan.Title))
}

func startPaymentForPlan(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, plan RatePlan) error {
	metadata := map[string]interface{}{
		"plan_id":     plan.ID,
		"plan_title":  plan.Title,
		"plan_days":   plan.Days,
		"plan_amount": plan.Amount,
	}

	email, _ := sqliteClient.GetEmail(strconv.FormatInt(chatID, 10))
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
	if sqliteClient.IsNewUser(telegramUser) {
		bonus = 7
	}

	info, err := ensureXrayAccess(xrCfg, telegramUser, fallbackEmail(telegramUser), bonus, true)
	if err != nil {
		log.Printf("ensureXrayAccess error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не удалось получить доступ. Напиши в поддержку.", "", mainMenuKeyboard())
		return
	}

	if err := sendAccess(info, telegramUser, chatID, int(bonus), userID, xrCfg, bot, session); err != nil {
		log.Printf("sendAccess error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не получилось отправить ссылку.", "", mainMenuKeyboard())
		return
	}

	sendMessageToAdmin(fmt.Sprintf("user id:%d запросил VPN", cq.From.ID), cq.From.UserName, bot, userID)
}

func handleStatus(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, xrCfg *xraySettings) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)

	text, err := buildStatusText(xrayCfg, int(userID))
	if err != nil {
		text = "Не удалось получить статус"
	}
	email, _ := sqliteClient.GetEmail(strconv.Itoa(int(userID)))
	if strings.TrimSpace(email) == "" {
		email = "-"
	}
	statusText := fmt.Sprintf("<b>Твой профиль:</b>\n• TG ID: <code>%d</code>\n• Mail: %s\n%s", userID, email, text)

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Изменить e-mail", "edit_email"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Меню", "nav_menu"),
		),
	)

	_ = updateSessionText(bot, chatID, session, stateStatus, statusText, "HTML", kb)
}

func buildStatusText(cfg *xraySettings, userID int) (string, error) {
	telegramUser := fmt.Sprint(userID)
	info, _ := ensureXrayAccess(cfg, telegramUser, fallbackEmail(telegramUser), 0, true)
	days, _ := sqliteClient.GetDays(strconv.Itoa(userID))
	if info != nil && info.daysLeft > 0 {
		days = info.daysLeft
	}
	status := "Не активна"
	if days > 0 {
		status = "Активна"
	}
	exp := "-"
	if info != nil && !info.expireAt.IsZero() {
		exp = info.expireAt.UTC().Format("02.01.2006 15:04 MST")
	}
	linkLine := ""
	if info != nil && strings.TrimSpace(info.link) != "" {
		linkLine = fmt.Sprintf("\n<b>Ссылка:</b> <code>%s</code>", info.link)
	}
	return fmt.Sprintf("<b>Статус:</b> %s\n<b>Остаток:</b> %d дн.\n<b>Действует до:</b> %s%s", status, days, exp, linkLine), nil
}

func handleEditEmail(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "Отправь новый e-mail сообщением."
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Назад", "nav_status"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateEditEmail, text, "HTML", kb)
	ackCallback(bot, cq, "Жду e-mail")
}

func handleInstructionsMenu(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	instruct.ResetState(chatID)
	text := "Выбери платформу для инструкции"
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Windows", "windows"),
			tgbotapi.NewInlineKeyboardButtonData("Android", "android"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("iOS", "ios"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Меню", "nav_menu"),
		),
	)
	_ = updateSessionText(bot, chatID, session, stateInstructions, text, "", kb)
}

func handleSuccessfulPayment(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, xrCfg *xraySettings, plan RatePlan, session *UserSession) error {
	chatID := msg.Chat.ID
	userID := int64(msg.From.ID)
	telegramUser := fmt.Sprint(userID)

	waitingText := fmt.Sprintf("Готовлю доступ по тарифу %s...", plan.Title)
	_ = updateSessionText(bot, chatID, session, stateTopUp, waitingText, "HTML", mainMenuKeyboard())

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
	if id == 623290294 {
		return
	}
	userLink := fmt.Sprintf(`<a href="tg://user?id=%d">user</a>`, id)
	if username != "" {
		userLink = fmt.Sprintf(`<a href="https://t.me/%s">@%s</a>`, username, username)
	}
	newText := fmt.Sprintf("%s:\n%s", userLink, text)
	msg := tgbotapi.NewMessage(623290294, newText)
	msg.ParseMode = "HTML"
	bot.Send(msg)
}
