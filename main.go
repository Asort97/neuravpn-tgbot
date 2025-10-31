package main

import (
	"fmt"
	"html"
	"log"
	"net/mail"
	"os"
	"strconv"
	"strings"
	"time"

	instruct "github.com/Asort97/vpnBot/clients/instruction"
	pfsense "github.com/Asort97/vpnBot/clients/pfSense"
	sqlite "github.com/Asort97/vpnBot/clients/sqLite"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const startText = `
Привет! <b>Добро пожаловать в HappyCat VPN</b> 😺🔐

Здесь ты можешь:
• Моментально получить или продлить доступ к VPN.
• Скачать пробный сертификат в пару кликов.
• Найти подробные инструкции для всех устройств.
• Оперативно связаться с поддержкой 24/7.

Готов? Выбирай нужный раздел в меню ниже и поехали! 🚀
`

var lastActionKey = make(map[int64]map[string]time.Time)

var yookassaClient *yookassa.YooKassaClient
var sqliteClient *sqlite.Store
var privacyURL string

// pfSense async job dispatcher to run heavy revoke/unrevoke in background
type pfOpType int

const (
	pfOpRevoke pfOpType = iota
	pfOpUnrevoke
)

type pfJob struct {
	op      pfOpType
	certRef string
}

var (
	pfJobs         chan pfJob
	pfClientGlobal *pfsense.PfSenseClient
)

func startPfWorkers(client *pfsense.PfSenseClient, concurrency int) {
	if concurrency < 1 {
		concurrency = 1
	}
	pfClientGlobal = client
	// buffered queue to avoid blocking bot handlers
	pfJobs = make(chan pfJob, 256)
	for i := 0; i < concurrency; i++ {
		workerID := i + 1
		go func() {
			for job := range pfJobs {
				switch job.op {
				case pfOpRevoke:
					if err := client.RevokeCertificate(job.certRef); err != nil {
						log.Printf("[pfWorker %d] revoke %s error: %v", workerID, job.certRef, err)
					}
				case pfOpUnrevoke:
					if err := client.UnrevokeCertificate(job.certRef); err != nil {
						log.Printf("[pfWorker %d] unrevoke %s error: %v", workerID, job.certRef, err)
					}
				}
			}
		}()
	}
}

// Non-blocking scheduling helpers
func scheduleRevoke(certRef string) {
	if certRef == "" {
		return
	}
	select {
	case pfJobs <- pfJob{op: pfOpRevoke, certRef: certRef}:
	default:
		// Fallback: run in separate goroutine to avoid blocking
		go func(ref string) {
			if pfClientGlobal == nil {
				return
			}
			if err := pfClientGlobal.RevokeCertificate(ref); err != nil {
				log.Printf("[pfFallback] revoke %s error: %v", ref, err)
			}
		}(certRef)
	}
}

func scheduleUnrevoke(certRef string) {
	if certRef == "" {
		return
	}
	select {
	case pfJobs <- pfJob{op: pfOpUnrevoke, certRef: certRef}:
	default:
		go func(ref string) {
			if pfClientGlobal == nil {
				return
			}
			if err := pfClientGlobal.UnrevokeCertificate(ref); err != nil {
				log.Printf("[pfFallback] unrevoke %s error: %v", ref, err)
			}
		}(certRef)
	}
}

type SessionState string

const (
	stateMenu         SessionState = "menu"
	stateGetVPN       SessionState = "get_vpn"
	stateTopUp        SessionState = "top_up"
	stateTrial        SessionState = "trial"
	stateStatus       SessionState = "status"
	stateSupport      SessionState = "support"
	stateInstructions SessionState = "instructions"
	stateChooseRate   SessionState = "choose_rate"
	stateCollectEmail SessionState = "collect_email"
	stateEditEmail    SessionState = "edit_email"
)

// RatePlan описывает тариф, который пользователь может выбрать.
type RatePlan struct {
	ID          string
	Title       string
	Amount      float64
	Days        int
	Description string
}

// ratePlans содержит список доступных тарифов. При необходимости поменяйте названия и цены.
var ratePlans = []RatePlan{
	{ID: "15d", Title: "15 дней", Amount: 25, Days: 15, Description: "Идеально, чтобы протестировать сервис или уехать на короткое время."},
	{ID: "30d", Title: "30 дней", Amount: 50, Days: 30, Description: "Идеально, чтобы протестировать сервис или уехать на короткое время."},
	{ID: "60d", Title: "60 дней", Amount: 100, Days: 60, Description: "Базовая подписка для постоянного доступа без ограничений."},
	{ID: "120d", Title: "120 дней", Amount: 200, Days: 120, Description: "Полугодовой тариф со скидкой по сравнению с помесячной оплатой."},
	{ID: "240d", Title: "240 дней", Amount: 300, Days: 240, Description: "Полугодовой тариф со скидкой по сравнению с помесячной оплатой."},
	{ID: "365d", Title: "365 дней", Amount: 400, Days: 365, Description: "Максимальная выгода для тех, кто пользуется VPN круглый год."},
}

var ratePlanByID = func() map[string]RatePlan {
	result := make(map[string]RatePlan)
	for _, plan := range ratePlans {
		result[plan.ID] = plan
	}
	return result
}()

type userState struct {
	TelegramUser string
	UserID       string
	UserExists   bool
	CertID       string
	CertRefID    string
	CertName     string
	CertExpireAt string
	CertExpired  bool
	CertDays     int
}

func fetchUserState(pfsenseClient *pfsense.PfSenseClient, telegramUser string) (userState, error) {
	state := userState{TelegramUser: telegramUser}

	userIDStr, exists := pfsenseClient.IsUserExist(telegramUser)
	if !exists {
		return state, nil
	}

	state.UserExists = true
	state.UserID = userIDStr

	userIDFromCert, certRefID, err := pfsenseClient.GetAttachedCertRefIDByUserName(telegramUser)
	if userIDFromCert != "" {
		state.UserID = userIDFromCert
	}
	if certRefID == "" || err != nil {
		return state, nil
	}

	state.CertRefID = certRefID

	certID, certName, err := pfsenseClient.GetCertificateIDByRefid(certRefID)
	if err != nil {
		return state, nil
	}
	state.CertID = certID
	state.CertName = certName
	state.CertDays = extractDaysFromCertName(certName)

	_, expiresAt, _, expired, err := pfsenseClient.GetDateOfCertificate(certID)
	if err != nil {
		return state, err
	}

	state.CertExpireAt = expiresAt
	state.CertExpired = expired
	return state, nil
}

func ensureVPNUser(pfsenseClient *pfsense.PfSenseClient, telegramUser string) (string, error) {
	userIDStr, exists := pfsenseClient.IsUserExist(telegramUser)
	if exists {
		return userIDStr, nil
	}

	return pfsenseClient.CreateUser(telegramUser, "123", "", "", false)
}

const certificateLifetimeDays = 3650

func createAndAttachCertificate(pfsenseClient *pfsense.PfSenseClient, telegramUser string, userID string) (string, string, error) {
	certName := fmt.Sprintf("Cert%s_permanent", telegramUser)

	if existingRefID, existingID, err := pfsenseClient.GetCertificateIDByName(certName); err == nil {
		if err := pfsenseClient.AttachCertificateToUser(userID, existingRefID); err != nil {
			return "", "", err
		}
		_, expiresAt, _, _, err := pfsenseClient.GetDateOfCertificate(existingID)
		if err != nil {
			return "", "", err
		}
		return existingRefID, expiresAt, nil
	}

	uuid, err := pfsenseClient.GetCARef()
	if err != nil {
		return "", "", err
	}

	certID, certRefID, err := pfsenseClient.CreateCertificate(certName, uuid, "RSA", 2048, certificateLifetimeDays, "", "sha256", telegramUser)
	if err != nil {
		return "", "", err
	}

	if err := pfsenseClient.AttachCertificateToUser(userID, certRefID); err != nil {
		return "", "", err
	}

	_, expiresAt, _, _, err := pfsenseClient.GetDateOfCertificate(certID)
	if err != nil {
		return "", "", err
	}

	return certRefID, expiresAt, nil
}

func ensureUserCertificate(pfsenseClient *pfsense.PfSenseClient, telegramUser string) (string, string, string, error) {
	state, err := fetchUserState(pfsenseClient, telegramUser)
	if err != nil {
		return "", "", "", err
	}

	userID := state.UserID
	if !state.UserExists || userID == "" {
		userID, err = ensureVPNUser(pfsenseClient, telegramUser)
		if err != nil {
			return "", "", "", err
		}
	}

	certRefID := state.CertRefID
	certID := state.CertID
	expiresAt := state.CertExpireAt

	if certRefID == "" {
		if ref, getErr := sqliteClient.GetCertRef(telegramUser); getErr == nil && ref != "" {
			certRefID = ref
		}
	}

	if certRefID != "" && certID == "" {
		if id, _, getErr := pfsenseClient.GetCertificateIDByRefid(certRefID); getErr == nil {
			certID = id
		} else {
			log.Printf("GetCertificateIDByRefid error: %v", getErr)
			certRefID = ""
		}
	}

	createdNew := false
	if certRefID == "" {
		certRefID, expiresAt, err = createAndAttachCertificate(pfsenseClient, telegramUser, userID)
		if err != nil {
			return "", "", "", err
		}
		createdNew = true
	} else {
		if err := pfsenseClient.AttachCertificateToUser(userID, certRefID); err != nil {
			return "", "", "", err
		}
		if expiresAt == "" {
			if certID == "" {
				if id, _, getErr := pfsenseClient.GetCertificateIDByRefid(certRefID); getErr == nil {
					certID = id
				} else {
					log.Printf("GetCertificateIDByRefid error: %v", getErr)
				}
			}
			if certID != "" {
				if _, expires, _, _, getErr := pfsenseClient.GetDateOfCertificate(certID); getErr == nil {
					expiresAt = expires
				} else {
					log.Printf("GetDateOfCertificate error: %v", getErr)
				}
			}
		}
	}

	if err := sqliteClient.SetCertRef(telegramUser, certRefID); err != nil {
		log.Printf("sqliteClient.SetCertRef error: %v", err)
	}

	if expiresAt == "" && !createdNew {
		if certID == "" {
			if id, _, getErr := pfsenseClient.GetCertificateIDByRefid(certRefID); getErr == nil {
				certID = id
			}
		}
		if certID != "" {
			if _, expires, _, _, getErr := pfsenseClient.GetDateOfCertificate(certID); getErr == nil {
				expiresAt = expires
			} else {
				log.Printf("GetDateOfCertificate error: %v", getErr)
			}
		}
	}

	return certRefID, expiresAt, userID, nil
}

func issuePlanCertificate(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, plan RatePlan, pfsenseClient *pfsense.PfSenseClient, telegramUser string, numericUserID int64) error {
	certRefID, _, _, err := ensureUserCertificate(pfsenseClient, telegramUser)
	if err != nil {
		return err
	}

	if plan.Days > 0 {
		if err := sqliteClient.AddDays(telegramUser, int64(plan.Days)); err != nil {
			log.Printf("sqliteClient.AddDays error: %v", err)
		}
	}

	// Run unrevoke asynchronously to avoid blocking
	scheduleUnrevoke(certRefID)

	return sendCertificate(certRefID, telegramUser, chatID, plan.Days, numericUserID, pfsenseClient, bot, session)
}
func resolvePlanFromMetadata(meta map[string]interface{}, session *UserSession) RatePlan {
	plan := RatePlan{}

	if meta != nil {
		if v, ok := meta["plan_id"]; ok {
			id := fmt.Sprint(v)
			plan.ID = id
			if preset, ok := ratePlanByID[id]; ok {
				plan = preset
			}
		}

		if plan.Title == "" {
			if v, ok := meta["plan_title"]; ok {
				plan.Title = fmt.Sprint(v)
			} else if v, ok := meta["product"]; ok {
				plan.Title = fmt.Sprint(v)
			}
		}

		if plan.Days == 0 {
			if v, ok := meta["plan_days"]; ok {
				switch value := v.(type) {
				case float64:
					plan.Days = int(value)
				case string:
					if n, err := strconv.Atoi(value); err == nil {
						plan.Days = n
					}
				}
			}
		}

		if plan.Amount == 0 {
			if v, ok := meta["plan_amount"]; ok {
				switch value := v.(type) {
				case float64:
					plan.Amount = value
				case string:
					if n, err := strconv.ParseFloat(value, 64); err == nil {
						plan.Amount = n
					}
				}
			}
		}
	}

	if plan.ID != "" {
		if preset, ok := ratePlanByID[plan.ID]; ok {
			if plan.Title == "" {
				plan.Title = preset.Title
			}
			if plan.Days == 0 {
				plan.Days = preset.Days
			}
			if plan.Amount == 0 {
				plan.Amount = preset.Amount
			}
		}
	}

	if plan.Days == 0 && plan.Title != "" {
		plan.Days = extractDaysFromCertName(plan.Title)
	}

	if plan.Days == 0 && session != nil && session.PendingPlanID != "" {
		if preset, ok := ratePlanByID[session.PendingPlanID]; ok {
			if plan.ID == "" {
				plan.ID = preset.ID
			}
			if plan.Title == "" {
				plan.Title = preset.Title
			}
			plan.Days = preset.Days
			if plan.Amount == 0 {
				plan.Amount = preset.Amount
			}
		}
	}

	if plan.Days == 0 {
		plan.Days = 30
	}

	if plan.ID == "" && session != nil && session.PendingPlanID != "" {
		plan.ID = session.PendingPlanID
	}

	if plan.Title == "" && plan.ID != "" {
		if preset, ok := ratePlanByID[plan.ID]; ok {
			plan.Title = preset.Title
			if plan.Amount == 0 {
				plan.Amount = preset.Amount
			}
		} else {
			plan.Title = fmt.Sprintf("Пакет %s", plan.ID)
		}
	}

	if plan.Amount == 0 && plan.ID != "" {
		if preset, ok := ratePlanByID[plan.ID]; ok {
			plan.Amount = preset.Amount
		}
	}

	return plan
}

func extractDaysFromCertName(name string) int {
	if name == "" {
		return 0
	}
	idx := strings.LastIndex(name, "_")
	if idx == -1 || idx+1 >= len(name) {
		return 0
	}
	suffix := name[idx+1:]
	digits := strings.Builder{}
	for _, r := range suffix {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	if digits.Len() == 0 {
		return 0
	}
	value, err := strconv.Atoi(digits.String())
	if err != nil {
		return 0
	}
	return value
}

type UserSession struct {
	MessageID      int
	State          SessionState
	ContentType    string
	PendingPlanID  string
	CertFileName   string // Имя файла сертификата для повторной отправки
	CertFileBytes  []byte // Данные сертификата для прикрепления к инструкциям
}

var userSessions = make(map[int64]*UserSession)

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
	if session, ok := userSessions[chatID]; ok {
		return session
	}
	session := &UserSession{}
	userSessions[chatID] = session
	return session
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
			tgbotapi.NewInlineKeyboardButtonData("🎁 Пригласить друга", "nav_referral"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📚 Инструкции", "nav_instructions"),
			tgbotapi.NewInlineKeyboardButtonData("💬 Поддержка", "nav_support"),
		),
	)
}

func instructionsMenuKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("💻 Windows", "windows"),
			tgbotapi.NewInlineKeyboardButtonData("📱 Android", "android"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🍎 iOS", "ios"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", "nav_menu"),
		),
	)
}

func singleBackKeyboard(target string) tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", target),
		),
	)
}

func rateSelectionKeyboard() tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	var currentRow []tgbotapi.InlineKeyboardButton

	for _, plan := range ratePlans {
		// На кнопке оставляем только цену — описание сверху объясняет, чему соответствует цена
		label := fmt.Sprintf("%.0f ₽", plan.Amount)
		btn := tgbotapi.NewInlineKeyboardButtonData(label, "rate_"+plan.ID)
		currentRow = append(currentRow, btn)

		// По 3 кнопки в строку
		if len(currentRow) == 3 {
			rows = append(rows, currentRow)
			currentRow = nil
		}
	}

	// Добавить остаток (если есть)
	if len(currentRow) > 0 {
		rows = append(rows, currentRow)
	}

	// Кнопка "Назад" всегда на отдельной строке
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", "nav_menu"),
	))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

func showRateSelection(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, intro string) error {
	session.PendingPlanID = ""
	// Всегда показываем сопоставление: "цена -> дни" в заголовке.
	var lines []string
	for _, p := range ratePlans {
		// Ещё более компактный формат: "25₽→15д." (без пробелов)
		lines = append(lines, fmt.Sprintf("%.0f₽→%dд.", p.Amount, p.Days))
	}

	var header string
	if strings.TrimSpace(intro) != "" {
		// Если есть intro (например, баланс), показываем его перед списком соответствия
		header = intro + "\n\n💰 <b>Выберите тариф:</b>\n\n" + strings.Join(lines, "\n") + "\n\n"
	} else {
		header = "💰 <b>Выберите тариф:</b>\n\n" + strings.Join(lines, "\n") + "\n\n"
	}

	message := header + "⚡️ <i>Чем дольше период — тем выгоднее!</i>"

	return updateSessionText(bot, chatID, session, stateChooseRate, message, "HTML", rateSelectionKeyboard())
}

func ackCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, text string) {
	cfg := tgbotapi.CallbackConfig{CallbackQueryID: cq.ID}
	if text != "" {
		cfg.Text = text
	}
	bot.Request(cfg)
}

func composeMenuText() string {
	trimmed := strings.TrimSpace(startText)
	if trimmed == "" {
		return "Выберите действие в меню ниже."
	}
	return trimmed + "\n\n<b>Выберите нужный раздел ниже:</b>"
}

func main() {
	pfsenseApiKey := os.Getenv("PFSENSE_API_KEY")
	yookassaApiKey := os.Getenv("YOOKASSA_API_KEY")
	yookassaStoreID := os.Getenv("YOOKASSA_STORE_ID")
	botToken := os.Getenv("TG_BOT_TOKEN")
	tlsKey := os.Getenv("TLS_CRYPT_KEY")
	privacyURL = os.Getenv("PRIVACY_URL")
	tlsBytes, _ := os.ReadFile(tlsKey)

	pfsenseClient := pfsense.New(pfsenseApiKey, []byte(tlsBytes))
	yookassaClient = yookassa.New(yookassaStoreID, yookassaApiKey)
	sqliteClient = sqlite.New("database/data.json")

	// Start pfSense async workers (do not block bot on revoke/unrevoke)
	startPfWorkers(pfsenseClient, 5)
	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Panic(err)
	}

	go dailyDeductWorker(sqliteClient, bot, pfsenseClient)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.PreCheckoutQuery != nil {
			handlePreCheckout(bot, update.PreCheckoutQuery)
			continue
		}

		if msg := update.Message; msg != nil {
			// handle admin commands safely only when message is present
			if msg.Text == "/revoke" {
				// example: schedule a test revoke without blocking
				scheduleRevoke("68b043fdeeb8d")
				continue
			}
			if msg.Text == "/unrevoke" {
				// example: schedule a test unrevoke without blocking
				scheduleUnrevoke("68b043fdeeb8d")
				continue
			}

			handleIncomingMessage(bot, msg, pfsenseClient)
			continue
		}

		if cq := update.CallbackQuery; cq != nil && cq.Message != nil {
			handleCallback(bot, cq, pfsenseClient)
		}
	}
}

func revokeAllCertificates(certs []string, _ *pfsense.PfSenseClient) {
	// Schedule all revokes asynchronously; don't block caller
	for _, ref := range certs {
		if strings.TrimSpace(ref) == "" {
			continue
		}
		scheduleRevoke(ref)
	}
	// No waiting here; workers will process in background
}

func handleIncomingMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, pfsenseClient *pfsense.PfSenseClient) {
	chatID := msg.Chat.ID
	session := getSession(chatID)

	if msg.SuccessfulPayment != nil {
		plan, ok := ratePlanByID[session.PendingPlanID]
		if !ok {
			log.Printf("successful payment received but plan is unknown")
			_ = updateSessionText(bot, chatID, session, stateTopUp, "❌ Не нашли информацию об оплате. Напишите в поддержку.", "", singleBackKeyboard("nav_menu"))
			return
		}
		if err := handleSuccessfulPayment(bot, msg, pfsenseClient, plan, session); err != nil {
			log.Printf("handleSuccessfulPayment error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "❌ Не удалось обработать оплату. Попробуйте позже.", "", singleBackKeyboard("nav_menu"))
		}
		return
	}

	if msg.IsCommand() {
		switch msg.Command() {
		case "start":
			handleStart(bot, msg, session, pfsenseClient)
		case "referral":
			handleReferralStats(bot, msg)
		case "pay":
			fakeCallback := &tgbotapi.CallbackQuery{Message: msg, From: msg.From}
			handleGetVPN(bot, fakeCallback, session, pfsenseClient)
		default:
			// ignore other commands
		}
		return
	}

	// Обработка шага ввода e-mail для согласия с политикой
	if session.State == stateCollectEmail {
		userID := strconv.FormatInt(msg.From.ID, 10)
		addr, err := mail.ParseAddress(strings.TrimSpace(msg.Text))
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(
				bot, chatID, session, stateCollectEmail,
				"❌ Похоже, это не e-mail. Отправьте корректный адрес, например: name@example.com",
				"HTML",
				tgbotapi.NewInlineKeyboardMarkup(
					tgbotapi.NewInlineKeyboardRow(
						tgbotapi.NewInlineKeyboardButtonURL("📄 Политика", getPrivacyURL()),
					),
					tgbotapi.NewInlineKeyboardRow(
						tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", "nav_menu"),
					),
				),
			)
			return
		}

		// Сохраняем e-mail и фиксируем согласие
		_ = sqliteClient.SetEmail(userID, addr.Address)
		_ = sqliteClient.AcceptPrivacy(userID, time.Now())

		// Переходим к оплате выбранного тарифа
		planID := session.PendingPlanID
		plan, ok := ratePlanByID[planID]
		if !ok {
			_ = updateSessionText(bot, chatID, session, stateTopUp, "❌ Не удалось определить выбранный тариф. Выберите снова.", "HTML", rateSelectionKeyboard())
			return
		}
		if err := startPaymentForPlan(bot, chatID, session, plan); err != nil {
			log.Printf("startPaymentForPlan error: %v", err)
			_ = updateSessionText(bot, chatID, session, stateTopUp, "❌ Не удалось сформировать счет. Попробуйте позже.", "", singleBackKeyboard("nav_menu"))
			return
		}
		return
	}

	// Обработка редактирования e-mail
	if session.State == stateEditEmail {
		userID := strconv.FormatInt(msg.From.ID, 10)
		addr, err := mail.ParseAddress(strings.TrimSpace(msg.Text))
		if err != nil || addr.Address == "" || !strings.Contains(addr.Address, "@") {
			_ = updateSessionText(
				bot, chatID, session, stateEditEmail,
				"❌ Неверный формат. Отправьте корректный e-mail.",
				"HTML",
				tgbotapi.NewInlineKeyboardMarkup(
					tgbotapi.NewInlineKeyboardRow(
						tgbotapi.NewInlineKeyboardButtonData("⬅️ Отмена", "nav_status"),
					),
				),
			)
			return
		}

		_ = sqliteClient.SetEmail(userID, addr.Address)

		// Возвращаемся к статусу без дополнительных сообщений
		handleStatusDirect(bot, chatID, session, pfsenseClient, int(msg.From.ID))
		return
	}
}

func handleStart(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, session *UserSession, pfsenseClient *pfsense.PfSenseClient) {
	chatID := msg.Chat.ID
	userID := strconv.FormatInt(msg.From.ID, 10)

	// Проверяем, новый ли пользователь
	isNew := sqliteClient.IsNewUser(userID)

	// Парсим аргументы команды (реферальный код)
	args := msg.CommandArguments()
	referrerID := ""
	if args != "" && strings.HasPrefix(args, "ref_") {
		referrerID = strings.TrimPrefix(args, "ref_")
	}

	// Если новый пользователь
	if isNew {
		// Даем 7 дней новому пользователю
		if err := sqliteClient.AddDays(userID, 7); err != nil {
			log.Printf("AddDays error for new user %s: %v", userID, err)
		} else {
			log.Printf("New user %s received 7 days welcome bonus", userID)
		}

		// Если пришел по реферальной ссылке
		if referrerID != "" && referrerID != userID {
			// Записываем реферала
			if err := sqliteClient.RecordReferral(userID, referrerID); err != nil {
				log.Printf("RecordReferral error: %v", err)
			} else {
				// Даем 15 дней пригласившему
				if err := sqliteClient.AddDays(referrerID, 15); err != nil {
					log.Printf("AddDays error for referrer %s: %v", referrerID, err)
				} else {
					log.Printf("Referrer %s received 15 days bonus", referrerID)

					// Уведомляем пригласившего
					referrerChatID, _ := strconv.ParseInt(referrerID, 10, 64)
					notifyMsg := tgbotapi.NewMessage(referrerChatID, "🎉 По вашей реферальной ссылке зарегистрировался новый пользователь! Вам начислено 15 дней.")
					notifyMsg.ParseMode = "HTML"
					bot.Send(notifyMsg)

					// Уведомляем админа о реферальной регистрации
					sendMessageToAdmin(
						fmt.Sprintf("🎁 Новая реферальная регистрация!\n• Новый пользователь: %s\n• Пригласивший: %s\n• Бонус рефереру: +15 дней", userID, referrerID),
						msg.From.UserName,
						bot,
						msg.From.ID,
					)
				}
			}

			// Приветствие с упоминанием реферального бонуса
			welcomeText := startText + "\n\n🎁 <b>Вы получили 7 дней в подарок за регистрацию по реферальной ссылке!</b>"
			if err := updateSessionText(bot, chatID, session, stateMenu, welcomeText+"\n\n<b>Выберите нужный раздел ниже:</b>", "HTML", mainMenuInlineKeyboard()); err != nil {
				log.Printf("updateSessionText error: %v", err)
			}
			return
		}

		// Обычное приветствие для нового пользователя без реферала
		welcomeText := startText + "\n\n🎁 <b>Вам начислено 7 дней бесплатно!</b>"
		if err := updateSessionText(bot, chatID, session, stateMenu, welcomeText+"\n\n<b>Выберите нужный раздел ниже:</b>", "HTML", mainMenuInlineKeyboard()); err != nil {
			log.Printf("updateSessionText error: %v", err)
		}
		return
	}

	// Для существующих пользователей — обычное меню
	if err := showMainMenu(bot, chatID, session); err != nil {
		log.Printf("showMainMenu error: %v", err)
	}
}

func handleReferralStats(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userID := strconv.FormatInt(msg.From.ID, 10)

	// Генерируем реферальную ссылку
	botUsername := bot.Self.UserName
	referralLink := fmt.Sprintf("https://t.me/%s?start=ref_%s", botUsername, userID)

	// Получаем статистику
	referralsCount := sqliteClient.GetReferralsCount(userID)

	statsText := fmt.Sprintf(`🔗 <b>Ваша реферальная ссылка:</b>
<code>%s</code>

📊 <b>Статистика:</b>
• Приглашено пользователей: %d
• Заработано дней: %d

💡 <b>Как это работает?</b>
• Вы получаете <b>15 дней</b> за каждого приглашенного
• Ваш друг получает <b>7 дней</b> в подарок

Поделитесь ссылкой с друзьями!`, referralLink, referralsCount, referralsCount*15)

	reply := tgbotapi.NewMessage(chatID, statsText)
	reply.ParseMode = "HTML"
	bot.Send(reply)
}

func handleCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, pfsenseClient *pfsense.PfSenseClient) {
	chatID := cq.Message.Chat.ID
	session := getSession(chatID)
	data := cq.Data
	ackText := ""

	switch {
	case data == "nav_menu":
		if err := showMainMenu(bot, chatID, session); err != nil {
			log.Printf("showMainMenu error: %v", err)
		}
	case data == "nav_get_vpn":
		handleGetVPN(bot, cq, session, pfsenseClient)
	case data == "nav_topup":
		handleTopUp(bot, cq, session, pfsenseClient)
	case data == "nav_status":
		handleStatus(bot, cq, session, pfsenseClient)
	case data == "edit_email":
		handleEditEmail(bot, cq, session)
	case data == "nav_referral":
		handleReferralCallback(bot, cq, session)
		sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл реферальную программу", cq.From.ID), cq.From.UserName, bot, int64(cq.From.ID))
	case data == "nav_support":
		handleSupport(bot, cq, session)
	case data == "nav_instructions":
		handleInstructionsMenu(bot, cq, session)
		sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл меню инструкций", cq.From.ID), cq.From.UserName, bot, int64(cq.From.ID))
	case data == "windows":
		handleInstructionSelection(bot, cq, session, instruct.Windows)
		sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл инструкцию для Windows", cq.From.ID), cq.From.UserName, bot, int64(cq.From.ID))
	case data == "android":
		handleInstructionSelection(bot, cq, session, instruct.Android)
		sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл инструкцию для Android", cq.From.ID), cq.From.UserName, bot, int64(cq.From.ID))
	case data == "ios":
		handleInstructionSelection(bot, cq, session, instruct.IOS)
		sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл инструкцию для iOS", cq.From.ID), cq.From.UserName, bot, int64(cq.From.ID))
	case strings.HasPrefix(data, "win_prev_"):
		step, _ := strconv.Atoi(strings.TrimPrefix(data, "win_prev_"))
		instruct.InstructionWindows(chatID, bot, step-1)
	case strings.HasPrefix(data, "win_next_"):
		step, _ := strconv.Atoi(strings.TrimPrefix(data, "win_next_"))
		instruct.InstructionWindows(chatID, bot, step+1)
	case strings.HasPrefix(data, "android_prev_"):
		step, _ := strconv.Atoi(strings.TrimPrefix(data, "android_prev_"))
		instruct.InstructionAndroid(chatID, bot, step-1)
	case strings.HasPrefix(data, "android_next_"):
		step, _ := strconv.Atoi(strings.TrimPrefix(data, "android_next_"))
		instruct.InstructionAndroid(chatID, bot, step+1)
	case strings.HasPrefix(data, "ios_prev_"):
		step, _ := strconv.Atoi(strings.TrimPrefix(data, "ios_prev_"))
		instruct.InstructionIos(chatID, bot, step-1)
	case strings.HasPrefix(data, "ios_next_"):
		step, _ := strconv.Atoi(strings.TrimPrefix(data, "ios_next_"))
		instruct.InstructionIos(chatID, bot, step+1)
	case data == "resend_certificate":
		// Повторная отправка сертификата, если он есть в сессии
		if session.CertFileBytes != nil && session.CertFileName != "" {
			fileBytes := tgbotapi.FileBytes{
				Name:  session.CertFileName,
				Bytes: session.CertFileBytes,
			}
			doc := tgbotapi.NewDocument(chatID, fileBytes)
			doc.Caption = "📥 <b>Ваш VPN-сертификат</b>\n\nИспользуйте его для подключения согласно инструкции."
			doc.ParseMode = "HTML"
			if _, err := bot.Send(doc); err != nil {
				log.Printf("resend certificate error: %v", err)
				ackText = "❌ Не удалось отправить сертификат"
			} else {
				ackText = "✅ Сертификат отправлен"
			}
		} else {
			ackText = "❌ Сертификат не найден. Получите его через меню 'Подключить VPN'"
		}
	case data == "check_payment":
		handleCheckPayment(bot, cq, session, pfsenseClient)
	case strings.HasPrefix(data, "rate_"):
		planID := strings.TrimPrefix(data, "rate_")
		if plan, ok := ratePlanByID[planID]; ok {
			handleRateSelection(bot, cq, session, plan, pfsenseClient)
			return
		}
		ackText = "❌ Неизвестный тариф"
	default:
		// ignore
	}

	ackCallback(bot, cq, ackText)
}

func dailyDeductWorker(store *sqlite.Store, bot *tgbotapi.BotAPI, pfsenseClient *pfsense.PfSenseClient) {
	const (
		checkInterval   = time.Hour
		consumptionStep = 24 * time.Hour
	)

	ticker := time.NewTicker(checkInterval) // регулярная проверка баланса
	defer ticker.Stop()

	for range ticker.C {
		users := store.GetAllUsers()
		now := time.Now().UTC()

		var certsToRevoke []string

		for userID, userData := range users {
			if userData.Days <= 0 {
				continue
			}

			lastDeduct, err := time.Parse(time.RFC3339, userData.LastDeduct)
			if err != nil {
				log.Printf("invalid lastDeduct for user %s: %v", userID, err)
				continue
			}

			elapsed := now.Sub(lastDeduct)
			if elapsed < consumptionStep {
				continue
			}

			daysToCharge := int64(elapsed / consumptionStep)
			if daysToCharge <= 0 {
				continue
			}

			nextCheckpoint := lastDeduct.Add(time.Duration(daysToCharge) * consumptionStep)
			remaining, err := store.ConsumeDays(userID, daysToCharge, nextCheckpoint)
			if err != nil {
				log.Printf("failed to deduct %d day(s) for user %s: %v", daysToCharge, userID, err)
				continue
			}

			log.Printf("deducted %d day(s) from user %s (remaining: %d)", daysToCharge, userID, remaining)

			if remaining == 0 {
				certRef, err := store.GetCertRef(userID)
				if err != nil {
					log.Printf("failed to find certref of user %s: %v", userID, err)
					continue
				}
				if certRef != "" {
					certsToRevoke = append(certsToRevoke, certRef)
				}

				chatID, err := strconv.ParseInt(userID, 10, 64)
				if err != nil {
					log.Printf("failed to parse chat id %s: %v", userID, err)
					continue
				}
				notifyUserSubscriptionExpired(bot, chatID)
			}
		}

		if len(certsToRevoke) > 0 {
			revokeAllCertificates(certsToRevoke, pfsenseClient)
		}
	}
}

func notifyUserSubscriptionExpired(bot *tgbotapi.BotAPI, chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "⚠️ Баланс исчерпан! Продлите подписку, чтобы продолжить пользоваться VPN.")
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

func showMainMenu(bot *tgbotapi.BotAPI, chatID int64, session *UserSession) error {
	session.PendingPlanID = ""
	return updateSessionText(bot, chatID, session, stateMenu, composeMenuText(), "HTML", mainMenuInlineKeyboard())
}

func handleGetVPN(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, pfsenseClient *pfsense.PfSenseClient) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)

	if !canProceedKey(userID, "get_vpn", 5*time.Second) {
		ackCallback(bot, cq, "Пожалуйста, немного подождите перед повторным запросом.")
		return
	}

	waitingText := "Готовим для вас конфигурацию VPN..."
	if err := updateSessionText(bot, chatID, session, stateGetVPN, waitingText, "HTML", singleBackKeyboard("nav_menu")); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}

	session.PendingPlanID = ""
	telegramUser := fmt.Sprint(userID)

	// Проверяем, новый ли пользователь, и даём бонус
	if sqliteClient.IsNewUser(telegramUser) {
		if err := sqliteClient.AddDays(telegramUser, 7); err != nil {
			log.Printf("AddDays error for new user %s: %v", telegramUser, err)
		} else {
			log.Printf("New user %s received 7 days welcome bonus via GetVPN", telegramUser)
		}
	}

	certRefID, _, _, err := ensureUserCertificate(pfsenseClient, telegramUser)
	if err != nil {
		log.Printf("ensureUserCertificate error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не удалось подготовить сертификат. Попробуйте позже или обратитесь в поддержку.", "", singleBackKeyboard("nav_menu"))
		return
	}

	// Если на балансе 0 дней, гарантируем, что сертификат остаётся ревоукнутым (на всякий случай)
	if days, _ := sqliteClient.GetDays(telegramUser); days <= 0 {
		scheduleRevoke(certRefID)
	}

	if err := sendCertificate(certRefID, telegramUser, chatID, 0, userID, pfsenseClient, bot, session); err != nil {
		log.Printf("sendCertificate error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateGetVPN, "Не удалось отправить файл. Попробуйте позже или обратитесь в поддержку.", "", singleBackKeyboard("nav_menu"))
		return
	}

	sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d запросил выдачу VPN-конфига", cq.From.ID), cq.From.UserName, bot, userID)
}

func handleTopUp(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, pfsenseClient *pfsense.PfSenseClient) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)

	if !canProceedKey(userID, "top_up", 5*time.Second) {
		ackCallback(bot, cq, "Подождите пару секунд перед новым запросом.")
		return
	}

	telegramUser := fmt.Sprint(userID)
	currentDays, err := sqliteClient.GetDays(telegramUser)
	if err != nil {
		currentDays = 0
	}

	intro := fmt.Sprintf("Текущий баланс: %d дней. Выберите пополнение.", currentDays)
	if err := showRateSelection(bot, chatID, session, intro); err != nil {
		log.Printf("showRateSelection error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось показать варианты пополнения. Попробуйте позже.", "", singleBackKeyboard("nav_menu"))
		return
	}

	sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл меню пополнения.", cq.From.ID), cq.From.UserName, bot, userID)
}

func handleStatus(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, pfsenseClient *pfsense.PfSenseClient) {
	chatID := cq.Message.Chat.ID
	userID := int64(cq.From.ID)

	if !canProceedKey(userID, "check_status", 3*time.Second) {
		ackCallback(bot, cq, "⏳ Подождите пару секунд и попробуйте ещё раз.")
		return
	}

	handleStatusDirect(bot, chatID, session, pfsenseClient, int(userID))
	sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d проверил статус сертификата", cq.From.ID), cq.From.UserName, bot, userID)
}

func handleStatusDirect(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, pfsenseClient *pfsense.PfSenseClient, userID int) {
	text, err := buildStatusText(pfsenseClient, userID)
	if err != nil {
		log.Printf("buildStatusText error: %v", err)
		text = "❌ Не удалось получить информацию о сертификате. Попробуйте позже."
	}
	email, _ := sqliteClient.GetEmail(strconv.Itoa(userID))
	if strings.TrimSpace(email) == "" {
		email = "—"
	}
	finalText := fmt.Sprintf(
		"<b>👤 Профиль:</b>\n"+
			"├ 🪪 ID: <code>%d</code>\n"+
			"└ ✉️ Mail: %s\n"+
			"%s",
		userID, email, text,
	)
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✏️ Изменить e-mail", "edit_email"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", "nav_menu"),
		),
	)
	if err := updateSessionText(bot, chatID, session, stateStatus, finalText, "HTML", kb); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}
}

func handleEditEmail(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	text := "✉️ Отправьте новый e-mail одним сообщением:"
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Отмена", "nav_status"),
		),
	)
	if err := updateSessionText(bot, chatID, session, stateEditEmail, text, "HTML", kb); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}
	ackCallback(bot, cq, "✏️ Введите новый e-mail")
}

func handleSupport(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	supportText := `📞 <b>Служба поддержки HappyCat VPN</b>

Напиши нам в Telegram: @happycatvpn
<i>Мы отвечаем 24/7 и всегда рядом, если нужна помощь.</i>`

	if err := updateSessionText(bot, chatID, session, stateSupport, supportText, "HTML", singleBackKeyboard("nav_menu")); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}

	sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d открыл раздел поддержки", cq.From.ID), cq.From.UserName, bot, int64(cq.From.ID))
}

func handleReferralCallback(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	userID := strconv.FormatInt(cq.From.ID, 10)

	botUsername := bot.Self.UserName
	referralLink := fmt.Sprintf("https://t.me/%s?start=ref_%s", botUsername, userID)

	referralsCount := sqliteClient.GetReferralsCount(userID)

	statsText := fmt.Sprintf(`🎁 <b>Реферальная программа</b>

🔗 <b>Ваша ссылка:</b>
<code>%s</code>

📊 <b>Статистика:</b>
• Приглашено: %d чел.
• Заработано: %d дней

💡 <b>Условия:</b>
• Вы: <b>+15 дней</b> за каждого друга
• Друг: <b>+7 дней</b> в подарок

Поделитесь ссылкой и получайте дни!`, referralLink, referralsCount, referralsCount*15)

	if err := updateSessionText(bot, chatID, session, stateMenu, statsText, "HTML", singleBackKeyboard("nav_menu")); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}
}

func handleInstructionsMenu(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession) {
	chatID := cq.Message.Chat.ID
	instruct.ResetState(chatID)
	text := "Выберите платформу, для которой нужна инструкция:"
	if err := updateSessionText(bot, chatID, session, stateInstructions, text, "", instructionsMenuKeyboard()); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}
}

func handleRateSelection(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, plan RatePlan, pfsenseClient *pfsense.PfSenseClient) {
	chatID := cq.Message.Chat.ID
	_ = pfsenseClient

	// Сохраняем выбранный тариф, чтобы вернуться к оплате после ввода e-mail
	session.PendingPlanID = plan.ID

	// Проверяем наличие e-mail
	userID := strconv.FormatInt(cq.From.ID, 10)
	if email, _ := sqliteClient.GetEmail(userID); strings.TrimSpace(email) == "" {
		text := fmt.Sprintf(
			"Укажите e-mail, написав его в чате. Продолжая вы подтверждаете согласие с <a href=\"%s\">Политикой конфиденциальности</a>.\n\nОтправьте ваш e-mail одним сообщением.",
			getPrivacyURL(),
		)
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("📄 Политика", getPrivacyURL()),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", "nav_menu"),
			),
		)
		if err := updateSessionText(bot, chatID, session, stateCollectEmail, text, "HTML", kb); err != nil {
			log.Printf("updateSessionText error: %v", err)
		}
		ackCallback(bot, cq, "Введите e-mail")
		return
	}

	// Если e-mail уже есть — фиксируем согласие и продолжаем к оплате
	_ = sqliteClient.AcceptPrivacy(userID, time.Now())
	if err := startPaymentForPlan(bot, chatID, session, plan); err != nil {
		log.Printf("startPaymentForPlan error: %v", err)
		_ = updateSessionText(bot, chatID, session, stateTopUp, "Не удалось сформировать счет. Попробуйте позже.", "", singleBackKeyboard("nav_menu"))
		ackCallback(bot, cq, "Не удалось сформировать счет")
		return
	}

	ackCallback(bot, cq, fmt.Sprintf("Счет на <%s> готов", plan.Title))
}

func startPaymentForPlan(bot *tgbotapi.BotAPI, chatID int64, session *UserSession, plan RatePlan) error {
	metadataPlanID := plan.ID
	if metadataPlanID == "" {
		metadataPlanID = strings.ReplaceAll(strings.ToLower(plan.Title), " ", "_")
	}
	metadata := map[string]interface{}{
		"plan_id":     metadataPlanID,
		"plan_title":  plan.Title,
		"plan_days":   plan.Days,
		"plan_amount": plan.Amount,
	}

	// Попытаемся передать e-mail в YooKassa, чтобы сформировать чек
	email, _ := sqliteClient.GetEmail(strconv.FormatInt(chatID, 10))
	newID, replaced, err := yookassaClient.SendVPNPayment(bot, chatID, session.MessageID, plan.Amount, plan.Title, metadata, email)
	if err != nil {
		return err
	}

	if replaced && session.MessageID != 0 && session.MessageID != newID {
		_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, session.MessageID))
	}

	session.MessageID = newID
	session.State = stateTopUp
	session.ContentType = "text"
	session.PendingPlanID = metadataPlanID

	instruct.ResetState(chatID)
	return nil
}

func handleInstructionSelection(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, t instruct.InstructType) {
	chatID := cq.Message.Chat.ID
	instruct.SetInstructKeyboard(session.MessageID, chatID, t)

	// Включаем кнопку сертификата, если он есть в сессии
	if session.CertFileBytes != nil && session.CertFileName != "" {
		instruct.EnableCertButton(chatID, true)
	} else {
		instruct.EnableCertButton(chatID, false)
	}

	switch t {
	case instruct.Windows:
		instruct.InstructionWindows(chatID, bot, 0)
	case instruct.Android:
		instruct.InstructionAndroid(chatID, bot, 0)
	case instruct.IOS:
		instruct.InstructionIos(chatID, bot, 0)
	}

	session.State = stateInstructions
	session.ContentType = "photo"
}

func handleCheckPayment(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery, session *UserSession, pfsenseClient *pfsense.PfSenseClient) {
	chatID := cq.Message.Chat.ID
	payment, ok, err := yookassaClient.FindSucceededPayment(chatID)
	if err != nil {
		log.Printf("FindSucceededPayment error: %v", err)
		ackCallback(bot, cq, "Не удалось проверить платеж. Попробуйте позже.")
		return
	}
	if !ok || payment == nil {
		ackCallback(bot, cq, "Платеж еще обрабатывается или не найден. Если вы уже оплатили — подождите 5–10 секунд и нажмите еще раз.")
		return
	}

	// очищаем историю платежей после успешной обработки
	yookassaClient.ClearPayments(chatID)

	meta := payment.Metadata
	plan := resolvePlanFromMetadata(meta, session)
	if plan.Title == "" {
		ackCallback(bot, cq, "Не удалось определить выбранный тариф. Напишите в поддержку.")
		return
	}

	fake := &tgbotapi.Message{Chat: cq.Message.Chat, From: cq.From}

	if err := handleSuccessfulPayment(bot, fake, pfsenseClient, plan, session); err != nil {
		log.Printf("handleSuccessfulPayment error: %v", err)
		ackCallback(bot, cq, "Не удалось выдать сертификат. Свяжитесь с поддержкой.")
		return
	}

	ackCallback(bot, cq, fmt.Sprintf("Оплата подтверждена! Тариф «%s» активирован.", plan.Title))
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

func handleSuccessfulPayment(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, pfsenseClient *pfsense.PfSenseClient, plan RatePlan, session *UserSession) error {
	chatID := msg.Chat.ID
	userID := int64(msg.From.ID)
	telegramUser := fmt.Sprint(userID)

	waitingText := fmt.Sprintf("Готовим пополнение «%s». Пожалуйста, подождите...", plan.Title)
	if err := updateSessionText(bot, chatID, session, stateTopUp, waitingText, "HTML", singleBackKeyboard("nav_menu")); err != nil {
		log.Printf("updateSessionText error: %v", err)
	}

	err := issuePlanCertificate(bot, chatID, session, plan, pfsenseClient, telegramUser, userID)
	if err != nil {
		return err
	}

	session.PendingPlanID = ""

	sendMessageToAdmin(fmt.Sprintf("Пользователь id:%d пополнил баланс пакетом «%s»", msg.From.ID, plan.Title), msg.From.UserName, bot, userID)
	return nil
}

func sendCertificate(certRefID, telegramUserID string, chatID int64, days int, userID int64, pfsenseClient *pfsense.PfSenseClient, bot *tgbotapi.BotAPI, session *UserSession) error {
	certName := fmt.Sprintf("Cert%s_permanent", telegramUserID)

	ovpnData, err := pfsenseClient.GenerateOVPN(certRefID, "", "213.21.200.205")
	if err != nil {
		return err
	}

	fileBytes := tgbotapi.FileBytes{
		Name:  certName + ".ovpn",
		Bytes: ovpnData,
	}

	// Сохраняем данные сертификата в сессии для повторного использования в инструкциях
	session.CertFileName = certName + ".ovpn"
	session.CertFileBytes = ovpnData

	caption := fmt.Sprintf("🔐 <b>VPN-конфигурация готова!</b>\n\n🪪 ID: <code>%d</code>", userID)

	if days > 0 {
		caption += fmt.Sprintf("\n✅ Пополнение: +%d дней", days)
	}
	if balance, err := sqliteClient.GetDays(telegramUserID); err == nil {
		caption += fmt.Sprintf("\n💰 Баланс: %d дней", balance)
	}
	caption += "\n\n━━━━━━━━━━━━━━━━━━━━\n"
	caption += "💡 <b>Важно:</b> Это ваш <b>постоянный</b> сертификат!\n"
	caption += "• Скачайте его <b>один раз</b>\n"
	caption += "• Импортируйте в OpenVPN\n"
	caption += "• При пополнении баланса <b>ничего менять не нужно</b> — VPN продолжит работать автоматически\n"
	caption += "━━━━━━━━━━━━━━━━━━━━"

	// Добавляем кнопку Инструкции рядом с "Назад в меню"
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📚 Инструкции", "nav_instructions"),
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад в меню", "nav_menu"),
		),
	)

	return replaceSessionWithDocument(bot, chatID, session, stateMenu, fileBytes, caption, "HTML", keyboard)
}

func buildStatusText(pfsenseClient *pfsense.PfSenseClient, userID int) (string, error) {
	telegramUser := fmt.Sprint(userID)
	_, _, err := pfsenseClient.GetAttachedCertRefIDByUserName(telegramUser)
	days, _ := sqliteClient.GetDays(strconv.Itoa(userID))

	if err != nil {
		return fmt.Sprintf(`🔒 <b>Статус подписки:</b>
<b>├ 🔴 Неактивна</b>
<b>└ ⏳ Дней на балансе:</b> %d
💡 Пополните баланс, чтобы пользоваться VPN.`, days), nil
	}

	if days == 0 {
		return fmt.Sprintf(`🔒 <b>Статус подписки:</b>
<b>├ 🔴 Неактивна</b>
<b>└ ⏳ Дней на балансе:</b> %d
💡 Пополните баланс, чтобы пользоваться VPN.`, days), nil
	}

	return fmt.Sprintf(`🔒 <b>Статус подписки:</b>
<b>├ 🟢 Активна</b>
<b>└ ⏳ Дней на балансе:</b> %d
────────────────────────
✅ Отличная новость — VPN работает!`, days), nil
}

func sendMessageToAdmin(text string, username string, bot *tgbotapi.BotAPI, id int64) {
	if id == 623290294 {
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

// getPrivacyURL возвращает ссылку на Политику конфиденциальности
func getPrivacyURL() string {
	if strings.TrimSpace(privacyURL) != "" {
		return privacyURL
	}
	// Резервная ссылка, замените на ваш Telegraph URL
	return "https://telegra.ph/HappyCat-VPN-Privacy-Policy"
}
