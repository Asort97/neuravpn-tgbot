package yookassa

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type YooKassaClient struct {
	yookassaShopID    string
	yookassaSecretKey string
}

type YooKassaPaymentRequest struct {
	Amount struct {
		Value    string `json:"value"`
		Currency string `json:"currency"`
	} `json:"amount"`
	Capture           bool                   `json:"capture"`
	Confirmation      map[string]interface{} `json:"confirmation,omitempty"`
	Description       string                 `json:"description"`
	Metadata          map[string]interface{} `json:"metadata"`
	Receipt           *Receipt               `json:"receipt,omitempty"`
	SavePaymentMethod bool                   `json:"save_payment_method,omitempty"`
	PaymentMethodID   string                 `json:"payment_method_id,omitempty"`
	ExpiresAt         string                 `json:"expires_at,omitempty"`
}

type Receipt struct {
	Customer struct {
		Email string `json:"email"`
	} `json:"customer"`
	Items []ReceiptItem `json:"items"`
}

type ReceiptItem struct {
	Description string `json:"description"`
	Quantity    string `json:"quantity"`
	Amount      struct {
		Value    string `json:"value"`
		Currency string `json:"currency"`
	} `json:"amount"`
	VatCode        int    `json:"vat_code"`
	PaymentMode    string `json:"payment_mode"`
	PaymentSubject string `json:"payment_subject"`
}

var (
	userPayments      = make(map[int64][]string) // хранит историю платежей пользователя (последние N)
	processedPayments = make(map[string]bool)    // ID уже обработанных платежей (idempotency)
	payMu             sync.Mutex
)

type YooKassaPaymentResponse struct {
	ID            string                 `json:"id"`
	Status        string                 `json:"status"`
	Amount        map[string]interface{} `json:"amount"`
	Description   string                 `json:"description"`
	Recipient     map[string]interface{} `json:"recipient"`
	CreatedAt     string                 `json:"created_at"`
	Confirmation  map[string]interface{} `json:"confirmation"`
	Paid          bool                   `json:"paid"`
	Refundable    bool                   `json:"refundable"`
	Metadata      map[string]interface{} `json:"metadata"`
	Receipt       *Receipt               `json:"receipt,omitempty"`
	PaymentMethod *PaymentMethodInfo     `json:"payment_method,omitempty"`
}

type PaymentMethodInfo struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Saved bool   `json:"saved"`
	Title string `json:"title"`
}

func New(shopID, apiKey string) *YooKassaClient {
	return &YooKassaClient{
		yookassaShopID:    shopID,
		yookassaSecretKey: apiKey,
	}
}

func (y *YooKassaClient) CreateYooKassaPayment(amount float64, description string, chatID int64, product string, extraMeta map[string]interface{}, userEmail string, saveCard bool) (*YooKassaPaymentResponse, error) {
	paymentReq := YooKassaPaymentRequest{}

	paymentReq.Amount.Value = fmt.Sprintf("%.2f", amount)
	paymentReq.Amount.Currency = "RUB"
	paymentReq.Capture = true

	paymentReq.Confirmation = map[string]interface{}{
		"type":       "redirect",
		"return_url": "https://t.me/happyCatVpnBot",
	}

	paymentReq.Description = description
	paymentReq.SavePaymentMethod = saveCard
	paymentReq.ExpiresAt = time.Now().UTC().Add(20 * time.Minute).Format(time.RFC3339)

	paymentReq.Metadata = map[string]interface{}{
		"chat_id":  chatID,
		"product":  product,
		"order_id": fmt.Sprintf("order_%d", chatID),
	}

	for k, v := range extraMeta {
		paymentReq.Metadata[k] = v
	}

	if userEmail != "" {
		paymentReq.Receipt = &Receipt{
			Items: []ReceiptItem{
				{
					Description: description,
					Quantity:    "1.00",
					Amount: struct {
						Value    string `json:"value"`
						Currency string `json:"currency"`
					}{
						Value:    fmt.Sprintf("%.2f", amount),
						Currency: "RUB",
					},
					VatCode:        1,
					PaymentMode:    "full_payment",
					PaymentSubject: "service",
				},
			},
		}
		paymentReq.Receipt.Customer.Email = userEmail
	}

	jsonData, err := json.Marshal(paymentReq)
	if err != nil {
		return nil, fmt.Errorf("не удалось подготовить тело запроса: %v", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("POST",
		"https://api.yookassa.ru/v3/payments",
		bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("не удалось создать запрос к YooKassa: %v", err)
	}

	auth := fmt.Sprintf("%s:%s", y.yookassaShopID, y.yookassaSecretKey)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotence-Key", fmt.Sprintf("%d", time.Now().UnixNano()))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("не удалось выполнить запрос к YooKassa: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать ответ YooKassa: %v", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ошибка API YooKassa: %s, ответ: %s", resp.Status, string(body))
	}

	var paymentResp YooKassaPaymentResponse
	if err := json.Unmarshal(body, &paymentResp); err != nil {
		return nil, fmt.Errorf("не удалось разобрать ответ YooKassa: %v", err)
	}

	return &paymentResp, nil
}

func (y *YooKassaClient) GetYooKassaPaymentStatus(paymentID string) (*YooKassaPaymentResponse, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET",
		fmt.Sprintf("https://api.yookassa.ru/v3/payments/%s", paymentID),
		nil)
	if err != nil {
		return nil, err
	}

	auth := fmt.Sprintf("%s:%s", y.yookassaShopID, y.yookassaSecretKey)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var paymentResp YooKassaPaymentResponse
	if err := json.Unmarshal(body, &paymentResp); err != nil {
		return nil, err
	}

	return &paymentResp, nil
}

func (y *YooKassaClient) sendYooKassaPaymentButton(bot *tgbotapi.BotAPI, chatID int64, messageID int, amount float64, productName string, metadata map[string]interface{}, userEmail string, saveCard bool) (int, string, error) {
	payment, err := y.CreateYooKassaPayment(
		amount,
		productName,
		chatID,
		productName,
		metadata,
		userEmail,
		saveCard,
	)
	if err != nil {
		return messageID, "", fmt.Errorf("не удалось создать платёж: %v", err)
	}

	// записываем ID платежа в историю пользователя
	payMu.Lock()
	userPayments[chatID] = append(userPayments[chatID], payment.ID)
	// ограничим историю до 5 последних записей, чтобы не разрасталась
	if len(userPayments[chatID]) > 5 {
		userPayments[chatID] = userPayments[chatID][len(userPayments[chatID])-5:]
	}
	payMu.Unlock()

	confirmationURL := ""
	if confirmation, ok := payment.Confirmation["confirmation_url"].(string); ok {
		confirmationURL = confirmation
	} else {
		return messageID, "", fmt.Errorf("не получена ссылка на оплату от YooKassa")
	}

	message := fmt.Sprintf(`💳 *%s*

💰 Сумма к оплате: *%.2f ₽*
📝 Описание: %s

Нажмите «Оплатить», чтобы продолжить.`,
		productName, amount, productName)

	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("💳 Оплатить", confirmationURL),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Я оплатил", "check_payment"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Назад в меню", "nav_menu"),
		),
	)

	if messageID > 0 {
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, messageID, message, keyboard)
		edit.ParseMode = "Markdown"
		if _, err := bot.Send(edit); err == nil {
			return messageID, confirmationURL, nil
		}
	}

	msg := tgbotapi.NewMessage(chatID, message)
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = keyboard

	sent, err := bot.Send(msg)
	if err != nil {
		return messageID, "", err
	}

	return sent.MessageID, confirmationURL, nil
}

func (y *YooKassaClient) SendVPNPayment(bot *tgbotapi.BotAPI, chatID int64, messageID int, amount float64, productName string, metadata map[string]interface{}, userEmail string, saveCard bool) (int, string, error) {
	return y.sendYooKassaPaymentButton(bot, chatID, messageID, amount, productName, metadata, userEmail, saveCard)
}

// FindSucceededPayment ищет любой успешный платёж среди последних платежей пользователя.
// Возвращает платёж и true, если найден успешно оплаченный и ещё не обработанный.
func (y *YooKassaClient) FindSucceededPayment(chatID int64) (*YooKassaPaymentResponse, bool, error) {
	payMu.Lock()
	ids := append([]string(nil), userPayments[chatID]...) // копия
	payMu.Unlock()

	// обходим от самого нового к старому
	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]

		payment, err := y.GetYooKassaPaymentStatus(id)
		if err != nil {
			// пропускаем сбойные
			continue
		}
		if payment.Status == "succeeded" || payment.Paid {
			return payment, true, nil
		}
	}
	return nil, false, nil
}

// ClearPayments очищает историю платежей пользователя
func (y *YooKassaClient) ClearPayments(chatID int64) {
	payMu.Lock()
	delete(userPayments, chatID)
	payMu.Unlock()
}

// CreateAutoPayment создаёт автоплатёж через сохранённый метод оплаты (recurring, без confirmation).
func (y *YooKassaClient) CreateAutoPayment(methodID string, amount float64, description, userEmail string, metadata map[string]interface{}) (*YooKassaPaymentResponse, error) {
	paymentReq := YooKassaPaymentRequest{}
	paymentReq.Amount.Value = fmt.Sprintf("%.2f", amount)
	paymentReq.Amount.Currency = "RUB"
	paymentReq.Capture = true
	paymentReq.PaymentMethodID = methodID
	paymentReq.Description = description
	paymentReq.Metadata = metadata

	if userEmail != "" {
		paymentReq.Receipt = &Receipt{
			Items: []ReceiptItem{
				{
					Description: description,
					Quantity:    "1.00",
					Amount: struct {
						Value    string `json:"value"`
						Currency string `json:"currency"`
					}{
						Value:    fmt.Sprintf("%.2f", amount),
						Currency: "RUB",
					},
					VatCode:        1,
					PaymentMode:    "full_payment",
					PaymentSubject: "service",
				},
			},
		}
		paymentReq.Receipt.Customer.Email = userEmail
	}

	jsonData, err := json.Marshal(paymentReq)
	if err != nil {
		return nil, fmt.Errorf("autopay marshal: %v", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("POST", "https://api.yookassa.ru/v3/payments", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("autopay request: %v", err)
	}

	auth := fmt.Sprintf("%s:%s", y.yookassaShopID, y.yookassaSecretKey)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotence-Key", fmt.Sprintf("autopay_%s_%d", methodID, time.Now().UnixNano()))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("autopay http: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("autopay read body: %v", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("autopay API error %s: %s", resp.Status, string(body))
	}

	var paymentResp YooKassaPaymentResponse
	if err := json.Unmarshal(body, &paymentResp); err != nil {
		return nil, fmt.Errorf("autopay unmarshal: %v", err)
	}

	return &paymentResp, nil
}
