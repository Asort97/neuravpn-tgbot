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
)

type YooKassaClient struct {
	yookassaShopID    string
	yookassaSecretKey string
	returnURL         string // URL to return after payment (e.g. https://vk.com/club123)
}

type YooKassaPaymentRequest struct {
	Amount struct {
		Value    string `json:"value"`
		Currency string `json:"currency"`
	} `json:"amount"`
	Capture      bool                   `json:"capture"`
	Confirmation map[string]interface{} `json:"confirmation"`
	Description  string                 `json:"description"`
	Metadata     map[string]interface{} `json:"metadata"`
	Receipt      *Receipt               `json:"receipt,omitempty"`
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
	ID           string                 `json:"id"`
	Status       string                 `json:"status"`
	Amount       map[string]interface{} `json:"amount"`
	Description  string                 `json:"description"`
	Recipient    map[string]interface{} `json:"recipient"`
	CreatedAt    string                 `json:"created_at"`
	Confirmation map[string]interface{} `json:"confirmation"`
	Paid         bool                   `json:"paid"`
	Refundable   bool                   `json:"refundable"`
	Metadata     map[string]interface{} `json:"metadata"`
	Receipt      *Receipt               `json:"receipt,omitempty"`
}

func New(shopID, apiKey string) *YooKassaClient {
	return &YooKassaClient{
		yookassaShopID:    shopID,
		yookassaSecretKey: apiKey,
		returnURL:         "https://vk.com",
	}
}

// SetReturnURL overrides the default return URL after payment.
func (y *YooKassaClient) SetReturnURL(url string) {
	y.returnURL = url
}

func (y *YooKassaClient) CreateYooKassaPayment(amount float64, description string, chatID int64, product string, extraMeta map[string]interface{}, userEmail string) (*YooKassaPaymentResponse, error) {
	paymentReq := YooKassaPaymentRequest{}

	paymentReq.Amount.Value = fmt.Sprintf("%.2f", amount)
	paymentReq.Amount.Currency = "RUB"
	paymentReq.Capture = true

	returnURL := y.returnURL
	if returnURL == "" {
		returnURL = "https://vk.com"
	}

	paymentReq.Confirmation = map[string]interface{}{
		"type":       "redirect",
		"return_url": returnURL,
	}

	paymentReq.Description = description

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

// CreatePaymentURL creates a YooKassa payment and returns the confirmation URL.
// Records the payment ID in user history for later checking.
func (y *YooKassaClient) CreatePaymentURL(peerID int64, amount float64, productName string, metadata map[string]interface{}, userEmail string) (confirmationURL string, err error) {
	payment, err := y.CreateYooKassaPayment(
		amount,
		productName,
		peerID,
		productName,
		metadata,
		userEmail,
	)
	if err != nil {
		return "", fmt.Errorf("не удалось создать платёж: %v", err)
	}

	// записываем ID платежа в историю пользователя
	payMu.Lock()
	userPayments[peerID] = append(userPayments[peerID], payment.ID)
	if len(userPayments[peerID]) > 5 {
		userPayments[peerID] = userPayments[peerID][len(userPayments[peerID])-5:]
	}
	payMu.Unlock()

	if confirmation, ok := payment.Confirmation["confirmation_url"].(string); ok {
		return confirmation, nil
	}
	return "", fmt.Errorf("не получена ссылка на оплату от YooKassa")
}

// FindSucceededPayment ищет любой успешный платёж среди последних платежей пользователя.
func (y *YooKassaClient) FindSucceededPayment(chatID int64) (*YooKassaPaymentResponse, bool, error) {
	payMu.Lock()
	ids := append([]string(nil), userPayments[chatID]...)
	payMu.Unlock()

	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		payment, err := y.GetYooKassaPaymentStatus(id)
		if err != nil {
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
