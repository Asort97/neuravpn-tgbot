package xray

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	colorfulprint "github.com/Asort97/vpnBot/clients/colorfulPrint"
)

// Client describes a single VLESS client entry in 3X-UI/Xray.
type Client struct {
	ID         string `json:"id"`
	Email      string `json:"email"`
	Enable     bool   `json:"enable"`
	Flow       string `json:"flow"`
	LimitIP    int    `json:"limitIp"`
	TotalGB    int64  `json:"totalGB"`
	ExpiryTime int64  `json:"expiryTime"`
	SubID      string `json:"subId"`
	TgID       string `json:"tgId"`
	Comment    string `json:"comment"`
	Reset      int    `json:"reset"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}

// clientDTO mirrors API payload but allows tgId to be either string or number.
type clientDTO struct {
	ID         string      `json:"id"`
	Email      string      `json:"email"`
	Enable     bool        `json:"enable"`
	Flow       string      `json:"flow"`
	LimitIP    int         `json:"limitIp"`
	TotalGB    int64       `json:"totalGB"`
	ExpiryTime int64       `json:"expiryTime"`
	SubID      string      `json:"subId"`
	TgID       interface{} `json:"tgId"`
	Comment    string      `json:"comment"`
	Reset      int         `json:"reset"`
	CreatedAt  int64       `json:"created_at"`
	UpdatedAt  int64       `json:"updated_at"`
}

// UnmarshalJSON allows tgId to be either string or number.
func (c *Client) UnmarshalJSON(data []byte) error {
	var dto clientDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}
	*c = Client{
		ID:         dto.ID,
		Email:      dto.Email,
		Enable:     dto.Enable,
		Flow:       dto.Flow,
		LimitIP:    dto.LimitIP,
		TotalGB:    dto.TotalGB,
		ExpiryTime: dto.ExpiryTime,
		SubID:      dto.SubID,
		TgID:       normalizeTgID(dto.TgID),
		Comment:    dto.Comment,
		Reset:      dto.Reset,
		CreatedAt:  dto.CreatedAt,
		UpdatedAt:  dto.UpdatedAt,
	}
	return nil
}

func normalizeTgID(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

// InboundSettings describes inbound settings payload with embedded clients.
type InboundSettings struct {
	Clients    []Client      `json:"clients"`
	Decryption string        `json:"decryption"`
	Fallbacks  []interface{} `json:"fallbacks"`
}

// InboundData mirrors inbound API response object.
type InboundData struct {
	ID             int    `json:"id"`
	Remark         string `json:"remark"`
	Enable         bool   `json:"enable"`
	Port           int    `json:"port"`
	Protocol       string `json:"protocol"`
	Settings       string `json:"settings"`
	StreamSettings string `json:"streamSettings"`
	Tag            string `json:"tag"`
	Sniffing       string `json:"sniffing"`
}

type InboundResponse struct {
	Success bool        `json:"success"`
	Msg     string      `json:"msg"`
	Obj     InboundData `json:"obj"`
}

type XRayClient struct {
	username    string
	password    string
	host        string
	port        string
	webBasePath string
	serverURL   string
	httpClient  *http.Client
}

func New(username, password, host, port, webBasePath string) *XRayClient {
	if webBasePath != "" && !strings.HasPrefix(webBasePath, "/") {
		webBasePath = "/" + webBasePath
	}
	serverURL := fmt.Sprintf("http://%s:%s%s", host, port, webBasePath)

	jar, _ := cookiejar.New(nil)

	return &XRayClient{
		username:    username,
		password:    password,
		host:        host,
		port:        port,
		webBasePath: webBasePath,
		serverURL:   serverURL,
		httpClient: &http.Client{
			Jar: jar, // keep cookies from /login
		},
	}
}

// LoginToServer must be called before any other API calls.
func (x *XRayClient) LoginToServer() error {
	url := fmt.Sprintf("%s/login", x.serverURL)

	payload := map[string]interface{}{
		"username": x.username,
		"password": x.password,
	}

	jsonBody, err := json.Marshal(payload)
	if err != nil {
		colorfulprint.PrintError("Payload marshal failed", err)
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		colorfulprint.PrintError("Request failed", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := x.httpClient.Do(req)
	if err != nil {
		colorfulprint.PrintError("Response login failed", err)
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	colorfulprint.PrintState(fmt.Sprintf("login status=%d\n%s", resp.StatusCode, string(body)))

	return nil
}

func (x *XRayClient) GetInboundById(id int) ([]Client, error) {
	url := fmt.Sprintf("%s/panel/api/inbounds/get/%d", x.serverURL, id)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		colorfulprint.PrintError("Failed request", err)
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := x.httpClient.Do(req)
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		colorfulprint.PrintError("Failed to read response body", err)
		return nil, err
	}

	var inboundResp InboundResponse
	if err := json.Unmarshal(body, &inboundResp); err != nil {
		colorfulprint.PrintError("Failed to unmarshal inbound response", err)
		return nil, err
	}

	if !inboundResp.Success {
		err := fmt.Errorf("API returned success=false: %s", inboundResp.Msg)
		colorfulprint.PrintError("API error", err)
		return nil, err
	}

	var settings InboundSettings
	if err := json.Unmarshal([]byte(inboundResp.Obj.Settings), &settings); err != nil {
		colorfulprint.PrintError("Failed to unmarshal settings", err)
		return nil, err
	}

	colorfulprint.PrintState(fmt.Sprintf("inbound id=%d, protocol=%s, clients=%d",
		inboundResp.Obj.ID, inboundResp.Obj.Protocol, len(settings.Clients)))

	return settings.Clients, nil
}

func (x *XRayClient) GetClientByEmail(inboundID int, email string) (*Client, error) {
	clients, err := x.GetInboundById(inboundID)
	if err != nil {
		return nil, err
	}

	for _, client := range clients {
		if strings.EqualFold(strings.TrimSpace(client.Email), strings.TrimSpace(email)) {
			return &client, nil
		}
	}

	return nil, fmt.Errorf("client with email '%s' not found", email)
}

// GetClientByUUID returns client by UUID inside inbound.
func (x *XRayClient) GetClientByUUID(inboundID int, uuid string) (*Client, error) {
	clients, err := x.GetInboundById(inboundID)
	if err != nil {
		return nil, err
	}

	for _, client := range clients {
		if client.ID == uuid {
			return &client, nil
		}
	}

	return nil, fmt.Errorf("client with UUID '%s' not found", uuid)
}

// GetClientByTelegram searches client by Telegram ID saved in tgId field.
func (x *XRayClient) GetClientByTelegram(inboundID int, tgID string) (*Client, error) {
	clients, err := x.GetInboundById(inboundID)
	if err != nil {
		return nil, err
	}

	for _, client := range clients {
		if strings.TrimSpace(client.TgID) == strings.TrimSpace(tgID) {
			return &client, nil
		}
	}

	return nil, nil
}

func (x *XRayClient) GenerateVLESSLink(client *Client, serverAddress string, port int, serverName string, publicKey string, shortID string, spiderX string) string {
	spx := spiderX
	if strings.TrimSpace(spx) == "" {
		spx = "/"
	}

	link := fmt.Sprintf("vless://%s@%s:%d?encryption=none&security=reality&sni=%s&fp=chrome&pbk=%s&sid=%s&spx=%s&type=tcp&headerType=none",
		client.ID,
		serverAddress,
		port,
		serverName,
		publicKey,
		shortID,
		spx,
	)

	if client.Flow != "" {
		link += fmt.Sprintf("&flow=%s", client.Flow)
	}

	if client.Email != "" {
		link += fmt.Sprintf("#%s", client.Email)
	}

	return link
}

func (x *XRayClient) AddClient(inboundID int, tgUserId string) (string, error) {
	client := Client{
		ID:         uuid.New().String(),
		Email:      tgUserId,
		Flow:       "xtls-rprx-vision",
		LimitIP:    0,
		TotalGB:    0,
		ExpiryTime: 0,
		Enable:     true,
		TgID:       tgUserId,
		SubID:      "",
		Comment:    "tg:" + tgUserId,
		Reset:      0,
	}

	if _, err := x.AddClientWithData(inboundID, client); err != nil {
		return "", err
	}
	return client.ID, nil
}

// AddClientWithData sends full client struct to add a new entry.
func (x *XRayClient) AddClientWithData(inboundID int, client Client) (*Client, error) {
	url := fmt.Sprintf("%s/panel/api/inbounds/addClient", x.serverURL)

	if client.ID == "" {
		client.ID = uuid.New().String()
	}
	if client.Flow == "" {
		client.Flow = "xtls-rprx-vision"
	}

	jsonBody, err := buildClientPayload(inboundID, client)
	if err != nil {
		colorfulprint.PrintError("Failed marshal settings", err)
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		colorfulprint.PrintError("Failed request", err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := x.httpClient.Do(req)
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	colorfulprint.PrintState(fmt.Sprintf("add client status=%d\n%s", resp.StatusCode, string(body)))

	return &client, nil
}

func buildClientPayload(inboundID int, client Client) ([]byte, error) {
	settings := map[string]interface{}{
		"clients": []Client{client},
	}

	settingsJSON, err := json.Marshal(settings)
	if err != nil {
		return nil, err
	}

	payload := map[string]interface{}{
		"id":       inboundID,
		"settings": string(settingsJSON), // raw JSON string expected by API
	}

	return json.Marshal(payload)
}

func (x *XRayClient) UpdateClient(inboundID int, client Client) error {
	if client.ID == "" {
		return fmt.Errorf("client uuid is empty")
	}
	if client.Flow == "" {
		client.Flow = "xtls-rprx-vision"
	}

	url := fmt.Sprintf("%s/panel/api/inbounds/updateClient/%s", x.serverURL, client.ID)

	jsonBody, err := buildClientPayload(inboundID, client)
	if err != nil {
		colorfulprint.PrintError("Failed marshal json", err)
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		colorfulprint.PrintError("Failed request", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := x.httpClient.Do(req)
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	colorfulprint.PrintState(fmt.Sprintf("update client status=%d\n%s", resp.StatusCode, string(body)))

	return nil
}

// EnsureExpiry updates expiryTime for client by adding given days (from now or existing expiry).
func (x *XRayClient) EnsureExpiry(inboundID int, client *Client, daysToAdd int64) (time.Time, error) {
	now := time.Now()
	expireAt := now
	if client.ExpiryTime > 0 {
		expireAt = time.UnixMilli(client.ExpiryTime)
	}
	if expireAt.Before(now) {
		expireAt = now
	}
	if daysToAdd > 0 {
		expireAt = expireAt.Add(time.Duration(daysToAdd) * 24 * time.Hour)
	}

	client.ExpiryTime = expireAt.UnixMilli()
	if client.ID == "" {
		client.ID = uuid.New().String()
	}

	var err error
	if _, err = x.GetClientByUUID(inboundID, client.ID); err != nil {
		_, err = x.AddClientWithData(inboundID, *client)
	} else {
		err = x.UpdateClient(inboundID, *client)
	}

	return expireAt, err
}
