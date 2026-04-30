package xray

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"sync"
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
	authMu      sync.Mutex
}

func New(username, password, host, port, webBasePath string) *XRayClient {
	if webBasePath != "" && !strings.HasPrefix(webBasePath, "/") {
		webBasePath = "/" + webBasePath
	}

	// Auto-detect protocol: use https for common secure ports or if host starts with https://
	protocol := "http"
	if port == "443" || port == "8443" || strings.HasPrefix(host, "https://") {
		protocol = "https"
		host = strings.TrimPrefix(host, "https://")
	}
	host = strings.TrimPrefix(host, "http://")

	serverURL := fmt.Sprintf("%s://%s:%s%s", protocol, host, port, webBasePath)

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
	x.authMu.Lock()
	defer x.authMu.Unlock()

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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("xray login returned status=%d", resp.StatusCode)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return fmt.Errorf("xray login returned empty body")
	}

	return nil
}

func responseSnippet(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "<empty>"
	}
	if len(trimmed) > 300 {
		return trimmed[:300] + "..."
	}
	return trimmed
}

func shouldRetryAfterRelogin(statusCode int, body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
		return true
	}
	if len(trimmed) == 0 {
		return true
	}
	if bytes.HasPrefix(trimmed, []byte("<")) {
		return true
	}
	return false
}

func (x *XRayClient) doAPIRequest(method, url string, payload []byte, headers map[string]string) (int, []byte, error) {
	return x.doAPIRequestOnce(method, url, payload, headers, true)
}

func (x *XRayClient) doAPIRequestOnce(method, url string, payload []byte, headers map[string]string, allowRetry bool) (int, []byte, error) {
	var bodyReader io.Reader
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := x.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}

	if allowRetry && shouldRetryAfterRelogin(resp.StatusCode, body) {
		log.Printf("[XRAY] retry after re-login method=%s url=%s status=%d body=%s", method, url, resp.StatusCode, responseSnippet(body))
		if err := x.LoginToServer(); err != nil {
			return resp.StatusCode, body, fmt.Errorf("xray relogin failed: %w", err)
		}
		return x.doAPIRequestOnce(method, url, payload, headers, false)
	}

	return resp.StatusCode, body, nil
}

func (x *XRayClient) GetInboundById(id int) ([]Client, error) {
	url := fmt.Sprintf("%s/panel/api/inbounds/get/%d", x.serverURL, id)

	statusCode, body, err := x.doAPIRequest("GET", url, nil, map[string]string{"Accept": "application/json"})
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return nil, err
	}
	if statusCode < 200 || statusCode >= 300 {
		err := fmt.Errorf("unexpected status=%d body=%s", statusCode, responseSnippet(body))
		colorfulprint.PrintError("Unexpected inbound status", err)
		return nil, err
	}

	var inboundResp InboundResponse
	if err := json.Unmarshal(body, &inboundResp); err != nil {
		wrappedErr := fmt.Errorf("%w; body=%s", err, responseSnippet(body))
		colorfulprint.PrintError("Failed to unmarshal inbound response", wrappedErr)
		return nil, wrappedErr
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

// GetAllInbounds retrieves all inbound objects from 3X-UI.
func (x *XRayClient) GetAllInbounds() ([]InboundData, error) {
	url := fmt.Sprintf("%s/panel/api/inbounds/list", x.serverURL)

	statusCode, body, err := x.doAPIRequest("GET", url, nil, map[string]string{"Accept": "application/json"})
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return nil, err
	}
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("unexpected status=%d body=%s", statusCode, responseSnippet(body))
	}

	// 3X-UI returns { success, obj: [ ... ] }
	var raw struct {
		Success bool          `json:"success"`
		Msg     string        `json:"msg"`
		Obj     []InboundData `json:"obj"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		wrappedErr := fmt.Errorf("%w; body=%s", err, responseSnippet(body))
		colorfulprint.PrintError("Failed to unmarshal inbounds list", wrappedErr)
		return nil, wrappedErr
	}
	if !raw.Success {
		return nil, fmt.Errorf("API returned success=false: %s", raw.Msg)
	}
	return raw.Obj, nil
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

// GetClientBySubID searches client by SubID inside inbound.
func (x *XRayClient) GetClientBySubID(inboundID int, subID string) (*Client, error) {
	clients, err := x.GetInboundById(inboundID)
	if err != nil {
		return nil, err
	}

	needle := strings.TrimSpace(subID)
	for _, client := range clients {
		if needle != "" && strings.TrimSpace(client.SubID) == needle {
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

// parseTransportParams extracts transport-specific query params from raw streamSettings JSON.
// Returns params string like "type=xhttp" or "type=tcp&headerType=none" etc.
func parseTransportParams(streamSettings string) string {
	raw := strings.TrimSpace(streamSettings)
	if raw == "" {
		return "type=tcp&headerType=none"
	}

	var ss struct {
		Network       string `json:"network"`
		Security      string `json:"security"`
		XhttpSettings struct {
			Path  string          `json:"path"`
			Host  string          `json:"host"`
			Mode  string          `json:"mode"`
			Extra json.RawMessage `json:"extra"`
		} `json:"xhttpSettings"`
		WsSettings struct {
			Path    string            `json:"path"`
			Headers map[string]string `json:"headers"`
		} `json:"wsSettings"`
		GrpcSettings struct {
			ServiceName string `json:"serviceName"`
			Mode        string `json:"mode"`
		} `json:"grpcSettings"`
		HttpSettings struct {
			Path string   `json:"path"`
			Host []string `json:"host"`
		} `json:"httpSettings"`
		SplitHttpSettings struct {
			Path string `json:"path"`
			Host string `json:"host"`
		} `json:"splitHttpSettings"`
		TcpSettings struct {
			Header struct {
				Type string `json:"type"`
			} `json:"header"`
		} `json:"tcpSettings"`
	}

	if err := json.Unmarshal([]byte(raw), &ss); err != nil {
		return "type=tcp&headerType=none"
	}

	network := strings.ToLower(strings.TrimSpace(ss.Network))
	if network == "" {
		network = "tcp"
	}

	switch network {
	case "xhttp", "splithttp":
		path := strings.TrimSpace(ss.XhttpSettings.Path)
		if path == "" {
			path = strings.TrimSpace(ss.SplitHttpSettings.Path)
		}
		if path == "" {
			path = "/"
		}
		host := strings.TrimSpace(ss.XhttpSettings.Host)
		if host == "" {
			host = strings.TrimSpace(ss.SplitHttpSettings.Host)
		}
		mode := strings.TrimSpace(ss.XhttpSettings.Mode)
		params := fmt.Sprintf("type=%s&path=%s", network, url.QueryEscape(path))
		if host != "" {
			params += "&host=" + url.QueryEscape(host)
		}
		if mode != "" {
			params += "&mode=" + url.QueryEscape(mode)
		}
		if len(ss.XhttpSettings.Extra) > 0 && string(ss.XhttpSettings.Extra) != "null" {
			params += "&extra=" + url.QueryEscape(string(ss.XhttpSettings.Extra))
		}
		return params
	case "ws":
		path := strings.TrimSpace(ss.WsSettings.Path)
		if path == "" {
			path = "/"
		}
		host := strings.TrimSpace(ss.WsSettings.Headers["Host"])
		params := fmt.Sprintf("type=ws&path=%s", url.QueryEscape(path))
		if host != "" {
			params += "&host=" + url.QueryEscape(host)
		}
		return params
	case "grpc":
		svcName := strings.TrimSpace(ss.GrpcSettings.ServiceName)
		mode := strings.TrimSpace(ss.GrpcSettings.Mode)
		params := "type=grpc"
		if svcName != "" {
			params += "&serviceName=" + url.QueryEscape(svcName)
		}
		if mode != "" {
			params += "&mode=" + url.QueryEscape(mode)
		}
		return params
	case "h2", "http":
		path := strings.TrimSpace(ss.HttpSettings.Path)
		if path == "" {
			path = "/"
		}
		params := fmt.Sprintf("type=h2&path=%s", url.QueryEscape(path))
		if len(ss.HttpSettings.Host) > 0 {
			params += "&host=" + url.QueryEscape(ss.HttpSettings.Host[0])
		}
		return params
	default: // tcp
		headerType := strings.TrimSpace(ss.TcpSettings.Header.Type)
		if headerType == "" {
			headerType = "none"
		}
		return fmt.Sprintf("type=tcp&headerType=%s", headerType)
	}
}

// GenerateVLESSLinkForInbound generates a VLESS link using the actual transport settings
// read from the specified inbound, instead of hardcoding tcp.
func (x *XRayClient) GenerateVLESSLinkForInbound(client *Client, inboundID int, serverAddress string, port int, serverName string, publicKey string, shortID string, spiderX string, fingerprint string) string {
	spx := spiderX
	if strings.TrimSpace(spx) == "" {
		spx = "/"
	}
	fp := fingerprint
	if strings.TrimSpace(fp) == "" {
		fp = "chrome"
	}

	transportParams := "type=tcp&headerType=none"
	inbounds, err := x.GetAllInbounds()
	if err == nil {
		for _, ib := range inbounds {
			if ib.ID == inboundID {
				transportParams = parseTransportParams(ib.StreamSettings)
				break
			}
		}
	}

	link := fmt.Sprintf("vless://%s@%s:%d?encryption=none&security=reality&sni=%s&fp=%s&pbk=%s&sid=%s&spx=%s&%s",
		client.ID,
		serverAddress,
		port,
		url.QueryEscape(serverName),
		fp,
		publicKey,
		shortID,
		url.QueryEscape(spx),
		transportParams,
	)

	if client.Flow != "" {
		link += fmt.Sprintf("&flow=%s", client.Flow)
	}

	if client.Email != "" {
		link += fmt.Sprintf("#%s", url.PathEscape(client.Email))
	}

	return link
}

func inferFlowFromInbound(inbound InboundData) string {
	stream := strings.ToLower(strings.TrimSpace(inbound.StreamSettings))
	if stream == "" {
		return ""
	}

	if strings.Contains(stream, `"network":"xhttp"`) ||
		strings.Contains(stream, `"network":"ws"`) ||
		strings.Contains(stream, `"network":"grpc"`) ||
		strings.Contains(stream, `"network":"httpupgrade"`) ||
		strings.Contains(stream, `"network":"splithttp"`) {
		return ""
	}

	if strings.Contains(stream, `"network":"tcp"`) && strings.Contains(stream, `"security":"reality"`) {
		return "xtls-rprx-vision"
	}

	return ""
}

func (x *XRayClient) defaultFlowForInbound(inboundID int) string {
	inbounds, err := x.GetAllInbounds()
	if err != nil {
		return ""
	}
	for _, inbound := range inbounds {
		if inbound.ID == inboundID {
			return inferFlowFromInbound(inbound)
		}
	}
	return ""
}

func (x *XRayClient) AddClient(inboundID int, tgUserId string) (string, error) {
	client := Client{
		ID:         uuid.New().String(),
		Email:      tgUserId,
		Flow:       x.defaultFlowForInbound(inboundID),
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
	client.Flow = strings.TrimSpace(client.Flow)
	if client.Flow == "" {
		client.Flow = x.defaultFlowForInbound(inboundID)
	}

	jsonBody, err := buildClientPayload(inboundID, client)
	if err != nil {
		colorfulprint.PrintError("Failed marshal settings", err)
		return nil, err
	}

	statusCode, body, err := x.doAPIRequest("POST", url, jsonBody, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return nil, err
	}
	colorfulprint.PrintState(fmt.Sprintf("add client status=%d\n%s", statusCode, string(body)))
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("add client returned status=%d body=%s", statusCode, responseSnippet(body))
	}

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
	client.Flow = strings.TrimSpace(client.Flow)
	if client.Flow == "" {
		client.Flow = x.defaultFlowForInbound(inboundID)
	}

	url := fmt.Sprintf("%s/panel/api/inbounds/updateClient/%s", x.serverURL, client.ID)

	jsonBody, err := buildClientPayload(inboundID, client)
	if err != nil {
		colorfulprint.PrintError("Failed marshal json", err)
		return err
	}

	statusCode, body, err := x.doAPIRequest("POST", url, jsonBody, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		colorfulprint.PrintError("Failed response", err)
		return err
	}
	colorfulprint.PrintState(fmt.Sprintf("update client status=%d\n%s", statusCode, string(body)))
	if statusCode < 200 || statusCode >= 300 {
		return fmt.Errorf("update client returned status=%d body=%s", statusCode, responseSnippet(body))
	}

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
	if daysToAdd != 0 {
		expireAt = expireAt.Add(time.Duration(daysToAdd) * 24 * time.Hour)
		if expireAt.Before(now) {
			expireAt = now
		}
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

// EnsureClientAcrossInbounds ensures a client with given Telegram ID exists across all provided inbound IDs.
// It will set SubID in each inbound to "sub"+tgID, enable client, and extend expiry by daysToAdd.
// Returns the primary client (from first inbound) and its expiry.
func (x *XRayClient) EnsureClientAcrossInbounds(inboundIDs []int, tgID string, email string, daysToAdd int64, subID string) (*Client, time.Time, error) {
	if len(inboundIDs) == 0 {
		return nil, time.Time{}, fmt.Errorf("no inbound IDs provided")
	}
	log.Printf("[XRAY] ensure across inbounds=%v tg=%s daysToAdd=%d", inboundIDs, tgID, daysToAdd)

	// First ensure on primary inbound, capturing UUID and expiry
	primaryID := inboundIDs[0]
	primaryEmail := buildXrayClientEmail(email, tgID, primaryID)
	primaryFlow := x.defaultFlowForInbound(primaryID)
	primaryClient, err := x.GetClientByTelegram(primaryID, tgID)
	if err != nil {
		return nil, time.Time{}, err
	}

	if primaryClient == nil {
		primaryClient = &Client{
			ID:      uuid.New().String(),
			Email:   primaryEmail,
			Enable:  true,
			Flow:    primaryFlow,
			LimitIP: 0,
			TotalGB: 0,
			TgID:    tgID,
			SubID:   strings.TrimSpace(subID),
			Comment: "tg:" + tgID,
		}
		if _, err := x.AddClientWithData(primaryID, *primaryClient); err != nil {
			return nil, time.Time{}, err
		}
	} else {
		// normalize fields
		if strings.TrimSpace(primaryClient.Email) == "" || primaryClient.Email != primaryEmail {
			primaryClient.Email = primaryEmail
		}
		primaryClient.Enable = true
		primaryClient.Flow = primaryFlow
		primaryClient.TgID = tgID
		if strings.TrimSpace(subID) != "" {
			primaryClient.SubID = strings.TrimSpace(subID)
		} else {
			primaryClient.SubID = "sub" + tgID
		}
		if strings.TrimSpace(primaryClient.Comment) == "" {
			primaryClient.Comment = "tg:" + tgID
		}
		if err := x.UpdateClient(primaryID, *primaryClient); err != nil {
			return nil, time.Time{}, err
		}
	}

	exp, err := x.EnsureExpiry(primaryID, primaryClient, daysToAdd)
	if err != nil {
		return nil, time.Time{}, err
	}

	// Mirror client to other inbounds using the same UUID
	for _, inboundID := range inboundIDs[1:] {
		log.Printf("[XRAY] sync client tg=%s to inbound=%d", tgID, inboundID)
		c, err := x.GetClientByTelegram(inboundID, tgID)
		if err != nil {
			return nil, time.Time{}, err
		}

		// Prepare client data with same UUID and expiry from primary
		inboundFlow := x.defaultFlowForInbound(inboundID)
		clientData := &Client{
			ID:         primaryClient.ID, // keep same UUID across inbounds
			Email:      buildXrayClientEmail(email, tgID, inboundID),
			Enable:     true,
			Flow:       inboundFlow,
			LimitIP:    0,
			TotalGB:    0,
			ExpiryTime: exp.UnixMilli(), // use expiry from primary
			TgID:       tgID,
			SubID:      primaryClient.SubID,
			Comment:    "tg:" + tgID,
		}

		if c == nil {
			// Client doesn't exist on this inbound, create it
			if _, err := x.AddClientWithData(inboundID, *clientData); err != nil {
				log.Printf("[XRAY] add client failed inbound=%d tg=%s err=%v", inboundID, tgID, err)
				return nil, time.Time{}, err
			}
		} else {
			// Client exists, update all fields
			clientData.CreatedAt = c.CreatedAt
			clientData.UpdatedAt = c.UpdatedAt
			if err := x.UpdateClient(inboundID, *clientData); err != nil {
				log.Printf("[XRAY] update client failed inbound=%d tg=%s err=%v", inboundID, tgID, err)
				return nil, time.Time{}, err
			}
		}
	}

	return primaryClient, exp, nil
}

// buildXrayClientEmail returns a deterministic technical email for Xray client identity.
// It is unique per Telegram user and inbound, while preserving the base domain if billing email is provided.
func buildXrayClientEmail(billingEmail, tgID string, inboundID int) string {
	billingEmail = strings.TrimSpace(billingEmail)
	tgID = sanitizeEmailToken(tgID)
	if tgID == "" {
		tgID = "unknown"
	}

	parts := strings.SplitN(billingEmail, "@", 2)
	if len(parts) == 2 {
		local := sanitizeEmailToken(parts[0])
		domain := strings.TrimSpace(parts[1])
		if local != "" && domain != "" {
			return fmt.Sprintf("%s+tg%s+inb%d@%s", local, tgID, inboundID, domain)
		}
	}

	return fmt.Sprintf("tg%s_inb%d@happycat", tgID, inboundID)
}

func sanitizeEmailToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-' || r == '.':
			b.WriteRune(r)
		}
	}
	return strings.Trim(b.String(), ".")
}
