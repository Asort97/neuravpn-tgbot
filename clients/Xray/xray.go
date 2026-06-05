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

func (c Client) MarshalJSON() ([]byte, error) {
	dto := clientDTO{
		ID:         c.ID,
		Email:      c.Email,
		Enable:     c.Enable,
		Flow:       c.Flow,
		LimitIP:    c.LimitIP,
		TotalGB:    c.TotalGB,
		ExpiryTime: c.ExpiryTime,
		SubID:      c.SubID,
		TgID:       tgIDAsNumber(c.TgID),
		Comment:    c.Comment,
		Reset:      c.Reset,
		CreatedAt:  c.CreatedAt,
		UpdatedAt:  c.UpdatedAt,
	}
	return json.Marshal(dto)
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

func tgIDAsNumber(value string) int64 {
	value = strings.ReplaceAll(strings.TrimSpace(value), " ", "")
	if value == "" {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// InboundSettings describes inbound settings payload with embedded clients.
type InboundSettings struct {
	Clients    []Client      `json:"clients"`
	Decryption string        `json:"decryption"`
	Fallbacks  []interface{} `json:"fallbacks"`
}

// InboundData mirrors inbound API response object.
type InboundData struct {
	ID                   int    `json:"id"`
	Up                   int64  `json:"up"`
	Down                 int64  `json:"down"`
	Total                int64  `json:"total"`
	Remark               string `json:"remark"`
	Enable               bool   `json:"enable"`
	ExpiryTime           int64  `json:"expiryTime"`
	TrafficReset         string `json:"trafficReset"`
	LastTrafficResetTime int64  `json:"lastTrafficResetTime"`
	Listen               string `json:"listen"`
	Port                 int    `json:"port"`
	Protocol             string `json:"protocol"`
	Settings             string `json:"settings"`
	StreamSettings       string `json:"streamSettings"`
	Tag                  string `json:"tag"`
	Sniffing             string `json:"sniffing"`
	NodeID               *int   `json:"nodeId,omitempty"`
}

func (i *InboundData) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID                   int             `json:"id"`
		Up                   int64           `json:"up"`
		Down                 int64           `json:"down"`
		Total                int64           `json:"total"`
		Remark               string          `json:"remark"`
		Enable               bool            `json:"enable"`
		ExpiryTime           int64           `json:"expiryTime"`
		TrafficReset         string          `json:"trafficReset"`
		LastTrafficResetTime int64           `json:"lastTrafficResetTime"`
		Listen               string          `json:"listen"`
		Port                 int             `json:"port"`
		Protocol             string          `json:"protocol"`
		Settings             json.RawMessage `json:"settings"`
		StreamSettings       json.RawMessage `json:"streamSettings"`
		Tag                  string          `json:"tag"`
		Sniffing             json.RawMessage `json:"sniffing"`
		NodeID               *int            `json:"nodeId"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*i = InboundData{
		ID:                   raw.ID,
		Up:                   raw.Up,
		Down:                 raw.Down,
		Total:                raw.Total,
		Remark:               raw.Remark,
		Enable:               raw.Enable,
		ExpiryTime:           raw.ExpiryTime,
		TrafficReset:         raw.TrafficReset,
		LastTrafficResetTime: raw.LastTrafficResetTime,
		Listen:               raw.Listen,
		Port:                 raw.Port,
		Protocol:             raw.Protocol,
		Settings:             rawJSONString(raw.Settings),
		StreamSettings:       rawJSONString(raw.StreamSettings),
		Tag:                  raw.Tag,
		Sniffing:             rawJSONString(raw.Sniffing),
		NodeID:               raw.NodeID,
	}
	return nil
}

func rawJSONString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err == nil {
		return buf.String()
	}
	return string(raw)
}

type InboundResponse struct {
	Success bool        `json:"success"`
	Msg     string      `json:"msg"`
	Obj     InboundData `json:"obj"`
}

// ClientTraffic describes 3X-UI traffic counters for a client email.
type ClientTraffic struct {
	ID         int    `json:"id"`
	InboundID  int    `json:"inboundId"`
	Enable     bool   `json:"enable"`
	Email      string `json:"email"`
	Up         int64  `json:"up"`
	Down       int64  `json:"down"`
	ExpiryTime int64  `json:"expiryTime"`
	Total      int64  `json:"total"`
	Reset      int    `json:"reset"`
}

type XRayClient struct {
	username    string
	password    string
	host        string
	port        string
	webBasePath string
	serverURL   string
	apiToken    string
	httpClient  *http.Client
	authMu      sync.Mutex
	csrfToken   string
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

func (x *XRayClient) SetAPIToken(token string) {
	x.apiToken = strings.TrimSpace(token)
}

func (x *XRayClient) hasAPIToken() bool {
	return strings.TrimSpace(x.apiToken) != ""
}

// LoginToServer must be called before any other API calls.
func (x *XRayClient) LoginToServer() error {
	x.authMu.Lock()
	defer x.authMu.Unlock()

	if x.hasAPIToken() {
		return nil
	}

	// 1. Fetch CSRF token for 3x-ui v3.0.0+
	csrfUrl := fmt.Sprintf("%s/csrf-token", x.serverURL)
	csrfReq, err := http.NewRequest("GET", csrfUrl, nil)
	if err != nil {
		return err
	}
	csrfResp, csrfErr := x.httpClient.Do(csrfReq)
	if csrfErr != nil {
		return fmt.Errorf("xray csrf request failed: %w", csrfErr)
	}
	defer csrfResp.Body.Close()
	csrfBody, _ := io.ReadAll(csrfResp.Body)
	if csrfResp.StatusCode < 200 || csrfResp.StatusCode >= 300 {
		if !isEndpointUnsupported(csrfResp.StatusCode) {
			return fmt.Errorf("xray csrf returned status=%d body=%s", csrfResp.StatusCode, responseSnippet(csrfBody))
		}
	} else {
		var csrfResult struct {
			Success bool   `json:"success"`
			Msg     string `json:"msg"`
			Obj     string `json:"obj"`
		}
		if err := json.Unmarshal(csrfBody, &csrfResult); err != nil {
			return fmt.Errorf("xray csrf invalid response: %w; body=%s", err, responseSnippet(csrfBody))
		}
		if !csrfResult.Success || strings.TrimSpace(csrfResult.Obj) == "" {
			return fmt.Errorf("xray csrf failed: %s", strings.TrimSpace(csrfResult.Msg))
		}
		x.csrfToken = csrfResult.Obj
	}

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
	if x.csrfToken != "" {
		req.Header.Set("X-CSRF-Token", x.csrfToken)
	}

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
	var loginResult struct {
		Success bool   `json:"success"`
		Msg     string `json:"msg"`
	}
	if err := json.Unmarshal(body, &loginResult); err != nil {
		return fmt.Errorf("xray login invalid response: %w; body=%s", err, responseSnippet(body))
	}
	if !loginResult.Success {
		return fmt.Errorf("xray login failed: %s", strings.TrimSpace(loginResult.Msg))
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
	if statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed {
		return false
	}
	if len(trimmed) == 0 {
		return true
	}
	if bytes.HasPrefix(trimmed, []byte("<")) {
		return true
	}
	return false
}

func isEndpointUnsupported(statusCode int) bool {
	return statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed
}

func checkAPISuccess(action string, statusCode int, body []byte) error {
	if statusCode < 200 || statusCode >= 300 {
		return fmt.Errorf("%s returned status=%d body=%s", action, statusCode, responseSnippet(body))
	}
	var raw struct {
		Success *bool  `json:"success"`
		Msg     string `json:"msg"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("%s invalid response: %w; body=%s", action, err, responseSnippet(body))
	}
	if raw.Success != nil && !*raw.Success {
		return fmt.Errorf("%s returned success=false: %s", action, strings.TrimSpace(raw.Msg))
	}
	return nil
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
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	if x.hasAPIToken() {
		req.Header.Set("Authorization", "Bearer "+x.apiToken)
	}
	if x.csrfToken != "" {
		req.Header.Set("X-CSRF-Token", x.csrfToken)
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

func (x *XRayClient) GetClientTrafficByEmail(email string) (*ClientTraffic, error) {
	email = strings.TrimSpace(email)
	if email == "" {
		return nil, fmt.Errorf("client email is empty")
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/traffic/%s", x.serverURL, url.PathEscape(email))

	statusCode, body, err := x.doAPIRequest("GET", requestURL, nil, map[string]string{"Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	if isEndpointUnsupported(statusCode) {
		requestURL = fmt.Sprintf("%s/panel/api/inbounds/getClientTraffics/%s", x.serverURL, url.PathEscape(email))
		statusCode, body, err = x.doAPIRequest("GET", requestURL, nil, map[string]string{"Accept": "application/json"})
		if err != nil {
			return nil, err
		}
	}
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("get client traffic returned status=%d body=%s", statusCode, responseSnippet(body))
	}

	var raw struct {
		Success bool            `json:"success"`
		Msg     string          `json:"msg"`
		Obj     json.RawMessage `json:"obj"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("%w; body=%s", err, responseSnippet(body))
	}
	if !raw.Success {
		return nil, fmt.Errorf("API returned success=false: %s", raw.Msg)
	}
	if len(raw.Obj) == 0 || string(raw.Obj) == "null" {
		return nil, nil
	}

	var traffic ClientTraffic
	if err := json.Unmarshal(raw.Obj, &traffic); err == nil {
		return &traffic, nil
	}

	var trafficList []ClientTraffic
	if err := json.Unmarshal(raw.Obj, &trafficList); err != nil {
		return nil, fmt.Errorf("unexpected traffic payload: %s", responseSnippet(raw.Obj))
	}
	if len(trafficList) == 0 {
		return nil, nil
	}
	return &trafficList[0], nil
}

func (x *XRayClient) ResetClientTraffic(inboundID int, email string) error {
	email = strings.TrimSpace(email)
	if inboundID <= 0 {
		return fmt.Errorf("inboundID is invalid")
	}
	if email == "" {
		return fmt.Errorf("client email is empty")
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/resetTraffic/%s", x.serverURL, url.PathEscape(email))

	statusCode, body, err := x.doAPIRequest("POST", requestURL, nil, map[string]string{"Accept": "application/json"})
	if err != nil {
		return err
	}
	if isEndpointUnsupported(statusCode) {
		requestURL = fmt.Sprintf("%s/panel/api/inbounds/%d/resetClientTraffic/%s", x.serverURL, inboundID, url.PathEscape(email))
		statusCode, body, err = x.doAPIRequest("POST", requestURL, nil, map[string]string{"Accept": "application/json"})
		if err != nil {
			return err
		}
	}
	return checkAPISuccess("reset client traffic", statusCode, body)
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

// RealityParams holds the TLS/Reality params parsed from a panel inbound.
type RealityParams struct {
	ServerName  string
	PublicKey   string
	ShortID     string
	SpiderX     string
	Fingerprint string
	ServerPort  int
}

// ExtractRealityParamsFromInbound reads streamSettings of the given inbound
// and returns the Reality params. Returns nil if reality is not configured.
func (x *XRayClient) ExtractRealityParamsFromInbound(inboundID int) (*RealityParams, error) {
	inbounds, err := x.GetAllInbounds()
	if err != nil {
		return nil, err
	}
	for _, ib := range inbounds {
		if ib.ID != inboundID {
			continue
		}
		p := parseRealityParams(ib.StreamSettings, ib.Port)
		return p, nil
	}
	return nil, fmt.Errorf("inbound %d not found", inboundID)
}

// ExtractRealityParamsFromFirstInbound tries all inboundIDs (or auto-detects
// VLESS inbounds) and returns the first inbound that has realitySettings.
func (x *XRayClient) ExtractRealityParamsFromFirstInbound(inboundIDs []int) (*RealityParams, error) {
	inbounds, err := x.GetAllInbounds()
	if err != nil {
		return nil, err
	}

	allowed := map[int]bool{}
	for _, id := range inboundIDs {
		allowed[id] = true
	}

	for _, ib := range inbounds {
		if len(allowed) > 0 && !allowed[ib.ID] {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(ib.Protocol), "vless") {
			continue
		}
		if p := parseRealityParams(ib.StreamSettings, ib.Port); p != nil {
			return p, nil
		}
	}
	return nil, fmt.Errorf("no inbound with realitySettings found")
}

func parseRealityParams(streamSettings string, inboundPort int) *RealityParams {
	raw := strings.TrimSpace(streamSettings)
	if raw == "" {
		return nil
	}

	// In 3x-ui the structure is:
	// realitySettings.settings.publicKey / .fingerprint / .spiderX
	// realitySettings.shortIds[]
	// realitySettings.serverNames[]
	// realitySettings.dest
	var outer struct {
		Security        string `json:"security"`
		RealitySettings struct {
			Dest        string   `json:"dest"`
			Target      string   `json:"target"`
			ServerNames []string `json:"serverNames"`
			ShortIds    []string `json:"shortIds"`
			// Client-visible params nested inside "settings"
			Settings struct {
				PublicKey   string `json:"publicKey"`
				Fingerprint string `json:"fingerprint"`
				SpiderX     string `json:"spiderX"`
			} `json:"settings"`
			// Some older panel versions store them flat (fallback)
			PublicKey    string   `json:"publicKey"`
			Fingerprint  string   `json:"fingerprint"`
			Fingerprints []string `json:"fingerprints"`
			SpiderX      string   `json:"spiderX"`
		} `json:"realitySettings"`
	}

	if err := json.Unmarshal([]byte(raw), &outer); err != nil {
		return nil
	}
	if !strings.EqualFold(outer.Security, "reality") {
		return nil
	}

	r := outer.RealitySettings

	// publicKey: prefer settings.publicKey, fallback to flat
	pk := strings.TrimSpace(r.Settings.PublicKey)
	if pk == "" {
		pk = strings.TrimSpace(r.PublicKey)
	}
	if pk == "" {
		return nil
	}

	// shortID — first non-empty from shortIds list
	shortID := ""
	for _, s := range r.ShortIds {
		if strings.TrimSpace(s) != "" {
			shortID = strings.TrimSpace(s)
			break
		}
	}

	// serverName — first from serverNames, or parse from dest
	serverName := ""
	if len(r.ServerNames) > 0 {
		serverName = strings.TrimSpace(r.ServerNames[0])
	}
	if serverName == "" {
		// target/dest can be "hostname:port" or just "hostname"
		host := strings.TrimSpace(r.Target)
		if host == "" {
			host = strings.TrimSpace(r.Dest)
		}
		if idx := strings.LastIndex(host, ":"); idx >= 0 {
			host = host[:idx]
		}
		serverName = strings.TrimSpace(host)
	}

	// fingerprint: prefer settings.fingerprint, then flat fingerprints[], then flat fingerprint
	fp := strings.TrimSpace(r.Settings.Fingerprint)
	if fp == "" && len(r.Fingerprints) > 0 {
		fp = strings.TrimSpace(r.Fingerprints[0])
	}
	if fp == "" {
		fp = strings.TrimSpace(r.Fingerprint)
	}
	if fp == "" {
		fp = "chrome"
	}

	// spiderX: prefer settings.spiderX, fallback to flat
	spx := strings.TrimSpace(r.Settings.SpiderX)
	if spx == "" {
		spx = strings.TrimSpace(r.SpiderX)
	}
	if spx == "" {
		spx = "/"
	}

	// port: use the inbound port if non-zero
	port := inboundPort

	return &RealityParams{
		ServerName:  serverName,
		PublicKey:   pk,
		ShortID:     shortID,
		SpiderX:     spx,
		Fingerprint: fp,
		ServerPort:  port,
	}
}

// parseTransportParams extracts transport-specific query params from raw streamSettings JSON.
// Returns params string like "type=xhttp" or "type=tcp&headerType=none" etc.
func parseTransportParams(streamSettings string) string {
	raw := strings.TrimSpace(streamSettings)
	if raw == "" {
		return "type=tcp&headerType=none"
	}

	// First pass: get network type and raw xhttpSettings blob
	var outer struct {
		Network           string          `json:"network"`
		XhttpSettings     json.RawMessage `json:"xhttpSettings"`
		SplitHttpSettings json.RawMessage `json:"splitHttpSettings"`
		WsSettings        struct {
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
		TcpSettings struct {
			Header struct {
				Type string `json:"type"`
			} `json:"header"`
		} `json:"tcpSettings"`
	}

	if err := json.Unmarshal([]byte(raw), &outer); err != nil {
		return "type=tcp&headerType=none"
	}

	network := strings.ToLower(strings.TrimSpace(outer.Network))
	if network == "" {
		network = "tcp"
	}

	switch network {
	case "xhttp", "splithttp":
		// Pick the right settings blob
		settingsRaw := outer.XhttpSettings
		if len(settingsRaw) == 0 || string(settingsRaw) == "null" {
			settingsRaw = outer.SplitHttpSettings
		}

		// Second pass: extract individual fields for standard params
		var xhttpFields struct {
			Path string `json:"path"`
			Host string `json:"host"`
			Mode string `json:"mode"`
		}
		if len(settingsRaw) > 0 && string(settingsRaw) != "null" {
			_ = json.Unmarshal(settingsRaw, &xhttpFields)
		}

		path := strings.TrimSpace(xhttpFields.Path)
		if path == "" {
			path = "/"
		}
		host := strings.TrimSpace(xhttpFields.Host)
		mode := strings.TrimSpace(xhttpFields.Mode)

		params := fmt.Sprintf("type=%s&path=%s", network, url.QueryEscape(path))
		if host != "" {
			params += "&host=" + url.QueryEscape(host)
		}
		if mode != "" {
			params += "&mode=" + url.QueryEscape(mode)
		}
		// Pass entire xhttpSettings as extra so clients get all server-side params
		if len(settingsRaw) > 0 && string(settingsRaw) != "null" {
			params += "&extra=" + url.QueryEscape(string(settingsRaw))
		}
		return params
	case "ws":
		path := strings.TrimSpace(outer.WsSettings.Path)
		if path == "" {
			path = "/"
		}
		host := strings.TrimSpace(outer.WsSettings.Headers["Host"])
		params := fmt.Sprintf("type=ws&path=%s", url.QueryEscape(path))
		if host != "" {
			params += "&host=" + url.QueryEscape(host)
		}
		return params
	case "grpc":
		svcName := strings.TrimSpace(outer.GrpcSettings.ServiceName)
		mode := strings.TrimSpace(outer.GrpcSettings.Mode)
		params := "type=grpc"
		if svcName != "" {
			params += "&serviceName=" + url.QueryEscape(svcName)
		}
		if mode != "" {
			params += "&mode=" + url.QueryEscape(mode)
		}
		return params
	case "h2", "http":
		path := strings.TrimSpace(outer.HttpSettings.Path)
		if path == "" {
			path = "/"
		}
		params := fmt.Sprintf("type=h2&path=%s", url.QueryEscape(path))
		if len(outer.HttpSettings.Host) > 0 {
			params += "&host=" + url.QueryEscape(outer.HttpSettings.Host[0])
		}
		return params
	default: // tcp
		headerType := strings.TrimSpace(outer.TcpSettings.Header.Type)
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
				log.Printf("[GenerateVLESSLinkForInbound] inbound=%d streamSettings=%s", inboundID, ib.StreamSettings)
				transportParams = parseTransportParams(ib.StreamSettings)
				log.Printf("[GenerateVLESSLinkForInbound] transportParams=%s", transportParams)
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
	if inboundID <= 0 {
		return nil, fmt.Errorf("inboundID is invalid")
	}
	if client.ID == "" {
		client.ID = uuid.New().String()
	}
	client.Flow = strings.TrimSpace(client.Flow)
	if client.Flow == "" {
		client.Flow = x.defaultFlowForInbound(inboundID)
	}

	if err := x.AddClientToInbounds(client, []int{inboundID}); err == nil {
		return &client, nil
	} else {
		log.Printf("[XRAY] clients/add failed inbound=%d email=%s uuid=%s err=%v", inboundID, client.Email, client.ID, err)
		if legacyErr := x.addClientWithLegacyAPI(inboundID, client); legacyErr != nil {
			return nil, fmt.Errorf("add client failed: clients/add: %v; inbounds/addClient: %v", err, legacyErr)
		}
	}
	return &client, nil
}

func (x *XRayClient) UpdateClient(inboundID int, client Client) error {
	if client.ID == "" {
		return fmt.Errorf("client uuid is empty")
	}
	if inboundID <= 0 {
		return fmt.Errorf("inboundID is invalid")
	}
	client.Flow = strings.TrimSpace(client.Flow)
	if client.Flow == "" {
		client.Flow = x.defaultFlowForInbound(inboundID)
	}

	if err := x.UpdateClientByEmail(client.Email, client); err == nil {
		return nil
	} else {
		log.Printf("[XRAY] clients/update failed inbound=%d email=%s uuid=%s err=%v", inboundID, client.Email, client.ID, err)
		if legacyErr := x.updateClientWithLegacyAPI(inboundID, client); legacyErr != nil {
			return fmt.Errorf("update client failed: clients/update: %v; inbounds/updateClient: %v", err, legacyErr)
		}
	}
	return nil
}

func (x *XRayClient) addClientWithLegacyAPI(inboundID int, client Client) error {
	requestURL := fmt.Sprintf("%s/panel/api/inbounds/addClient", x.serverURL)
	bodyPayload, err := buildLegacyClientPayload(inboundID, client)
	if err != nil {
		return err
	}
	statusCode, body, err := x.doAPIRequest("POST", requestURL, bodyPayload, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		return err
	}
	return checkAPISuccess("add client", statusCode, body)
}

func (x *XRayClient) updateClientWithLegacyAPI(inboundID int, client Client) error {
	requestURL := fmt.Sprintf("%s/panel/api/inbounds/updateClient/%s", x.serverURL, url.PathEscape(client.ID))
	bodyPayload, err := buildLegacyClientPayload(inboundID, client)
	if err != nil {
		return err
	}
	statusCode, body, err := x.doAPIRequest("POST", requestURL, bodyPayload, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		return err
	}
	return checkAPISuccess("update client", statusCode, body)
}

func buildLegacyClientPayload(inboundID int, client Client) ([]byte, error) {
	settings := map[string]interface{}{
		"clients": []Client{client},
	}
	settingsJSON, err := json.Marshal(settings)
	if err != nil {
		return nil, err
	}
	payload := map[string]interface{}{
		"id":       inboundID,
		"settings": string(settingsJSON),
	}
	return json.Marshal(payload)
}

func (x *XRayClient) GetClientRecordByEmail(email string) (*Client, []int, error) {
	email = strings.TrimSpace(email)
	if email == "" {
		return nil, nil, fmt.Errorf("client email is empty")
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/get/%s", x.serverURL, url.PathEscape(email))
	statusCode, body, err := x.doAPIRequest("GET", requestURL, nil, map[string]string{"Accept": "application/json"})
	if err != nil {
		return nil, nil, err
	}
	if statusCode == http.StatusNotFound {
		return nil, nil, nil
	}
	if statusCode < 200 || statusCode >= 300 {
		return nil, nil, fmt.Errorf("get client record returned status=%d body=%s", statusCode, responseSnippet(body))
	}

	var raw struct {
		Success bool   `json:"success"`
		Msg     string `json:"msg"`
		Obj     struct {
			Client struct {
				UUID       string      `json:"uuid"`
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
				CreatedAt  int64       `json:"createdAt"`
				UpdatedAt  int64       `json:"updatedAt"`
			} `json:"client"`
			InboundIDs []int `json:"inboundIds"`
		} `json:"obj"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, nil, fmt.Errorf("%w; body=%s", err, responseSnippet(body))
	}
	if !raw.Success {
		return nil, nil, nil
	}
	c := &Client{
		ID:         raw.Obj.Client.UUID,
		Email:      raw.Obj.Client.Email,
		Enable:     raw.Obj.Client.Enable,
		Flow:       raw.Obj.Client.Flow,
		LimitIP:    raw.Obj.Client.LimitIP,
		TotalGB:    raw.Obj.Client.TotalGB,
		ExpiryTime: raw.Obj.Client.ExpiryTime,
		SubID:      raw.Obj.Client.SubID,
		TgID:       normalizeTgID(raw.Obj.Client.TgID),
		Comment:    raw.Obj.Client.Comment,
		Reset:      raw.Obj.Client.Reset,
		CreatedAt:  raw.Obj.Client.CreatedAt,
		UpdatedAt:  raw.Obj.Client.UpdatedAt,
	}
	return c, raw.Obj.InboundIDs, nil
}

func (x *XRayClient) AddClientToInbounds(client Client, inboundIDs []int) error {
	if len(inboundIDs) == 0 {
		return fmt.Errorf("no inbound IDs provided")
	}
	payload := map[string]interface{}{
		"client":     client,
		"inboundIds": inboundIDs,
	}
	bodyPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/add", x.serverURL)
	statusCode, body, err := x.doAPIRequest("POST", requestURL, bodyPayload, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		return err
	}
	return checkAPISuccess("add client", statusCode, body)
}

func (x *XRayClient) AttachClientToInbounds(email string, inboundIDs []int) error {
	email = strings.TrimSpace(email)
	if email == "" || len(inboundIDs) == 0 {
		return nil
	}
	payload := map[string]interface{}{
		"inboundIds": inboundIDs,
	}
	bodyPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/%s/attach", x.serverURL, url.PathEscape(email))
	statusCode, body, err := x.doAPIRequest("POST", requestURL, bodyPayload, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		return err
	}
	return checkAPISuccess("attach client", statusCode, body)
}

func (x *XRayClient) DeleteClientByEmail(email string, keepTraffic bool) error {
	email = strings.TrimSpace(email)
	if email == "" {
		return fmt.Errorf("client email is empty")
	}
	suffix := ""
	if keepTraffic {
		suffix = "?keepTraffic=1"
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/del/%s%s", x.serverURL, url.PathEscape(email), suffix)
	statusCode, body, err := x.doAPIRequest("POST", requestURL, nil, map[string]string{"Accept": "application/json"})
	if err != nil {
		return err
	}
	return checkAPISuccess("delete client", statusCode, body)
}

func (x *XRayClient) UpdateClientByEmail(routeEmail string, client Client) error {
	routeEmail = strings.TrimSpace(routeEmail)
	if routeEmail == "" {
		return fmt.Errorf("client email is empty")
	}
	bodyPayload, err := json.Marshal(client)
	if err != nil {
		return err
	}
	requestURL := fmt.Sprintf("%s/panel/api/clients/update/%s", x.serverURL, url.PathEscape(routeEmail))
	statusCode, body, err := x.doAPIRequest("POST", requestURL, bodyPayload, map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})
	if err != nil {
		return err
	}
	return checkAPISuccess("update client", statusCode, body)
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

	stableSubID := strings.TrimSpace(subID)
	if stableSubID == "" {
		stableSubID = "sub" + strings.TrimSpace(tgID)
	}

	type inboundClient struct {
		inboundID int
		client    Client
	}
	var matches []inboundClient
	var lastErr error
	for _, inboundID := range inboundIDs {
		clients, err := x.GetInboundById(inboundID)
		if err != nil {
			lastErr = err
			log.Printf("[XRAY] scan inbound failed inbound=%d tg=%s err=%v", inboundID, tgID, err)
			continue
		}
		for _, candidate := range clients {
			if clientMatchesTelegram(candidate, tgID, stableSubID) {
				matches = append(matches, inboundClient{inboundID: inboundID, client: candidate})
			}
		}
	}

	now := time.Now()
	expireAt := now
	for _, item := range matches {
		if item.client.ExpiryTime <= 0 {
			continue
		}
		candidateExpire := time.UnixMilli(item.client.ExpiryTime)
		if candidateExpire.After(expireAt) {
			expireAt = candidateExpire
		}
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

	if len(matches) > 0 {
		var primary *Client
		updated := 0
		for _, item := range matches {
			client := item.client
			client.Enable = true
			client.ExpiryTime = expireAt.UnixMilli()
			client.TgID = strings.TrimSpace(tgID)
			client.SubID = stableSubID
			if strings.TrimSpace(client.Comment) == "" || strings.HasPrefix(strings.TrimSpace(client.Comment), "tg:") {
				client.Comment = "tg:" + strings.TrimSpace(tgID)
			}
			if strings.TrimSpace(client.Flow) == "" {
				client.Flow = x.defaultFlowForInbound(item.inboundID)
			}
			if err := x.UpdateClient(item.inboundID, client); err != nil {
				lastErr = err
				log.Printf("[XRAY] update duplicate failed inbound=%d email=%s tg=%s err=%v", item.inboundID, client.Email, tgID, err)
				continue
			}
			updated++
			if primary == nil {
				copyClient := client
				primary = &copyClient
			}
		}
		if updated > 0 && primary != nil {
			return primary, expireAt, nil
		}
		if lastErr != nil {
			return nil, time.Time{}, lastErr
		}
		return nil, time.Time{}, fmt.Errorf("no xray duplicate clients updated for tg=%s", tgID)
	}

	client := Client{
		ID:         uuid.New().String(),
		Email:      buildStableXrayClientEmail(email, tgID),
		Enable:     true,
		Flow:       x.defaultFlowForInbound(inboundIDs[0]),
		LimitIP:    0,
		TotalGB:    0,
		ExpiryTime: expireAt.UnixMilli(),
		TgID:       strings.TrimSpace(tgID),
		SubID:      stableSubID,
		Comment:    "tg:" + strings.TrimSpace(tgID),
	}
	if err := x.AddClientToInbounds(client, inboundIDs); err == nil {
		return &client, expireAt, nil
	} else {
		lastErr = err
		log.Printf("[XRAY] create multi-inbound client failed inbounds=%v email=%s tg=%s err=%v", inboundIDs, client.Email, tgID, err)
	}

	created := 0
	var primary *Client
	for _, inboundID := range inboundIDs {
		perInboundClient := client
		perInboundClient.Email = buildXrayClientEmail(email, tgID, inboundID)
		perInboundClient.Flow = x.defaultFlowForInbound(inboundID)
		if err := x.addClientWithLegacyAPI(inboundID, perInboundClient); err != nil {
			lastErr = err
			log.Printf("[XRAY] create legacy duplicate failed inbound=%d email=%s tg=%s err=%v", inboundID, perInboundClient.Email, tgID, err)
			continue
		}
		created++
		if primary == nil {
			copyClient := perInboundClient
			primary = &copyClient
		}
	}
	if created > 0 && primary != nil {
		return primary, expireAt, nil
	}
	return nil, time.Time{}, lastErr
}

func clientMatchesTelegram(client Client, tgID string, subID string) bool {
	tgID = strings.TrimSpace(tgID)
	subID = strings.TrimSpace(subID)
	if tgID != "" && strings.TrimSpace(client.TgID) == tgID {
		return true
	}
	if subID != "" && strings.TrimSpace(client.SubID) == subID {
		return true
	}
	if tgID != "" && strings.TrimSpace(client.Comment) == "tg:"+tgID {
		return true
	}
	email := strings.ToLower(strings.TrimSpace(client.Email))
	if tgID != "" && (strings.Contains(email, "tg"+strings.ToLower(tgID)) || strings.HasPrefix(email, strings.ToLower(tgID)+"@")) {
		return true
	}
	return false
}

func buildStableXrayClientEmail(billingEmail, tgID string) string {
	tgID = sanitizeEmailToken(tgID)
	if tgID == "" {
		tgID = "unknown"
	}
	billingEmail = strings.TrimSpace(billingEmail)
	parts := strings.SplitN(billingEmail, "@", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		return fmt.Sprintf("tg%s@%s", tgID, strings.TrimSpace(parts[1]))
	}
	return fmt.Sprintf("tg%s@happycat", tgID)
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
