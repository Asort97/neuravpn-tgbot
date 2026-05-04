package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type app struct {
	db           *pgxpool.Pool
	authSecret   []byte
	corsOrigin   string
	publicBase   string
	subBase      string
	mergedBase   string
	mergedSecret string
	yooShopID    string
	yooSecret    string
}

type plan struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Amount float64 `json:"amount"`
	Days   int     `json:"days"`
}

var plans = []plan{
	{ID: "30d", Title: "30 дней", Amount: 99, Days: 30},
	{ID: "60d", Title: "60 дней", Amount: 169, Days: 60},
	{ID: "90d", Title: "90 дней", Amount: 249, Days: 90},
	{ID: "365d", Title: "365 дней", Amount: 949, Days: 365},
}

func main() {
	dsn := strings.TrimSpace(os.Getenv("DB_DSN"))
	if dsn == "" {
		log.Fatal("DB_DSN is required")
	}
	secret := strings.TrimSpace(os.Getenv("WEB_AUTH_SECRET"))
	if secret == "" {
		secret = strings.TrimSpace(os.Getenv("TG_BOT_TOKEN"))
	}
	if secret == "" {
		log.Fatal("WEB_AUTH_SECRET or TG_BOT_TOKEN is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("postgres connect failed: %v", err)
	}
	defer db.Close()

	a := &app{
		db:           db,
		authSecret:   []byte(secret),
		corsOrigin:   strings.TrimSpace(os.Getenv("WEB_CORS_ORIGIN")),
		publicBase:   strings.TrimRight(strings.TrimSpace(os.Getenv("WEB_PUBLIC_BASE_URL")), "/"),
		subBase:      strings.TrimRight(strings.TrimSpace(os.Getenv("SUB_BASE_URL")), "/"),
		mergedBase:   strings.TrimRight(strings.TrimSpace(os.Getenv("MERGED_SUB_PUBLIC_BASE_URL")), "/"),
		mergedSecret: strings.TrimSpace(os.Getenv("MERGED_SUB_SECRET")),
		yooShopID:    strings.TrimSpace(os.Getenv("YOOKASSA_STORE_ID")),
		yooSecret:    strings.TrimSpace(os.Getenv("YOOKASSA_API_KEY")),
	}
	if err := a.initSchema(context.Background()); err != nil {
		log.Fatalf("schema init failed: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/request-code", a.handleRequestCode)
	mux.HandleFunc("/api/auth/verify-code", a.handleVerifyCode)
	mux.HandleFunc("/api/auth/logout", a.handleLogout)
	mux.HandleFunc("/api/me", a.requireAuth(a.handleMe))
	mux.HandleFunc("/api/plans", a.handlePlans)
	mux.HandleFunc("/api/payments/create", a.requireAuth(a.handleCreatePayment))
	mux.HandleFunc("/api/autopay/disable", a.requireAuth(a.handleDisableAutopay))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, map[string]any{"ok": true}) })

	port := strings.TrimSpace(os.Getenv("WEB_PORT"))
	if port == "" {
		port = "8090"
	}
	log.Printf("neuravpn web API listening on :%s", port)
	if err := http.ListenAndServe(":"+port, a.withCORS(mux)); err != nil {
		log.Fatal(err)
	}
}

func (a *app) initSchema(ctx context.Context) error {
	_, err := a.db.Exec(ctx, `
CREATE TABLE IF NOT EXISTS email_login_codes (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_email_login_codes_email_created_at ON email_login_codes (lower(email), created_at DESC);
CREATE TABLE IF NOT EXISTS web_sessions (
    token_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_web_sessions_user_id ON web_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_web_sessions_expires_at ON web_sessions(expires_at);
DELETE FROM email_login_codes WHERE expires_at < NOW() - INTERVAL '1 day';
DELETE FROM web_sessions WHERE expires_at < NOW();
`)
	return err
}

func (a *app) handleRequestCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errResp("method not allowed"))
		return
	}
	var req struct {
		Email string `json:"email"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("bad json"))
		return
	}
	email, err := normalizeEmail(req.Email)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("некорректный email"))
		return
	}

	code := randomDigits(6)
	hash := a.codeHash(email, code)
	_, err = a.db.Exec(r.Context(), `INSERT INTO email_login_codes (email, code_hash, expires_at) VALUES ($1, $2, NOW() + INTERVAL '10 minutes')`, email, hash)
	if err != nil {
		log.Printf("request code insert failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, errResp("не удалось создать код"))
		return
	}

	if err := sendLoginCode(email, code); err != nil {
		log.Printf("email send failed email=%s code=%s err=%v", email, code, err)
	} else {
		log.Printf("email login code sent email=%s", email)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleVerifyCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errResp("method not allowed"))
		return
	}
	var req struct {
		Email  string `json:"email"`
		Code   string `json:"code"`
		UserID string `json:"user_id"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("bad json"))
		return
	}
	email, err := normalizeEmail(req.Email)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("некорректный email"))
		return
	}
	code := strings.TrimSpace(req.Code)
	if len(code) < 4 || len(code) > 8 {
		writeJSON(w, http.StatusBadRequest, errResp("некорректный код"))
		return
	}

	var id int64
	var codeHash string
	var attempts int
	err = a.db.QueryRow(r.Context(), `
SELECT id, code_hash, attempts FROM email_login_codes
WHERE lower(email)=lower($1) AND used_at IS NULL AND expires_at > NOW()
ORDER BY created_at DESC LIMIT 1`, email).Scan(&id, &codeHash, &attempts)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errResp("код истёк или не найден"))
		return
	}
	if attempts >= 5 {
		writeJSON(w, http.StatusTooManyRequests, errResp("слишком много попыток"))
		return
	}
	if subtle.ConstantTimeCompare([]byte(codeHash), []byte(a.codeHash(email, code))) != 1 {
		_, _ = a.db.Exec(r.Context(), `UPDATE email_login_codes SET attempts = attempts + 1 WHERE id=$1`, id)
		writeJSON(w, http.StatusUnauthorized, errResp("неверный код"))
		return
	}

	accounts, err := a.usersByEmail(r.Context(), email)
	if err != nil {
		log.Printf("users by email failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, errResp("ошибка поиска аккаунта"))
		return
	}
	if len(accounts) == 0 {
		writeJSON(w, http.StatusNotFound, errResp("аккаунт с этим email не найден"))
		return
	}
	userID := strings.TrimSpace(req.UserID)
	if len(accounts) > 1 && userID == "" {
		writeJSON(w, http.StatusOK, map[string]any{"multiple": true, "accounts": publicAccounts(accounts)})
		return
	}
	if userID == "" {
		userID = accounts[0].ID
	}
	if !accountContains(accounts, userID) {
		writeJSON(w, http.StatusForbidden, errResp("аккаунт не относится к этому email"))
		return
	}

	_, _ = a.db.Exec(r.Context(), `UPDATE email_login_codes SET used_at=NOW() WHERE id=$1`, id)
	token := randomToken(32)
	expires := time.Now().Add(30 * 24 * time.Hour)
	_, err = a.db.Exec(r.Context(), `INSERT INTO web_sessions (token_hash, user_id, expires_at) VALUES ($1,$2,$3)`, sessionHash(token), userID, expires)
	if err != nil {
		log.Printf("session insert failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, errResp("не удалось создать сессию"))
		return
	}
	setSessionCookie(w, token, expires)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("nvpn_session"); err == nil {
		_, _ = a.db.Exec(r.Context(), `DELETE FROM web_sessions WHERE token_hash=$1`, sessionHash(cookie.Value))
	}
	clearSessionCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleMe(w http.ResponseWriter, r *http.Request, userID string) {
	var email, subID, autopayPlan string
	var days int64
	var autopay bool
	err := a.db.QueryRow(r.Context(), `
SELECT COALESCE(email,''), days, COALESCE(subscription_id,''), autopay_enabled, COALESCE(autopay_plan_id,'')
FROM users WHERE id=$1`, userID).Scan(&email, &days, &subID, &autopay, &autopayPlan)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errResp("пользователь не найден"))
		return
	}
	var expiresAt any
	if days > 0 {
		expiresAt = time.Now().Add(time.Duration(days) * 24 * time.Hour).Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":          userID,
		"masked_id":        maskID(userID),
		"email":            email,
		"days":             days,
		"expires_at":       expiresAt,
		"subscription_id":  subID,
		"subscription_url": a.subscriptionURL(userID, subID),
		"autopay_enabled":  autopay,
		"autopay_plan_id":  autopayPlan,
	})
}

func (a *app) handlePlans(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"plans": plans})
}

func (a *app) handleCreatePayment(w http.ResponseWriter, r *http.Request, userID string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errResp("method not allowed"))
		return
	}
	var req struct {
		PlanID   string `json:"plan_id"`
		SaveCard bool   `json:"save_card"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("bad json"))
		return
	}
	p, ok := findPlan(req.PlanID)
	if !ok {
		writeJSON(w, http.StatusBadRequest, errResp("тариф не найден"))
		return
	}
	if a.yooShopID == "" || a.yooSecret == "" {
		writeJSON(w, http.StatusServiceUnavailable, errResp("YooKassa не настроена для web API"))
		return
	}
	var email string
	_ = a.db.QueryRow(r.Context(), `SELECT COALESCE(email,'') FROM users WHERE id=$1`, userID).Scan(&email)
	paymentURL, paymentID, err := a.createYooPayment(r.Context(), userID, email, p, req.SaveCard)
	if err != nil {
		log.Printf("web payment create failed user=%s plan=%s: %v", userID, p.ID, err)
		writeJSON(w, http.StatusBadGateway, errResp("не удалось создать платёж"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"payment_id": paymentID, "confirmation_url": paymentURL})
}

func (a *app) handleDisableAutopay(w http.ResponseWriter, r *http.Request, userID string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errResp("method not allowed"))
		return
	}
	_, err := a.db.Exec(r.Context(), `UPDATE users SET autopay_enabled=FALSE, updated_at=NOW() WHERE id=$1`, userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errResp("не удалось отключить автопродление"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) requireAuth(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("nvpn_session")
		if err != nil || strings.TrimSpace(cookie.Value) == "" {
			writeJSON(w, http.StatusUnauthorized, errResp("нужен вход"))
			return
		}
		var userID string
		err = a.db.QueryRow(r.Context(), `SELECT user_id FROM web_sessions WHERE token_hash=$1 AND expires_at > NOW()`, sessionHash(cookie.Value)).Scan(&userID)
		if err != nil {
			clearSessionCookie(w)
			writeJSON(w, http.StatusUnauthorized, errResp("сессия истекла"))
			return
		}
		next(w, r, userID)
	}
}

func (a *app) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.corsOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", a.corsOrigin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type account struct {
	ID   string
	Days int64
}

func (a *app) usersByEmail(ctx context.Context, email string) ([]account, error) {
	rows, err := a.db.Query(ctx, `SELECT id, days FROM users WHERE lower(email)=lower($1) ORDER BY created_at DESC`, email)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []account
	for rows.Next() {
		var ac account
		if err := rows.Scan(&ac.ID, &ac.Days); err != nil {
			return nil, err
		}
		out = append(out, ac)
	}
	return out, rows.Err()
}

func publicAccounts(accounts []account) []map[string]any {
	out := make([]map[string]any, 0, len(accounts))
	for _, ac := range accounts {
		out = append(out, map[string]any{"id": ac.ID, "masked_id": maskID(ac.ID), "label": fmt.Sprintf("%s · %d дней", maskID(ac.ID), ac.Days)})
	}
	return out
}

func accountContains(accounts []account, userID string) bool {
	for _, ac := range accounts {
		if ac.ID == userID {
			return true
		}
	}
	return false
}

func (a *app) subscriptionURL(userID, subID string) string {
	if a.mergedBase != "" && a.mergedSecret != "" {
		h := hmac.New(sha256.New, []byte(a.mergedSecret))
		_, _ = h.Write([]byte(userID))
		return fmt.Sprintf("%s/merged-sub/%s/%s", a.mergedBase, url.PathEscape(userID), hex.EncodeToString(h.Sum(nil)))
	}
	if a.subBase != "" && strings.TrimSpace(subID) != "" {
		return fmt.Sprintf("%s/s-39fj3r9f3j/%s", a.subBase, url.PathEscape(subID))
	}
	return ""
}

func (a *app) createYooPayment(ctx context.Context, userID, email string, p plan, saveCard bool) (string, string, error) {
	chatID, _ := strconv.ParseInt(userID, 10, 64)
	returnURL := a.publicBase + "/cabinet/?payment=return"
	if a.publicBase == "" {
		returnURL = "https://t.me/neuravpn_bot"
	}
	reqBody := map[string]any{
		"amount":              map[string]string{"value": fmt.Sprintf("%.2f", p.Amount), "currency": "RUB"},
		"capture":             true,
		"confirmation":        map[string]any{"type": "redirect", "return_url": returnURL},
		"description":         "NeuraVPN " + p.Title,
		"save_payment_method": saveCard,
		"expires_at":          time.Now().UTC().Add(20 * time.Minute).Format(time.RFC3339),
		"metadata":            map[string]any{"chat_id": chatID, "user_id": userID, "plan_id": p.ID, "plan_days": p.Days, "source": "website"},
	}
	if email != "" {
		reqBody["receipt"] = receipt(email, p)
	}
	payload, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.yookassa.ru/v3/payments", bytes.NewReader(payload))
	if err != nil {
		return "", "", err
	}
	auth := base64.StdEncoding.EncodeToString([]byte(a.yooShopID + ":" + a.yooSecret))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotence-Key", "web-"+userID+"-"+p.ID+"-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	resp, err := (&http.Client{Timeout: 25 * time.Second}).Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var data struct {
		ID           string         `json:"id"`
		Confirmation map[string]any `json:"confirmation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("yookassa status %s", resp.Status)
	}
	confirmationURL, _ := data.Confirmation["confirmation_url"].(string)
	if confirmationURL == "" {
		return "", data.ID, errors.New("confirmation_url is empty")
	}
	return confirmationURL, data.ID, nil
}

func receipt(email string, p plan) map[string]any {
	return map[string]any{"customer": map[string]string{"email": email}, "items": []map[string]any{{"description": "NeuraVPN " + p.Title, "quantity": "1.00", "amount": map[string]string{"value": fmt.Sprintf("%.2f", p.Amount), "currency": "RUB"}, "vat_code": 1, "payment_mode": "full_payment", "payment_subject": "service"}}}
}

func findPlan(id string) (plan, bool) {
	for _, p := range plans {
		if p.ID == id {
			return p, true
		}
	}
	return plan{}, false
}
func (a *app) codeHash(email, code string) string {
	h := hmac.New(sha256.New, a.authSecret)
	_, _ = h.Write([]byte(strings.ToLower(email) + ":" + code))
	return hex.EncodeToString(h.Sum(nil))
}
func sessionHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func errResp(message string) map[string]any { return map[string]any{"error": message} }

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(dst)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func normalizeEmail(value string) (string, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	addr, err := mail.ParseAddress(value)
	if err != nil || addr.Address == "" {
		return "", errors.New("bad email")
	}
	return addr.Address, nil
}
func randomDigits(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	var sb strings.Builder
	for _, x := range b {
		sb.WriteByte(byte('0' + int(x)%10))
	}
	return sb.String()
}
func randomToken(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func setSessionCookie(w http.ResponseWriter, token string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{Name: "nvpn_session", Value: token, Path: "/", Expires: expires, HttpOnly: true, Secure: webCookieSecure(), SameSite: webCookieSameSite()})
}
func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: "nvpn_session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: webCookieSecure(), SameSite: webCookieSameSite()})
}

func webCookieSecure() bool {
	return strings.ToLower(strings.TrimSpace(os.Getenv("WEB_COOKIE_SECURE"))) != "false"
}

func webCookieSameSite() http.SameSite {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("WEB_COOKIE_SAMESITE"))) {
	case "none":
		return http.SameSiteNoneMode
	case "strict":
		return http.SameSiteStrictMode
	default:
		return http.SameSiteLaxMode
	}
}

func maskID(id string) string {
	if len(id) <= 6 {
		return id
	}
	return id[:4] + strings.Repeat("*", int(math.Min(4, float64(len(id)-6)))) + id[len(id)-3:]
}

func sendLoginCode(email, code string) error {
	host := strings.TrimSpace(os.Getenv("SMTP_HOST"))
	user := strings.TrimSpace(os.Getenv("SMTP_USER"))
	pass := strings.TrimSpace(os.Getenv("SMTP_PASS"))
	from := strings.TrimSpace(os.Getenv("SMTP_FROM"))
	port := strings.TrimSpace(os.Getenv("SMTP_PORT"))
	if host == "" || user == "" || pass == "" {
		log.Printf("WEB LOGIN CODE email=%s code=%s", email, code)
		return nil
	}
	if from == "" {
		from = user
	}
	if port == "" {
		port = "587"
	}
	addr := net.JoinHostPort(host, port)
	msg := []byte("From: " + from + "\r\nTo: " + email + "\r\nSubject: NeuraVPN login code\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\nКод входа в NeuraVPN: " + code + "\r\nОн действует 10 минут.\r\n")
	auth := smtp.PlainAuth("", user, pass, host)
	if port == "465" {
		conn, err := tls.Dial("tcp", addr, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err != nil {
			return err
		}
		defer conn.Close()
		client, err := smtp.NewClient(conn, host)
		if err != nil {
			return err
		}
		defer client.Quit()
		if err := client.Auth(auth); err != nil {
			return err
		}
		if err := client.Mail(from); err != nil {
			return err
		}
		if err := client.Rcpt(email); err != nil {
			return err
		}
		wc, err := client.Data()
		if err != nil {
			return err
		}
		_, err = wc.Write(msg)
		if closeErr := wc.Close(); err == nil {
			err = closeErr
		}
		return err
	}
	return smtp.SendMail(addr, auth, from, []string{email}, msg)
}
