package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const webDevicesPath = "/internal/web/devices"

func webDevicesMAC(secret, message string) string {
	key := hmac.New(sha256.New, []byte(secret))
	key.Write([]byte("neuravpn-web-devices-v1"))
	mac := hmac.New(sha256.New, key.Sum(nil))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

// This endpoint is called only by the local, authenticated web API. Public
// subscription tokens must never authorize device deletion.
func newWebDevicesHandler(primary *xraySettings, secret string) http.HandlerFunc {
	var mu sync.Mutex
	nonces := make(map[string]int64)
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		fail := func(code int, message string) {
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
		}
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if r.Method != http.MethodPost || ip == nil || !ip.IsLoopback() || secret == "" {
			fail(http.StatusForbidden, "forbidden")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		stamp, nonce := r.Header.Get("X-Device-Time"), r.Header.Get("X-Device-Nonce")
		ts, tsErr := strconv.ParseInt(stamp, 10, 64)
		now := time.Now().Unix()
		nonceBytes, nonceErr := hex.DecodeString(nonce)
		proof := webDevicesMAC(secret, "POST\n"+webDevicesPath+"\n"+stamp+"\n"+nonce+"\n"+string(body))
		if err != nil || tsErr != nil || ts < now-60 || ts > now+60 || nonceErr != nil || len(nonceBytes) != 16 || !hmac.Equal([]byte(proof), []byte(r.Header.Get("X-Device-Signature"))) {
			fail(http.StatusForbidden, "forbidden")
			return
		}
		mu.Lock()
		for key, expiry := range nonces {
			if expiry < now {
				delete(nonces, key)
			}
		}
		_, replay := nonces[nonce]
		full := len(nonces) >= 10000
		if !replay && !full {
			nonces[nonce] = now + 121
		}
		mu.Unlock()
		if replay || full {
			fail(http.StatusConflict, "request already used; refresh devices")
			return
		}
		var req struct {
			UserID string `json:"user_id"`
			Action string `json:"action"`
			ID     string `json:"id"`
		}
		if json.Unmarshal(body, &req) != nil {
			fail(http.StatusBadRequest, "bad request")
			return
		}
		uid, err := strconv.ParseInt(req.UserID, 10, 64)
		if err != nil || uid <= 0 || strconv.FormatInt(uid, 10) != req.UserID || (req.Action != "list" && req.Action != "delete" && req.Action != "purchase" && req.Action != "check" && req.Action != "reduce") || (req.Action == "delete" && len(req.ID) != 64) || (req.Action == "check" && len(req.ID) != 36) {
			fail(http.StatusBadRequest, "bad request")
			return
		}
		session := getSession(uid)
		if !session.DevicesMu.TryLock() {
			fail(http.StatusConflict, "Предыдущий запрос ещё выполняется. Повторите чуть позже.")
			return
		}
		defer session.DevicesMu.Unlock()
		if req.Action == "reduce" {
			if err := reduceDeviceSlots(req.UserID); err != nil {
				fail(409, "Не удалось убрать место. Сначала удалите лишнее устройство и обновите список.")
				return
			}
		}
		if req.Action == "purchase" {
			order, err := createDeviceSlotPayment(req.UserID)
			if err != nil {
				fail(409, "Не удалось создать платёж. Проверьте срок подписки и доступность докупки мест.")
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"devices": []any{}, "order": order})
			return
		}
		if req.Action == "check" {
			s, ok := userStore.(devicePlanStore)
			if !ok {
				fail(503, "Оплата недоступна")
				return
			}
			o, err := s.GetDeviceSlotOrder(req.ID, req.UserID)
			if err != nil {
				fail(404, "Платёж не найден")
				return
			}
			if !o.Applied && o.PaymentID != "" {
				_, _ = applyDeviceSlotPayment(o.ID, req.UserID, o.PaymentID)
				o, _ = s.GetDeviceSlotOrder(req.ID, req.UserID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"devices": []any{}, "order": o})
			return
		}
		configs := deviceProviderConfigs(primary)
		read := func() subscriptionDevices {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			return readSubscriptionDevices(ctx, configs, req.UserID)
		}
		view := read()
		partial := view.Panels == 0 || view.Unavailable+view.Unsupported > 0
		idFor := func(key string) string { return webDevicesMAC(secret, "device\n"+req.UserID+"\n"+key) }
		result := ""
		if req.Action == "delete" {
			result = "absent"
			failed := partial
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			for _, device := range view.Devices {
				if !hmac.Equal([]byte(idFor(device.Key)), []byte(req.ID)) {
					continue
				}
				result = "deleted"
				for _, record := range device.Records {
					if record.Config.client.DeleteHWIDDevice(ctx, record.Email, record.Device.ID) != nil {
						failed = true
					}
				}
			}
			cancel()
			invalidateMergedSubscriptionCache(req.UserID)
			if failed {
				result = "partial"
			}
			view = read()
			partial = view.Panels == 0 || view.Unavailable+view.Unsupported > 0
		}
		type deviceJSON struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			LastSeen int64  `json:"last_seen"`
		}
		view = filterLimitedDeviceView(view, primary, req.UserID)
		devices := make([]deviceJSON, 0, len(view.Devices))
		for _, device := range view.Devices {
			devices = append(devices, deviceJSON{ID: idFor(device.Key), Name: deviceLabel(device.Info), LastSeen: device.Info.LastSeen})
		}
		plan, planErr := currentDevicePlan(req.UserID)
		if planErr != nil {
			fail(503, "Не удалось проверить лимит устройств")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"devices": devices, "partial": partial, "result": result, "device_plan": plan})
	}
}
