package xray

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrHWIDUnsupported = errors.New("panel does not support HWID API")

// Preserve the complete client record, including panel fields unknown to this
// bot. Changing a slot limit must not reset transport/authentication settings.
func (x *XRayClient) SetHWIDLimit(ctx context.Context, email, userID string, limit int) error {
	if email == "" || userID == "" || limit < 0 || limit > 100 {
		return errors.New("invalid HWID limit target")
	}
	endpoint := x.PanelIdentity() + "/panel/api/clients/"
	code, body, err := x.doAPIRequestContext(ctx, http.MethodGet, endpoint+"get/"+url.PathEscape(email), nil, map[string]string{"Accept": "application/json"}, true)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("client lookup HTTP %d", code)
	}
	var payload struct {
		Success bool `json:"success"`
		Obj     struct {
			Client map[string]any `json:"client"`
		} `json:"obj"`
	}
	if json.Unmarshal(body, &payload) != nil || !payload.Success || payload.Obj.Client == nil {
		return errors.New("invalid client record")
	}
	record := payload.Obj.Client
	if normalizeTgID(record["tgId"]) != userID {
		return errors.New("device limit ownership mismatch")
	}
	if v, ok := record["uuid"]; ok {
		record["id"] = v
	}
	// The lookup returns ClientRecord, while update accepts Client: WireGuard
	// addresses are stored as CSV and timestamps use different JSON names.
	if csv, ok := record["allowedIPs"].(string); ok {
		addresses := make([]string, 0)
		for _, part := range strings.Split(csv, ",") {
			if address := strings.TrimSpace(part); address != "" {
				addresses = append(addresses, address)
			}
		}
		record["allowedIPs"] = addresses
	}
	for _, field := range []struct{ from, to string }{{"createdAt", "created_at"}, {"updatedAt", "updated_at"}} {
		if value, ok := record[field.from]; ok {
			record[field.to] = value
		}
	}
	record["limitHwid"] = limit
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	code, body, err = x.doAPIRequestContext(ctx, http.MethodPost, endpoint+"update/"+url.PathEscape(email), data, map[string]string{"Accept": "application/json", "Content-Type": "application/json"}, false)
	if err != nil {
		return err
	}
	return checkAPISuccess("set HWID limit", code, body)
}

type HWIDDevice struct {
	ID          int    `json:"id"`
	FirstSeen   int64  `json:"firstSeen"`
	LastSeen    int64  `json:"lastSeen"`
	UserAgent   string `json:"userAgent"`
	DeviceOS    string `json:"deviceOs"`
	OSVersion   string `json:"osVersion"`
	DeviceModel string `json:"deviceModel"`
	Fingerprint string `json:"fingerprint"`
}

// PanelIdentity is used only for internal de-duplication, never for button data or logs.
func (x *XRayClient) PanelIdentity() string { return strings.TrimRight(x.serverURL, "/") }

func (x *XRayClient) ListHWIDDevices(ctx context.Context, email string) ([]HWIDDevice, error) {
	if strings.TrimSpace(email) == "" {
		return nil, errors.New("empty client email")
	}
	code, body, err := x.doAPIRequestContext(ctx, http.MethodPost, x.PanelIdentity()+"/panel/api/clients/hwids/"+url.PathEscape(email), nil, map[string]string{"Accept": "application/json"}, true)
	if err != nil {
		return nil, err
	}
	if isEndpointUnsupported(code) {
		return nil, ErrHWIDUnsupported
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("HWID list returned HTTP %d", code)
	}
	var response struct {
		Success bool         `json:"success"`
		Obj     []HWIDDevice `json:"obj"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, errors.New("invalid HWID list response")
	}
	if !response.Success {
		return nil, errors.New("panel rejected HWID list request")
	}
	return response.Obj, nil
}

func (x *XRayClient) DeleteHWIDDevice(ctx context.Context, email string, id int) error {
	if strings.TrimSpace(email) == "" || id <= 0 {
		return errors.New("invalid HWID deletion target")
	}
	code, body, err := x.doAPIRequestContext(ctx, http.MethodDelete, x.PanelIdentity()+"/panel/api/clients/hwids/"+url.PathEscape(email)+"/"+strconv.Itoa(id), nil, map[string]string{"Accept": "application/json"}, false)
	// Do not replay a destructive request after an ambiguous response.
	if err != nil {
		return err
	}
	if code == http.StatusUnauthorized || code == http.StatusForbidden {
		if err := x.loginWithContext(ctx); err != nil {
			return err
		}
		code, body, err = x.doAPIRequestContext(ctx, http.MethodDelete, x.PanelIdentity()+"/panel/api/clients/hwids/"+url.PathEscape(email)+"/"+strconv.Itoa(id), nil, map[string]string{"Accept": "application/json"}, false)
		if err != nil {
			return err
		}
	}
	if code != http.StatusOK {
		return fmt.Errorf("HWID delete returned HTTP %d", code)
	}
	var response struct {
		Success bool `json:"success"`
	}
	if json.Unmarshal(body, &response) != nil || !response.Success {
		return errors.New("panel did not confirm HWID deletion")
	}
	return nil
}
