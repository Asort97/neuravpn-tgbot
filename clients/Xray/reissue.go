package xray

import (
	"context"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

type ReissueRecord struct {
	Client     map[string]json.RawMessage `json:"client"`
	InboundIDs []int                      `json:"inboundIds"`
}

func rawString(v json.RawMessage) string { var s string; _ = json.Unmarshal(v, &s); return s }

// Read native records rather than the reduced subscription DTO: updating must
// preserve the panel's renewal, quota, host and protocol-specific fields.
func (x *XRayClient) ReissueRecords(ctx context.Context, userID string) ([]ReissueRecord, error) {
	code, body, err := x.doAPIRequestContext(ctx, http.MethodGet, x.serverURL+"/panel/api/clients/get/tgId/"+url.PathEscape(userID), nil, nil, true)
	if err != nil {
		return nil, err
	}
	if err = checkAPISuccess("reissue lookup", code, body); err != nil {
		return nil, err
	}
	var result struct {
		Obj []ReissueRecord `json:"obj"`
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	inbounds, err := x.getAllInboundsContext(ctx)
	if err != nil {
		return nil, err
	}
	protocols := map[int]string{}
	for _, in := range inbounds {
		protocols[in.ID] = in.Protocol
	}
	for _, r := range result.Obj {
		var tg any
		if json.Unmarshal(r.Client["tgId"], &tg) != nil || normalizeTgID(tg) != userID {
			return nil, errors.New("reissue ownership mismatch")
		}
		if rawString(r.Client["email"]) == "" || rawString(r.Client["uuid"]) == "" || len(r.InboundIDs) == 0 {
			return nil, errors.New("incomplete reissue record")
		}
		tunnels := 0
		for _, id := range r.InboundIDs {
			switch protocols[id] {
			case "vless", "hysteria":
			case "wireguard", "amneziawg":
				tunnels++
			default:
				return nil, errors.New("unsupported reissue protocol")
			}
		}
		// Multiple tunnel attachments can have different per-inbound peer keys.
		if tunnels > 1 || (tunnels == 1 && (rawString(r.Client["privateKey"]) == "" || rawString(r.Client["publicKey"]) == "")) {
			return nil, errors.New("unsupported or incomplete tunnel peer")
		}
	}
	return result.Obj, nil
}

func reissuePayload(record ReissueRecord, key, subID string) (map[string]json.RawMessage, error) {
	payload := make(map[string]json.RawMessage, len(record.Client))
	for k, v := range record.Client {
		payload[k] = v
	}
	for _, k := range []string{"id", "password", "auth"} {
		payload[k], _ = json.Marshal(key)
	}
	payload["subId"], _ = json.Marshal(subID)
	delete(payload, "uuid")
	for _, k := range []string{"reverse"} {
		if value := rawString(payload[k]); value != "" {
			if !json.Valid([]byte(value)) {
				return nil, errors.New("invalid structured client field")
			}
			payload[k] = json.RawMessage(value)
		} else if string(payload[k]) == `""` {
			delete(payload, k)
		}
	}
	var csv string
	if json.Unmarshal(payload["allowedIPs"], &csv) == nil {
		addresses := []string{}
		for _, part := range strings.Split(csv, ",") {
			if address := strings.TrimSpace(part); address != "" {
				addresses = append(addresses, address)
			}
		}
		payload["allowedIPs"], _ = json.Marshal(addresses)
	}
	if rawString(record.Client["privateKey"]) != "" || rawString(record.Client["publicKey"]) != "" {
		private, public := reissueTunnelKeys(key)
		payload["privateKey"], _ = json.Marshal(private)
		payload["publicKey"], _ = json.Marshal(public)
	}
	if v, ok := payload["createdAt"]; ok {
		payload["created_at"] = v
	}
	delete(payload, "createdAt")
	delete(payload, "updatedAt")
	return payload, nil
}

func reissueTunnelKeys(key string) (string, string) {
	seed := sha256.Sum256([]byte("reissue-tunnel-v1\n" + key))
	seed[0] &= 248
	seed[31] = (seed[31] & 127) | 64
	private, _ := ecdh.X25519().NewPrivateKey(seed[:])
	return base64.StdEncoding.EncodeToString(private.Bytes()), base64.StdEncoding.EncodeToString(private.PublicKey().Bytes())
}

func (x *XRayClient) ApplyReissue(ctx context.Context, record ReissueRecord, key, subID string) error {
	payload, err := reissuePayload(record, key, subID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := x.serverURL + "/panel/api/clients/update/" + url.PathEscape(rawString(record.Client["email"]))
	code, body, err := x.doAPIRequestContext(ctx, http.MethodPost, endpoint, data, map[string]string{"Content-Type": "application/json"}, true)
	if err != nil {
		return err
	}
	return checkAPISuccess("reissue update", code, body)
}

func VerifyReissue(before, after ReissueRecord, key, subID string) error {
	if rawString(before.Client["privateKey"]) != "" || rawString(before.Client["publicKey"]) != "" {
		private, public := reissueTunnelKeys(key)
		if rawString(after.Client["privateKey"]) != private || rawString(after.Client["publicKey"]) != public {
			return errors.New("tunnel peer key not rotated")
		}
	}
	for _, field := range []string{"uuid", "auth", "password"} {
		if rawString(after.Client[field]) != key {
			return fmt.Errorf("reissue credential not applied: %s", field)
		}
	}
	if rawString(after.Client["subId"]) != subID {
		return errors.New("reissue subID not applied")
	}
	for _, field := range []string{"id", "email", "tgId", "expiryTime", "totalGB", "limitHwid", "limitIp", "enable", "reset", "resetDay", "resetMax", "resetWeekday", "trafficReset", "trafficResetDay", "group", "clientHostRuleIds", "flow", "allowedIPs", "preSharedKey", "keepAlive", "forwardedPorts"} {
		var a, b any
		_ = json.Unmarshal(before.Client[field], &a)
		_ = json.Unmarshal(after.Client[field], &b)
		if !reflect.DeepEqual(a, b) {
			return fmt.Errorf("reissue changed preserved field: %s", field)
		}
	}
	if !reflect.DeepEqual(before.InboundIDs, after.InboundIDs) {
		return errors.New("reissue changed inbound attachments")
	}
	return nil
}
