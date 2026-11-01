package xray

import (
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func reissueFixture() ReissueRecord {
	var c map[string]json.RawMessage
	_ = json.Unmarshal([]byte(`{"id":17,"uuid":"old","password":"old-pass","auth":"old-auth","email":"owner","tgId":42,"subId":"old-sub","expiryTime":2000000000000,"enable":true,"totalGB":1234,"limitHwid":5,"resetDay":3,"unknown":{"preserve":true},"reverse":"","allowedIPs":"","createdAt":123}`), &c)
	return ReissueRecord{Client: c, InboundIDs: []int{1}}
}

func TestReissueTunnelKeyAndAddressPreservation(t *testing.T) {
	r := reissueFixture()
	r.Client["privateKey"] = json.RawMessage(`"old-private"`)
	r.Client["publicKey"] = json.RawMessage(`"old-public"`)
	r.Client["allowedIPs"] = json.RawMessage(`"10.0.0.2/32, fd00::2/128"`)
	p, err := reissuePayload(r, "new-key", "new-sub")
	if err != nil {
		t.Fatal(err)
	}
	private, public := reissueTunnelKeys("new-key")
	raw, _ := base64.StdEncoding.DecodeString(private)
	pk, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil || base64.StdEncoding.EncodeToString(pk.PublicKey().Bytes()) != public {
		t.Fatal("invalid tunnel keypair")
	}
	if rawString(p["privateKey"]) != private || rawString(p["publicKey"]) != public || string(p["allowedIPs"]) != `["10.0.0.2/32","fd00::2/128"]` {
		t.Fatal("tunnel fields lost")
	}
	other, _ := reissueTunnelKeys("different-key")
	if private == other {
		t.Fatal("tunnel key reused")
	}
}
func TestReissuePayloadPreservesEntitlements(t *testing.T) {
	before := reissueFixture()
	p, err := reissuePayload(before, "new-key", "new-sub")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"expiryTime", "enable", "totalGB", "limitHwid", "resetDay", "unknown", "email", "tgId"} {
		if !reflect.DeepEqual(p[k], before.Client[k]) {
			t.Fatal("lost " + k)
		}
	}
	if rawString(p["id"]) != "new-key" || rawString(p["auth"]) != "new-key" || rawString(p["subId"]) != "new-sub" || rawString(before.Client["uuid"]) != "old" {
		t.Fatal("credential replacement failed")
	}
	after := reissueFixture()
	for _, k := range []string{"uuid", "auth", "password"} {
		after.Client[k] = json.RawMessage(`"new-key"`)
	}
	after.Client["subId"] = json.RawMessage(`"new-sub"`)
	if err = VerifyReissue(before, after, "new-key", "new-sub"); err != nil {
		t.Fatal(err)
	}
	after.Client["expiryTime"] = json.RawMessage(`0`)
	if VerifyReissue(before, after, "new-key", "new-sub") == nil {
		t.Fatal("expiry loss accepted")
	}
}
func TestReissueRejectsForeignAndUnsupportedRecords(t *testing.T) {
	for _, protocol := range []string{"vless", "wireguard"} {
		client := hwidTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/panel/api/inbounds/list" {
				json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": []any{map[string]any{"id": 1, "protocol": protocol}}})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"success": true, "obj": []ReissueRecord{reissueFixture()}})
		})
		if _, err := client.ReissueRecords(context.Background(), "99"); err == nil {
			t.Fatal("foreign owner accepted")
		}
		_, err := client.ReissueRecords(context.Background(), "42")
		if (err == nil) != (protocol == "vless") {
			t.Fatal("protocol preflight", err)
		}
	}
}
