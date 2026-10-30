package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/Asort97/vpnBot/clients/postgres"
	yookassa "github.com/Asort97/vpnBot/clients/yooKassa"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type deviceTestStore struct {
	DataStore
	devicePlanStore
	enabled bool
}

func (s *deviceTestStore) GetDevicePlan(id string) (postgres.DevicePlan, error) {
	return postgres.DevicePlan{Enabled: s.enabled && id == "42"}, nil
}
func (s *deviceTestStore) GetDays(string) (int64, error)            { return 10, nil }
func (s *deviceTestStore) GetSubscriptionID(string) (string, error) { return "test-sub", nil }

func TestDeviceGateBeforeCacheAndMergedLinks(t *testing.T) {
	oldStore, oldConfig, oldSecret := userStore, xrayCfg, mergedSubSecret
	t.Cleanup(func() {
		userStore, xrayCfg, mergedSubSecret = oldStore, oldConfig, oldSecret
		invalidateMergedSubscriptionCache("42")
		invalidateMergedSubscriptionCache("43")
	})
	userStore = &deviceTestStore{enabled: true}
	mergedSubSecret = "test-secret"
	calls := 0
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.RawQuery != "" || r.Header.Get("Accept") != "text/plain" {
			t.Error("browser bypass reached HWID gate")
		}
		switch r.Header.Get("X-Hwid") {
		case "known":
			w.Header().Set("X-Hwid-Active", "true")
			w.Header().Set("X-Hwid-Limit", "true")
		case "sixth":
			w.Header().Set("X-Hwid-Max-Devices-Reached", "true")
			w.WriteHeader(404)
		case "":
			w.Header().Set("X-Hwid-Not-Supported", "true")
			w.WriteHeader(404)
		default:
			w.WriteHeader(503)
		}
	}))
	defer panel.Close()
	xrayCfg = &xraySettings{subBaseURL: panel.URL}
	for _, tc := range []struct {
		user, hwid string
		code       int
		allow      bool
	}{{"42", "known", 200, true}, {"42", "sixth", 200, false}, {"42", "", 200, false}, {"42", "offline", 503, false}, {"43", "sixth", 200, true}} {
		r := httptest.NewRequest("GET", "/merged-sub/"+tc.user+"/"+mergedSubscriptionToken(tc.user)+"?format=html", nil)
		r.Header.Set("X-Hwid", tc.hwid)
		r.Header.Set("User-Agent", "Mozilla/5.0")
		r.Header.Set("Accept", "text/html")
		key := mergedSubscriptionCacheKey(tc.user, mergedSubscriptionVariantFromRequest(r))
		storeMergedSubscriptionCache(key, mergedSubscriptionCacheEntry{body: []byte("REAL_MAIN_AND_MERGED_CONFIGS"), statusCode: 200, headers: make(http.Header)})
		w := httptest.NewRecorder()
		handleMergedSubscription(w, r)
		if w.Code != tc.code || strings.Contains(w.Body.String(), "REAL_MAIN_AND_MERGED_CONFIGS") != tc.allow {
			t.Fatalf("case %+v: %d %s", tc, w.Code, w.Body.String())
		}
		if !tc.allow && tc.code == 200 {
			b, err := base64.StdEncoding.DecodeString(w.Body.String())
			if err != nil || !strings.Contains(string(b), "192.0.2.1:1") || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("invalid nonworking placeholder")
			}
		}
	}
	if calls != 4 {
		t.Fatalf("unlimited user reached panel gate: %d", calls)
	}
}

func TestHWIDDenialDuringCacheBuild(t *testing.T) {
	w := httptest.NewRecorder()
	h := make(http.Header)
	h.Set("X-Hwid-Max-Devices-Reached", "true")
	writeMergedSubscriptionEntry(w, mergedSubscriptionCacheEntry{statusCode: 404, headers: h, body: []byte("forbidden-real-config")}, "miss")
	if w.Code != 200 || w.Header().Get("X-Neura-Device-Limit") != "true" || strings.Contains(w.Body.String(), "forbidden-real-config") {
		t.Fatal("denial was not replaced")
	}
}

func TestDevicePaymentMustBeVerifiedForExactOwnerOrderAndPrice(t *testing.T) {
	raw := `{"id":"p1","paid":true,"status":"succeeded","amount":{"value":"50.00","currency":"RUB"},"metadata":{"product_type":"device_slot","device_order_id":"o1","chat_id":623290294}}`
	for _, field := range []string{"valid", "id", "paid", "status", "value", "currency", "order", "owner", "product"} {
		var p yookassa.YooKassaPaymentResponse
		json.Unmarshal([]byte(raw), &p)
		switch field {
		case "id":
			p.ID = "p2"
		case "paid":
			p.Paid = false
		case "status":
			p.Status = "pending"
		case "value":
			p.Amount["value"] = "1.00"
		case "currency":
			p.Amount["currency"] = "USD"
		case "order":
			p.Metadata["device_order_id"] = "o2"
		case "owner":
			p.Metadata["chat_id"] = "99"
		case "product":
			p.Metadata["product_type"] = "vpn"
		}
		if validDeviceSlotPayment(&p, "o1", "623290294", "p1") != (field == "valid") {
			t.Fatal(field)
		}
	}
}

func TestStarsExtraSlotsAndOldPayload(t *testing.T) {
	p := ratePlanByID["90d"]
	base := starsAmountForPlan(p)
	p.DeviceExtra = 2
	if starsAmountForPlan(p) != base+100 {
		t.Fatal("extra slots discounted")
	}
	for _, n := range []int{0, 2} {
		p.DeviceExtra = n
		got, ok := parseStarsPlan(strings.TrimPrefix(starsPlanPayload(p), starsPayloadPrefix))
		if !ok || got.DeviceExtra != n {
			t.Fatal("payload changed")
		}
	}
	for _, bad := range []string{"90d:d-1", "90d:d96", "90d:d2:d3"} {
		if _, ok := parseStarsPlan(bad); ok {
			t.Fatal("invalid extra slots accepted")
		}
	}
}
