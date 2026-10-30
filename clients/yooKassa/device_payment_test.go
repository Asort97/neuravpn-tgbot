package yookassa

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type deviceTransport func(*http.Request) (*http.Response, error)

func (f deviceTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestDevicePaymentUsesStableKeyAndExactPrice(t *testing.T) {
	old := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = old })
	var first string
	calls := 0
	http.DefaultTransport = deviceTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		b, _ := io.ReadAll(r.Body)
		if r.URL.String() != "https://api.yookassa.ru/v3/payments" || r.Header.Get("Idempotence-Key") != "device-slot-test" {
			t.Fatal("wrong endpoint or idempotency key")
		}
		if first != "" && first != string(b) {
			t.Fatal("retry changed payment payload")
		}
		first = string(b)
		var p map[string]any
		json.Unmarshal(b, &p)
		if p["amount"].(map[string]any)["value"] != "50.00" || p["expires_at"] != nil || p["save_payment_method"] == true {
			t.Fatal("incorrect slot payment")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"test-payment","confirmation":{"confirmation_url":"https://example.invalid"}}`))}, nil
	})
	c := New("test", "test")
	for i := 0; i < 2; i++ {
		if _, err := c.CreatePaymentWithKey(50, "slot", 42, "device_slot", map[string]any{"device_order_id": "order", "product_type": "device_slot"}, "test@example.invalid", false, "device-slot-test"); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatal("missing retry")
	}
}
