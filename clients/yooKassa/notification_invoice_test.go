package yookassa

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestTrackedInvoiceKeepsCurrentID(t *testing.T) {
	old := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = old })
	c := New("test", "test")
	defer c.ClearPayments(42)
	count := 0
	http.DefaultTransport = deviceTransport(func(r *http.Request) (*http.Response, error) {
		count++
		if r.Method != "POST" || r.URL.Path != "/v3/payments" {
			t.Fatal("unexpected lookup")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"id":"invoice-%d","status":"pending","confirmation":{"confirmation_url":"https://example.invalid/pay"}}`, count)))}, nil
	})
	for i := 1; i <= 2; i++ {
		p, err := c.CreateTrackedPayment(149, "30 дней", 42, map[string]interface{}{"plan_id": "30d"}, "", false)
		if err != nil || p.ID != fmt.Sprintf("invoice-%d", i) || c.LatestPaymentID(42) != p.ID {
			t.Fatal(p, err)
		}
	}
}
