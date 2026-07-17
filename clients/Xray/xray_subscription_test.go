package xray

import (
	"net/url"
	"strings"
	"testing"
)

func TestGenerateVLESSLinkForInboundDataUsesInboundRealityAndTransport(t *testing.T) {
	client := &XRayClient{}
	inbound := InboundData{
		ID:     2,
		Port:   8443,
		Remark: "second",
		StreamSettings: `{
			"network":"tcp",
			"security":"reality",
			"realitySettings":{
				"target":"web.max.ru:443",
				"serverNames":["web.max.ru"],
				"shortIds":["second-id"],
				"settings":{
					"publicKey":"second-public-key",
					"fingerprint":"firefox",
					"spiderX":"/second"
				}
			},
			"tcpSettings":{"header":{"type":"none"}}
		}`,
	}
	link := client.GenerateVLESSLinkForInboundData(
		&Client{ID: "client-uuid", Email: "user@example.com"},
		inbound,
		"ru.example.com",
		443,
		"wrong.example.com",
		"wrong-public-key",
		"wrong-id",
		"/wrong",
		"chrome",
	)
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatalf("parse generated link: %v", err)
	}
	if parsed.Host != "ru.example.com:8443" {
		t.Fatalf("unexpected host: %s", parsed.Host)
	}
	query := parsed.Query()
	checks := map[string]string{
		"sni":  "web.max.ru",
		"pbk":  "second-public-key",
		"sid":  "second-id",
		"fp":   "firefox",
		"spx":  "/second",
		"type": "tcp",
	}
	for key, want := range checks {
		if got := query.Get(key); got != want {
			t.Errorf("%s: got %q, want %q", key, got, want)
		}
	}
	if !strings.HasSuffix(link, "#user@example.com") {
		t.Fatalf("email display name missing: %s", link)
	}
}
