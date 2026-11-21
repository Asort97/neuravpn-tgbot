package main

import (
	"strings"
	"testing"
)

func TestAppendStaticSubscriptionLinks(t *testing.T) {
	t.Setenv("SUBSCRIPTION_STATIC_LINKS", "vless://id@1.2.3.4:443?security=reality#A  garbage  ftp://x@y/ hysteria2://pw@5.6.7.8:443#B")
	out, err := appendStaticSubscriptionLinks([]byte("vless://u@9.9.9.9:443#main\n"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "vless://id@1.2.3.4:443") || !strings.Contains(s, "hysteria2://pw@5.6.7.8:443") || strings.Contains(s, "garbage") || strings.Contains(s, "ftp://") {
		t.Fatalf("unexpected body: %q", s)
	}
	if !strings.HasPrefix(s, "vless://u@9.9.9.9:443#main\n") {
		t.Fatalf("original links must stay first: %q", s)
	}
}

func TestHappHelloFragmentForStaticEndpoint(t *testing.T) {
	t.Setenv("SUBSCRIPTION_STATIC_LINKS", "vless://id@1.2.3.4:443?security=reality#A")
	body := []byte(`[{"outbounds":[{"protocol":"vless","settings":{"vnext":[{"address":"1.2.3.4","port":443,"users":[{"id":"x"}]}]},"streamSettings":{"network":"tcp","security":"reality"}},{"protocol":"vless","settings":{"vnext":[{"address":"9.9.9.9","port":443,"users":[{"id":"y"}]}]},"streamSettings":{"network":"tcp","security":"reality"}}]}]`)
	out := string(applyHappHelloProfile(body, "1", "Happ/3.0"))
	if strings.Count(out, `"finalmask"`) != 2 || !strings.Contains(out, `"tlshello"`) {
		t.Fatalf("expected fragment on every REALITY endpoint: %s", out)
	}
	if !isStaticLinkEndpoint("1.2.3.4", 443) || isStaticLinkEndpoint("9.9.9.9", 443) {
		t.Fatal("static endpoint detection")
	}
	if string(applyHappHelloProfile(body, "1", "v2rayN/7")) != string(body) {
		t.Fatal("non-Happ clients must stay unchanged")
	}
}

func TestAppendStaticSubscriptionLinksUnset(t *testing.T) {
	t.Setenv("SUBSCRIPTION_STATIC_LINKS", "")
	in := []byte("vless://u@9.9.9.9:443#main\n")
	out, err := appendStaticSubscriptionLinks(in)
	if err != nil || string(out) != string(in) {
		t.Fatalf("body changed without static links: %q %v", out, err)
	}
}
