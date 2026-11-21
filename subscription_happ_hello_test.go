package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestHappHelloScope(t *testing.T) {
	for _, c := range []struct {
		host, user, ua, security string
		port                     int
		extra                    string
		want                     bool
	}{
		{"217.60.27.166", "other", "Happ/4.8", "reality", 444, "", true},
		{"217.60.27.166", "other", "Incy/4.8", "reality", 444, "", false},
		{"217.60.27.166", "other", "Happ", "reality", 443, "", true},
		{"213.21.208.254", autoSelectPilotUser, "Happ", "reality", 10001, "", true},
		{"213.21.208.254", "other", "Happ", "reality", 10001, "", true},
		{"213.21.208.254", "other", "Happ", "tls", 10001, "", true},
		{"213.21.208.254", "other", "Happ", "none", 10001, "", false},
		{"213.21.208.254", autoSelectPilotUser, "Happ", "reality", 10001, `,"finalmask":{"tcp":[]}`, false},
		{"95.181.226.64", "other", "Happ", "reality", 443, "", true},
		{"95.181.226.64", "other", "v2rayNG/1.9", "reality", 443, "", false},
	} {
		body := fmt.Sprintf(`[{"outbounds":[{"protocol":"vless","settings":{"vnext":[{"address":%q,"port":%d}]},"streamSettings":{"network":"xhttp","security":%q%s}}]}]`, c.host, c.port, c.security, c.extra)
		got := string(applyHappHelloProfile([]byte(body), c.user, c.ua))
		if strings.Contains(got, "tlshello") != c.want || !c.want && got != body {
			t.Fatal(c)
		}
	}
}
