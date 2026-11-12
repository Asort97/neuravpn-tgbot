package main

import (
	"net/url"
	"strings"
)

const lvSubscriptionFingerprint = "firefox"

func isLVMainBypass(u *url.URL) bool {
	if u.Scheme != "vless" || !strings.EqualFold(u.Hostname(), "lv.staticdeliverycdn.com") || u.Query().Get("security") != "reality" {
		return false
	}
	switch u.Port() {
	case "443", "10001", "10002":
	default:
		return false
	}
	switch u.Query().Get("type") {
	case "", "tcp", "raw", "xhttp":
		return true
	default:
		return false
	}
}

// Apply the mobile-tested client fingerprint without changing panel credentials,
// merged nodes, or the original URI/base64 subscription format.
func normalizeLVSubscriptionFingerprint(body []byte) ([]byte, error) {
	text := string(body)
	var encode func([]byte) string
	if !looksLikeSubscriptionText(text) {
		var err error
		text, encode, err = decodeSubscriptionPayload(text)
		if err != nil {
			return body, err
		}
	}
	lines := strings.Split(text, "\n")
	changed := false
	for i, line := range lines {
		u, err := url.Parse(strings.TrimSpace(line))
		if err != nil || !isLVMainBypass(u) {
			continue
		}
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			continue
		}
		if values := q["fp"]; len(values) == 1 && values[0] == lvSubscriptionFingerprint {
			continue
		}
		q.Set("fp", lvSubscriptionFingerprint)
		u.RawQuery = q.Encode()
		lines[i] = u.String()
		changed = true
	}
	if !changed {
		return body, nil
	}
	result := []byte(strings.Join(lines, "\n"))
	if encode != nil {
		return []byte(encode(result)), nil
	}
	return result, nil
}
