package main

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Shared test endpoints appended to every authorized subscription.
// SUBSCRIPTION_STATIC_LINKS holds whitespace-separated vless:// or hysteria2:// URIs.
func staticSubscriptionLinks() []string {
	var links []string
	for _, raw := range strings.Fields(os.Getenv("SUBSCRIPTION_STATIC_LINKS")) {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" || u.User == nil {
			continue
		}
		switch u.Scheme {
		case "vless", "hysteria2", "hy2":
			links = append(links, raw)
		}
	}
	return links
}

// Reports whether host:port belongs to one of the shared test endpoints.
func isStaticLinkEndpoint(host string, port float64) bool {
	for _, raw := range staticSubscriptionLinks() {
		u, err := url.Parse(raw)
		if err == nil && u.Hostname() == host && u.Port() == strconv.Itoa(int(port)) {
			return true
		}
	}
	return false
}

func appendStaticSubscriptionLinks(body []byte) ([]byte, error) {
	links := staticSubscriptionLinks()
	if len(links) == 0 {
		return body, nil
	}
	return mergeSubscriptionBody(body, links)
}
