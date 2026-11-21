package main

import (
	"encoding/json"
	"os"
	"strings"
)

// Russian DPI drops the tail of a TLS ClientHello that spans several TCP
// segments (post-quantum hellos are ~2 KB). Happ splits the hello via finalmask.
// Large pieces with short pauses are enough towards foreign servers; towards
// Russian-hosted entries only small pieces spread over ~1 s pass, so those hosts
// are listed in SUBSCRIPTION_FRAGMENT_SLOW_HOSTS. Cores read the pause as
// "interval" or "delay", so both keys are sent.
func happFragmentSettings(host string) map[string]any {
	length := envOr("SUBSCRIPTION_FRAGMENT_LENGTH", "100-200")
	pause := envOr("SUBSCRIPTION_FRAGMENT_INTERVAL", "1-3")
	if isSlowFragmentHost(host) {
		length = envOr("SUBSCRIPTION_FRAGMENT_SLOW_LENGTH", "10-20")
		pause = envOr("SUBSCRIPTION_FRAGMENT_SLOW_INTERVAL", "10-20")
	}
	return map[string]any{"packets": "tlshello", "length": length, "interval": pause, "delay": pause}
}

func isSlowFragmentHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	for _, h := range strings.FieldsFunc(os.Getenv("SUBSCRIPTION_FRAGMENT_SLOW_HOSTS"), func(r rune) bool { return r == ',' || r == ' ' }) {
		if strings.EqualFold(h, host) {
			return true
		}
	}
	return false
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// Happ and the NeuraVPN app (when it asks for JSON) run the same Xray core
// and receive the same transport tuning.
func isTunedJSONAgent(agent, user string) bool {
	ua := strings.ToLower(strings.TrimSpace(agent))
	if ua == "happ" || strings.HasPrefix(ua, "happ/") || strings.HasPrefix(ua, "neuravpn/") {
		return true
	}
	return (ua == "incy" || strings.HasPrefix(ua, "incy/")) && incyTuningEnabled(user)
}

// INCY runs Xray too, but its core version is not pinned by us: the tuning
// (finalmask fragment, fast-start balancers) starts with the owner and goes to
// everyone with SUBSCRIPTION_INCY_TUNING=all; "off" disables it entirely.
func incyTuningEnabled(user string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SUBSCRIPTION_INCY_TUNING"))) {
	case "all":
		return true
	case "off":
		return false
	}
	return user == autoSelectPilotUser
}

// Fragment the TLS ClientHello of every TCP-based TLS/REALITY profile for Happ;
// preserve existing transport masks.
func applyHappHelloProfile(body []byte, user string, agent string) []byte {
	if !isTunedJSONAgent(agent, user) {
		return body
	}
	var profiles []map[string]any
	if json.Unmarshal(body, &profiles) != nil {
		return body
	}
	changed := false
	for _, p := range profiles {
		outs, _ := p["outbounds"].([]any)
		for _, value := range outs {
			o := obj(value)
			s := obj(o["streamSettings"])
			if o["protocol"] != "vless" || s["security"] != "reality" && s["security"] != "tls" || s["finalmask"] != nil {
				continue
			}
			switch network, _ := s["network"].(string); network {
			case "", "tcp", "raw", "xhttp", "ws", "httpupgrade", "grpc":
			default:
				continue
			}
			host, _ := endpoint(obj(o["settings"]))["address"].(string)
			s["finalmask"] = map[string]any{"tcp": []any{map[string]any{"type": "fragment", "settings": happFragmentSettings(host)}}}
			changed = true
		}
	}
	if !changed {
		return body
	}
	out, err := json.Marshal(profiles)
	if err != nil {
		return body
	}
	return out
}
