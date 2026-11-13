package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const autoSelectPilotUser = "623290294"
const autoSelectMediaType = "application/vnd.neuravpn.xray+json"

// Explicitly approved, assigned LV addresses. Do not derive endpoints from
// subscriber input or add unverified addresses when the DNS pool changes.
var autoSelectPilotIPs = [...]string{
	"213.21.208.254", "213.21.200.223", "213.21.200.218",
	"213.21.201.167", "213.21.215.131", "213.21.217.252", "193.68.78.66",
}

func autoSelectEnabled(userID string) bool {
	if strings.TrimSpace(userID) == "" {
		return false
	}
	if value, set := os.LookupEnv("SUBSCRIPTION_AUTOSELECT_ENABLED"); set {
		enabled, err := strconv.ParseBool(strings.TrimSpace(value))
		return err == nil && enabled
	}
	// Retain the previous owner-only switch for a reversible deployment.
	return os.Getenv("SUBSCRIPTION_AUTOSELECT_PILOT_USER_ID") == autoSelectPilotUser && userID == autoSelectPilotUser
}

func supportsAutoSelect(userAgent, accept string) bool {
	if strings.Contains(strings.ToLower(userAgent), "happ") {
		return true
	}
	for _, part := range strings.Split(accept, ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil || mediaType != autoSelectMediaType {
			continue
		}
		if q, present := params["q"]; present {
			weight, err := strconv.ParseFloat(q, 64)
			if err != nil || !(weight > 0 && weight <= 1) {
				continue
			}
		}
		return true
	}
	return false
}

// Convert only an already-authorized response. Unsupported formats retain the
// original subscription instead of dropping profiles or bypassing HWID checks.
func autoSelectSubscription(userID, userAgent, accept string, body []byte, headers http.Header) ([]byte, http.Header, error) {
	if !autoSelectEnabled(userID) || !supportsAutoSelect(userAgent, accept) || hwidDenial(headers) != "" {
		return body, headers, nil
	}
	converted, err := buildAutoSelectSubscription(body)
	if err == nil && userID == autoSelectPilotUser {
		converted, err = addPerBypassIPSelection(converted)
	}
	if err != nil {
		return body, headers, err
	}
	h := headers.Clone()
	if h == nil {
		h = make(http.Header)
	}
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Del("Content-Length")
	h.Del("Content-Encoding")
	h.Del("Content-Disposition")
	h.Set("X-Neura-Autoselect", "enabled")
	h.Add("Vary", "Accept")
	h.Add("Vary", "User-Agent")
	return converted, h, nil
}

// Owner-only pilot: keep each ordinary LV profile's transport and credentials,
// but select its entrance IP using the same client-side health checks.
func addPerBypassIPSelection(body []byte) ([]byte, error) {
	var profiles []map[string]any
	if err := json.Unmarshal(body, &profiles); err != nil || len(profiles) == 0 {
		return nil, errors.New("invalid per-bypass subscription")
	}
	for _, profile := range profiles[1:] {
		outs, ok := profile["outbounds"].([]any)
		if !ok || len(outs) != 1 {
			continue
		}
		out, ok := outs[0].(map[string]any)
		if !ok || out["protocol"] != "vless" {
			continue
		}
		settings, _ := out["settings"].(map[string]any)
		vnext, _ := settings["vnext"].([]any)
		if len(vnext) != 1 {
			continue
		}
		endpoint, _ := vnext[0].(map[string]any)
		if endpoint["address"] != "lv.staticdeliverycdn.com" {
			continue
		}
		port, _ := endpoint["port"].(float64)
		tag := map[float64]string{10001: "auto-bypass-1", 10002: "auto-bypass-2", 443: "auto-bypass-3"}[port]
		if tag == "" {
			continue
		}
		out["tag"] = tag
		candidates := make([]map[string]any, 0, len(autoSelectPilotIPs)+1)
		for _, ip := range autoSelectPilotIPs {
			candidate, err := autoSelectIPMember(out, ip)
			if err != nil {
				return nil, err
			}
			candidates = append(candidates, candidate)
		}
		candidates = append(candidates, map[string]any{"tag": "unavailable", "protocol": "blackhole", "settings": map[string]any{}})
		profile["outbounds"] = candidates
		observer := make(map[string]any)
		for key, value := range profiles[0]["observatory"].(map[string]any) {
			observer[key] = value
		}
		observer["probeInterval"] = "45s"
		profile["observatory"] = observer
		profile["routing"] = profiles[0]["routing"]
	}
	return json.Marshal(profiles)
}

func autoSelectClient(remarks string, outbounds []map[string]any) map[string]any {
	return map[string]any{
		"remarks": remarks,
		"log":     map[string]any{"loglevel": "warning"},
		"dns":     map[string]any{"tag": "dns_out", "queryStrategy": "UseIPv4", "servers": []string{"https://1.1.1.1/dns-query"}},
		"inbounds": []any{
			map[string]any{"tag": "socks", "listen": "127.0.0.1", "port": 10808, "protocol": "socks",
				"settings": map[string]any{"auth": "noauth", "udp": true},
				"sniffing": map[string]any{"enabled": true, "routeOnly": true, "destOverride": []string{"http", "tls", "quic"}}},
			map[string]any{"tag": "http", "listen": "127.0.0.1", "port": 10809, "protocol": "http", "settings": map[string]any{}},
		},
		"outbounds": outbounds,
		"routing": map[string]any{"domainStrategy": "AsIs", "rules": []any{
			map[string]any{"type": "field", "network": "tcp,udp", "outboundTag": outbounds[0]["tag"]},
		}},
	}
}

func buildAutoSelectSubscription(body []byte) ([]byte, error) {
	text := strings.TrimSpace(string(body))
	if !looksLikeSubscriptionText(text) {
		var err error
		text, _, err = decodeSubscriptionPayload(text)
		if err != nil {
			return nil, errors.New("pilot expects a URI subscription")
		}
	}
	var profiles []map[string]any
	members := make(map[string]map[string]any)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		u, err := url.Parse(line)
		if err != nil {
			return nil, errors.New("invalid pilot URI")
		}
		// AmneziaWG's vpn:// export is not supported by Happ, even in raw subscriptions.
		if u.Scheme == "vpn" {
			continue
		}
		out, err := autoSelectOutbound(u)
		if err != nil {
			return nil, err
		}
		tag := fmt.Sprintf("profile-%d", len(profiles)+1)
		out["tag"] = tag
		profiles = append(profiles, autoSelectClient(u.Fragment, []map[string]any{out}))
		member := ""
		if u.Scheme == "vless" && u.Hostname() == "lv.staticdeliverycdn.com" {
			member = map[string]string{"10001": "auto-bypass-1", "10002": "auto-bypass-2", "443": "auto-bypass-3"}[u.Port()]
		} else if (u.Scheme == "hysteria2" || u.Scheme == "hy2") && u.Hostname() == "213.21.200.218" && u.Port() == "10010" {
			member = "auto-hysteria2"
		}
		if member != "" {
			if members[member] != nil {
				return nil, errors.New("duplicate pilot member")
			}
			copyOut := make(map[string]any, len(out))
			for k, v := range out {
				copyOut[k] = v
			}
			copyOut["tag"] = member
			members[member] = copyOut
		}
	}
	var outbounds []map[string]any
	for _, tag := range []string{"auto-bypass-1", "auto-bypass-2", "auto-bypass-3"} {
		if members[tag] == nil {
			continue
		}
		for _, ip := range autoSelectPilotIPs {
			member, err := autoSelectIPMember(members[tag], ip)
			if err != nil {
				return nil, err
			}
			outbounds = append(outbounds, member)
		}
	}
	// Hysteria currently listens only on this IP; do not advertise fictitious UDP endpoints.
	if hy := members["auto-hysteria2"]; hy != nil {
		hy["tag"] = "auto-hysteria2-ip-213.21.200.218"
		outbounds = append(outbounds, hy)
	}
	if len(outbounds) < 2 {
		return nil, errors.New("not enough authorized auto-select candidates")
	}
	outbounds = append(outbounds, map[string]any{"tag": "unavailable", "protocol": "blackhole", "settings": map[string]any{}})
	auto := autoSelectClient("⚡ Автоматический выбор", outbounds)
	auto["observatory"] = map[string]any{"subjectSelector": []string{"auto-"}, "probeUrl": "https://www.google.com/generate_204", "probeInterval": "10s", "enableConcurrency": true}
	auto["routing"] = map[string]any{"domainStrategy": "AsIs",
		"rules":     []any{map[string]any{"type": "field", "network": "tcp,udp", "balancerTag": "auto"}},
		"balancers": []any{map[string]any{"tag": "auto", "selector": []string{"auto-"}, "strategy": map[string]any{"type": "leastPing"}, "fallbackTag": "unavailable"}},
	}
	return json.Marshal(append([]map[string]any{auto}, profiles...))
}

func autoSelectIPMember(template map[string]any, ip string) (map[string]any, error) {
	// Deep copy so neither ordinary profiles nor another IP variant changes.
	encoded, err := json.Marshal(template)
	if err != nil {
		return nil, errors.New("cannot copy pilot member")
	}
	var member map[string]any
	if json.Unmarshal(encoded, &member) != nil {
		return nil, errors.New("cannot decode pilot member")
	}
	settings, ok := member["settings"].(map[string]any)
	if !ok {
		return nil, errors.New("pilot member settings missing")
	}
	vnext, ok := settings["vnext"].([]any)
	if !ok || len(vnext) != 1 {
		return nil, errors.New("pilot member requires one endpoint")
	}
	endpoint, ok := vnext[0].(map[string]any)
	if !ok {
		return nil, errors.New("invalid pilot endpoint")
	}
	endpoint["address"] = ip
	member["tag"] = fmt.Sprintf("%s-ip-%s", template["tag"], ip)
	return member, nil
}

func autoSelectOutbound(u *url.URL) (map[string]any, error) {
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || u.Hostname() == "" || u.User == nil || u.User.Username() == "" {
		return nil, errors.New("pilot URI endpoint missing")
	}
	q := u.Query()
	stream := map[string]any{}
	out := map[string]any{"streamSettings": stream, "mux": map[string]any{"enabled": false}}
	switch u.Scheme {
	case "vless":
		if q.Get("security") != "reality" || q.Get("pbk") == "" || q.Get("sni") == "" || q.Get("encryption") != "none" {
			return nil, errors.New("pilot requires supported REALITY VLESS")
		}
		out["protocol"] = "vless"
		out["settings"] = map[string]any{"vnext": []any{map[string]any{"address": u.Hostname(), "port": port,
			"users": []any{map[string]any{"id": u.User.Username(), "encryption": "none", "flow": q.Get("flow")}}}}}
		fp := q.Get("fp")
		if isLVMainBypass(u) {
			fp = lvSubscriptionFingerprint
		} else if fp == "" {
			fp = "chrome"
		}
		stream["security"] = "reality"
		stream["realitySettings"] = map[string]any{"serverName": q.Get("sni"), "publicKey": q.Get("pbk"), "shortId": q.Get("sid"), "spiderX": q.Get("spx"), "fingerprint": fp}
		switch q.Get("type") {
		case "tcp", "raw", "":
			if q.Get("headerType") != "" && q.Get("headerType") != "none" {
				return nil, errors.New("unsupported TCP header")
			}
			stream["network"] = "tcp"
		case "xhttp":
			stream["network"] = "xhttp"
			x := map[string]any{"path": q.Get("path"), "host": q.Get("host"), "mode": q.Get("mode")}
			if extra := q.Get("extra"); extra != "" {
				var value map[string]any
				if json.Unmarshal([]byte(extra), &value) != nil || value == nil {
					return nil, errors.New("invalid XHTTP extra settings")
				}
				x["extra"] = value
			}
			if padding := q.Get("x_padding_bytes"); padding != "" {
				x["xPaddingBytes"] = padding
			}
			stream["xhttpSettings"] = x
		default:
			return nil, errors.New("unsupported pilot transport")
		}
	case "hysteria2", "hy2":
		if q.Get("sni") == "" || q.Get("insecure") == "1" || q.Get("insecure") == "true" || q.Get("allowInsecure") == "1" || q.Get("allowInsecure") == "true" {
			return nil, errors.New("authenticated Hysteria TLS required")
		}
		auth := u.User.Username()
		if password, ok := u.User.Password(); ok {
			auth += ":" + password
		}
		out["protocol"] = "hysteria"
		out["settings"] = map[string]any{"version": 2, "address": u.Hostname(), "port": port}
		stream["network"], stream["security"] = "hysteria", "tls"
		stream["hysteriaSettings"] = map[string]any{"version": 2, "auth": auth}
		alpn := q.Get("alpn")
		if alpn == "" {
			alpn = "h3"
		}
		stream["tlsSettings"] = map[string]any{"serverName": q.Get("sni"), "alpn": strings.Split(alpn, ",")}
		if obfs := q.Get("obfs"); obfs != "" {
			if obfs != "salamander" || q.Get("obfs-password") == "" {
				return nil, errors.New("unsupported Hysteria obfuscation")
			}
			stream["finalmask"] = map[string]any{"udp": []any{map[string]any{"type": "salamander", "settings": map[string]any{"password": q.Get("obfs-password")}}}}
		}
	default:
		return nil, errors.New("unsupported protocol in pilot subscription")
	}
	return out, nil
}
