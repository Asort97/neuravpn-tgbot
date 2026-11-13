package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

// The collector can include pilot listeners by owner identity, not just subId.
// Keep them inside the primary profile instead of importing Gecko as a URI.
func withoutHysteriaPilotAuxiliaryLinks(body []byte) []byte {
	text := strings.TrimSpace(string(body))
	var encode func([]byte) string
	if !looksLikeSubscriptionText(text) {
		var err error
		text, encode, err = decodeSubscriptionPayload(text)
		if err != nil {
			return body
		}
	}
	primary := false
	removed := false
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		u, err := url.Parse(strings.TrimSpace(line))
		if err == nil && (u.Scheme == "hysteria2" || u.Scheme == "hy2") && u.Hostname() == "213.21.200.218" {
			if u.Port() == "10010" {
				primary = true
			}
			if u.Port() == "10011" || u.Port() == "10012" {
				removed = true
				continue
			}
		}
		kept = append(kept, line)
	}
	if !primary || !removed {
		return body
	}
	result := []byte(strings.Join(kept, "\n"))
	if encode != nil {
		return []byte(encode(result))
	}
	return result
}

// Pilot listeners share the owner's existing auth, TLS and Salamander password.
// Never synthesize credentials or include these endpoints for another account.
func addHysteriaVariantSelection(body []byte) ([]byte, error) {
	var profiles []map[string]any
	if err := json.Unmarshal(body, &profiles); err != nil || len(profiles) == 0 {
		return nil, errors.New("invalid Hysteria pilot subscription")
	}
	for _, profile := range profiles[1:] {
		outs, _ := profile["outbounds"].([]any)
		if len(outs) != 1 {
			continue
		}
		out, _ := outs[0].(map[string]any)
		settings, _ := out["settings"].(map[string]any)
		if out["protocol"] != "hysteria" || settings["address"] != "213.21.200.218" || settings["port"] != float64(10010) {
			continue
		}
		stream, _ := out["streamSettings"].(map[string]any)
		mask, _ := stream["finalmask"].(map[string]any)
		udp, _ := mask["udp"].([]any)
		if len(udp) != 1 {
			continue
		}
		entry, _ := udp[0].(map[string]any)
		ms, _ := entry["settings"].(map[string]any)
		password, _ := ms["password"].(string)
		if entry["type"] != "salamander" || password == "" || ms["packetSize"] != nil {
			continue
		}
		candidates := make([]map[string]any, 0, 4)
		for i, kind := range []string{"salamander", "plain", "gecko"} {
			data, err := json.Marshal(out)
			if err != nil {
				return nil, err
			}
			var member map[string]any
			if err := json.Unmarshal(data, &member); err != nil {
				return nil, err
			}
			member["tag"] = "auto-hy-" + kind
			member["settings"].(map[string]any)["port"] = 10010 + i
			st := member["streamSettings"].(map[string]any)
			if kind == "plain" {
				delete(st, "finalmask")
			}
			if kind == "gecko" {
				st["finalmask"] = map[string]any{"udp": []any{map[string]any{"type": "salamander", "settings": map[string]any{"password": password, "packetSize": "512-1200"}}}}
			}
			candidates = append(candidates, member)
		}
		// All three listeners bind the assigned pool and share canonical clients.
		var pool []map[string]any
		for _, variant := range candidates {
			for _, ip := range autoSelectPilotIPs {
				data, err := json.Marshal(variant)
				if err != nil {
					return nil, err
				}
				var member map[string]any
				if err := json.Unmarshal(data, &member); err != nil {
					return nil, err
				}
				member["tag"] = variant["tag"].(string) + "-ip-" + ip
				member["settings"].(map[string]any)["address"] = ip
				pool = append(pool, member)
			}
		}
		candidates = pool
		candidates = append(candidates, map[string]any{"tag": "unavailable", "protocol": "blackhole", "settings": map[string]any{}})
		profile["outbounds"] = candidates
		profile["observatory"] = map[string]any{"subjectSelector": []string{"auto-hy-"}, "probeUrl": "https://www.google.com/generate_204", "probeInterval": "45s", "enableConcurrency": true}
		profile["routing"] = profiles[0]["routing"]
	}
	return json.Marshal(profiles)
}
