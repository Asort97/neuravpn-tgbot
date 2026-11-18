package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"time"
)

// A short-lived, single-peer trial. The gateway independently expires the key.
func appendIncyAWGPilot(body []byte, user, agent, path string, now time.Time) []byte {
	ua := strings.ToLower(strings.TrimSpace(agent))
	if user != autoSelectPilotUser || !(ua == "incy" || strings.HasPrefix(ua, "incy/")) ||
		strings.Contains(ua, "windows") || strings.Contains(ua, "macos") || strings.Contains(ua, "linux") && !strings.Contains(ua, "android") {
		return body
	}
	var pilot struct {
		Expires int64
		Config  string
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &pilot) != nil || now.Unix() >= pilot.Expires || pilot.Expires > now.Add(24*time.Hour).Unix() {
		return body
	}
	ini, err := base64.RawURLEncoding.DecodeString(pilot.Config)
	if err != nil || !strings.HasPrefix(string(ini), "[Interface]\n") || !strings.Contains(string(ini), "\n[Peer]\n") {
		return body
	}
	var profiles []json.RawMessage
	if json.Unmarshal(body, &profiles) != nil || len(profiles) == 0 {
		return body
	}
	item, _ := json.Marshal(map[string]any{"type": "amneziawg", "version": 1, "servers": []map[string]string{
		{"name": "Германия · AmneziaWG (тест)", "config": pilot.Config},
	}})
	result, err := json.Marshal(append(profiles, item))
	if err != nil {
		return body
	}
	return result
}
