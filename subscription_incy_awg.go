package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
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
	servers := []map[string]string{}
	for _, trial := range []struct{ name, path string }{
		{"Германия · AmneziaWG (тест)", path},
		{"Латвия · AmneziaWG (тест)", filepath.Join(filepath.Dir(path), "incy-awg-lv.json")},
	} {
		if cfg := readIncyTrial(trial.path, now); cfg != "" {
			servers = append(servers, map[string]string{"name": trial.name, "config": cfg})
		}
	}
	if len(servers) == 0 {
		return body
	}
	var profiles []json.RawMessage
	if json.Unmarshal(body, &profiles) != nil || len(profiles) == 0 {
		return body
	}
	item, _ := json.Marshal(map[string]any{"type": "amneziawg", "version": 1, "servers": servers})
	result, err := json.Marshal(append(profiles, item))
	if err != nil {
		return body
	}
	return result
}

func readIncyTrial(path string, now time.Time) string {
	var pilot struct {
		Expires int64
		Config  string
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &pilot) != nil || now.Unix() >= pilot.Expires || pilot.Expires > now.Add(24*time.Hour).Unix() {
		return ""
	}
	ini, err := base64.RawURLEncoding.DecodeString(pilot.Config)
	if err != nil || !strings.HasPrefix(string(ini), "[Interface]\n") || !strings.Contains(string(ini), "\n[Peer]\n") {
		return ""
	}
	return pilot.Config
}
