package main

import (
	"encoding/json"
	"strings"
)

// Keep the balancer and its measurements; only change its no-result path.
func applyLVStartupPilot(body []byte, user, agent string) []byte {
	ua := strings.ToLower(strings.TrimSpace(agent))
	if user != autoSelectPilotUser || !(ua == "happ" || strings.HasPrefix(ua, "happ/")) {
		return body
	}
	var docs []json.RawMessage
	if json.Unmarshal(body, &docs) != nil {
		return body
	}
	changed := false
	for i, raw := range docs {
		var p map[string]any
		if json.Unmarshal(raw, &p) != nil {
			continue
		}
		outs, _ := p["outbounds"].([]any)
		bal, _ := obj(p["routing"])["balancers"].([]any)
		if len(bal) != 1 || obj(bal[0])["fallbackTag"] != "unavailable" {
			continue
		}
		name, _ := p["remarks"].(string)
		tag := ""
		switch {
		case name == "⚡ Автоматический выбор", strings.Contains(name, "Обход 3") && strings.Contains(name, "Латвия"):
			tag = "auto-bypass-3-ip-193.68.78.66"
		case strings.Contains(name, "Обход 1") && strings.Contains(name, "Латвия"):
			tag = "auto-bypass-1-ip-193.68.78.66"
		case strings.Contains(name, "Обход 2") && strings.Contains(name, "Латвия"):
			tag = "auto-bypass-2-ip-213.21.201.167"
		case name == recoveryPilotTitle:
			tag = "trial-auto-bypass-3-ip-193.68.78.66-split"
		default:
			continue
		}
		for _, v := range outs {
			o := obj(v)
			if o["tag"] != tag || o["protocol"] != "vless" {
				continue
			}
			obj(bal[0])["fallbackTag"] = tag
			doc, err := json.Marshal(p)
			if err != nil {
				return body
			}
			docs[i], changed = doc, true
			break
		}
	}
	if !changed {
		return body
	}
	result, err := json.Marshal(docs)
	if err != nil {
		return body
	}
	return result
}
