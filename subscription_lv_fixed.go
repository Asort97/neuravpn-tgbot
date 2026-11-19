package main

import (
	"encoding/json"
	"strings"
)

const fixedLVTitle = "Латвия · Обход 3 · фиксированный IP"
const fixedLVAddress = "193.68.78.66"

func appendHappFixedLVPilot(body []byte, user, agent string) []byte {
	ua := strings.ToLower(strings.TrimSpace(agent))
	if user != autoSelectPilotUser || !(ua == "happ" || strings.HasPrefix(ua, "happ/")) {
		return body
	}
	var profiles []struct {
		Remarks   string
		Outbounds []map[string]any
	}
	if json.Unmarshal(body, &profiles) != nil || len(profiles) == 0 || profiles[0].Remarks != "⚡ Автоматический выбор" {
		return body
	}
	for _, p := range profiles {
		if p.Remarks == fixedLVTitle {
			return body
		}
	}
	for _, out := range profiles[0].Outbounds {
		if out["tag"] != "auto-bypass-3-ip-"+fixedLVAddress || out["protocol"] != "vless" {
			continue
		}
		ep := endpoint(obj(out["settings"]))
		s := obj(out["streamSettings"])
		if ep["address"] == fixedLVAddress && ep["port"] == float64(443) && endpointID(ep) != "" && s["network"] == "xhttp" && s["security"] == "reality" {
			return appendHappProfile(body, autoSelectClient(fixedLVTitle, []map[string]any{out}))
		}
	}
	return body
}
