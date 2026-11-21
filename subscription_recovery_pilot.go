package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

const recoveryPilotTitle = "Автовыбор · тест стабильности"
const recoveryProbeURL = "https://speed.cloudflare.com/__down?bytes=16384"

// Use only outbounds from the authorized subscription. Pair the two handshakes on
// each IP but rotate transports across the pool to bound the phone's probe cost.
func appendHappRecoveryPilot(body []byte, user, agent string) []byte {
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
	byTag := make(map[string]map[string]any)
	for _, o := range profiles[0].Outbounds {
		tag, _ := o["tag"].(string)
		byTag[tag] = o
	}
	var candidates []map[string]any
	add := func(src map[string]any, tag string, plain bool) {
		o := make(map[string]any, len(src))
		for k, v := range src {
			o[k] = v
		}
		o["tag"] = "trial-" + tag
		if plain {
			s := make(map[string]any)
			for k, v := range obj(src["streamSettings"]) {
				if k != "finalmask" {
					s[k] = v
				}
			}
			o["streamSettings"] = s
		}
		candidates = append(candidates, o)
	}
	for i, ip := range autoSelectPilotIPs {
		for j := 0; j < 3; j++ {
			n := (i+j+2)%3 + 1
			tag := "auto-bypass-" + strconv.Itoa(n) + "-ip-" + ip
			o := byTag[tag]
			if o == nil || o["protocol"] != "vless" || endpoint(obj(o["settings"]))["address"] != ip || obj(o["streamSettings"])["finalmask"] == nil {
				continue
			}
			add(o, tag+"-split", false)
			add(o, tag+"-plain", true)
			break
		}
	}
	if len(candidates) < 2 {
		return body
	}
	for _, p := range profiles {
		if p.Remarks == recoveryPilotTitle {
			return body
		}
		for _, o := range p.Outbounds {
			tag, _ := o["tag"].(string)
			e := endpoint(obj(o["settings"]))
			switch {
			case p.Remarks == profiles[0].Remarks && o["protocol"] == "hysteria" && e["address"] == "213.21.200.218" && e["port"] == float64(10010):
				add(o, "udp", false)
			case p.Remarks == "Обход 🇩🇪 Германия" && o["protocol"] == "vless" && e["address"] == "217.60.27.166" && e["port"] == float64(444):
				add(o, "de", false)
			case strings.Contains(p.Remarks, "Белые списки") && tag == "bs-ws" && o["protocol"] == "vless":
				add(o, "bs", false)
			default:
				continue
			}
			break
		}
	}
	candidates = append(candidates, map[string]any{"tag": "unavailable", "protocol": "blackhole", "settings": map[string]any{}})
	p := autoSelectClient(recoveryPilotTitle, candidates)
	p["burstObservatory"] = map[string]any{"subjectSelector": []string{"trial-"}, "pingConfig": map[string]any{
		"destination": recoveryProbeURL, "httpMethod": "GET", "timeout": "6s", "interval": "3m", "sampling": 1,
	}}
	p["routing"] = map[string]any{"domainStrategy": "AsIs", "rules": []any{map[string]any{"type": "field", "network": "tcp,udp", "balancerTag": "trial"}},
		"balancers": []any{map[string]any{"tag": "trial", "selector": []string{"trial-"}, "strategy": map[string]any{"type": "leastLoad", "settings": map[string]any{"expected": 1}}, "fallbackTag": "unavailable"}}}
	return appendHappProfile(body, p)
}

func appendHappProfile(body []byte, p map[string]any) []byte {
	raw, err := json.Marshal(p)
	if err != nil {
		return body
	}
	// Append without rewriting the other profiles or mutating a cached slice.
	b := bytes.TrimSpace(body)
	out := make([]byte, 0, len(b)+len(raw)+1)
	out = append(out, b[:len(b)-1]...)
	out = append(out, ',')
	out = append(out, raw...)
	return append(out, ']')
}
