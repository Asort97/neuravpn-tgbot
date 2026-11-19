package main

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Append opt-in copies; never replace the existing ten-second failover profiles.
func appendHappLVDataTrials(body []byte, user, agent string) []byte {
	ua := strings.ToLower(strings.TrimSpace(agent))
	if user != autoSelectPilotUser || !(ua == "happ" || strings.HasPrefix(ua, "happ/")) {
		return body
	}
	var docs []json.RawMessage
	if json.Unmarshal(body, &docs) != nil {
		return body
	}
	names := map[string]bool{}
	for _, raw := range docs {
		var meta struct{ Remarks string }
		if json.Unmarshal(raw, &meta) != nil {
			return body
		}
		names[meta.Remarks] = true
	}
	n := len(docs)
	for _, raw := range docs {
		var p map[string]any
		if json.Unmarshal(raw, &p) != nil {
			continue
		}
		name, _ := p["remarks"].(string)
		if !strings.Contains(name, "Латвия") || strings.Contains(name, "тест") {
			continue
		}
		var idx int
		for i := 1; i <= 3; i++ {
			if strings.Contains(name, "Обход "+strconv.Itoa(i)+" ") {
				idx = i
				break
			}
		}
		if idx == 0 {
			continue
		}
		title := "Латвия · Обход " + strconv.Itoa(idx) + " · тест данных"
		selector := "auto-bypass-" + strconv.Itoa(idx) + "-ip-"
		outs, _ := p["outbounds"].([]any)
		bal, _ := obj(p["routing"])["balancers"].([]any)
		if names[title] || len(outs) < 3 || len(bal) != 1 || obj(bal[0])["fallbackTag"] != "unavailable" || p["observatory"] == nil {
			continue
		}
		valid, blocked := true, false
		for _, v := range outs {
			o := obj(v)
			tag, _ := o["tag"].(string)
			if tag == "unavailable" && o["protocol"] == "blackhole" {
				blocked = true
			} else if !strings.HasPrefix(tag, selector) || o["protocol"] != "vless" {
				valid = false
			}
		}
		if !valid || !blocked {
			continue
		}
		delete(p, "observatory")
		p["remarks"] = title
		p["burstObservatory"] = map[string]any{"subjectSelector": []string{selector}, "pingConfig": map[string]any{
			"destination": recoveryProbeURL, "httpMethod": "GET", "interval": "1m", "timeout": "2s", "sampling": 1,
		}}
		obj(bal[0])["strategy"] = map[string]any{"type": "leastLoad", "settings": map[string]any{"expected": 1}}
		data, err := json.Marshal(p)
		if err != nil {
			return body
		}
		docs = append(docs, data)
		names[title] = true
	}
	if len(docs) == n {
		return body
	}
	out, err := json.Marshal(docs)
	if err != nil {
		return body
	}
	return out
}
