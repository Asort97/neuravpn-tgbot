package main

import (
	"encoding/json"
	"os"
	"strings"
)

// Probe a download large enough to cross the ~16 KB stall seen on Russian paths.
const tunedProbeURL = "https://speed.cloudflare.com/__down?bytes=32768"

func probeTuningEnabled(user string) bool {
	if os.Getenv("SUBSCRIPTION_PROBE_TUNING_ENABLED") == "true" {
		return true
	}
	return user == autoSelectPilotUser && os.Getenv("SUBSCRIPTION_PROBE_TUNING_PILOT") != "false"
}

// For Happ balancers that blackhole traffic until probes finish: start on a real
// outbound immediately, probe less often with a real download, spread over time.
func applyProbeTuning(body []byte, user, agent string) []byte {
	if !probeTuningEnabled(user) || !isTunedJSONAgent(agent, user) {
		return body
	}
	var profiles []map[string]any
	if json.Unmarshal(body, &profiles) != nil {
		return body
	}
	changed := false
	for _, p := range profiles {
		routing := obj(p["routing"])
		balancers, _ := routing["balancers"].([]any)
		if len(balancers) != 1 {
			continue
		}
		b := obj(balancers[0])
		if b["fallbackTag"] != "unavailable" {
			continue
		}
		selectors, _ := b["selector"].([]any)
		var prefixes []string
		for _, s := range selectors {
			if v, ok := s.(string); ok && v != "" {
				prefixes = append(prefixes, v)
			}
		}
		// The main auto profile mixes TCP, XHTTP and Hysteria; random picks before the
		// first probes land cost seconds. Keep it on XHTTP, which reuses connections.
		if p["remarks"] == "⚡ Автоматический выбор" && hasOutboundPrefix(p, "auto-bypass-3-") {
			prefixes = []string{"auto-bypass-3-"}
			b["selector"] = []any{"auto-bypass-3-"}
			delete(p, "burstObservatory")
		}
		fallback := tunedFallback(p, prefixes)
		if fallback == "" {
			continue
		}
		b["fallbackTag"] = fallback
		b["strategy"] = map[string]any{"type": "leastLoad", "settings": map[string]any{"expected": 1}}
		subjects := prefixes
		if bo := obj(p["burstObservatory"]); bo != nil {
			if existing, ok := bo["subjectSelector"].([]any); ok && len(existing) > 0 {
				subjects = nil
				for _, s := range existing {
					if v, ok := s.(string); ok {
						subjects = append(subjects, v)
					}
				}
			}
		}
		delete(p, "observatory")
		p["burstObservatory"] = map[string]any{
			"subjectSelector": subjects,
			"pingConfig": map[string]any{
				"destination": tunedProbeURL, "httpMethod": "GET",
				"interval": "1m", "timeout": "8s", "sampling": 2,
			},
		}
		changed = true
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

// Prefer the main XHTTP bypass on the primary address, then any selected member.
func tunedFallback(p map[string]any, prefixes []string) string {
	outs, _ := p["outbounds"].([]any)
	var first string
	for _, value := range outs {
		tag, _ := obj(value)["tag"].(string)
		if tag == "" || tag == "unavailable" {
			continue
		}
		matched := false
		for _, pre := range prefixes {
			if strings.HasPrefix(tag, pre) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if strings.HasPrefix(tag, "auto-bypass-3-ip-213.21.208.254") {
			return tag
		}
		if first == "" {
			first = tag
		}
	}
	return first
}

func hasOutboundPrefix(p map[string]any, prefix string) bool {
	outs, _ := p["outbounds"].([]any)
	for _, value := range outs {
		if tag, _ := obj(value)["tag"].(string); strings.HasPrefix(tag, prefix) {
			return true
		}
	}
	return false
}
