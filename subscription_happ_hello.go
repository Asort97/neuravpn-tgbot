package main

import (
	"encoding/json"
	"strings"
)

// Change only the tested REALITY endpoints; preserve existing transport masks.
func applyHappHelloProfile(body []byte, user, agent string) []byte {
	ua := strings.ToLower(strings.TrimSpace(agent))
	if ua != "happ" && !strings.HasPrefix(ua, "happ/") {
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
			if o["protocol"] != "vless" || s["security"] != "reality" || s["finalmask"] != nil {
				continue
			}
			e := endpoint(obj(o["settings"]))
			host, _ := e["address"].(string)
			port, _ := e["port"].(float64)
			match := host == "217.60.27.166" && port == 444 && s["network"] == "xhttp"
			if user == autoSelectPilotUser && (port == 443 || port == 10001 || port == 10002) {
				for _, ip := range autoSelectPilotIPs {
					if ip == host {
						match = true
						break
					}
				}
			}
			if match {
				s["finalmask"] = map[string]any{"tcp": []any{map[string]any{"type": "fragment", "settings": map[string]string{"packets": "tlshello", "length": "10-20", "interval": "10-20"}}}}
				changed = true
			}
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
