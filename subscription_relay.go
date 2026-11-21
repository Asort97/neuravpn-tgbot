package main

import (
	"encoding/json"
	"net"
	"os"
	"strings"
)

// Relays are cheap VPSes in other networks that forward the Latvian ports in
// the kernel. REALITY and Hysteria still terminate in Latvia, so a relay copy
// of a candidate keeps every credential and only changes the entry address.
//
//	SUBSCRIPTION_RELAY_IPS=89.40.6.125   relay entry addresses
//	SUBSCRIPTION_RELAY_ALL=true          offer them to everyone, not only the owner
var relayForwardedPorts = map[string]map[float64]bool{
	"vless":    {443: true, 10001: true, 10002: true},
	"hysteria": {10010: true, 10011: true, 10012: true},
}

func subscriptionRelayIPs() []string {
	var ips []string
	for _, raw := range strings.Split(os.Getenv("SUBSCRIPTION_RELAY_IPS"), ",") {
		if ip := net.ParseIP(strings.TrimSpace(raw)); ip != nil && ip.To4() != nil {
			ips = append(ips, ip.String())
		}
	}
	return ips
}

func relayEnabledFor(userID string) bool {
	return userID == autoSelectPilotUser || os.Getenv("SUBSCRIPTION_RELAY_ALL") == "true"
}

// relayEndpoint returns the dial endpoint of a vless or hysteria outbound.
func relayEndpoint(out map[string]any) map[string]any {
	settings, _ := out["settings"].(map[string]any)
	switch out["protocol"] {
	case "vless":
		vnext, _ := settings["vnext"].([]any)
		if len(vnext) == 1 {
			ep, _ := vnext[0].(map[string]any)
			return ep
		}
	case "hysteria":
		return settings
	}
	return nil
}

func addRelayCandidates(body []byte, relays []string) ([]byte, error) {
	if len(relays) == 0 {
		return body, nil
	}
	var profiles []map[string]any
	if err := json.Unmarshal(body, &profiles); err != nil {
		return nil, err
	}
	for _, profile := range profiles {
		outs, _ := profile["outbounds"].([]any)
		have := map[string]bool{}
		for _, v := range outs {
			if o, ok := v.(map[string]any); ok {
				tag, _ := o["tag"].(string)
				have[tag] = true
			}
		}
		var added []any
		for _, v := range outs {
			o, ok := v.(map[string]any)
			if !ok {
				continue
			}
			tag, _ := o["tag"].(string)
			base, _, found := strings.Cut(tag, "-ip-")
			if !found || !(strings.HasPrefix(tag, "auto-bypass-") || strings.HasPrefix(tag, "auto-hy")) {
				continue
			}
			ep := relayEndpoint(o)
			proto, _ := o["protocol"].(string)
			port, _ := ep["port"].(float64)
			if ep == nil || !relayForwardedPorts[proto][port] {
				continue
			}
			for _, relay := range relays {
				newTag := base + "-ip-" + relay
				if have[newTag] {
					continue
				}
				encoded, err := json.Marshal(o)
				if err != nil {
					return nil, err
				}
				var clone map[string]any
				if err := json.Unmarshal(encoded, &clone); err != nil {
					return nil, err
				}
				relayEndpoint(clone)["address"] = relay
				clone["tag"] = newTag
				have[newTag] = true
				added = append(added, clone)
			}
		}
		if len(added) > 0 {
			profile["outbounds"] = append(outs, added...)
		}
	}
	return json.Marshal(profiles)
}
