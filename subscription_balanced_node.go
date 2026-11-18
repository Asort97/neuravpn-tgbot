package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func validateBalancedNode(n mergedXrayNodeDefinition) error {
	if n.BalancedJSONURL == "" && n.BalancedRemark == "" {
		return nil
	}
	u, err := url.Parse(n.BalancedJSONURL)
	if err != nil || u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() || u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.TrimSpace(n.BalancedRemark) == "" {
		return errors.New("balanced node requires a private loopback JSON endpoint and display name")
	}
	return nil
}

var balancedHTTP = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil, IdleConnTimeout: 30 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func balancedFallbackLink(link, remark string) string {
	u, err := url.Parse(link)
	if err != nil || u.Scheme != "vless" || u.User == nil {
		return ""
	}
	q := u.Query()
	if q.Get("type") == "xhttp" {
		q.Del("flow")
		u.RawQuery = q.Encode()
	}
	u.Fragment = remark
	return u.String()
}

func obj(v any) map[string]any { m, _ := v.(map[string]any); return m }
func endpoint(s map[string]any) map[string]any {
	for _, k := range []string{"vnext", "servers"} {
		if a, ok := s[k].([]any); ok && len(a) == 1 {
			return obj(a[0])
		}
	}
	return s
}
func endpointID(e map[string]any) string {
	if a, ok := e["users"].([]any); ok && len(a) == 1 {
		e = obj(a[0])
	}
	id, _ := e["id"].(string)
	return id
}

func buildBalancedNode(ctx context.Context, cfg *xraySettings, subID, authorizedID string) (map[string]any, error) {
	if subID == "" || subID == "." || subID == ".." || len(subID) > 256 || strings.ContainsAny(subID, "/\\?#%") || authorizedID == "" {
		return nil, errors.New("missing subscription identity")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(cfg.balancedJSONURL, "/")+"/"+url.PathEscape(subID), nil)
	if err != nil {
		return nil, errors.New("invalid node URL")
	}
	resp, err := balancedHTTP.Do(req)
	if err != nil {
		return nil, errors.New("node JSON unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("node JSON not available for account")
	}
	const limit = 2 << 20
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || len(b) > limit {
		return nil, errors.New("invalid node response size")
	}
	var docs []struct {
		Outbounds []map[string]any `json:"outbounds"`
	}
	if json.Unmarshal(b, &docs) != nil || len(docs) < 2 || len(docs) != len(cfg.inboundIDs) {
		return nil, errors.New("incomplete node JSON")
	}
	candidates := make([]map[string]any, 0, len(docs))
	seen := map[string]bool{}
	fallback := ""
	for i, doc := range docs {
		if len(doc.Outbounds) == 0 {
			return nil, errors.New("missing node outbound")
		}
		out := doc.Outbounds[0]
		proto, _ := out["protocol"].(string)
		switch proto {
		case "vless", "vmess", "trojan", "shadowsocks", "hysteria":
		default:
			return nil, errors.New("unexpected node protocol")
		}
		s := obj(out["settings"])
		e := endpoint(s)
		port, ok := e["port"].(float64)
		if !ok || port < 1 || port > 65535 || float64(int(port)) != port || e["address"] != cfg.serverAddress {
			return nil, errors.New("unexpected node endpoint")
		}
		if proto == "vless" || proto == "vmess" {
			if endpointID(e) != authorizedID {
				return nil, errors.New("node subscription owner mismatch")
			}
			if _, flat := s["id"]; flat {
				user := map[string]any{"id": authorizedID}
				for _, key := range []string{"encryption", "flow", "security", "alterId", "level"} {
					if v, ok := s[key]; ok {
						user[key] = v
					}
				}
				s = map[string]any{"vnext": []any{map[string]any{"address": cfg.serverAddress, "port": port, "users": []any{user}}}}
			}
		}
		tag := "de-member-" + strconv.Itoa(i+1)
		key := proto + ":" + strconv.Itoa(int(port))
		if seen[key] {
			return nil, errors.New("duplicate node endpoint")
		}
		seen[key] = true
		if fallback == "" || proto == "hysteria" {
			fallback = tag
		}
		candidates = append(candidates, map[string]any{"tag": tag, "protocol": proto, "settings": s, "streamSettings": out["streamSettings"], "mux": map[string]any{"enabled": false}})
	}
	profile := autoSelectClient(cfg.balancedRemark, candidates)
	profile["observatory"] = map[string]any{"subjectSelector": []string{"de-member-"}, "probeUrl": "https://www.gstatic.com/generate_204", "probeInterval": "2m", "enableConcurrency": true}
	profile["routing"] = map[string]any{"domainStrategy": "AsIs", "rules": []any{map[string]any{"type": "field", "network": "tcp,udp", "balancerTag": "node-auto"}}, "balancers": []any{map[string]any{"tag": "node-auto", "selector": []string{"de-member-"}, "strategy": map[string]any{"type": "leastPing"}, "fallbackTag": fallback}}}
	return profile, nil
}

func enrichBalancedNodes(ctx context.Context, body []byte, subID string, nodes []*xraySettings) []byte {
	var profiles []map[string]any
	changed := false
	for _, cfg := range nodes {
		if cfg == nil || cfg.balancedJSONURL == "" {
			continue
		}
		if profiles == nil && json.Unmarshal(body, &profiles) != nil {
			return body
		}
		for i, p := range profiles {
			if p["remarks"] != cfg.balancedRemark {
				continue
			}
			a, _ := p["outbounds"].([]any)
			if len(a) != 1 {
				continue
			}
			out := obj(a[0])
			e := endpoint(obj(out["settings"]))
			if out["protocol"] != "vless" || e["address"] != cfg.serverAddress {
				continue
			}
			next, err := buildBalancedNode(ctx, cfg, subID, endpointID(e))
			if err != nil {
				log.Printf("[merged-sub] %s: %v; keeping authorized fallback", cfg.balancedRemark, err)
				continue
			}
			profiles[i] = next
			changed = true
		}
	}
	if changed {
		if b, err := json.Marshal(profiles); err == nil {
			return b
		}
	}
	return body
}
