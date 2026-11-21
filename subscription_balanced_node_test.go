package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBalancedMergedNode(t *testing.T) {
	const name = "Обход 🇩🇪 Германия"
	const ip = "192.0.2.4"
	const anchor = `[{"remarks":"Обход 🇩🇪 Германия","outbounds":[{"protocol":"vless","settings":{"vnext":[{"address":"192.0.2.4","users":[{"id":"alice"}]}]}}]}]`
	owner := "alice"
	hits := 0
	sv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path != "/json/sub-1" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]any{map[string]any{"outbounds": []any{map[string]any{"protocol": "vless", "settings": map[string]any{"address": ip, "port": 443, "id": owner}}}}, map[string]any{"outbounds": []any{map[string]any{"protocol": "hysteria", "settings": map[string]any{"address": ip, "port": 443, "version": 2}}}}})
	}))
	defer sv.Close()
	cfg := &xraySettings{balancedJSONURL: sv.URL + "/json", balancedRemark: name, serverAddress: ip, inboundIDs: []int{1, 2}}
	convert := func(body string) string {
		return string(enrichBalancedNodes(context.Background(), []byte(body), "sub-1", []*xraySettings{cfg}))
	}
	if b := convert("[]"); b != "[]" || hits != 0 {
		t.Fatal("requested credentials without authorized anchor")
	}
	if b := convert(anchor); !strings.Contains(b, `"probeInterval":"2m"`) || !strings.Contains(b, `"fallbackTag":"de-member-2"`) || !strings.Contains(b, `"type":"leastPing"`) {
		t.Fatal("missing balanced profile")
	}
	owner = "another-user"
	if b := convert(anchor); b != anchor {
		t.Fatal("foreign JSON replaced anchor")
	}
	cfg.inboundIDs = append(cfg.inboundIDs, 3)
	if b := convert(anchor); b != anchor {
		t.Fatal("partial response replaced anchor")
	}
	if validateBalancedNode(mergedXrayNodeDefinition{BalancedJSONURL: "https://example.org/json", BalancedRemark: name}) == nil {
		t.Fatal("accepted external management endpoint")
	}
	if strings.Contains(balancedFallbackLink("vless://alice@192.0.2.4:444?type=xhttp&flow=xtls-rprx-vision", name), "flow=") {
		t.Fatal("incompatible fallback flow")
	}
}
