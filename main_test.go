package main

import (
	"net/http"
	"testing"
	"time"
)

func TestMergedTrafficIsUnlimited(t *testing.T) {
	if got := mergedTrafficLimitForUser("623290294"); got != 0 {
		t.Fatalf("merged traffic limit must be unlimited (0), got %d", got)
	}
}

func TestParseMergedXrayNodeDefinitions(t *testing.T) {
	raw := `[
		{
			"name":"ru-extra",
			"panel_url":"https://panel.example.com/secret/",
			"api_token":"token",
			"inbound_ids":[7,7,9],
			"server_address":"ru.example.com",
			"server_port":443
		}
	]`
	nodes, err := parseMergedXrayNodeDefinitions(raw)
	if err != nil {
		t.Fatalf("parseMergedXrayNodeDefinitions returned error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("unexpected node count: %d", len(nodes))
	}
	node := nodes[0]
	if node.Name != "ru-extra" || node.ServerAddress != "ru.example.com" || node.ServerPort != 443 {
		t.Fatalf("unexpected node: %+v", node)
	}
	if len(node.InboundIDs) != 2 || node.InboundIDs[0] != 7 || node.InboundIDs[1] != 9 {
		t.Fatalf("inbound IDs were not normalized: %v", node.InboundIDs)
	}
}

func TestParseMergedXrayNodeDefinitionsRejectsUnsafeNodes(t *testing.T) {
	tests := []string{
		`[{"name":"missing-inbounds","host":"panel.example.com","api_token":"token"}]`,
		`[{"name":"missing-auth","host":"panel.example.com","inbound_ids":[1]}]`,
		`[{"name":"missing-panel","api_token":"token","inbound_ids":[1]}]`,
		`[{"name":"missing-server-address","host":"panel.example.com","api_token":"token","inbound_ids":[1]}]`,
		`not-json`,
	}
	for _, raw := range tests {
		if _, err := parseMergedXrayNodeDefinitions(raw); err == nil {
			t.Fatalf("expected invalid config to fail: %s", raw)
		}
	}
}

func TestParseMergedXrayNodeDefinitionsAllowsMissingServerPort(t *testing.T) {
	raw := `[{"name":"dynamic-ports","host":"panel.example.com","api_token":"token","inbound_ids":[443,444,445],"server_address":"node.example.com"}]`
	nodes, err := parseMergedXrayNodeDefinitions(raw)
	if err != nil {
		t.Fatalf("parseMergedXrayNodeDefinitions returned error: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ServerPort != 0 {
		t.Fatalf("server_port must remain unset for dynamic inbound ports: %+v", nodes)
	}
}

func TestParseInboundIDs(t *testing.T) {
	got := parseInboundIDs("5, 6, 5, broken, 0", 7)
	if len(got) != 2 || got[0] != 5 || got[1] != 6 {
		t.Fatalf("unexpected inbound IDs: %v", got)
	}
	fallback := parseInboundIDs("", 7)
	if len(fallback) != 1 || fallback[0] != 7 {
		t.Fatalf("fallback inbound ID was not used: %v", fallback)
	}
}

func TestNormalizeCompensationID(t *testing.T) {
	got, err := normalizeCompensationID(" Outage_Aug05 ")
	if err != nil {
		t.Fatalf("normalizeCompensationID returned error: %v", err)
	}
	if got != "outage_aug05" {
		t.Fatalf("unexpected normalized id: %q", got)
	}

	for _, value := range []string{"", "campaign with spaces", "авария", "campaign/one"} {
		if _, err := normalizeCompensationID(value); err == nil {
			t.Fatalf("expected invalid id %q to fail", value)
		}
	}
}

func TestParseCompensationCreateArgs(t *testing.T) {
	id, days, validForDays, err := parseCompensationCreateArgs("outage_aug05 1 7")
	if err != nil {
		t.Fatalf("parseCompensationCreateArgs returned error: %v", err)
	}
	if id != "outage_aug05" || days != 1 || validForDays != 7 {
		t.Fatalf("unexpected parsed values: id=%q days=%d valid=%d", id, days, validForDays)
	}

	for _, args := range []string{
		"outage_aug05 0 7",
		"outage_aug05 1 0",
		"outage_aug05 31 7",
		"outage_aug05 1 31",
		"outage_aug05 1",
	} {
		if _, _, _, err := parseCompensationCreateArgs(args); err == nil {
			t.Fatalf("expected invalid args %q to fail", args)
		}
	}
}

func TestMergedSubscriptionCacheClonesMutableData(t *testing.T) {
	key := "test-user|variant"
	original := mergedSubscriptionCacheEntry{
		body:         []byte("subscription"),
		headers:      http.Header{"Subscription-Userinfo": []string{"expire=123"}},
		statusCode:   http.StatusOK,
		mergedStatus: "merged:2",
	}
	storeMergedSubscriptionCache(key, original)
	t.Cleanup(func() {
		mergedSubscriptionCacheMu.Lock()
		delete(mergedSubscriptionCache, key)
		mergedSubscriptionCacheMu.Unlock()
	})

	original.body[0] = 'X'
	original.headers.Set("Subscription-Userinfo", "expire=999")
	first, ok := getMergedSubscriptionCache(key)
	if !ok {
		t.Fatal("cache entry missing")
	}
	if got := string(first.body); got != "subscription" {
		t.Fatalf("cached body mutated through source: %q", got)
	}
	if got := first.headers.Get("Subscription-Userinfo"); got != "expire=123" {
		t.Fatalf("cached header mutated through source: %q", got)
	}

	first.body[0] = 'Y'
	first.headers.Set("Subscription-Userinfo", "expire=456")
	second, ok := getMergedSubscriptionCache(key)
	if !ok {
		t.Fatal("cache entry missing on second read")
	}
	if got := string(second.body); got != "subscription" {
		t.Fatalf("cached body mutated through read result: %q", got)
	}
	if got := second.headers.Get("Subscription-Userinfo"); got != "expire=123" {
		t.Fatalf("cached header mutated through read result: %q", got)
	}
}

func TestMergedSubscriptionEntryFreshUsesShortTTLForPrimaryOnly(t *testing.T) {
	now := time.Now()
	merged := mergedSubscriptionCacheEntry{mergedStatus: "merged:2", cachedAt: now.Add(-time.Minute)}
	if !mergedSubscriptionEntryFresh(merged, now) {
		t.Fatal("merged entry should still be fresh")
	}
	primaryOnly := mergedSubscriptionCacheEntry{mergedStatus: "primary_only", cachedAt: now.Add(-time.Minute)}
	if mergedSubscriptionEntryFresh(primaryOnly, now) {
		t.Fatal("primary-only entry should use the short retry TTL")
	}
}
