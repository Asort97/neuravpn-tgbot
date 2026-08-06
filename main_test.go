package main

import (
	"net/http"
	"testing"
	"time"
)

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
