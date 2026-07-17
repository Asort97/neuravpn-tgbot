package main

import (
	"net/http"
	"testing"
	"time"
)

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
