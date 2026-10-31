package main

import (
	"context"
	"log"
	"net/http"
	"strings"

	xray "github.com/Asort97/vpnBot/clients/Xray"
)

// Panel-side changes can leave the local subscription ID stale. Recover only
// from a fresh, owner-scoped read, never by creating or extending access.
func fetchPrimarySubscriptionBody(ctx context.Context, cfg *xraySettings, userID, upstreamURL string, request *http.Request) ([]byte, http.Header, int, error) {
	body, headers, code, err := fetchSubscriptionBody(ctx, upstreamURL, request)
	if err != nil || code != http.StatusNotFound || cfg == nil || cfg.client == nil {
		return body, headers, code, err
	}
	client, lookupErr := readPrimarySubscriptionClient(ctx, cfg, userID)
	if lookupErr != nil {
		return nil, nil, 0, lookupErr
	}
	if client == nil || client.TgID != userID || strings.TrimSpace(client.SubID) == "" {
		return body, headers, code, nil
	}
	canonical := generateSubscriptionURL(cfg, client)
	if canonical == upstreamURL {
		return body, headers, code, nil
	}
	body, headers, code, err = fetchSubscriptionBody(ctx, canonical, request)
	if err == nil && code == http.StatusOK {
		if store, ok := userStore.(interface {
			CompareAndSwapSubscriptionID(context.Context, string, string, string) error
		}); ok {
			oldID, readErr := userStore.GetSubscriptionID(userID)
			if readErr == nil && generateSubscriptionURL(cfg, &xray.Client{SubID: oldID, TgID: userID}) == upstreamURL {
				if saveErr := store.CompareAndSwapSubscriptionID(ctx, userID, oldID, client.SubID); saveErr != nil {
					log.Printf("[merged-sub] subscription metadata repair failed user=%s", userID)
				}
			}
		}
		log.Printf("[merged-sub] recovered stale subscription ID user=%s", userID)
	}
	return body, headers, code, err
}
