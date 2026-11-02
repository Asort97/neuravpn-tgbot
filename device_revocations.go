package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Asort97/vpnBot/clients/postgres"
)

const removedDeviceTitle = "устройство было удалено. если хотите добавить это устройство, то разблокируйте его в списках удаленных"

type deviceRevocationStore interface {
	RevokeAndQueueDeviceDeletion(context.Context, string, string, string, map[string][]postgres.DeviceDeletionTarget) error
	DeviceRevoked(context.Context, string, string) (bool, error)
	RevokedDevices(context.Context, string) ([]postgres.DeviceRevocation, error)
	RestoreDevice(context.Context, string, string) (bool, error)
}

func requestDeviceKey(r *http.Request) string {
	raw := strings.TrimSpace(r.Header.Get("X-HWID"))
	if len(raw) < 6 {
		return ""
	}
	// 3x-ui hashes the trimmed, case-sensitive HWID and exposes 12 hex digits.
	sum := sha256.Sum256([]byte(raw))
	return "hwid:" + hex.EncodeToString(sum[:])
}

func gateRevokedDevice(w http.ResponseWriter, r *http.Request, userID string) bool {
	key := requestDeviceKey(r)
	if key == "" {
		return true
	} // Existing HWID-limit policy handles unsupported clients.
	s, ok := userStore.(deviceRevocationStore)
	if !ok {
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	revoked, err := s.DeviceRevoked(ctx, userID, key)
	if err != nil {
		log.Printf("[devices] revocation lookup failed user=%s", userID)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "subscription temporarily unavailable", http.StatusServiceUnavailable)
		return false
	}
	if revoked {
		w.Header().Set("X-Neura-Device-Revoked", "true")
		limitPlaceholder(w, removedDeviceTitle)
		return false
	}
	return true
}

// Recheck after an asynchronous build: a deletion may have committed in flight.
func writeDeviceAuthorizedSubscriptionEntry(w http.ResponseWriter, r *http.Request, entry mergedSubscriptionCacheEntry, state, userID, token string) bool {
	if gateRevokedDevice(w, r, userID) {
		writeAuthorizedSubscriptionEntry(w, entry, state, userID, token)
		return true
	}
	return false
}

func revokedDevices(ctx context.Context, userID string) ([]postgres.DeviceRevocation, error) {
	s, ok := userStore.(deviceRevocationStore)
	if !ok {
		return nil, errors.New("revocation storage unavailable")
	}
	return s.RevokedDevices(ctx, userID)
}

func restoreDevice(ctx context.Context, userID, id string) (bool, error) {
	s, ok := userStore.(deviceRevocationStore)
	if !ok {
		return false, errors.New("revocation storage unavailable")
	}
	restored, err := s.RestoreDevice(ctx, userID, id)
	if err == nil {
		invalidateMergedSubscriptionCache(userID)
		deviceSnapshots.Lock()
		for key := range deviceSnapshots.values {
			if strings.HasPrefix(key, userID+":") {
				delete(deviceSnapshots.values, key)
			}
		}
		deviceSnapshots.Unlock()
	}
	return restored, err
}
