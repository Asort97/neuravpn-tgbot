package main

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"net/http"
	"time"
)

// Local DB only: an offline merged panel cannot prevent listing or unblocking.
func serveRevokedDevices(w http.ResponseWriter, r *http.Request, secret, userID, action, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	items, err := revokedDevices(ctx, userID)
	if err != nil {
		http.Error(w, "device storage unavailable", 503)
		return
	}
	idFor := func(generation string) string {
		return webDevicesMAC(secret, "restore-device\n"+userID+"\n"+generation)
	}
	result := ""
	if action == "restore" {
		result = "absent"
		for _, item := range items {
			if !hmac.Equal([]byte(idFor(item.ID)), []byte(id)) {
				continue
			}
			restored, err := restoreDevice(ctx, userID, item.ID)
			if err != nil {
				http.Error(w, "device storage unavailable", 503)
				return
			}
			if restored {
				result = "restored"
			}
			break
		}
		items, err = revokedDevices(ctx, userID)
		if err != nil {
			http.Error(w, "device storage unavailable", 503)
			return
		}
	}
	type deviceJSON struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		LastSeen int64  `json:"last_seen"`
	}
	devices := make([]deviceJSON, 0, len(items))
	for _, item := range items {
		devices = append(devices, deviceJSON{idFor(item.ID), item.Name, item.RevokedAt.UnixMilli()})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"devices": devices, "partial": false, "result": result})
}
