package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Asort97/vpnBot/clients/postgres"
)

type deviceDeletionStore interface {
	QueueDeviceDeletion(context.Context, string, string, map[string][]postgres.DeviceDeletionTarget) error
	PendingDeviceDeletions(context.Context, string) ([]postgres.DeviceDeletion, error)
	ClaimDeviceDeletions(context.Context, int) ([]postgres.DeviceDeletion, error)
	ClaimPanelDeviceDeletions(context.Context, string) ([]postgres.DeviceDeletion, error)
	CompleteDeviceDeletion(context.Context, string) error
	ResolveDeviceDeletionTargets(context.Context, string, []postgres.DeviceDeletionTarget) error
}

var deviceDeletionWakes = struct {
	sync.Mutex
	panels map[string]chan struct{}
}{panels: make(map[string]chan struct{})}

type deviceSnapshot struct {
	view    subscriptionDevices
	expires time.Time
}

var deviceSnapshots = struct {
	sync.Mutex
	values map[string]deviceSnapshot
}{values: make(map[string]deviceSnapshot)}

func deviceView(ctx context.Context, configs []*xraySettings, userID string, deleting bool) subscriptionDevices {
	parts := []string{userID}
	for _, cfg := range configs {
		parts = append(parts, deletionPanelID(cfg))
	}
	key := strings.Join(parts, ":")
	deviceSnapshots.Lock()
	cached, ok := deviceSnapshots.values[key]
	deviceSnapshots.Unlock()
	if deleting && ok && time.Now().Before(cached.expires) {
		return cached.view
	}
	view := readSubscriptionDevices(ctx, configs, userID)
	deviceSnapshots.Lock()
	for k, v := range deviceSnapshots.values {
		if time.Now().After(v.expires) {
			delete(deviceSnapshots.values, k)
		}
	}
	if len(deviceSnapshots.values) >= 2048 {
		for k := range deviceSnapshots.values {
			delete(deviceSnapshots.values, k)
			break
		}
	}
	deviceSnapshots.values[key] = deviceSnapshot{view: view, expires: time.Now().Add(30 * time.Minute)}
	deviceSnapshots.Unlock()
	return view
}

func deletionPanelID(cfg *xraySettings) string {
	sum := sha256.Sum256([]byte(cfg.client.PanelIdentity()))
	return hex.EncodeToString(sum[:])
}

// Only server-resolved devices can reach this function. Public device IDs never
// contain a panel URL, client email or native record ID supplied by the caller.
func queueDeviceDeletion(ctx context.Context, configs []*xraySettings, userID string, device subscriptionDevice) error {
	s, ok := userStore.(deviceDeletionStore)
	if !ok {
		return errors.New("device deletion storage unavailable")
	}
	panels := make(map[string][]postgres.DeviceDeletionTarget, len(configs))
	for _, cfg := range configs {
		panels[deletionPanelID(cfg)] = nil
	}
	for _, record := range device.Records {
		panel := deletionPanelID(record.Config)
		panels[panel] = append(panels[panel], deletionTarget(record))
	}
	if err := s.QueueDeviceDeletion(ctx, userID, device.Key, panels); err != nil {
		return err
	}
	deviceSnapshots.Lock()
	for key, snapshot := range deviceSnapshots.values {
		if strings.HasPrefix(key, userID+":") {
			snapshot.view = withoutDevice(snapshot.view, device.Key)
			deviceSnapshots.values[key] = snapshot
		}
	}
	deviceSnapshots.Unlock()
	invalidateMergedSubscriptionCache(userID)
	deviceDeletionWakes.Lock()
	for panel := range panels {
		if wake := deviceDeletionWakes.panels[panel]; wake != nil {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}
	deviceDeletionWakes.Unlock()
	return nil
}

func withoutDevice(view subscriptionDevices, key string) subscriptionDevices {
	filtered := make([]subscriptionDevice, 0, len(view.Devices))
	for _, device := range view.Devices {
		if device.Key != key {
			filtered = append(filtered, device)
		}
	}
	view.Devices = filtered
	return view
}

func hidePendingDeviceDeletions(ctx context.Context, userID string, view subscriptionDevices) (subscriptionDevices, error) {
	s, ok := userStore.(deviceDeletionStore)
	if !ok {
		return view, nil
	}
	jobs, err := s.PendingDeviceDeletions(ctx, userID)
	if err != nil {
		return view, err
	}
	var remaining []ownedHWIDDevice
	for _, d := range view.Devices {
		for _, record := range d.Records {
			hidden := false
			for _, job := range jobs {
				if job.DeviceKey == d.Key && job.Panel == deletionPanelID(record.Config) && (job.Targets == nil || containsDeletionTarget(job.Targets, record)) {
					hidden = true
					break
				}
			}
			if !hidden {
				remaining = append(remaining, record)
			}
		}
	}
	view.Devices = groupSubscriptionDevices(remaining)
	return view, nil
}

func deletionTarget(record ownedHWIDDevice) postgres.DeviceDeletionTarget {
	return postgres.DeviceDeletionTarget{ID: record.Device.ID, SubID: record.SubID, FirstSeen: record.Device.FirstSeen}
}

func containsDeletionTarget(targets []postgres.DeviceDeletionTarget, record ownedHWIDDevice) bool {
	for _, target := range targets {
		if target == deletionTarget(record) {
			return true
		}
	}
	return false
}

func executeDeviceDeletion(ctx context.Context, cfg *xraySettings, job postgres.DeviceDeletion) error {
	// Refresh ownership on the target panel before using its native DELETE API.
	records, err := readPanelDevices(ctx, cfg, job.UserID)
	if err != nil {
		return err
	}
	if job.Targets == nil {
		job.Targets = make([]postgres.DeviceDeletionTarget, 0)
		for _, device := range groupSubscriptionDevices(records) {
			if device.Key == job.DeviceKey {
				for _, record := range device.Records {
					job.Targets = append(job.Targets, deletionTarget(record))
				}
			}
		}
		s, ok := userStore.(deviceDeletionStore)
		if !ok {
			return errors.New("device deletion storage unavailable")
		}
		if err := s.ResolveDeviceDeletionTargets(ctx, job.ID, job.Targets); err != nil {
			return err
		}
	}
	for _, device := range groupSubscriptionDevices(records) {
		if device.Key != job.DeviceKey {
			continue
		}
		for _, record := range device.Records {
			// Native identity is stable across independent panel/database clocks.
			if !containsDeletionTarget(job.Targets, record) {
				continue
			}
			deleteErr := cfg.client.DeleteHWIDDevice(ctx, record.Email, record.Device.ID)
			remaining, checkErr := cfg.client.ListHWIDDevices(ctx, record.Email)
			if checkErr != nil {
				return checkErr
			}
			for _, candidate := range remaining {
				if candidate.ID == record.Device.ID {
					if deleteErr != nil {
						return deleteErr
					}
					return errors.New("panel retained deleted device")
				}
			}
		}
	}
	return nil
}

func startDeviceDeletionWorker() {
	s, ok := userStore.(deviceDeletionStore)
	if !ok {
		return
	}
	for _, cfg := range deviceProviderConfigs(xrayCfg) {
		panel := deletionPanelID(cfg)
		wake := make(chan struct{}, 1)
		deviceDeletionWakes.Lock()
		deviceDeletionWakes.panels[panel] = wake
		deviceDeletionWakes.Unlock()
		go func(cfg *xraySettings, panel string, wake chan struct{}) {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				jobs, err := s.ClaimPanelDeviceDeletions(ctx, panel)
				cancel()
				if err != nil {
					log.Print("[devices] deletion queue unavailable")
				}
				if err != nil || len(jobs) == 0 {
					select {
					case <-ticker.C:
					case <-wake:
					}
					continue
				}
				for _, job := range jobs {
					func(job postgres.DeviceDeletion) {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := executeDeviceDeletion(ctx, cfg, job); err != nil {
							log.Printf("[devices] deletion retry scheduled job=%s", job.ID)
							return
						}
						if err := s.CompleteDeviceDeletion(ctx, job.ID); err != nil {
							log.Printf("[devices] deletion acknowledgement failed job=%s", job.ID)
							return
						}
						invalidateMergedSubscriptionCache(job.UserID)
					}(job)
				}
			}
		}(cfg, panel, wake)
	}
}
