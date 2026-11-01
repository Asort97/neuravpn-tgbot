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
	QueueDeviceDeletion(context.Context, string, string, []string) error
	PendingDeviceDeletions(context.Context, string) (map[string]bool, error)
	ClaimDeviceDeletions(context.Context, int) ([]postgres.DeviceDeletion, error)
	CompleteDeviceDeletion(context.Context, string) error
}

var deviceDeletionWake = make(chan struct{}, 1)

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
	panels := make([]string, 0, len(configs))
	for _, cfg := range configs {
		panels = append(panels, deletionPanelID(cfg))
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
	select {
	case deviceDeletionWake <- struct{}{}:
	default:
	}
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
	keys, err := s.PendingDeviceDeletions(ctx, userID)
	if err != nil {
		return view, err
	}
	filtered := make([]subscriptionDevice, 0, len(view.Devices))
	for _, d := range view.Devices {
		if !keys[d.Key] {
			filtered = append(filtered, d)
		}
	}
	view.Devices = filtered
	return view, nil
}

func executeDeviceDeletion(ctx context.Context, cfg *xraySettings, job postgres.DeviceDeletion) error {
	// Refresh ownership on the target panel before using its native DELETE API.
	records, err := readPanelDevices(ctx, cfg, job.UserID)
	if err != nil {
		return err
	}
	for _, device := range groupSubscriptionDevices(records) {
		if device.Key != job.DeviceKey {
			continue
		}
		for _, record := range device.Records {
			// A device explicitly registered again after this request is not an old record.
			if record.Device.FirstSeen > job.RequestedAt.UnixMilli() {
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
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-deviceDeletionWake:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			jobs, err := s.ClaimDeviceDeletions(ctx, 4)
			cancel()
			if err != nil {
				log.Print("[devices] deletion queue unavailable")
				continue
			}
			configs := make(map[string]*xraySettings)
			for _, cfg := range deviceProviderConfigs(xrayCfg) {
				configs[deletionPanelID(cfg)] = cfg
			}
			var wg sync.WaitGroup
			for _, job := range jobs {
				wg.Add(1)
				go func(job postgres.DeviceDeletion) {
					defer wg.Done()
					cfg := configs[job.Panel]
					if cfg == nil {
						log.Printf("[devices] deletion target no longer configured job=%s", job.ID)
						return
					}
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
			wg.Wait()
		}
	}()
}
