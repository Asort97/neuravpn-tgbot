package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Asort97/vpnBot/clients/postgres"
)

type deletionTestStore struct {
	DataStore
	jobs []postgres.DeviceDeletion
	fail bool
}

func (s *deletionTestStore) QueueDeviceDeletion(_ context.Context, uid, key string, panels []string) error {
	if s.fail {
		return errors.New("database unavailable")
	}
	for _, panel := range panels {
		found := false
		for _, j := range s.jobs {
			if j.UserID == uid && j.DeviceKey == key && j.Panel == panel {
				found = true
			}
		}
		if !found {
			s.jobs = append(s.jobs, postgres.DeviceDeletion{ID: fmt.Sprint(len(s.jobs)), UserID: uid, DeviceKey: key, Panel: panel, RequestedAt: time.Now()})
		}
	}
	return nil
}
func (s *deletionTestStore) PendingDeviceDeletions(_ context.Context, uid string) (map[string]bool, error) {
	out := make(map[string]bool)
	for _, j := range s.jobs {
		if j.UserID == uid {
			out[j.DeviceKey] = true
		}
	}
	return out, nil
}
func (s *deletionTestStore) ClaimDeviceDeletions(_ context.Context, n int) ([]postgres.DeviceDeletion, error) {
	return append([]postgres.DeviceDeletion(nil), s.jobs[:min(n, len(s.jobs))]...), nil
}
func (s *deletionTestStore) CompleteDeviceDeletion(_ context.Context, id string) error {
	for i, j := range s.jobs {
		if j.ID == id {
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
			break
		}
	}
	return nil
}

func drainDeviceDeletions(t *testing.T, primary *xraySettings) {
	t.Helper()
	s := userStore.(deviceDeletionStore)
	jobs, err := s.ClaimDeviceDeletions(context.Background(), 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		for _, cfg := range deviceProviderConfigs(primary) {
			if deletionPanelID(cfg) == job.Panel {
				if executeDeviceDeletion(context.Background(), cfg, job) == nil {
					if err = s.CompleteDeviceDeletion(context.Background(), job.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func TestDeviceDeletionUsesNativeAPIAndVerifiesAbsence(t *testing.T) {
	cfg, f := devicePanel(t, "a", 1, "0123456789ab")
	job := postgres.DeviceDeletion{UserID: "42", DeviceKey: "hwid:0123456789ab", RequestedAt: time.Now()}
	if err := executeDeviceDeletion(context.Background(), cfg, job); err != nil {
		t.Fatal(err)
	}
	if f.deletes != 1 || f.lists != 2 {
		t.Fatalf("native deletion and verification missing: %+v", f)
	}
	if err := executeDeviceDeletion(context.Background(), cfg, job); err != nil || f.deletes != 1 {
		t.Fatal("absent record is not idempotent", err)
	}
}

func TestDeviceDeletionNeverDeletesNewRegistration(t *testing.T) {
	cfg, f := devicePanel(t, "a", 1, "0123456789ab")
	now := time.Now()
	f.devices[0].FirstSeen = now.Add(time.Minute).UnixMilli()
	if err := executeDeviceDeletion(context.Background(), cfg, postgres.DeviceDeletion{UserID: "42", DeviceKey: "hwid:0123456789ab", RequestedAt: now}); err != nil {
		t.Fatal(err)
	}
	if f.deletes != 0 {
		t.Fatal("deleted a newer registration")
	}
}

func TestDeviceDeletionVerifiesActualStateAfterAmbiguousReply(t *testing.T) {
	for _, retained := range []bool{false, true} {
		cfg, f := devicePanel(t, "a", 1, "0123456789ab")
		f.keepDeleted = retained
		f.lostDeleteReply = !retained
		err := executeDeviceDeletion(context.Background(), cfg, postgres.DeviceDeletion{UserID: "42", DeviceKey: "hwid:0123456789ab", RequestedAt: time.Now()})
		if (err != nil) != retained {
			t.Fatalf("retained=%v error=%v", retained, err)
		}
	}
}

func TestDeviceDeletionDoesNotHideWhenQueueCommitFails(t *testing.T) {
	cfg, _ := devicePanel(t, "a", 1, "0123456789ab")
	setDeviceTestProviders(t)
	userStore.(*deletionTestStore).fail = true
	view := deviceView(context.Background(), []*xraySettings{cfg}, "42", false)
	if err := queueDeviceDeletion(context.Background(), []*xraySettings{cfg}, "42", view.Devices[0]); err == nil {
		t.Fatal("unsaved command acknowledged")
	}
	view, err := hidePendingDeviceDeletions(context.Background(), "42", view)
	if err != nil || len(view.Devices) != 1 {
		t.Fatal("device hidden without durable command")
	}
}
