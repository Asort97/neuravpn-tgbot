package postgres

import (
	"context"
	"testing"
)

func testDeviceRevocations(t *testing.T, s *Store) {
	ctx := context.Background()
	key := "hwid:bef57ec7f53a"
	fullKey := "hwid:bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721"
	if err := s.initDeviceDeletions(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAndQueueDeviceDeletion(ctx, "42", key, "phone", map[string][]DeviceDeletionTarget{"": nil}); err == nil {
		t.Fatal("invalid queue accepted")
	}
	if banned, err := s.DeviceRevoked(ctx, "42", fullKey); err != nil || banned {
		t.Fatal("rollback retained ban", err)
	}
	panels := map[string][]DeviceDeletionTarget{"panel-a": {{ID: 1, SubID: "owned", FirstSeen: 1}}, "panel-b": nil}
	if err := s.RevokeAndQueueDeviceDeletion(ctx, "42", key, "phone", panels); err != nil {
		t.Fatal(err)
	}
	list, err := s.RevokedDevices(ctx, "42")
	if err != nil || len(list) != 1 || list[0].Name != "phone" {
		t.Fatal(err, list)
	}
	id := list[0].ID
	if err := s.RevokeAndQueueDeviceDeletion(ctx, "42", key, "phone renamed", panels); err != nil {
		t.Fatal(err)
	}
	list, _ = s.RevokedDevices(ctx, "42")
	if len(list) != 1 || list[0].ID != id || list[0].Name != "phone renamed" {
		t.Fatal("duplicate deletion changed generation", list)
	}
	// Another Store instance models a restart: revocations cannot live only in RAM.
	restarted := &Store{pool: s.pool}
	if banned, err := restarted.DeviceRevoked(ctx, "42", fullKey); err != nil || !banned {
		t.Fatal("ban did not persist", err)
	}
	if banned, err := s.DeviceRevoked(ctx, "43", fullKey); err != nil || banned {
		t.Fatal("cross-account ban", err)
	}
	if restored, err := s.RestoreDevice(ctx, "43", id); err != nil || restored {
		t.Fatal("foreign user restored device", err)
	}
	jobs, err := s.PendingDeviceDeletions(ctx, "42")
	if err != nil || len(jobs) != 2 {
		t.Fatal(err, jobs)
	}
	if err := s.CompleteDeviceDeletion(ctx, jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	if banned, err := s.DeviceRevoked(ctx, "42", fullKey); err != nil || !banned {
		t.Fatal("cleanup removed persistent ban", err)
	}
	if restored, err := s.RestoreDevice(ctx, "42", id); err != nil || !restored {
		t.Fatal("restore failed", err)
	}
	for _, j := range jobs {
		if pending, err := s.DeviceDeletionPending(ctx, j.ID); err != nil || pending {
			t.Fatal("restore retained cleanup job", err)
		}
	}
	if banned, err := s.DeviceRevoked(ctx, "42", fullKey); err != nil || banned {
		t.Fatal("restore retained ban", err)
	}
	if err := s.RevokeAndQueueDeviceDeletion(ctx, "42", key, "phone", panels); err != nil {
		t.Fatal(err)
	}
	if restored, err := s.RestoreDevice(ctx, "42", id); err != nil || restored {
		t.Fatal("stale restore removed newer ban", err)
	}
	list, _ = s.RevokedDevices(ctx, "42")
	if len(list) != 1 || list[0].ID == id {
		t.Fatal("missing new generation", list)
	}
	if _, err := s.RestoreDevice(ctx, "42", list[0].ID); err != nil {
		t.Fatal(err)
	}
	var days int
	if err := s.pool.QueryRow(ctx, `SELECT days FROM users WHERE id='42'`).Scan(&days); err != nil || days != 123 {
		t.Fatal("subscription days changed", err, days)
	}
}
