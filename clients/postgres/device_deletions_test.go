package postgres

import (
	"context"
	"testing"
)

func testDeviceDeletionQueue(t *testing.T, s *Store) {
	ctx := context.Background()
	if err := s.initDeviceDeletions(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", []string{"panel-a", ""}); err == nil {
		t.Fatal("invalid batch committed")
	}
	pending, err := s.PendingDeviceDeletions(ctx, "42")
	if err != nil || len(pending) != 0 {
		t.Fatal("partial transaction persisted", err)
	}
	for i := 0; i < 2; i++ {
		if err = s.QueueDeviceDeletion(ctx, "42", "test", []string{"panel-a", "panel-b"}); err != nil {
			t.Fatal(err)
		}
	}
	// A new Store instance represents a process restart; the command is not in RAM.
	restarted := &Store{pool: s.pool}
	pending, err = restarted.PendingDeviceDeletions(ctx, "42")
	if err != nil || !pending["test"] {
		t.Fatal("queued deletion lost after restart", err)
	}
	foreign, err := restarted.PendingDeviceDeletions(ctx, "43")
	if err != nil || len(foreign) != 0 {
		t.Fatal("cross-user pending deletion", err)
	}
	jobs, err := restarted.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(jobs) != 2 {
		t.Fatal("missing or duplicate jobs", err, len(jobs))
	}
	other, err := s.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(other) != 0 {
		t.Fatal("worker lease shared", err)
	}
	if err = s.CompleteDeviceDeletion(ctx, jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.PendingDeviceDeletions(ctx, "42")
	if !pending["test"] {
		t.Fatal("unfinished panel forgotten")
	}
	if err = s.CompleteDeviceDeletion(ctx, jobs[1].ID); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.PendingDeviceDeletions(ctx, "42")
	if len(pending) != 0 {
		t.Fatal("completed deletion still pending")
	}
	if err = s.QueueDeviceDeletion(ctx, "42", "test", []string{"panel-a", "panel-b"}); err != nil {
		t.Fatal(err)
	}
	for _, j := range jobs {
		if err = s.CompleteDeviceDeletion(ctx, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	pending, _ = s.PendingDeviceDeletions(ctx, "42")
	if !pending["test"] {
		t.Fatal("stale acknowledgement completed newer deletion")
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM device_deletions`)
	if err != nil {
		t.Fatal(err)
	}
}
