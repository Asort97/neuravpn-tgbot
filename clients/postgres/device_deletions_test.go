package postgres

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestDeviceDeletionTargetsJSON(t *testing.T) {
	for _, tc := range []struct {
		name    string
		targets []DeviceDeletionTarget
		json    string
	}{
		{"unresolved", nil, `null`},
		{"observed empty", []DeviceDeletionTarget{}, `[]`},
		{"observed identities", []DeviceDeletionTarget{{ID: 7, SubID: "sub-a", FirstSeen: 0}, {ID: 8, SubID: "sub-b", FirstSeen: 4102444800000}}, `[{"id":7,"sub_id":"sub-a","first_seen":0},{"id":8,"sub_id":"sub-b","first_seen":4102444800000}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := encodeDeviceDeletionTargets(tc.targets)
			if err != nil || string(data) != tc.json {
				t.Fatalf("unexpected targets JSON: %s, %v", data, err)
			}
			// Exercise the same pgx JSONB decoder used by Pending and Claim.
			decoded := []DeviceDeletionTarget{{ID: 99, SubID: "stale"}}
			if err := pgtype.NewMap().Scan(pgtype.JSONBOID, pgtype.TextFormatCode, data, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, tc.targets) {
				t.Fatalf("targets changed on JSONB round trip: %#v", decoded)
			}
		})
	}
}

func TestDeviceDeletionTargetsCanonicalOrder(t *testing.T) {
	ordered := []DeviceDeletionTarget{{ID: 1, SubID: "a", FirstSeen: 1}, {ID: 1, SubID: "a", FirstSeen: 2}, {ID: 2, SubID: "a", FirstSeen: 1}, {ID: 1, SubID: "b", FirstSeen: 1}}
	reversed := []DeviceDeletionTarget{ordered[3], ordered[2], ordered[1], ordered[0]}
	before := append([]DeviceDeletionTarget{}, reversed...)
	want, err := encodeDeviceDeletionTargets(ordered)
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeDeviceDeletionTargets(reversed)
	if err != nil || string(got) != string(want) {
		t.Fatal("target order changed identity comparison", err)
	}
	if !reflect.DeepEqual(reversed, before) {
		t.Fatal("encoding mutated caller targets")
	}
}

func TestDeviceDeletionClaimValidation(t *testing.T) {
	s := &Store{}
	ctx := context.Background()
	if _, err := s.ClaimPanelDeviceDeletions(ctx, ""); err == nil {
		t.Fatal("empty panel reached the database")
	}
	for _, limit := range []int{0, -1, 33} {
		if _, err := s.ClaimDeviceDeletions(ctx, limit); err == nil {
			t.Fatal("invalid limit reached the database", limit)
		}
	}
}

func testDeviceDeletionQueue(t *testing.T, s *Store) {
	ctx := context.Background()
	// Reproduce the deployed schema before the targets migration.
	_, err := s.pool.Exec(ctx, `CREATE TABLE device_deletions (
 id TEXT PRIMARY KEY,
 user_id TEXT NOT NULL REFERENCES users(id),
 device_key TEXT NOT NULL,
 panel TEXT NOT NULL,
 requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 completed_at TIMESTAMPTZ,
 UNIQUE(user_id,device_key,panel)
);
INSERT INTO device_deletions(id,user_id,device_key,panel) VALUES('legacy','42','legacy','panel-a');`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := s.pool.Exec(ctx, `DELETE FROM device_deletions`); err != nil {
			t.Error(err)
		}
	}()
	for i := 0; i < 2; i++ {
		if err := s.initDeviceDeletions(ctx); err != nil {
			t.Fatal(err)
		}
	}
	legacy, err := s.PendingDeviceDeletions(ctx, "42")
	if err != nil || len(legacy) != 1 || legacy[0].ID != "legacy" || legacy[0].Targets != nil {
		t.Fatal("migration did not preserve unresolved legacy job", err, legacy)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM device_deletions WHERE id='legacy'`); err != nil {
		t.Fatal(err)
	}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", map[string][]DeviceDeletionTarget{"panel-a": nil, "": nil}); err == nil {
		t.Fatal("invalid batch committed")
	}
	pending, err := s.PendingDeviceDeletions(ctx, "42")
	if err != nil || len(pending) != 0 {
		t.Fatal("partial transaction persisted", err)
	}
	observed := []DeviceDeletionTarget{{ID: 7, SubID: "sub-a", FirstSeen: 0}, {ID: 8, SubID: "sub-b", FirstSeen: 4102444800000}}
	panels := map[string][]DeviceDeletionTarget{"panel-a": observed, "panel-b": nil, "panel-c": {}}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", panels); err != nil {
		t.Fatal(err)
	}
	byPanel := func(jobs []DeviceDeletion) map[string]DeviceDeletion {
		t.Helper()
		out := make(map[string]DeviceDeletion)
		for _, job := range jobs {
			if job.UserID != "42" || job.DeviceKey != "test" || job.ID == "" || job.RequestedAt.IsZero() {
				t.Fatalf("incomplete job: %+v", job)
			}
			if _, duplicate := out[job.Panel]; duplicate {
				t.Fatal("duplicate panel job", job.Panel)
			}
			out[job.Panel] = job
		}
		return out
	}
	readPending := func(store *Store) map[string]DeviceDeletion {
		t.Helper()
		jobs, err := store.PendingDeviceDeletions(ctx, "42")
		if err != nil {
			t.Fatal(err)
		}
		return byPanel(jobs)
	}
	initial := readPending(s)
	if len(initial) != len(panels) {
		t.Fatal("missing destinations", initial)
	}
	for panel, targets := range panels {
		if !reflect.DeepEqual(initial[panel].Targets, targets) {
			t.Fatal("target identities changed", panel, initial[panel].Targets)
		}
	}
	// Pending duplicates must not replace IDs, observations, or request metadata.
	replacement := []DeviceDeletionTarget{{ID: 99, SubID: "new-sub", FirstSeen: 1}}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", map[string][]DeviceDeletionTarget{"panel-a": {observed[1], observed[0]}, "panel-b": nil, "panel-c": {}}); err != nil {
		t.Fatal(err)
	}
	restarted := &Store{pool: s.pool}
	if got := readPending(restarted); !reflect.DeepEqual(got, initial) {
		t.Fatal("pending generation changed or was lost after restart", got)
	}
	foreign, err := restarted.PendingDeviceDeletions(ctx, "43")
	if err != nil || len(foreign) != 0 {
		t.Fatal("cross-user pending deletion", err)
	}
	if err := s.ResolveDeviceDeletionTargets(ctx, initial["panel-b"].ID, nil); err == nil {
		t.Fatal("nil observation accepted as resolved")
	}
	// Concurrent observations may win once, but cannot overwrite each other.
	resolved := []DeviceDeletionTarget{{ID: 10, SubID: "sub-c", FirstSeen: 123}}
	results := make(chan error, 2)
	for _, targets := range [][]DeviceDeletionTarget{resolved, replacement} {
		go func(targets []DeviceDeletionTarget) {
			results <- restarted.ResolveDeviceDeletionTargets(ctx, initial["panel-b"].ID, targets)
		}(targets)
	}
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	frozen := readPending(restarted)
	if !reflect.DeepEqual(frozen["panel-b"].Targets, resolved) && !reflect.DeepEqual(frozen["panel-b"].Targets, replacement) {
		t.Fatal("resolution lost both observations")
	}
	for _, job := range frozen {
		if err := s.ResolveDeviceDeletionTargets(ctx, job.ID, replacement); err != nil {
			t.Fatal(err)
		}
	}
	if got := readPending(s); !reflect.DeepEqual(got, frozen) {
		t.Fatal("resolved observations, including empty, were overwritten", got)
	}
	// Claims carry persisted identities regardless of request clock values.
	if _, err := s.pool.Exec(ctx, `UPDATE device_deletions SET requested_at=CASE WHEN panel='panel-a' THEN TIMESTAMPTZ '2100-01-01' ELSE TIMESTAMPTZ '1970-01-01' END`); err != nil {
		t.Fatal(err)
	}
	type claimResult struct {
		jobs []DeviceDeletion
		err  error
	}
	claims := make(chan claimResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			jobs, err := restarted.ClaimDeviceDeletions(ctx, 2)
			claims <- claimResult{jobs, err}
		}()
	}
	var jobs []DeviceDeletion
	for i := 0; i < 2; i++ {
		result := <-claims
		if result.err != nil {
			t.Fatal(result.err)
		}
		jobs = append(jobs, result.jobs...)
	}
	claimed := byPanel(jobs)
	if len(claimed) != 3 {
		t.Fatal("missing or duplicate claims", claimed)
	}
	for panel, job := range claimed {
		if job.ID != initial[panel].ID || !reflect.DeepEqual(job.Targets, frozen[panel].Targets) {
			t.Fatal("claim changed generation or target identities", job)
		}
	}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", panels); err != nil {
		t.Fatal(err)
	}
	other, err := s.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(other) != 0 {
		t.Fatal("duplicate enqueue reset worker lease", err, other)
	}
	for _, limit := range []int{0, 33} {
		if _, err := s.ClaimDeviceDeletions(ctx, limit); err == nil {
			t.Fatal("invalid claim limit accepted", limit)
		}
	}
	if _, err := s.pool.Exec(ctx, `UPDATE device_deletions SET retry_at=NOW()-INTERVAL '1 second' WHERE id=$1`, initial["panel-a"].ID); err != nil {
		t.Fatal(err)
	}
	retry, err := restarted.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(retry) != 1 || retry[0].ID != initial["panel-a"].ID || !reflect.DeepEqual(retry[0].Targets, observed) {
		t.Fatal("retry lost original targets or generation", err, retry)
	}
	if err := s.CompleteDeviceDeletion(ctx, initial["panel-a"].ID); err != nil {
		t.Fatal(err)
	}
	if got := readPending(s); len(got) != 2 {
		t.Fatal("unfinished panels forgotten", got)
	}
	for _, job := range jobs {
		if err := s.CompleteDeviceDeletion(ctx, job.ID); err != nil {
			t.Fatal(err)
		}
	}
	if got := readPending(s); len(got) != 0 {
		t.Fatal("completed deletion still pending", got)
	}
	// Reopen completed rows with new IDs; stale writes cannot affect the new generation.
	if err := s.QueueDeviceDeletion(ctx, "42", "test", map[string][]DeviceDeletionTarget{"panel-a": replacement, "panel-b": nil, "panel-c": nil}); err != nil {
		t.Fatal(err)
	}
	next := readPending(s)
	if len(next) != 3 || !reflect.DeepEqual(next["panel-a"].Targets, replacement) || next["panel-b"].Targets != nil || next["panel-c"].Targets != nil {
		t.Fatal("new generation did not replace old targets", next)
	}
	for _, old := range jobs {
		if next[old.Panel].ID == old.ID {
			t.Fatal("completed job ID reused")
		}
		if err := s.ResolveDeviceDeletionTargets(ctx, old.ID, replacement); err != nil {
			t.Fatal(err)
		}
		if err := s.CompleteDeviceDeletion(ctx, old.ID); err != nil {
			t.Fatal(err)
		}
	}
	if got := readPending(restarted); !reflect.DeepEqual(got, next) {
		t.Fatal("stale resolution or acknowledgement affected newer generation", got)
	}
	if err := s.ResolveDeviceDeletionTargets(ctx, next["panel-b"].ID, []DeviceDeletionTarget{}); err != nil {
		t.Fatal(err)
	}
	if got := readPending(s)["panel-b"].Targets; got == nil || len(got) != 0 {
		t.Fatal("confirmed empty observation remained unresolved", got)
	}
	newJobs, err := restarted.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(newJobs) != 3 {
		t.Fatal("new generations not immediately claimable", err, newJobs)
	}
	// A new observed native identity supersedes a still-leased pending deletion.
	beforeSupersede := readPending(s)
	newTargets := map[string][]DeviceDeletionTarget{"panel-a": observed, "panel-b": replacement, "panel-c": resolved}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", newTargets); err != nil {
		t.Fatal(err)
	}
	superseded := readPending(restarted)
	for panel, targets := range newTargets {
		job := superseded[panel]
		if job.ID == beforeSupersede[panel].ID || !reflect.DeepEqual(job.Targets, targets) {
			t.Fatal("fresh observation did not supersede pending generation", panel, job)
		}
		if err := s.CompleteDeviceDeletion(ctx, beforeSupersede[panel].ID); err != nil {
			t.Fatal(err)
		}
		if err := s.ResolveDeviceDeletionTargets(ctx, beforeSupersede[panel].ID, replacement); err != nil {
			t.Fatal(err)
		}
	}
	if got := readPending(s); !reflect.DeepEqual(got, superseded) {
		t.Fatal("stale worker affected superseding generation", got)
	}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", map[string][]DeviceDeletionTarget{"panel-a": nil, "panel-b": nil, "panel-c": nil}); err != nil {
		t.Fatal(err)
	}
	if got := readPending(s); !reflect.DeepEqual(got, superseded) {
		t.Fatal("unresolved duplicate replaced observed generation", got)
	}
	newJobs, err = s.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(newJobs) != 3 {
		t.Fatal("superseding generation retained old lease", err, newJobs)
	}
	if err := s.QueueDeviceDeletion(ctx, "42", "test", newTargets); err != nil {
		t.Fatal(err)
	}
	if got := readPending(s); !reflect.DeepEqual(got, superseded) {
		t.Fatal("identical observation replaced pending generation", got)
	}
	other, err = s.ClaimDeviceDeletions(ctx, 4)
	if err != nil || len(other) != 0 {
		t.Fatal("identical observation reset pending lease", err, other)
	}
	t.Run("panel claims isolate destinations", func(t *testing.T) {
		if _, err := s.ClaimPanelDeviceDeletions(ctx, ""); err == nil {
			t.Fatal("empty panel accepted")
		}
		if _, err := s.pool.Exec(ctx, `UPDATE device_deletions SET retry_at=TIMESTAMPTZ '1970-01-01' WHERE panel IN ('panel-a','panel-b')`); err != nil {
			t.Fatal(err)
		}
		// A second user supplies another due B job to check the one-job bound.
		if err := s.QueueDeviceDeletion(ctx, "43", "panel-claim", map[string][]DeviceDeletionTarget{"panel-b": {}}); err != nil {
			t.Fatal(err)
		}
		claim := func(panel, wantID string) {
			t.Helper()
			jobs, err := restarted.ClaimPanelDeviceDeletions(ctx, panel)
			if err != nil || len(jobs) != 1 || jobs[0].Panel != panel || jobs[0].ID != wantID {
				t.Fatal("wrong panel claim", panel, err, jobs)
			}
		}
		claim("panel-b", superseded["panel-b"].ID)
		// B's first claim must not lease the older pending A job.
		claim("panel-a", superseded["panel-a"].ID)
		foreign, err := s.PendingDeviceDeletions(ctx, "43")
		if err != nil || len(foreign) != 1 {
			t.Fatal("missing second B job", err, foreign)
		}
		// A is now leased; B still obtains its next job independently.
		claim("panel-b", foreign[0].ID)
		for _, panel := range []string{"panel-a", "panel-b", "panel-c", "missing-panel"} {
			jobs, err := s.ClaimPanelDeviceDeletions(ctx, panel)
			if err != nil || len(jobs) != 0 {
				t.Fatal("panel claim escaped its scope or lease", panel, err, jobs)
			}
		}
		jobs, err := s.ClaimDeviceDeletions(ctx, 4)
		if err != nil || len(jobs) != 0 {
			t.Fatal("generic claim ignored panel worker leases", err, jobs)
		}
	})
}
