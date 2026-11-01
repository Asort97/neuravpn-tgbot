package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
)

type DeviceDeletionTarget struct {
	ID        int    `json:"id"`
	SubID     string `json:"sub_id"`
	FirstSeen int64  `json:"first_seen"`
}

type DeviceDeletion struct {
	ID          string
	UserID      string
	DeviceKey   string
	Panel       string
	RequestedAt time.Time
	Targets     []DeviceDeletionTarget
}

func encodeDeviceDeletionTargets(targets []DeviceDeletionTarget) ([]byte, error) {
	if targets != nil {
		// Native list order may change with activity without changing identities.
		targets = append([]DeviceDeletionTarget{}, targets...)
		sort.Slice(targets, func(i, j int) bool {
			if targets[i].SubID != targets[j].SubID {
				return targets[i].SubID < targets[j].SubID
			}
			if targets[i].ID != targets[j].ID {
				return targets[i].ID < targets[j].ID
			}
			return targets[i].FirstSeen < targets[j].FirstSeen
		})
	}
	return json.Marshal(targets)
}

func (s *Store) initDeviceDeletions(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS device_deletions (
 id TEXT PRIMARY KEY,
 user_id TEXT NOT NULL REFERENCES users(id),
 device_key TEXT NOT NULL,
 panel TEXT NOT NULL,
 targets JSONB NOT NULL DEFAULT 'null'::jsonb,
 requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 completed_at TIMESTAMPTZ,
 UNIQUE(user_id,device_key,panel)
);
ALTER TABLE device_deletions ADD COLUMN IF NOT EXISTS targets JSONB NOT NULL DEFAULT 'null'::jsonb;
CREATE INDEX IF NOT EXISTS device_deletions_pending ON device_deletions(retry_at) WHERE completed_at IS NULL;`)
	return err
}

// Commit every destination before acknowledging a deletion. Retries survive
// process restarts and do not depend on the browser or Telegram request lifetime.
func (s *Store) QueueDeviceDeletion(ctx context.Context, userID, key string, panels map[string][]DeviceDeletionTarget) error {
	if userID == "" || key == "" || len(panels) == 0 {
		return errors.New("invalid device deletion")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Every enqueuer takes conflicting rows in the same order.
	names := make([]string, 0, len(panels))
	for panel := range panels {
		names = append(names, panel)
	}
	sort.Strings(names)
	for _, panel := range names {
		if panel == "" {
			return errors.New("empty deletion panel")
		}
		targets, err := encodeDeviceDeletionTargets(panels[panel])
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO device_deletions(id,user_id,device_key,panel,targets) VALUES($1,$2,$3,$4,$5::jsonb)
ON CONFLICT(user_id,device_key,panel) DO UPDATE SET id=EXCLUDED.id,targets=EXCLUDED.targets,requested_at=NOW(),retry_at=NOW(),completed_at=NULL
WHERE device_deletions.completed_at IS NOT NULL
 OR (EXCLUDED.targets <> 'null'::jsonb AND device_deletions.targets <> EXCLUDED.targets)`, uuid.NewString(), userID, key, panel, targets)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) PendingDeviceDeletions(ctx context.Context, userID string) ([]DeviceDeletion, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,user_id,device_key,panel,requested_at,targets FROM device_deletions WHERE user_id=$1 AND completed_at IS NULL ORDER BY panel,device_key`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []DeviceDeletion
	for rows.Next() {
		var job DeviceDeletion
		if err = rows.Scan(&job.ID, &job.UserID, &job.DeviceKey, &job.Panel, &job.RequestedAt, &job.Targets); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ClaimDeviceDeletions(ctx context.Context, limit int) ([]DeviceDeletion, error) {
	if limit < 1 || limit > 32 {
		return nil, errors.New("invalid deletion batch")
	}
	return s.claimDeviceDeletions(ctx, limit, "")
}

func (s *Store) ClaimPanelDeviceDeletions(ctx context.Context, panel string) ([]DeviceDeletion, error) {
	if panel == "" {
		return nil, errors.New("empty deletion panel")
	}
	return s.claimDeviceDeletions(ctx, 1, panel)
}

func (s *Store) claimDeviceDeletions(ctx context.Context, limit int, panel string) ([]DeviceDeletion, error) {
	rows, err := s.pool.Query(ctx, `WITH batch AS (
 SELECT id FROM device_deletions WHERE completed_at IS NULL AND retry_at<=NOW() AND ($2::text='' OR panel=$2)
 ORDER BY retry_at,id LIMIT $1 FOR UPDATE SKIP LOCKED
) UPDATE device_deletions d SET retry_at=NOW()+INTERVAL '30 seconds' FROM batch
WHERE d.id=batch.id RETURNING d.id,d.user_id,d.device_key,d.panel,d.requested_at,d.targets`, limit, panel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []DeviceDeletion
	for rows.Next() {
		var job DeviceDeletion
		if err = rows.Scan(&job.ID, &job.UserID, &job.DeviceKey, &job.Panel, &job.RequestedAt, &job.Targets); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// Freeze the first successful panel observation for this job generation.
func (s *Store) ResolveDeviceDeletionTargets(ctx context.Context, id string, targets []DeviceDeletionTarget) error {
	if id == "" || targets == nil {
		return errors.New("invalid device deletion resolution")
	}
	data, err := encodeDeviceDeletionTargets(targets)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `UPDATE device_deletions SET targets=$2::jsonb WHERE id=$1 AND completed_at IS NULL AND targets='null'::jsonb`, id, data)
	return err
}

func (s *Store) CompleteDeviceDeletion(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE device_deletions SET completed_at=NOW() WHERE id=$1 AND completed_at IS NULL`, id)
	return err
}
