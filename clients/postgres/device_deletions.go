package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type DeviceDeletion struct {
	ID          string
	UserID      string
	DeviceKey   string
	Panel       string
	RequestedAt time.Time
}

func (s *Store) initDeviceDeletions(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS device_deletions (
 id TEXT PRIMARY KEY,
 user_id TEXT NOT NULL REFERENCES users(id),
 device_key TEXT NOT NULL,
 panel TEXT NOT NULL,
 requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 completed_at TIMESTAMPTZ,
 UNIQUE(user_id,device_key,panel)
);
CREATE INDEX IF NOT EXISTS device_deletions_pending ON device_deletions(retry_at) WHERE completed_at IS NULL;`)
	return err
}

// Commit every destination before acknowledging a deletion. Retries survive
// process restarts and do not depend on the browser or Telegram request lifetime.
func (s *Store) QueueDeviceDeletion(ctx context.Context, userID, key string, panels []string) error {
	if userID == "" || key == "" || len(panels) == 0 {
		return errors.New("invalid device deletion")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, panel := range panels {
		if panel == "" {
			return errors.New("empty deletion panel")
		}
		_, err = tx.Exec(ctx, `INSERT INTO device_deletions(id,user_id,device_key,panel) VALUES($1,$2,$3,$4)
ON CONFLICT(user_id,device_key,panel) DO UPDATE SET id=EXCLUDED.id,requested_at=NOW(),retry_at=NOW(),completed_at=NULL
WHERE device_deletions.completed_at IS NOT NULL`, uuid.NewString(), userID, key, panel)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) PendingDeviceDeletions(ctx context.Context, userID string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT device_key FROM device_deletions WHERE user_id=$1 AND completed_at IS NULL`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make(map[string]bool)
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		keys[key] = true
	}
	return keys, rows.Err()
}

func (s *Store) ClaimDeviceDeletions(ctx context.Context, limit int) ([]DeviceDeletion, error) {
	if limit < 1 || limit > 32 {
		return nil, errors.New("invalid deletion batch")
	}
	rows, err := s.pool.Query(ctx, `WITH batch AS (
 SELECT id FROM device_deletions WHERE completed_at IS NULL AND retry_at<=NOW()
 ORDER BY retry_at,id LIMIT $1 FOR UPDATE SKIP LOCKED
) UPDATE device_deletions d SET retry_at=NOW()+INTERVAL '30 seconds' FROM batch
WHERE d.id=batch.id RETURNING d.id,d.user_id,d.device_key,d.panel,d.requested_at`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []DeviceDeletion
	for rows.Next() {
		var job DeviceDeletion
		if err = rows.Scan(&job.ID, &job.UserID, &job.DeviceKey, &job.Panel, &job.RequestedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) CompleteDeviceDeletion(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE device_deletions SET completed_at=NOW() WHERE id=$1 AND completed_at IS NULL`, id)
	return err
}
