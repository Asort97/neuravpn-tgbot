package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type DeviceRevocation struct {
	ID        string
	DeviceKey string
	Name      string
	RevokedAt time.Time
}

func (s *Store) DeviceRevoked(ctx context.Context, userID, key string) (bool, error) {
	var revoked bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM device_revocations WHERE user_id=$1 AND device_key IN ($2,left($2,17)))`, userID, key).Scan(&revoked)
	return revoked, err
}

func (s *Store) RevokedDevices(ctx context.Context, userID string) ([]DeviceRevocation, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,device_key,name,revoked_at FROM device_revocations WHERE user_id=$1 ORDER BY revoked_at DESC,id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DeviceRevocation, 0)
	for rows.Next() {
		var device DeviceRevocation
		if err := rows.Scan(&device.ID, &device.DeviceKey, &device.Name, &device.RevokedAt); err != nil {
			return nil, err
		}
		out = append(out, device)
	}
	return out, rows.Err()
}

// The generation ID prevents a stale unblock button from revoking a newer ban.
// Cancel unresolved panel work before allowing registration again.
func (s *Store) RestoreDevice(ctx context.Context, userID, id string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 94012))`, userID); err != nil {
		return false, err
	}
	var key string
	err = tx.QueryRow(ctx, `DELETE FROM device_revocations WHERE user_id=$1 AND id=$2 RETURNING device_key`, userID, id).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE device_deletions SET completed_at=NOW() WHERE user_id=$1 AND device_key=$2 AND completed_at IS NULL`, userID, key)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) DeviceDeletionPending(ctx context.Context, id string) (bool, error) {
	var pending bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM device_deletions WHERE id=$1 AND completed_at IS NULL)`, id).Scan(&pending)
	return pending, err
}
