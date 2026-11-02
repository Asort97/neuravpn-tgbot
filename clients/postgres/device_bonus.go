package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
)

// A Telegram message is an idempotency key. Entire batches commit together;
// neither billing nor subscription expiry is touched by a free permanent grant.
func (s *Store) GrantDeviceBonus(ctx context.Context, requestKey, adminID string, ids []string, slots int) (bool, error) {
	if requestKey == "" || adminID == "" || len(ids) < 1 || len(ids) > 20 || slots < 1 || slots > 95 {
		return false, errors.New("invalid bonus grant")
	}
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	for i, id := range ids {
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n <= 0 || strconv.FormatInt(n, 10) != id || (i > 0 && ids[i-1] == id) {
			return false, errors.New("invalid user ID")
		}
	}
	data, _ := json.Marshal(ids)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO device_bonus_grants(request_key,admin_id,user_ids,slots) VALUES($1,$2,$3::jsonb,$4) ON CONFLICT DO NOTHING`, requestKey, adminID, data, slots)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		var same bool
		if err := tx.QueryRow(ctx, `SELECT admin_id=$2 AND user_ids=$3::jsonb AND slots=$4 FROM device_bonus_grants WHERE request_key=$1`, requestKey, adminID, data, slots).Scan(&same); err != nil {
			return false, err
		}
		if !same {
			return false, errors.New("grant request identity mismatch")
		}
		return false, nil
	}
	for _, id := range ids {
		_, err = tx.Exec(ctx, `INSERT INTO device_plans(user_id,enabled) VALUES($1,COALESCE((SELECT enabled FROM device_limit_policy WHERE singleton),FALSE)) ON CONFLICT DO NOTHING`, id)
		if err != nil {
			return false, err
		}
		tag, err = tx.Exec(ctx, `UPDATE device_plans SET bonus_slots=bonus_slots+$2,updated_at=NOW() WHERE user_id=$1 AND bonus_slots+$2<=95`, id, slots)
		if err != nil {
			return false, err
		}
		if tag.RowsAffected() != 1 {
			return false, errors.New("permanent bonus exceeds 95")
		}
	}
	return true, tx.Commit(ctx)
}
