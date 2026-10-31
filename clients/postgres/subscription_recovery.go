package postgres

import "context"

func (s *Store) CompareAndSwapSubscriptionID(ctx context.Context, userID, oldID, newID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE users SET subscription_id=$3,updated_at=NOW()
WHERE id=$1 AND COALESCE(subscription_id,'')=$2`, userID, oldID, newID)
	return err
}
