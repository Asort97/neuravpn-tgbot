package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Asort97/vpnBot/clients/lifecycle"
)

func (s *Store) notificationUsers(id string) ([]lifecycle.User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT u.id,u.created_at,u.start_bonus_claimed,
	 COALESCE(u.start_bonus_claimed_at,'epoch'::timestamptz),
	 (COALESCE(u.referred_by,'')<>'' OR u.referral_used),
	 EXISTS(SELECT 1 FROM applied_payments p WHERE p.user_id=u.id),
	 COALESCE((SELECT MAX(p.applied_at) FROM applied_payments p WHERE p.user_id=u.id),'epoch'::timestamptz),
	 COALESCE(u.autopay_enabled,false)
	 FROM users u WHERE u.id ~ '^[0-9]+$' AND ($1='' OR u.id=$1) ORDER BY u.id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lifecycle.User
	for rows.Next() {
		var u lifecycle.User
		if err = rows.Scan(&u.ID, &u.CreatedAt, &u.Bonus, &u.BonusAt, &u.Referred, &u.Paid, &u.LastPaidAt, &u.Autopay); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
func (s *Store) NotificationUsers() ([]lifecycle.User, error) { return s.notificationUsers("") }
func (s *Store) NotificationUser(id string) (lifecycle.User, error) {
	u, e := s.notificationUsers(id)
	if e != nil {
		return lifecycle.User{}, e
	}
	if len(u) == 0 {
		return lifecycle.User{}, fmt.Errorf("notification user missing")
	}
	return u[0], nil
}
