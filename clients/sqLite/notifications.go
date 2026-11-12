package sqlite

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Asort97/vpnBot/clients/lifecycle"
)

func (s *Store) readNotificationUsersLocked() (map[string]UserData, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err
	}
	var users map[string]UserData
	if err = json.Unmarshal(b, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func notificationUser(id string, u UserData) lifecycle.User {
	created, _ := time.Parse(time.RFC3339, u.CreatedAt)
	bonus, _ := time.Parse(time.RFC3339, u.StartBonusClaimedAt)
	x := lifecycle.User{ID: id, CreatedAt: created, Bonus: u.StartBonusClaimed, BonusAt: bonus, Referred: u.ReferredBy != "" || u.ReferralUsed, Paid: len(u.AppliedPayments) > 0, Autopay: u.AutopayEnabled}
	for _, p := range u.AppliedPayments {
		at, _ := time.Parse(time.RFC3339, p.AppliedAt)
		if at.After(x.LastPaidAt) {
			x.LastPaidAt = at
		}
	}
	return x
}
func (s *Store) NotificationUsers() ([]lifecycle.User, error) {
	dbMu.Lock()
	defer dbMu.Unlock()
	users, err := s.readNotificationUsersLocked()
	if err != nil {
		return nil, err
	}
	var out []lifecycle.User
	for id, u := range users {
		if n, e := strconv.ParseInt(id, 10, 64); e == nil && n > 0 {
			out = append(out, notificationUser(id, u))
		}
	}
	return out, nil
}
func (s *Store) NotificationUser(id string) (lifecycle.User, error) {
	dbMu.Lock()
	defer dbMu.Unlock()
	users, err := s.readNotificationUsersLocked()
	if err != nil {
		return lifecycle.User{}, err
	}
	u, ok := users[id]
	if !ok {
		return lifecycle.User{}, fmt.Errorf("notification user missing")
	}
	return notificationUser(id, u), nil
}
