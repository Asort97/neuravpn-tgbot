package sqlite

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNotificationSnapshotFreshAndFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	s := New(path)
	write := func(text string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"42":{"created_at":"2026-11-01T12:00:00Z","start_bonus_claimed":false}}`)
	u, err := s.NotificationUser("42")
	if err != nil || u.Bonus || u.Paid || u.CreatedAt.IsZero() {
		t.Fatal(u, err)
	}
	write(`{"42":{"created_at":"2026-11-01T12:00:00Z","start_bonus_claimed":true,"referred_by":"12","autopay_enabled":true,"applied_payments":{"paid":{"applied_at":"2026-11-02T12:00:00Z"}}}}`)
	u, err = s.NotificationUser("42")
	if err != nil || !u.Bonus || !u.Referred || !u.Paid || !u.Autopay || u.LastPaidAt.IsZero() {
		t.Fatal(u, err)
	}
	write(`broken`)
	if _, err = s.NotificationUser("42"); err == nil {
		t.Fatal("stale eligibility after failed read")
	}
	if _, err = s.NotificationUsers(); err == nil {
		t.Fatal("corrupt list accepted")
	}
}
