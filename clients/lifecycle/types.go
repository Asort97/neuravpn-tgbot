package lifecycle

import "time"

// User is a read-only eligibility snapshot, not a source of subscription expiry.
type User struct {
	ID         string
	CreatedAt  time.Time
	Bonus      bool
	BonusAt    time.Time
	Referred   bool
	Paid       bool
	LastPaidAt time.Time
	Autopay    bool
}

type Store interface {
	NotificationUsers() ([]User, error)
	NotificationUser(string) (User, error)
}
