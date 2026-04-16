package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store implements the DataStore interface using PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Postgres-backed store.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	s := &Store{pool: pool}
	if err := s.initSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    days BIGINT NOT NULL DEFAULT 0,
    last_deduct TIMESTAMPTZ,
    referred_by TEXT,
    referral_used BOOLEAN NOT NULL DEFAULT FALSE,
    referrals_count INT NOT NULL DEFAULT 0,
	referral_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
	referral_confirmed_at TIMESTAMPTZ,
	referrer_reward_given BOOLEAN NOT NULL DEFAULT FALSE,
    email TEXT,
	subscription_id TEXT,
	start_bonus_claimed BOOLEAN NOT NULL DEFAULT FALSE,
	start_bonus_source TEXT,
	start_bonus_claimed_at TIMESTAMPTZ,
    consent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return err
	}
	// Ensure new columns exist for already-created tables
	_, err := s.pool.Exec(ctx, `
		ALTER TABLE users
			ADD COLUMN IF NOT EXISTS start_bonus_claimed BOOLEAN NOT NULL DEFAULT FALSE,
			ADD COLUMN IF NOT EXISTS start_bonus_source TEXT,
			ADD COLUMN IF NOT EXISTS start_bonus_claimed_at TIMESTAMPTZ,
			ADD COLUMN IF NOT EXISTS referral_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
			ADD COLUMN IF NOT EXISTS referral_confirmed_at TIMESTAMPTZ,
			ADD COLUMN IF NOT EXISTS referrer_reward_given BOOLEAN NOT NULL DEFAULT FALSE,
			ADD COLUMN IF NOT EXISTS link_token TEXT,
			ADD COLUMN IF NOT EXISTS linked_to TEXT,
			ADD COLUMN IF NOT EXISTS autopay_method_id TEXT,
			ADD COLUMN IF NOT EXISTS autopay_plan_id TEXT,
			ADD COLUMN IF NOT EXISTS autopay_enabled BOOLEAN NOT NULL DEFAULT FALSE;
	`)
	if err != nil {
		return err
	}

	_, err = s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS applied_payments (
			payment_id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			provider TEXT NOT NULL,
			plan_id TEXT,
			amount_value NUMERIC(12,2) NOT NULL DEFAULT 0,
			currency TEXT NOT NULL DEFAULT 'RUB',
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_applied_payments_user_id ON applied_payments(user_id);
		CREATE INDEX IF NOT EXISTS idx_applied_payments_applied_at ON applied_payments(applied_at);
	`)
	if err != nil {
		return err
	}

	_, err = s.pool.Exec(ctx, `
		ALTER TABLE applied_payments
			ADD COLUMN IF NOT EXISTS amount_value NUMERIC(12,2) NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'RUB';
	`)
	return err
}

// Close releases the underlying pool.
func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) ensureUser(ctx context.Context, userID string) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO users (id, last_deduct, updated_at)
        VALUES ($1, NOW(), NOW())
        ON CONFLICT (id) DO NOTHING
    `, userID)
	return err
}

func (s *Store) AddDays(userID string, days int64) error {
	ctx := context.Background()
	if days == 0 {
		return s.ensureUser(ctx, userID)
	}
	_, err := s.pool.Exec(ctx, `
        INSERT INTO users (id, days, last_deduct, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            days = users.days + EXCLUDED.days,
            last_deduct = CASE WHEN users.days = 0 AND EXCLUDED.days > 0 THEN NOW() ELSE users.last_deduct END,
            updated_at = NOW()
    `, userID, days)
	return err
}

func (s *Store) GetDays(userID string) (int64, error) {
	ctx := context.Background()
	var days int64
	err := s.pool.QueryRow(ctx, `SELECT days FROM users WHERE id = $1`, userID).Scan(&days)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, fmt.Errorf("user %s not found", userID)
		}
		return 0, err
	}
	return days, nil
}

func (s *Store) SetDays(userID string, days int64) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
        INSERT INTO users (id, days, last_deduct, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            days = EXCLUDED.days,
            last_deduct = COALESCE(users.last_deduct, NOW()),
            updated_at = NOW()
    `, userID, days)
	return err
}

func (s *Store) SetEmail(userID, email string) error {
	ctx := context.Background()
	email = strings.TrimSpace(email)
	_, err := s.pool.Exec(ctx, `
        INSERT INTO users (id, email, last_deduct, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            email = EXCLUDED.email,
            updated_at = NOW()
    `, userID, email)
	return err
}

func (s *Store) GetEmail(userID string) (string, error) {
	ctx := context.Background()
	var email string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(email, '') FROM users WHERE id = $1`, userID).Scan(&email)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", fmt.Errorf("user %s not found", userID)
		}
		return "", err
	}
	return email, nil
}

// EnsureSubscriptionID returns existing subscription_id or creates a new UUIDv4 and stores it.
func (s *Store) EnsureSubscriptionID(userID string) (string, error) {
	ctx := context.Background()
	// Try get existing
	var subID string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(subscription_id, '') FROM users WHERE id = $1`, userID).Scan(&subID)
	if err != nil && err != pgx.ErrNoRows {
		return "", err
	}
	if strings.TrimSpace(subID) != "" {
		return subID, nil
	}
	// Create new
	newID := uuid.New().String()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO users (id, subscription_id, last_deduct, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (id) DO UPDATE SET
			subscription_id = EXCLUDED.subscription_id,
			updated_at = NOW()
	`, userID, newID)
	if err != nil {
		return "", err
	}
	return newID, nil
}

// GetSubscriptionID returns subscription_id or empty string if not set.
func (s *Store) GetSubscriptionID(userID string) (string, error) {
	ctx := context.Background()
	var subID string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(subscription_id, '') FROM users WHERE id = $1`, userID).Scan(&subID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", fmt.Errorf("user %s not found", userID)
		}
		return "", err
	}
	return subID, nil
}

func (s *Store) AcceptPrivacy(userID string, at time.Time) error {
	ctx := context.Background()
	if at.IsZero() {
		at = time.Now()
	}
	_, err := s.pool.Exec(ctx, `
        INSERT INTO users (id, consent_at, last_deduct, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            consent_at = EXCLUDED.consent_at,
            updated_at = NOW()
    `, userID, at.UTC())
	return err
}

func (s *Store) IsNewUser(userID string) bool {
	ctx := context.Background()
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT TRUE FROM users WHERE id = $1`, userID).Scan(&exists)
	if err != nil {
		return true
	}
	return false
}

func (s *Store) IsStartBonusClaimed(userID string) (bool, error) {
	ctx := context.Background()
	var claimed bool
	err := s.pool.QueryRow(ctx, `SELECT start_bonus_claimed FROM users WHERE id = $1`, userID).Scan(&claimed)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return claimed, nil
}

// ClaimStartBonus atomically marks start bonus as claimed for a user.
// Returns true if it was claimed now, false if already claimed.
func (s *Store) ClaimStartBonus(userID string, source string, at time.Time) (bool, error) {
	ctx := context.Background()
	if at.IsZero() {
		at = time.Now()
	}
	// Ensure user exists
	if err := s.ensureUser(ctx, userID); err != nil {
		return false, err
	}
	var updated int
	err := s.pool.QueryRow(ctx, `
		UPDATE users
		SET start_bonus_claimed = TRUE,
			start_bonus_source = $2,
			start_bonus_claimed_at = $3,
			updated_at = NOW()
		WHERE id = $1 AND start_bonus_claimed = FALSE
		RETURNING 1
	`, userID, strings.TrimSpace(source), at.UTC()).Scan(&updated)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) RecordReferral(newUserID, referrerID string) error {
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var referredBy string
	err = tx.QueryRow(ctx, `SELECT COALESCE(referred_by, '') FROM users WHERE id = $1`, newUserID).Scan(&referredBy)
	if err == pgx.ErrNoRows {
		if _, err := tx.Exec(ctx, `INSERT INTO users (id, last_deduct, updated_at) VALUES ($1, NOW(), NOW())`, newUserID); err != nil {
			return err
		}
		referredBy = ""
	} else if err != nil {
		return err
	}

	if referredBy != "" {
		return fmt.Errorf("user %s already used referral code", newUserID)
	}

	if _, err := tx.Exec(ctx, `UPDATE users SET referred_by = $2, referral_used = TRUE, updated_at = NOW() WHERE id = $1`, newUserID, referrerID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `INSERT INTO users (id, last_deduct, updated_at) VALUES ($1, NOW(), NOW()) ON CONFLICT (id) DO NOTHING`, referrerID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ConfirmReferralAndRewardReferrer confirms referral subscription for new user and rewards referrer once.
// Returns referrerID and whether reward was granted in this call.
func (s *Store) ConfirmReferralAndRewardReferrer(newUserID string, rewardDays int64, at time.Time) (string, bool, error) {
	ctx := context.Background()
	if at.IsZero() {
		at = time.Now()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)

	var referrerID string
	var referralConfirmed bool
	var rewardGiven bool
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(referred_by, ''),
		       referral_confirmed,
		       referrer_reward_given
		FROM users
		WHERE id = $1
		FOR UPDATE
	`, newUserID).Scan(&referrerID, &referralConfirmed, &rewardGiven)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	referrerID = strings.TrimSpace(referrerID)
	if referrerID == "" {
		if !referralConfirmed {
			if _, err := tx.Exec(ctx, `
				UPDATE users
				SET referral_confirmed = TRUE,
					referral_confirmed_at = $2,
					updated_at = NOW()
				WHERE id = $1
			`, newUserID, at.UTC()); err != nil {
				return "", false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return "", false, err
		}
		return "", false, nil
	}

	if !referralConfirmed {
		if _, err := tx.Exec(ctx, `
			UPDATE users
			SET referral_confirmed = TRUE,
				referral_confirmed_at = $2,
				updated_at = NOW()
			WHERE id = $1
		`, newUserID, at.UTC()); err != nil {
			return "", false, err
		}
	}

	if rewardGiven {
		if err := tx.Commit(ctx); err != nil {
			return referrerID, false, err
		}
		return referrerID, false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO users (id, last_deduct, updated_at)
		VALUES ($1, NOW(), NOW())
		ON CONFLICT (id) DO NOTHING
	`, referrerID); err != nil {
		return "", false, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE users
		SET referrals_count = referrals_count + 1,
			days = days + $2,
			last_deduct = CASE WHEN last_deduct IS NULL THEN NOW() ELSE last_deduct END,
			updated_at = NOW()
		WHERE id = $1
	`, referrerID, rewardDays); err != nil {
		return "", false, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE users
		SET referrer_reward_given = TRUE,
			updated_at = NOW()
		WHERE id = $1
	`, newUserID); err != nil {
		return "", false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}

	return referrerID, true, nil
}

func (s *Store) GetReferralsCount(userID string) int {
	ctx := context.Background()
	var count int
	err := s.pool.QueryRow(ctx, `SELECT referrals_count FROM users WHERE id = $1`, userID).Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

func (s *Store) IsPaymentApplied(userID, paymentID string) (bool, error) {
	ctx := context.Background()
	paymentID = strings.TrimSpace(paymentID)
	if paymentID == "" {
		return false, fmt.Errorf("paymentID is empty")
	}

	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM applied_payments
			WHERE payment_id = $1
		)
	`, paymentID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Store) MarkPaymentApplied(userID, paymentID, provider, planID string, amount float64, currency string, at time.Time) (bool, error) {
	ctx := context.Background()
	userID = strings.TrimSpace(userID)
	paymentID = strings.TrimSpace(paymentID)
	provider = strings.TrimSpace(provider)
	planID = strings.TrimSpace(planID)
	currency = strings.ToUpper(strings.TrimSpace(currency))

	if userID == "" {
		return false, fmt.Errorf("userID is empty")
	}
	if paymentID == "" {
		return false, fmt.Errorf("paymentID is empty")
	}
	if provider == "" {
		return false, fmt.Errorf("provider is empty")
	}
	if currency == "" {
		currency = "RUB"
	}
	if at.IsZero() {
		at = time.Now()
	}

	tag, err := s.pool.Exec(ctx, `
		INSERT INTO applied_payments (payment_id, user_id, provider, plan_id, amount_value, currency, applied_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (payment_id) DO NOTHING
	`, paymentID, userID, provider, planID, amount, currency, at.UTC())
	if err != nil {
		return false, err
	}

	return tag.RowsAffected() > 0, nil
}

func (s *Store) GetDailyStats(start, end time.Time) (int, int, float64, float64, error) {
	ctx := context.Background()
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return 0, 0, 0, 0, fmt.Errorf("invalid stats range")
	}

	var newUsers int
	var payingUsers int
	var rubTotal float64
	var starsTotal float64
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM users WHERE created_at >= $1 AND created_at < $2),
			(SELECT COUNT(DISTINCT user_id) FROM applied_payments WHERE applied_at >= $1 AND applied_at < $2),
			COALESCE((SELECT SUM(amount_value)::float8 FROM applied_payments WHERE applied_at >= $1 AND applied_at < $2 AND currency = 'RUB'), 0),
			COALESCE((SELECT SUM(amount_value)::float8 FROM applied_payments WHERE applied_at >= $1 AND applied_at < $2 AND currency = 'XTR'), 0)
	`, start.UTC(), end.UTC()).Scan(&newUsers, &payingUsers, &rubTotal, &starsTotal)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return newUsers, payingUsers, rubTotal, starsTotal, nil
}

func (s *Store) SetLinkToken(userID, token string) error {
	ctx := context.Background()
	_ = s.ensureUser(ctx, userID)
	_, err := s.pool.Exec(ctx, `UPDATE users SET link_token = $1, updated_at = NOW() WHERE id = $2`, token, userID)
	return err
}

func (s *Store) GetUserByLinkToken(token string) (string, error) {
	ctx := context.Background()
	var userID string
	err := s.pool.QueryRow(ctx, `SELECT id FROM users WHERE link_token = $1`, token).Scan(&userID)
	if err != nil {
		return "", fmt.Errorf("token not found: %w", err)
	}
	return userID, nil
}

func (s *Store) ClearLinkToken(userID string) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `UPDATE users SET link_token = NULL, updated_at = NOW() WHERE id = $1`, userID)
	return err
}

func (s *Store) SetLinkedTo(userID, linkedTo string) error {
	ctx := context.Background()
	_ = s.ensureUser(ctx, userID)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO users (id, linked_to, last_deduct, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (id) DO UPDATE SET
			linked_to = EXCLUDED.linked_to,
			updated_at = NOW()`, userID, linkedTo)
	return err
}

func (s *Store) GetLinkedTo(userID string) (string, error) {
	ctx := context.Background()
	var linked string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(linked_to, '') FROM users WHERE id = $1`, userID).Scan(&linked)
	if err != nil {
		return "", err
	}
	return linked, nil
}

func (s *Store) GetLinkedVKUsers(tgUserID string) ([]string, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `SELECT id FROM users WHERE linked_to = $1 AND id LIKE 'vk_%'`, tgUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// AutopayUser holds autopay info for a single user.
type AutopayUser = struct {
	UserID   string
	MethodID string
	PlanID   string
}

func (s *Store) SetAutopay(userID, methodID, planID string) error {
	ctx := context.Background()
	_ = s.ensureUser(ctx, userID)
	_, err := s.pool.Exec(ctx, `
		UPDATE users
		SET autopay_method_id = $2, autopay_plan_id = $3, autopay_enabled = TRUE, updated_at = NOW()
		WHERE id = $1
	`, userID, methodID, planID)
	return err
}

func (s *Store) DisableAutopay(userID string) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		UPDATE users SET autopay_enabled = FALSE, updated_at = NOW() WHERE id = $1
	`, userID)
	return err
}

func (s *Store) ClearAutopay(userID string) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		UPDATE users SET autopay_enabled = FALSE, autopay_method_id = NULL, autopay_plan_id = NULL, updated_at = NOW() WHERE id = $1
	`, userID)
	return err
}

func (s *Store) GetAutopay(userID string) (string, string, bool, error) {
	ctx := context.Background()
	var methodID, planID string
	var enabled bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(autopay_method_id, ''), COALESCE(autopay_plan_id, ''), autopay_enabled
		FROM users WHERE id = $1
	`, userID).Scan(&methodID, &planID, &enabled)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return methodID, planID, enabled, nil
}

func (s *Store) GetUsersWithAutopay() ([]AutopayUser, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `
		SELECT id, autopay_method_id, autopay_plan_id
		FROM users WHERE autopay_enabled = TRUE AND autopay_method_id IS NOT NULL
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []AutopayUser
	for rows.Next() {
		var u AutopayUser
		if err := rows.Scan(&u.UserID, &u.MethodID, &u.PlanID); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *Store) GetSleepingUsers(since time.Time) ([]string, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM users
		WHERE days = 0 AND updated_at < $1
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// GetAllUserIDs возвращает список всех user id из таблицы users
func (s *Store) GetAllUserIDs() ([]string, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `SELECT id FROM users`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}
