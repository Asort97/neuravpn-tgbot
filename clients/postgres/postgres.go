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
			ADD COLUMN IF NOT EXISTS referrer_reward_given BOOLEAN NOT NULL DEFAULT FALSE;
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
