package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

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
    email TEXT,
    consent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`
	_, err := s.pool.Exec(ctx, schema)
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

	if _, err := tx.Exec(ctx, `UPDATE users SET referrals_count = referrals_count + 1, updated_at = NOW() WHERE id = $1`, referrerID); err != nil {
		return err
	}

	return tx.Commit(ctx)
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
