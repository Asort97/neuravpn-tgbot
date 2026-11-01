package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type SubscriptionReissue struct {
	UserID     string
	Generation string
	SubID      string
	Active     bool
	Panel      string
	Primary    bool
}

func (s *Store) initSubscriptionReissues(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS subscription_reissues (
 user_id TEXT PRIMARY KEY REFERENCES users(id), generation TEXT NOT NULL, sub_id TEXT NOT NULL,
 active BOOLEAN NOT NULL DEFAULT FALSE, requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
 );
 CREATE TABLE IF NOT EXISTS subscription_reissue_jobs (
 user_id TEXT NOT NULL REFERENCES subscription_reissues(user_id), panel TEXT NOT NULL,
 generation TEXT NOT NULL, is_primary BOOLEAN NOT NULL, retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 completed_at TIMESTAMPTZ, before_state JSONB,
 PRIMARY KEY(user_id,panel));`)
	return err
}

func (s *Store) GetSubscriptionReissue(ctx context.Context, userID string) (SubscriptionReissue, error) {
	r := SubscriptionReissue{UserID: userID}
	err := s.pool.QueryRow(ctx, `SELECT generation,sub_id,active FROM subscription_reissues WHERE user_id=$1`, userID).Scan(&r.Generation, &r.SubID, &r.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, nil
	}
	return r, err
}

// The expected generation makes a repeated confirmation idempotent.
func (s *Store) BeginSubscriptionReissue(ctx context.Context, userID, expected string, panels map[string]bool) (SubscriptionReissue, error) {
	r := SubscriptionReissue{UserID: userID}
	primary := 0
	for name, p := range panels {
		if name == "" {
			return r, errors.New("empty panel")
		}
		if p {
			primary++
		}
	}
	if primary != 1 {
		return r, errors.New("one primary panel required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, err
	}
	defer tx.Rollback(ctx)
	var id string
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&id); err != nil {
		return r, err
	}
	err = tx.QueryRow(ctx, `SELECT generation,sub_id,active FROM subscription_reissues WHERE user_id=$1`, userID).Scan(&r.Generation, &r.SubID, &r.Active)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return r, err
	}
	if r.Generation != expected || (r.Generation != "" && !r.Active) {
		return r, tx.Commit(ctx)
	}
	// A leased secondary must not block the primary; stale completion is rejected.
	var leased bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM subscription_reissue_jobs WHERE user_id=$1 AND is_primary AND completed_at IS NULL AND retry_at>NOW())`, userID).Scan(&leased); err != nil {
		return r, err
	}
	if leased {
		return r, errors.New("reissue is still processing")
	}
	r.Generation = uuid.NewString()
	r.SubID = uuid.NewString()
	r.Active = false
	_, err = tx.Exec(ctx, `INSERT INTO subscription_reissues(user_id,generation,sub_id) VALUES($1,$2,$3)
 ON CONFLICT(user_id) DO UPDATE SET generation=$2,sub_id=$3,active=FALSE,requested_at=NOW()`, userID, r.Generation, r.SubID)
	if err != nil {
		return r, err
	}
	for panel, p := range panels {
		_, err = tx.Exec(ctx, `INSERT INTO subscription_reissue_jobs(user_id,panel,generation,is_primary) VALUES($1,$2,$3,$4)
  ON CONFLICT(user_id,panel) DO UPDATE SET generation=$3,is_primary=$4,retry_at=NOW(),completed_at=NULL,before_state=NULL`, userID, panel, r.Generation, p)
		if err != nil {
			return r, err
		}
	}
	return r, tx.Commit(ctx)
}

func (s *Store) ClaimSubscriptionReissues(ctx context.Context, panel string) ([]SubscriptionReissue, error) {
	rows, err := s.pool.Query(ctx, `WITH pick AS (
 SELECT j.user_id,j.panel FROM subscription_reissue_jobs j JOIN subscription_reissues r USING(user_id)
 WHERE j.panel=$1 AND j.generation=r.generation AND j.completed_at IS NULL AND j.retry_at<=NOW()
 AND (j.is_primary OR r.active) ORDER BY j.retry_at LIMIT 1 FOR UPDATE OF j SKIP LOCKED
 ) UPDATE subscription_reissue_jobs j SET retry_at=NOW()+INTERVAL '45 seconds' FROM pick,subscription_reissues r
 WHERE j.user_id=pick.user_id AND j.panel=pick.panel AND r.user_id=j.user_id
 RETURNING j.user_id,j.generation,r.sub_id,r.active,j.panel,j.is_primary`, panel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SubscriptionReissue
	for rows.Next() {
		var r SubscriptionReissue
		if err = rows.Scan(&r.UserID, &r.Generation, &r.SubID, &r.Active, &r.Panel, &r.Primary); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) SaveReissueBeforeState(ctx context.Context, r SubscriptionReissue, data []byte) error {
	tag, err := s.pool.Exec(ctx, `UPDATE subscription_reissue_jobs SET before_state=COALESCE(before_state,$4::jsonb)
 WHERE user_id=$1 AND panel=$2 AND generation=$3 AND completed_at IS NULL`, r.UserID, r.Panel, r.Generation, data)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("stale reissue")
	}
	return err
}

func (s *Store) CompleteSubscriptionReissue(ctx context.Context, r SubscriptionReissue) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE subscription_reissue_jobs SET completed_at=NOW() WHERE user_id=$1 AND panel=$2 AND generation=$3 AND completed_at IS NULL`, r.UserID, r.Panel, r.Generation)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("stale reissue completion")
	}
	if r.Primary {
		tag, err = tx.Exec(ctx, `UPDATE subscription_reissues SET active=TRUE WHERE user_id=$1 AND generation=$2`, r.UserID, r.Generation)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errors.New("stale reissue generation")
		}
		_, err = tx.Exec(ctx, `UPDATE users SET subscription_id=$2,updated_at=NOW() WHERE id=$1`, r.UserID, r.SubID)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) SubscriptionReissuePanelReady(ctx context.Context, userID, panel string) (bool, error) {
	var ready bool
	err := s.pool.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM subscription_reissue_jobs j
 JOIN subscription_reissues r USING(user_id) WHERE j.user_id=$1 AND j.panel=$2 AND j.generation=r.generation AND j.completed_at IS NULL)`, userID, panel).Scan(&ready)
	return ready, err
}
