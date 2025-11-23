package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type legacyUser struct {
	Days           int64  `json:"days"`
	CertRef        string `json:"certref"`
	LastDeduct     string `json:"last_deduct"`
	ReferredBy     string `json:"referred_by"`
	ReferralUsed   bool   `json:"referral_used"`
	ReferralsCount int    `json:"referrals_count"`
	Email          string `json:"email"`
	ConsentAt      string `json:"consent_at"`
}

func main() {
	jsonPath := flag.String("json", "database/data.json", "Path to legacy JSON storage")
	defaultDSN := os.Getenv("DB_DSN")
	flagDSN := flag.String("dsn", defaultDSN, "PostgreSQL connection string (can also use DB_DSN env)")
	flag.Parse()

	if strings.TrimSpace(*flagDSN) == "" {
		log.Fatal("PostgreSQL DSN is required. Set DB_DSN or pass -dsn.")
	}

	raw, err := os.ReadFile(*jsonPath)
	if err != nil {
		log.Fatalf("read legacy json: %v", err)
	}

	legacyData := make(map[string]legacyUser)
	if err := json.Unmarshal(raw, &legacyData); err != nil {
		log.Fatalf("parse legacy json: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *flagDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := initSchema(ctx, pool); err != nil {
		log.Fatalf("init schema: %v", err)
	}

	total := len(legacyData)
	migrated := 0
	skipped := 0

	for userID, rec := range legacyData {
		if strings.TrimSpace(userID) == "" {
			skipped++
			continue
		}

		lastDeduct := parseTime(rec.LastDeduct)
		consentAt := parseTime(rec.ConsentAt)

		_, err := pool.Exec(ctx, `
            INSERT INTO users (id, days, last_deduct, referred_by, referral_used, referrals_count, email, consent_at, updated_at)
            VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, NULLIF($7, ''), $8, NOW())
            ON CONFLICT (id) DO UPDATE SET
                days = EXCLUDED.days,
                last_deduct = COALESCE(EXCLUDED.last_deduct, users.last_deduct),
                referred_by = EXCLUDED.referred_by,
                referral_used = EXCLUDED.referral_used,
                referrals_count = EXCLUDED.referrals_count,
                email = EXCLUDED.email,
                consent_at = COALESCE(EXCLUDED.consent_at, users.consent_at),
                updated_at = NOW()
        `, userID, rec.Days, lastDeduct, rec.ReferredBy, rec.ReferralUsed, rec.ReferralsCount, rec.Email, consentAt)
		if err != nil {
			log.Printf("skip %s: %v", userID, err)
			skipped++
			continue
		}
		migrated++
	}

	fmt.Printf("Migration complete. Total: %d, migrated: %d, skipped: %d\n", total, migrated, skipped)
}

func parseTime(val string) interface{} {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return nil
	}
	return t
}

func initSchema(ctx context.Context, pool *pgxpool.Pool) error {
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
	_, err := pool.Exec(ctx, schema)
	return err
}
