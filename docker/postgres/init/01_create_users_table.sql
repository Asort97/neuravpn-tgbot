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
