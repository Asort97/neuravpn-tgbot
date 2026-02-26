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
	start_bonus_claimed BOOLEAN NOT NULL DEFAULT FALSE,
	start_bonus_source TEXT,
	start_bonus_claimed_at TIMESTAMPTZ,
    consent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
