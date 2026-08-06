CREATE TABLE IF NOT EXISTS compensation_campaigns (
    id TEXT PRIMARY KEY,
    days BIGINT NOT NULL CHECK (days > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    eligible_before TIMESTAMPTZ NOT NULL,
    source_chat_id BIGINT,
    source_message_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS compensation_claims (
    campaign_id TEXT NOT NULL REFERENCES compensation_campaigns(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced_at TIMESTAMPTZ,
    sync_error TEXT,
    PRIMARY KEY (campaign_id, user_id)
);

CREATE TABLE IF NOT EXISTS compensation_deliveries (
    campaign_id TEXT NOT NULL REFERENCES compensation_campaigns(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (campaign_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_compensation_campaigns_expires_at
    ON compensation_campaigns(expires_at);
