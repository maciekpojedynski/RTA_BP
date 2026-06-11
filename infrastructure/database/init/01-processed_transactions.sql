CREATE TABLE IF NOT EXISTS processed_transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    city TEXT NOT NULL,
    is_fraud BOOLEAN NOT NULL,
    risk_score INT NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    requires_manual_review BOOLEAN NOT NULL,
    processor_version TEXT NOT NULL,
    raw_payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_processed_transactions_user_id
    ON processed_transactions (user_id);

CREATE INDEX IF NOT EXISTS idx_processed_transactions_timestamp
    ON processed_transactions (timestamp);

CREATE INDEX IF NOT EXISTS idx_processed_transactions_manual_review
    ON processed_transactions (requires_manual_review, risk_level);