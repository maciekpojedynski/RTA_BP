CREATE TABLE IF NOT EXISTS fraud_alerts (
    id BIGSERIAL PRIMARY KEY,
    alert_id TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    transaction_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    city TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    risk_score INT NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    risk_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    recommended_action TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_user_id
    ON fraud_alerts (user_id);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_transaction_id
    ON fraud_alerts (transaction_id);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at
    ON fraud_alerts (created_at);