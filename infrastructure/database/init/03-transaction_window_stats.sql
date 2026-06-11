CREATE TABLE IF NOT EXISTS transaction_window_stats (
    id BIGSERIAL PRIMARY KEY,
    window_seconds INT NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    transaction_count INT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    avg_amount NUMERIC(12,2) NOT NULL,
    alerts_count INT NOT NULL,
    simulated_frauds_count INT NOT NULL,
    alert_rate NUMERIC(8,4) NOT NULL,
    by_city JSONB NOT NULL DEFAULT '{}'::jsonb,
    by_category JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_transaction_window_stats_window_end
    ON transaction_window_stats (window_end);

CREATE INDEX IF NOT EXISTS idx_transaction_window_stats_alerts_count
    ON transaction_window_stats (alerts_count);