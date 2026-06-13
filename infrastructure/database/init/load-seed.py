import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values, Json


BASE_DIR = Path(__file__).resolve().parent
SEED_FILE = BASE_DIR.parent / "init" / "seed-data.json"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "rta")
DB_USER = os.getenv("DB_USER", "rta_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rta_password")


def load_seed_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def prepare_processed_transactions(rows: list[dict]) -> list[tuple]:
    prepared = []
    for row in rows:
        raw_payload = row.get("raw_payload")
        prepared.append((
            row["transaction_id"],
            row["user_id"],
            row["timestamp"],
            row["processed_at"],
            row["amount"],
            row["currency"],
            row["merchant_category"],
            row["city"],
            row["is_fraud"],
            row["risk_score"],
            # Dodane z wersji 3.0 ML: pobieramy bezpiecznie przez .get()
            row.get("risk_score_rules"),
            row.get("risk_score_ml"),
            row["risk_level"],
            row["requires_manual_review"],
            row["processor_version"],
            Json(raw_payload) if raw_payload is not None else None,
        ))
    return prepared


def prepare_fraud_alerts(rows: list[dict]) -> list[tuple]:
    prepared = []
    for row in rows:
        prepared.append((
            row["alert_id"],
            row["created_at"],
            row["transaction_id"],
            row["user_id"],
            row["amount"],
            row["city"],
            row["merchant_category"],
            row["risk_score"],
            # Dodane z wersji 3.0 ML: pobieramy bezpiecznie przez .get()
            row.get("risk_score_rules"),
            row.get("risk_score_ml"),
            row["risk_level"],
            # NAPRAWA BŁĘDU: Owinięcie listy w adapter Json()
            Json(row["risk_flags"]) if row.get("risk_flags") is not None else Json([]),
            row["recommended_action"],
        ))
    return prepared


def prepare_transaction_window_stats(rows: list[dict]) -> list[tuple]:
    prepared = []
    for row in rows:
        prepared.append((
            row["window_seconds"],
            row["window_end"],
            row["transaction_count"],
            row["total_amount"],
            row["avg_amount"],
            row["alerts_count"],
            row["simulated_frauds_count"],
            row["alert_rate"],
            Json(row["by_city"]),
            Json(row["by_category"]),
        ))
    return prepared


def main():
    seed = load_seed_file(SEED_FILE)

    processed_rows = seed.get("processed_transactions", [])
    fraud_alerts_rows = seed.get("fraud_alerts", [])
    stats_rows = seed.get("transaction_window_stats", [])

    if not processed_rows and not fraud_alerts_rows and not stats_rows:
        print("Brak danych w seed-data.json")
        return

    processed_values = prepare_processed_transactions(processed_rows)
    fraud_alerts_values = prepare_fraud_alerts(fraud_alerts_rows)
    stats_values = prepare_transaction_window_stats(stats_rows)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                if processed_values:
                    execute_values(
                        cur,
                        """
                        INSERT INTO processed_transactions (
                            transaction_id,
                            user_id,
                            timestamp,
                            processed_at,
                            amount,
                            currency,
                            merchant_category,
                            city,
                            is_fraud,
                            risk_score,
                            risk_score_rules,
                            risk_score_ml,
                            risk_level,
                            requires_manual_review,
                            processor_version,
                            raw_payload
                        ) VALUES %s
                        ON CONFLICT (transaction_id) DO NOTHING
                        """,
                        processed_values
                    )
                    print(f"Załadowano {len(processed_values)} rekordów do processed_transactions")

                if fraud_alerts_values:
                    execute_values(
                        cur,
                        """
                        INSERT INTO fraud_alerts (
                            alert_id,
                            created_at,
                            transaction_id,
                            user_id,
                            amount,
                            city,
                            merchant_category,
                            risk_score,
                            risk_score_rules,
                            risk_score_ml,
                            risk_level,
                            risk_flags,
                            recommended_action
                        ) VALUES %s
                        ON CONFLICT (alert_id) DO NOTHING
                        """,
                        fraud_alerts_values
                    )
                    print(f"Załadowano {len(fraud_alerts_values)} rekordów do fraud_alerts")

                if stats_values:
                    execute_values(
                        cur,
                        """
                        INSERT INTO transaction_window_stats (
                            window_seconds,
                            window_end,
                            transaction_count,
                            total_amount,
                            avg_amount,
                            alerts_count,
                            simulated_frauds_count,
                            alert_rate,
                            by_city,
                            by_category
                        ) VALUES %s
                        """,
                        stats_values
                    )
                    print(f"Załadowano {len(stats_values)} rekordów do transaction_window_stats")

    finally:
        conn.close()


if __name__ == "__main__":
    main()