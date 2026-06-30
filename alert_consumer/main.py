"""Alert consumer: reads `alerts` and persists them to Postgres.

Kept separate from the scorer on purpose: a slow database write must never
backpressure the hot scoring path. This is the decoupling story for
interviews — scoring throughput is independent of persistence latency.
"""
import os
import time

import psycopg2
from kafka import KafkaConsumer

from common.schema import Alert, TOPIC_ALERTS

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG_DSN = os.getenv(
    "PG_DSN",
    "dbname=upishield user=upishield password=upishield host=postgres port=5432",
)

DDL = """
CREATE TABLE IF NOT EXISTS alerts (
    id        BIGSERIAL PRIMARY KEY,
    txn_id    TEXT UNIQUE NOT NULL,
    card_id   TEXT NOT NULL,
    amount    DOUBLE PRECISION NOT NULL,
    score     DOUBLE PRECISION NOT NULL,
    reasons   TEXT NOT NULL,
    ts        DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alerts_card ON alerts(card_id);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at DESC);
"""


def connect_with_retry():
    for attempt in range(30):
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(DDL)
            return conn
        except Exception as e:  # noqa: BLE001
            print(f"[alert] waiting for postgres ({attempt}): {e}", flush=True)
            time.sleep(2)
    raise RuntimeError("postgres never became available")


def main():
    conn = connect_with_retry()
    consumer = KafkaConsumer(
        TOPIC_ALERTS,
        bootstrap_servers=BOOTSTRAP,
        group_id="alert-writer",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b.decode("utf-8"),
    )
    print("[alert] persisting alerts to postgres", flush=True)
    for msg in consumer:
        a = Alert.from_json(msg.value)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO alerts (txn_id, card_id, amount, score, reasons, ts)
                   VALUES (%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (txn_id) DO NOTHING""",
                (a.txn_id, a.card_id, a.amount, a.score, ",".join(a.reasons), a.ts),
            )


if __name__ == "__main__":
    main()
