"""Fraud consumer: the hot path.

Reads from `transactions`, pulls per-card velocity features from Redis,
scores each transaction, and publishes flagged ones to `alerts`. Failures
go to a dead-letter topic so a single bad message never stalls the stream.

The scoring is deliberately simple (rules + a tiny weighted score). The
engineering interest is the pipeline, latency budget, and fault handling —
not the model. Swap in a trained model later (Week 5+) without touching the
rest of the system; that's the point of keeping it isolated in `score()`.
"""
import os
import time

import redis
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

from common.schema import (
    Transaction, Alert,
    TOPIC_TRANSACTIONS, TOPIC_ALERTS, TOPIC_DLQ,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "0.6"))
VELOCITY_WINDOW = int(os.getenv("VELOCITY_WINDOW", "60"))  # seconds

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# --- metrics (scraped by Prometheus) ---
PROCESSED = Counter("upishield_txns_processed_total", "Transactions scored")
FLAGGED = Counter("upishield_txns_flagged_total", "Transactions flagged as fraud")
FAILED = Counter("upishield_txns_failed_total", "Transactions sent to DLQ")
LATENCY = Histogram(
    "upishield_score_latency_seconds", "End-to-end scoring latency",
    buckets=(.001, .002, .005, .01, .025, .05, .1, .25, .5, 1),
)


def velocity(card_id: str) -> int:
    """Count this card's transactions in the recent window using a Redis
    sorted set keyed by timestamp. O(log n) add + range — fast enough for
    the hot path, which is why this lives in Redis and not Postgres."""
    now = time.time()
    key = f"vel:{card_id}"
    pipe = r.pipeline()
    pipe.zadd(key, {str(now): now})
    pipe.zremrangebyscore(key, 0, now - VELOCITY_WINDOW)
    pipe.zcard(key)
    pipe.expire(key, VELOCITY_WINDOW * 2)
    _, _, count, _ = pipe.execute()
    return int(count)


def score(txn: Transaction) -> tuple[float, list]:
    """Return (score in [0,1], list of human-readable reasons)."""
    reasons = []
    s = 0.0
    if txn.amount > 1500:
        s += 0.4
        reasons.append("high_amount")
    if txn.country in {"NG", "RU"}:
        s += 0.3
        reasons.append("risky_country")
    if txn.merchant in {"casino", "wire_transfer"}:
        s += 0.2
        reasons.append("risky_merchant")
    v = velocity(txn.card_id)
    if v > 8:
        s += 0.3
        reasons.append(f"velocity_{v}")
    return min(s, 1.0), reasons


def main():
    start_http_server(8001)  # /metrics for Prometheus
    consumer = KafkaConsumer(
        TOPIC_TRANSACTIONS,
        bootstrap_servers=BOOTSTRAP,
        group_id="fraud-scorer",
        enable_auto_commit=False,        # commit only after successful work
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b.decode("utf-8"),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    print("[fraud] scoring started; metrics on :8001", flush=True)

    for msg in consumer:
        try:
            with LATENCY.time():
                txn = Transaction.from_json(msg.value)
                s, reasons = score(txn)
                if s >= SCORE_THRESHOLD:
                    alert = Alert(
                        txn_id=txn.txn_id, card_id=txn.card_id,
                        amount=txn.amount, score=round(s, 3),
                        reasons=reasons, ts=time.time(),
                    )
                    producer.send(TOPIC_ALERTS, value=alert.to_json())
                    FLAGGED.inc()
            PROCESSED.inc()
            consumer.commit()  # offset advances only on success -> no loss on crash
        except Exception as e:  # noqa: BLE001 - route bad messages, don't die
            FAILED.inc()
            producer.send(TOPIC_DLQ, value=msg.value)
            print(f"[fraud] DLQ: {e}", flush=True)
            consumer.commit()


if __name__ == "__main__":
    main()
