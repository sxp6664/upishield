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
from fraud_consumer import model_scorer
from common.schema import (
    Transaction, Alert,
    TOPIC_TRANSACTIONS, TOPIC_ALERTS, TOPIC_DLQ,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "0.6"))
VELOCITY_WINDOW = int(os.getenv("VELOCITY_WINDOW", "60"))  # seconds
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "50"))
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
    """Return (score in [0,1], list of human-readable reasons).

    Uses the trained model when available; falls back to the original rules
    if the model failed to load. Threshold 0.70 was chosen from the
    precision/recall sweep in ml/tune_threshold.py — at that point the model
    beats the rule baseline on BOTH precision (.423 vs .386) and recall
    (.469 vs .399).
    """
    v = velocity(txn.card_id)

    if model_scorer.is_available():
        s = model_scorer.model_score(txn.amount, txn.merchant, txn.country, v)
        reasons = ["model"]
        # Keep human-readable context on the alert for the analyst.
        if txn.amount > 1500:
            reasons.append("high_amount")
        if txn.country in {"NG", "RU"}:
            reasons.append("risky_country")
        if txn.merchant in {"casino", "wire_transfer"}:
            reasons.append("risky_merchant")
        if v > 8:
            reasons.append(f"velocity_{v}")
        return s, reasons

    # --- fallback: original rule scorer ---
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
    if v > 8:
        s += 0.3
        reasons.append(f"velocity_{v}")
    return min(s, 1.0), reasons

def build_reasons(txn: Transaction, v: int) -> list:
    """Human-readable context attached to each alert for the analyst."""
    reasons = ["model"]
    if txn.amount > 1500:
        reasons.append("high_amount")
    if txn.country in {"NG", "RU"}:
        reasons.append("risky_country")
    if txn.merchant in {"casino", "wire_transfer"}:
        reasons.append("risky_merchant")
    if v > 8:
        reasons.append(f"velocity_{v}")
    return reasons

def main():
    start_http_server(8001)
    consumer = KafkaConsumer(
        TOPIC_TRANSACTIONS,
        bootstrap_servers=BOOTSTRAP,
        group_id="fraud-scorer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b.decode("utf-8"),
        consumer_timeout_ms=BATCH_TIMEOUT_MS,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    print(f"[fraud] scoring started (batch={BATCH_SIZE}); metrics on :8001", flush=True)

    buffer = []          # (Transaction, velocity, feature_dict)
    last_flush = time.time()

    def flush():
        """Score the buffered batch in one predict_proba call, emit alerts,
        then commit. Offsets advance only after the whole batch succeeds —
        same no-loss guarantee as before, just at batch granularity."""
        nonlocal buffer, last_flush
        if not buffer:
            last_flush = time.time()
            return
        try:
            with LATENCY.time():
                feats = [f for _, _, f in buffer]
                scores = model_scorer.model_score_batch(feats)
                for (txn, v, _), s in zip(buffer, scores):
                    if s >= SCORE_THRESHOLD:
                        alert = Alert(
                            txn_id=txn.txn_id, card_id=txn.card_id,
                            amount=txn.amount, score=round(s, 3),
                            reasons=build_reasons(txn, v), ts=time.time(),
                        )
                        producer.send(TOPIC_ALERTS, value=alert.to_json())
                        FLAGGED.inc()
            PROCESSED.inc(len(buffer))
            consumer.commit()
        except Exception as e:  # noqa: BLE001
            FAILED.inc(len(buffer))
            for txn, _, _ in buffer:
                producer.send(TOPIC_DLQ, value=txn.to_json())
            print(f"[fraud] DLQ batch of {len(buffer)}: {e}", flush=True)
            consumer.commit()
        buffer = []
        last_flush = time.time()

    while True:
        for msg in consumer:
            try:
                txn = Transaction.from_json(msg.value)
                v = velocity(txn.card_id)
                buffer.append((txn, v, {
                    "amount": txn.amount, "merchant": txn.merchant,
                    "country": txn.country, "velocity": v,
                }))
            except Exception as e:  # noqa: BLE001
                FAILED.inc()
                producer.send(TOPIC_DLQ, value=msg.value)
                print(f"[fraud] DLQ (parse): {e}", flush=True)

            if len(buffer) >= BATCH_SIZE:
                flush()

        # consumer_timeout_ms expired: flush whatever is waiting so a slow
        # stream never leaves transactions unscored.
        if buffer or (time.time() - last_flush) > BATCH_TIMEOUT_MS / 1000:
            flush()

if __name__ == "__main__":
    main()
