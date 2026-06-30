"""Week 4 load test helper.

Bumps the producer rate and reports the numbers you need for your resume
bullet. Run AFTER the stack is up:

    python loadtest.py --rate 500 --seconds 60

Then read p99 latency off Prometheus/Grafana (metric:
upishield_score_latency_seconds) and processed-count off
upishield_txns_processed_total. This script just drives load + prints a
throughput estimate; Prometheus is the source of truth for latency.
"""
import argparse
import time

from kafka import KafkaProducer

from common.schema import TOPIC_TRANSACTIONS
from producer.main import make_transaction


def run(rate: int, seconds: int, bootstrap: str):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
        linger_ms=2,
        acks=1,
    )
    interval = 1.0 / rate
    end = time.time() + seconds
    sent = 0
    start = time.time()
    while time.time() < end:
        t = make_transaction()
        producer.send(TOPIC_TRANSACTIONS, key=t.card_id.encode(), value=t.to_json())
        sent += 1
        time.sleep(interval)
    producer.flush()
    elapsed = time.time() - start
    print(f"sent={sent} elapsed={elapsed:.1f}s throughput={sent/elapsed:.0f} txns/sec")
    print("Now read p99 latency from Prometheus: upishield_score_latency_seconds")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--bootstrap", default="localhost:29092")
    args = ap.parse_args()
    run(args.rate, args.seconds, args.bootstrap)
