"""Producer: synthesizes a stream of transactions and publishes them to the
`transactions` Kafka topic. A configurable fraction are crafted to look
fraudulent so the downstream scorer has something to catch.

Run rate is controlled by RATE (txns/sec) — turn this up during load testing
to find your real throughput/latency numbers (Week 4).
"""
import os
import random
import time

from kafka import KafkaProducer

from common.schema import Transaction, TOPIC_TRANSACTIONS

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RATE = float(os.getenv("RATE", "20"))          # transactions per second
FRAUD_RATIO = float(os.getenv("FRAUD_RATIO", "0.05"))

CARDS = [f"card_{i:04d}" for i in range(200)]
MERCHANTS = ["grocer", "fuel", "airline", "electronics", "casino", "wire_transfer"]
COUNTRIES = ["US", "GB", "IN", "NG", "RU", "BR"]
DEVICES = [f"dev_{i:03d}" for i in range(80)]


def make_transaction() -> Transaction:
    card = random.choice(CARDS)
    if random.random() < FRAUD_RATIO:
        # Fraud-ish: large amount, risky merchant/country, unusual device.
        return Transaction.new(
            card_id=card,
            amount=round(random.uniform(2000, 9000), 2),
            merchant=random.choice(["casino", "wire_transfer", "electronics"]),
            device_id=random.choice(DEVICES[-5:]),  # rare devices
            country=random.choice(["NG", "RU"]),
        )
    return Transaction.new(
        card_id=card,
        amount=round(random.uniform(2, 300), 2),
        merchant=random.choice(MERCHANTS),
        device_id=random.choice(DEVICES[:40]),
        country=random.choice(["US", "GB", "IN"]),
    )


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: v.encode("utf-8"),
        linger_ms=5,
        acks=1,
    )
    interval = 1.0 / RATE if RATE > 0 else 0
    print(f"[producer] publishing ~{RATE} txns/sec to '{TOPIC_TRANSACTIONS}'", flush=True)
    sent = 0
    while True:
        txn = make_transaction()
        # key by card_id so all txns for a card land on the same partition
        producer.send(TOPIC_TRANSACTIONS, key=txn.card_id.encode(), value=txn.to_json())
        sent += 1
        if sent % 500 == 0:
            print(f"[producer] sent {sent}", flush=True)
        if interval:
            time.sleep(interval)


if __name__ == "__main__":
    main()
