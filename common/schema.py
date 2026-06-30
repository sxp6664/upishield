"""Shared transaction + alert schemas. Imported by every service so the
event contract lives in exactly one place."""
from dataclasses import dataclass, asdict
import json
import time
import uuid


@dataclass
class Transaction:
    txn_id: str
    card_id: str
    amount: float
    merchant: str
    device_id: str
    country: str
    ts: float  # epoch seconds

    @staticmethod
    def new(card_id, amount, merchant, device_id, country):
        return Transaction(
            txn_id=str(uuid.uuid4()),
            card_id=card_id,
            amount=amount,
            merchant=merchant,
            device_id=device_id,
            country=country,
            ts=time.time(),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "Transaction":
        return Transaction(**json.loads(raw))


@dataclass
class Alert:
    txn_id: str
    card_id: str
    amount: float
    score: float
    reasons: list
    ts: float

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "Alert":
        return Alert(**json.loads(raw))


# --- Kafka topic names (single source of truth) ---
TOPIC_TRANSACTIONS = "transactions"
TOPIC_ALERTS = "alerts"
TOPIC_DLQ = "alerts.dlq"  # dead-letter for failed scoring
