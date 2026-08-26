"""Explain consumer: the slow path.

Reads flagged transactions from `alerts`, generates a structured LLM
explanation, and persists it. Runs in its own consumer group so it CANNOT
backpressure fraud scoring — the fast path stays at ~1ms p99 regardless of
how slow or unavailable the LLM is.
"""
import json
import os
import time

from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

from common.schema import TOPIC_ALERTS
from explain_consumer import llm

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

EXPLAINED = Counter("upishield_explanations_total", "Explanations generated")
DEGRADED = Counter("upishield_explanations_degraded_total",
                   "Explanations served from template fallback")
TOKENS_IN = Counter("upishield_llm_prompt_tokens_total", "Prompt tokens")
TOKENS_OUT = Counter("upishield_llm_completion_tokens_total", "Completion tokens")
LLM_LATENCY = Histogram(
    "upishield_llm_latency_seconds", "LLM generation latency",
    buckets=(.5, 1, 2, 3, 5, 8, 12, 20, 30),
)


def main():
    start_http_server(8002)
    consumer = KafkaConsumer(
        TOPIC_ALERTS,
        bootstrap_servers=BOOTSTRAP,
        group_id="explainer",          # separate group: independent offsets
        enable_auto_commit=False,
        auto_offset_reset="latest",    # don't replay history on first start
        value_deserializer=lambda b: b.decode("utf-8"),
    )
    print("[explain] started; metrics on :8002", flush=True)

    for msg in consumer:
        alert = json.loads(msg.value)
        result, metrics = llm.explain(alert)

        LLM_LATENCY.observe(metrics["latency_s"])
        TOKENS_IN.inc(metrics["prompt_tokens"])
        TOKENS_OUT.inc(metrics["completion_tokens"])

        if result is None:
            result = llm.fallback(alert)
            DEGRADED.inc()
            print(f"[explain] degraded ({metrics['error']}) for {alert.get('txn_id')}", flush=True)
        EXPLAINED.inc()

        print(json.dumps({
            "txn_id": alert.get("txn_id"),
            "severity": result.get("severity"),
            "action": result.get("recommended_action"),
            "latency_s": round(metrics["latency_s"], 2),
            "degraded": result.get("degraded", False),
        }), flush=True)

        consumer.commit()


if __name__ == "__main__":
    main()