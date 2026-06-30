# UPIShield — Real-Time Payment Fraud Detection Platform

An event-driven pipeline that scores payment transactions for fraud in real
time. Transactions flow through Kafka into a scoring service that uses Redis
for sub-millisecond feature lookups, flagged transactions are persisted to
Postgres, and the whole pipeline is observable via Prometheus + Grafana.

> The interesting part is the **systems design** — the streaming pipeline,
> latency budget, decoupling, and fault tolerance — not the fraud model.
> The scorer is intentionally simple so the engineering stays in focus.

## Architecture

```
 producer ──▶ Kafka(transactions) ──▶ fraud_consumer ──▶ Kafka(alerts) ──▶ alert_consumer ──▶ Postgres
                                          │   ▲                                                    │
                                          ▼   │ features                                          ▼
                                        Redis (velocity / dedup)                          FastAPI + Dashboard
                                          │
                                          └─ on failure ──▶ Kafka(alerts.dlq)

           Prometheus ◀── /metrics (fraud_consumer:8001) ──▶ Grafana
```

## Services

| Service | Role |
|---|---|
| `producer` | Synthesizes a transaction stream (configurable rate + fraud ratio) |
| `fraud_consumer` | Hot path: scores txns using Redis features, emits alerts, exports metrics |
| `alert_consumer` | Persists alerts to Postgres (decoupled from scoring) |
| `api` | FastAPI read API (`/alerts`, `/stats`) + live dashboard |
| Redis | In-memory feature store (per-card velocity, dedup) |
| Postgres | System of record for alerts |
| Prometheus + Grafana | Throughput + latency monitoring |

## Run it

Requires Docker Desktop (macOS).

```bash
docker compose up --build
```

Then open:
- Dashboard: http://localhost:8000
- API docs: http://localhost:8000/docs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000  (anonymous admin; add Prometheus datasource `http://prometheus:9090`)

Tear down: `docker compose down -v`

## Design decisions (the "why")

- **Kafka, not a plain queue.** Durability + replay. If `fraud_consumer`
  crashes, offsets are retained and it resumes exactly where it stopped —
  no lost transactions. Offsets are committed only *after* successful
  scoring (`enable_auto_commit=False`), which is what guarantees no loss.
- **Redis on the hot path.** Velocity ("txns per card in the last 60s")
  needs sub-millisecond reads; a Postgres round-trip per transaction would
  blow the latency budget. Redis is the cache; Postgres is the record.
- **Separate alert consumer.** Persistence is decoupled from scoring so a
  slow DB write can't backpressure scoring throughput.
- **Trivial scorer by design.** Rules + a small weighted score. A heavy ML
  model would violate the real-time latency SLA — that tradeoff is the
  point. The model lives behind one `score()` function, so it can be
  swapped for a trained classifier without touching the pipeline.
- **Dead-letter topic.** A malformed message goes to `alerts.dlq` instead
  of crashing the consumer — the stream keeps flowing.

## Roadmap (what I'm building)

- [x] Streaming skeleton (Kafka, Redis, Postgres, producer)
- [x] Scoring + alert persistence
- [x] REST API + live dashboard
- [x] Prometheus metrics + Grafana
- [ ] Load test → record throughput + p99 latency
- [ ] Fault-tolerance demo: kill consumer mid-load, show zero loss on resume
- [ ] Replace rule scorer with a trained logistic-regression model
- [ ] Deploy to AWS (ECS Fargate)

## Benchmarks

_To be filled in after the Week-4 load test:_

| Metric | Value |
|---|---|
| Sustained throughput | _TBD_ txns/sec |
| p99 scoring latency | _TBD_ ms |
| Loss on consumer kill | _TBD_ (target: 0) |
