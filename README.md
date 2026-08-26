# UPIShield — Real-Time Fraud Detection with an LLM Explanation Layer

An event-driven pipeline that scores payment transactions for fraud in real time,
then generates structured, analyst-readable explanations for the flagged ones using
a locally-served LLM.

Two paths, two latency budgets:

- **Fast path** — every transaction is scored by a trained classifier in **~1 ms p99**,
  sustaining **~334 txns/sec** on a single consumer.
- **Slow path** — flagged transactions get an LLM-generated risk explanation in
  **~4 s**, on a separate consumer group that *cannot* backpressure scoring.

> The interesting part is the **systems design** — the latency budget, the
> decoupling, and what happens when things fail. Every number below was measured,
> and the method for measuring it is documented.

## Architecture

```
producer ──▶ Kafka(transactions) ──▶ fraud_consumer ──▶ Kafka(alerts) ─┬─▶ alert_consumer ──▶ Postgres
                                          │   ▲                        │
                                          ▼   │ features               └─▶ explain_consumer ──▶ LLM
                                    Redis (velocity / dedup)                    │
                                          │                                     ▼
                                          └─ on failure ──▶ Kafka(alerts.dlq)   structured JSON
                                                                                explanation

     Prometheus ◀── /metrics (fraud_consumer:8001, explain_consumer:8002) ──▶ Grafana
```

## Services

| Service | Role |
| --- | --- |
| `producer` | Synthesizes a transaction stream (configurable rate + fraud ratio) |
| `fraud_consumer` | **Fast path.** Batched model inference, emits alerts, exports metrics |
| `alert_consumer` | Persists alerts to Postgres (decoupled from scoring) |
| `explain_consumer` | **Slow path.** LLM risk explanations, own consumer group |
| `api` | FastAPI read API (`/alerts`, `/stats`) + live dashboard |
| Redis | In-memory feature store (per-card velocity, dedup) |
| Postgres | System of record for alerts |
| Prometheus + Grafana | Throughput, latency, token, and degradation metrics |

## Results

### Fraud model vs. the rule baseline

Logistic regression trained on 50k synthetic transactions with **deliberately
overlapping class distributions and 3% label noise** — without that, the classes are
trivially separable and any model scores ~0.99 AUC, which measures nothing.

| Metric | Rules | Model | |
| --- | --- | --- | --- |
| PR-AUC | 0.343 | **0.490** | +43% |
| ROC-AUC | 0.743 | **0.756** | |
| Precision @ threshold | 0.386 | **0.423** | |
| Recall @ threshold | 0.399 | **0.469** | |

PR-AUC is the metric that matters here — at a 7.9% fraud rate, accuracy is
misleading (predicting "never fraud" scores 92%).

The 0.70 threshold came from a sweep (`ml/tune_threshold.py`), chosen because it is
the point where the model beats the rules on **both** precision and recall
simultaneously. The full curve is a business dial: at 0.80 you get 71% precision but
catch only 38% of fraud; at 0.30 you catch 81% but 89% of flags are false.

### The latency regression, and the fix

Dropping the model into the hot path **broke the SLA**:

| | Rules | Model (single-row) | Model (batched) |
| --- | --- | --- | --- |
| p99 scoring latency | 0.99 ms | **6.23 ms** | — |
| Sustained throughput | 332.9/sec | **259.7/sec** | **334.5/sec** |
| Backlog after load | none | **yes, still draining at 185/sec after 60s** | none |

At ~331/sec offered and ~260/sec capacity, the consumer fell behind by ~70
transactions every second. Kafka's durability meant nothing was lost — the backlog
just grew.

Profiling (`ml/bench_inference.py`) located the cost, and it was not where I
expected:

| | ms/txn |
| --- | --- |
| Full path (DataFrame + `predict_proba`) | 2.481 |
| — DataFrame construction alone | 0.091 |
| — `predict_proba` alone | **2.211** |
| Batched, 100 rows at a time | **0.022** |

89% of the cost is fixed per-call overhead inside `predict_proba` — scikit-learn's
input validation and `ColumnTransformer` setup — paid once per row instead of once
per batch. Dropping pandas would have bought almost nothing.

**Fix:** micro-batching in the consumer (batch size 100, 50 ms timeout), which
amortizes that overhead across the batch. **113× faster per transaction**, and
throughput recovered to 334.5/sec with no backlog.

### LLM explanation layer

Flagged transactions are explained by `qwen2.5:3b` served locally via Ollama,
through an OpenAI-compatible endpoint so the backend is swappable (Ollama, vLLM, or
a hosted API) without touching anything downstream.

Output is schema-constrained JSON:

```json
{
  "risk_factors": ["high_amount", "risky_country", "velocity_15"],
  "severity": 4,
  "recommended_action": "Review and potentially decline the transaction",
  "summary": "High-value transaction from a risky country with unusual card velocity."
}
```

Generation takes **~4 s** — roughly 4,000× the fast path's latency budget. That gap
is exactly why it runs on its own topic and consumer group rather than inline.

### Graceful degradation under LLM failure

Killed the LLM backend mid-load and measured what happened:

| | Result |
| --- | --- |
| Fast-path scoring | **continued uninterrupted** |
| Alerts lost | **0** |
| Explanations | fell back to deterministic templates (`degraded: true`) |
| Consumer crashes | none — `ConnectionError` caught and handled |
| On backend restart | **recovered automatically, no intervention** |

An entire downstream dependency went offline and the critical path did not notice.

## Design decisions (the "why")

- **Kafka, not a plain queue.** Durability + replay. Offsets commit only *after*
  successful work (`enable_auto_commit=False`), which is what guarantees no loss on
  crash.
- **Redis on the hot path.** Per-card velocity needs sub-millisecond reads; a
  Postgres round-trip per transaction would blow the latency budget. Redis is the
  cache; Postgres is the record.
- **Separate consumer group for explanations.** The LLM is ~4,000× slower than
  scoring. On the same consumer it would destroy the SLA; on its own group it can be
  slow, fail, or disappear entirely without touching the fast path.
- **Micro-batching, not a faster model.** The bottleneck was per-call overhead, not
  model complexity. Measuring first meant fixing the right thing.
- **Rule fallback behind the model.** If `model.joblib` is missing or fails to load,
  scoring degrades to the original rules rather than the service dying. Same
  reasoning as the dead-letter topic: degrade, don't die.
- **Dead-letter topic.** A malformed message goes to `alerts.dlq` instead of
  crashing the consumer — the stream keeps flowing.

## Run it

Requires Docker Desktop. For the explanation layer, [Ollama](https://ollama.com)
running locally:

```bash
ollama serve
ollama pull qwen2.5:3b
```

Then:

```bash
docker compose up --build
```

- Dashboard: http://localhost:8000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Fast-path metrics: http://localhost:8001/metrics
- LLM metrics: http://localhost:8002/metrics

Tear down with `docker compose down -v`.

### Reproducing the results

```bash
# 1. Regenerate the dataset (seed 42 → identical output)
python ml/generate_dataset.py

# 2. Train and benchmark against the rule baseline
python ml/train.py

# 3. Sweep the decision threshold
python ml/tune_threshold.py

# 4. Profile single vs. batched inference
python ml/bench_inference.py

# 5. Load test (query Prometheus mid-run, not after — see below)
python loadtest.py --rate 400 --seconds 120 --bootstrap localhost:29092
```

**Measurement method.** Throughput and p99 are read from Prometheus *during* a
sustained run, at ~100s into a 120s test, so the full `rate[1m]` window sits inside
the load period. Querying after the run mixes in idle time and understates the
result. Absence of backlog is confirmed by re-querying 60s after the load stops and
checking that throughput returns to the idle rate (~19/sec).

## Known limitations

- **Synthetic data.** Fraud patterns are generated, not real. The overlap and label
  noise make the model's job non-trivial, but the numbers are not transferable to
  production fraud.
- **Card-fraud features, not UPI scam features.** Amount, merchant, country, and
  velocity detect *unauthorized* transactions. Most real UPI fraud is social
  engineering, where the legitimate account holder authorizes the payment — that
  needs behavioral features (first-time beneficiary, collect-vs-push, time-of-day)
  which this schema does not yet carry.
- **Single-node.** One partition, one consumer per group, no replication. Horizontal
  scaling would come from adding consumers to the group.
- **LLM output is not evaluated.** Explanations parse and look reasonable, but there
  is no ground truth to score them against.

## Troubleshooting

**Kafka fails to start with `NodeExists` / `Error while creating ephemeral at /brokers/ids/1`**
— a stale broker registration in Zookeeper from an unclean shutdown. Fix with
`docker compose down -v` (the `-v` clears the volumes holding that state), then bring
it back up.

**`NoBrokersAvailable` when running loadtest from the host** — use the host listener:
`--bootstrap localhost:29092`. Port 9092 advertises the in-Docker hostname.

## Roadmap

- [x] Streaming skeleton (Kafka, Redis, Postgres, producer)
- [x] Prometheus metrics + Grafana
- [x] Fault-tolerance demo: kill consumer mid-load, zero loss on resume
- [x] Replace rule scorer with a trained model, benchmarked against the baseline
- [x] Recover throughput via batched inference
- [x] LLM explanation layer on an isolated consumer group
- [x] Graceful degradation + automatic recovery under LLM failure
- [ ] Redis semantic caching for repeated explanation patterns
- [ ] vLLM serving benchmark (throughput vs. batch size on GPU)
- [ ] UPI-specific behavioral features
