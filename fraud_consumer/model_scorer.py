"""Model-based scorer with automatic fallback to rules.

If the model can't be loaded, we log it and fall back rather than failing
the service — the same reasoning as the dead-letter topic: degrade, don't die.
"""
import os

MODEL_PATH = os.getenv("MODEL_PATH", "ml/model.joblib")

_model = None
_available = False

try:
    import joblib
    import pandas as pd
    _model = joblib.load(MODEL_PATH)
    _available = True
    print(f"[fraud] model loaded from {MODEL_PATH}", flush=True)
except Exception as e:  # noqa: BLE001
    print(f"[fraud] model unavailable ({e}); using rules", flush=True)


def is_available() -> bool:
    return _available


def model_score(amount: float, merchant: str, country: str, velocity: int) -> float:
    """Return P(fraud) in [0, 1]."""
    row = pd.DataFrame(
        [{"amount": amount, "merchant": merchant,
          "country": country, "velocity": velocity}]
    )
    return float(_model.predict_proba(row)[0, 1])

def model_score_batch(rows: list[dict]) -> list[float]:
    """Score many transactions in one call.

    predict_proba carries ~2.2ms of fixed per-call overhead (validation +
    ColumnTransformer setup) regardless of batch size. Batching amortizes it:
    measured 2.481 ms/txn single vs 0.022 ms/txn at batch=100 — a 113x speedup.
    See ml/bench_inference.py.
    """
    df = pd.DataFrame(rows)
    return [float(p) for p in _model.predict_proba(df)[:, 1]]