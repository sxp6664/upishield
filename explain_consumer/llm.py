"""LLM client for generating structured risk explanations.

Talks to an OpenAI-compatible endpoint, so the backend is swappable:
Ollama locally, vLLM on a GPU, or a hosted API. Nothing downstream cares.
"""
import json
import os
import time

import requests

LLM_URL = os.getenv("LLM_URL", "http://localhost:11434/v1/chat/completions")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:3b")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "20"))

SYSTEM = (
    "You are a fraud analyst. Given a flagged payment transaction, return ONLY "
    "a JSON object with these exact keys: "
    '{"risk_factors": [list of short strings], "severity": integer 1-5, '
    '"recommended_action": short string, "summary": one sentence}. '
    "No markdown, no code fences, no text outside the JSON."
)


def build_prompt(alert: dict) -> str:
    return (
        f"Transaction {alert.get('txn_id')}: "
        f"amount {alert.get('amount')}, "
        f"card {alert.get('card_id')}, "
        f"model risk score {alert.get('score')}, "
        f"signals: {', '.join(alert.get('reasons', []))}."
    )


def explain(alert: dict) -> tuple[dict | None, dict]:
    """Return (explanation_or_None, metrics).

    Never raises: on any failure the caller falls back to a template.
    """
    t0 = time.perf_counter()
    metrics = {"latency_s": 0.0, "prompt_tokens": 0,
               "completion_tokens": 0, "ok": False, "error": None}
    try:
        resp = requests.post(
            LLM_URL,
            json={
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": build_prompt(alert)},
                ],
                "temperature": 0.2,
                "max_tokens": 200,
            },
            timeout=LLM_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()

        usage = body.get("usage", {})
        metrics["prompt_tokens"] = usage.get("prompt_tokens", 0)
        metrics["completion_tokens"] = usage.get("completion_tokens", 0)

        text = body["choices"][0]["message"]["content"].strip()
        # Small models sometimes wrap JSON in code fences despite instructions.
        if text.startswith("```"):
            text = text.split("```")[1].removeprefix("json").strip()

        parsed = json.loads(text)
        if not isinstance(parsed.get("severity"), int):
            raise ValueError("severity must be an int")
        metrics["ok"] = True
        return parsed, metrics

    except Exception as e:  # noqa: BLE001
        metrics["error"] = type(e).__name__
        return None, metrics
    finally:
        metrics["latency_s"] = time.perf_counter() - t0


def fallback(alert: dict) -> dict:
    """Deterministic explanation used when the LLM is unavailable."""
    score = alert.get("score", 0)
    return {
        "risk_factors": alert.get("reasons", []),
        "severity": min(5, max(1, int(score * 5) + 1)),
        "recommended_action": "manual review" if score < 0.9 else "block and contact cardholder",
        "summary": f"Flagged with model score {score}.",
        "degraded": True,
    }