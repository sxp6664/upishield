"""Generate a labeled training dataset for the fraud model.

Deliberately harder than producer/main.py: fraud and legit distributions
OVERLAP, and 3% of labels are flipped to simulate mislabeled ground truth.
Without this the classes are trivially separable and any model scores ~0.99
AUC, which tells you nothing about the model.
"""
import argparse
import random

import pandas as pd

MERCHANTS = ["grocer", "fuel", "airline", "electronics", "casino", "wire_transfer"]
LEGIT_COUNTRIES = ["US", "GB", "IN", "BR"]
RISKY_COUNTRIES = ["NG", "RU"]
LABEL_NOISE = 0.03


def make_row(is_fraud: bool) -> dict:
    if is_fraud:
        # Most fraud is large, but plenty of it hides in normal-looking amounts.
        amount = (
            round(random.uniform(800, 9000), 2)
            if random.random() < 0.65
            else round(random.uniform(5, 800), 2)
        )
        # Most fraud comes from risky countries, but not all.
        country = (
            random.choice(RISKY_COUNTRIES)
            if random.random() < 0.60
            else random.choice(LEGIT_COUNTRIES)
        )
        merchant = (
            random.choice(["casino", "wire_transfer"])
            if random.random() < 0.55
            else random.choice(MERCHANTS)
        )
        velocity = random.randint(1, 25) if random.random() < 0.5 else random.randint(1, 6)
    else:
        # Legit is usually small — but big legitimate purchases exist.
        amount = (
            round(random.uniform(2, 400), 2)
            if random.random() < 0.85
            else round(random.uniform(400, 4000), 2)
        )
        country = (
            random.choice(LEGIT_COUNTRIES)
            if random.random() < 0.92
            else random.choice(RISKY_COUNTRIES)
        )
        merchant = random.choice(MERCHANTS)
        velocity = random.randint(1, 9)

    return {
        "amount": amount,
        "merchant": merchant,
        "country": country,
        "velocity": velocity,
        "is_fraud": int(is_fraud),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=50000)
    ap.add_argument("--fraud-ratio", type=float, default=0.05)
    ap.add_argument("--out", default="ml/data/transactions.csv")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)

    rows = [make_row(random.random() < args.fraud_ratio) for _ in range(args.rows)]

    # Flip a small fraction of labels: real-world ground truth is imperfect.
    for row in rows:
        if random.random() < LABEL_NOISE:
            row["is_fraud"] = 1 - row["is_fraud"]

    df = pd.DataFrame(rows)
    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)

    print(f"wrote {len(df)} rows -> {args.out}")
    print(f"fraud rate: {df.is_fraud.mean():.3%}")
    print(df.head())


if __name__ == "__main__":
    main()