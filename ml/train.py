"""Train a logistic-regression fraud classifier.

Reports precision/recall/AUC on a held-out set and compares against the
rule-based scorer currently running in fraud_consumer, so we know whether
the model is actually an improvement.
"""
import argparse

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score, classification_report,
    confusion_matrix, roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

NUMERIC = ["amount", "velocity"]
CATEGORICAL = ["merchant", "country"]


def rule_score(row) -> float:
    """The current production rules, replicated so we can benchmark against them."""
    s = 0.0
    if row.amount > 1500:
        s += 0.4
    if row.country in ("NG", "RU"):
        s += 0.3
    if row.merchant in ("casino", "wire_transfer"):
        s += 0.2
    if row.velocity > 8:
        s += 0.3
    return min(s, 1.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="ml/data/transactions.csv")
    ap.add_argument("--out", default="ml/model.joblib")
    args = ap.parse_args()

    df = pd.read_csv(args.data)
    X = df[NUMERIC + CATEGORICAL]
    y = df["is_fraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    pipeline = Pipeline([
        ("prep", ColumnTransformer([
            ("num", StandardScaler(), NUMERIC),
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ])),
        ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])

    pipeline.fit(X_train, y_train)

    proba = pipeline.predict_proba(X_test)[:, 1]
    preds = (proba >= 0.5).astype(int)

    print("=" * 55)
    print("MODEL (logistic regression)")
    print("=" * 55)
    print(classification_report(y_test, preds, target_names=["legit", "fraud"], digits=3))
    print(f"ROC-AUC:  {roc_auc_score(y_test, proba):.4f}")
    print(f"PR-AUC:   {average_precision_score(y_test, proba):.4f}")
    print("confusion matrix [[TN FP] [FN TP]]:")
    print(confusion_matrix(y_test, preds))

    rules = X_test.apply(rule_score, axis=1)
    print()
    print("=" * 55)
    print("BASELINE (current production rules)")
    print("=" * 55)
    print(classification_report(
        y_test, (rules >= 0.6).astype(int),
        target_names=["legit", "fraud"], digits=3, zero_division=0,
    ))
    print(f"ROC-AUC:  {roc_auc_score(y_test, rules):.4f}")
    print(f"PR-AUC:   {average_precision_score(y_test, rules):.4f}")

    joblib.dump(pipeline, args.out)
    print(f"\nsaved -> {args.out}")


if __name__ == "__main__":
    main()