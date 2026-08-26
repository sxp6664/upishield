"""Pick an operating threshold. The default 0.5 is arbitrary; the right
threshold depends on the cost of a false positive vs. a missed fraud."""
import joblib
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split

NUMERIC = ["amount", "velocity"]
CATEGORICAL = ["merchant", "country"]

df = pd.read_csv("ml/data/transactions.csv")
X, y = df[NUMERIC + CATEGORICAL], df["is_fraud"]
_, X_test, _, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

model = joblib.load("ml/model.joblib")
proba = model.predict_proba(X_test)[:, 1]

print(f"{'thresh':>7} {'precision':>10} {'recall':>8} {'F1':>7} {'flagged':>8}")
print("-" * 45)
for t in [0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9]:
    preds = (proba >= t).astype(int)
    p, r, f, _ = precision_recall_fscore_support(
        y_test, preds, average="binary", zero_division=0
    )
    print(f"{t:>7.2f} {p:>10.3f} {r:>8.3f} {f:>7.3f} {preds.sum():>8}")

print("\nrules baseline:  precision 0.386  recall 0.399")