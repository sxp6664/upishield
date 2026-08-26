"""Where does the per-transaction inference time go?"""
import time
import joblib
import pandas as pd

model = joblib.load("ml/model.joblib")
N = 2000
row = {"amount": 6500.0, "merchant": "wire_transfer", "country": "NG", "velocity": 15}

# 1. Full path, as the consumer does it today
t0 = time.perf_counter()
for _ in range(N):
    df = pd.DataFrame([row])
    model.predict_proba(df)[0, 1]
full = (time.perf_counter() - t0) / N * 1000

# 2. DataFrame construction alone
t0 = time.perf_counter()
for _ in range(N):
    pd.DataFrame([row])
df_only = (time.perf_counter() - t0) / N * 1000

# 3. predict_proba alone, DataFrame prebuilt
df = pd.DataFrame([row])
t0 = time.perf_counter()
for _ in range(N):
    model.predict_proba(df)[0, 1]
predict_only = (time.perf_counter() - t0) / N * 1000

# 4. Batched: 100 rows at once, cost per row
batch = pd.DataFrame([row] * 100)
t0 = time.perf_counter()
for _ in range(N // 100):
    model.predict_proba(batch)[:, 1]
batched = (time.perf_counter() - t0) / (N // 100) / 100 * 1000

print(f"full path (DataFrame + predict): {full:8.3f} ms/txn")
print(f"  DataFrame construction only:   {df_only:8.3f} ms/txn")
print(f"  predict_proba only:            {predict_only:8.3f} ms/txn")
print(f"batched (100 rows at a time):    {batched:8.3f} ms/txn")
print(f"\nspeedup from batching: {full / batched:.1f}x")