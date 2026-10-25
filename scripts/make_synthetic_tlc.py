#!/usr/bin/env python3
"""Generate a SYNTHETIC yellow-taxi-like Parquet file (for offline pipeline tests only).

Not real TLC data: values are random with injected defects (nulls, negatives,
duplicates, out-of-window and impossible durations) to exercise the cleaning rules.
Usage: make_synthetic_tlc.py OUT_FILE YYYY-MM ROWS [SEED]
"""
import sys
import numpy as np
import pandas as pd

out, ym, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
rng = np.random.default_rng(int(sys.argv[4]) if len(sys.argv) > 4 else 42)
start = pd.Timestamp(f"{ym}-01")
end = start + pd.offsets.MonthBegin(1)
secs = int((end - start).total_seconds())
pickup = start + pd.to_timedelta(rng.integers(0, secs, n), unit="s")
dur = rng.integers(60, 3600, n)
df = pd.DataFrame({
    "VendorID": rng.integers(1, 3, n).astype("int32"),
    "tpep_pickup_datetime": pickup.astype("datetime64[us]"),
    "tpep_dropoff_datetime": (pickup + pd.to_timedelta(dur, unit="s")).astype("datetime64[us]"),
    "passenger_count": rng.integers(1, 5, n).astype("float64"),
    "trip_distance": np.round(rng.gamma(2.0, 1.5, n), 2),
    "PULocationID": rng.integers(1, 264, n).astype("int32"),
    "DOLocationID": rng.integers(1, 264, n).astype("int32"),
    "fare_amount": np.round(3 + dur / 60 * 2.5 + rng.normal(0, 1, n), 2),
    "total_amount": 0.0,
    "Airport_fee": 0.0,
})
df["total_amount"] = np.round(df["fare_amount"] * 1.25 + 0.5, 2)
k = max(n // 200, 1)                                   # ~0.5% per defect class
i = rng.choice(n, 7 * k, replace=False)
df.loc[i[0:k], "fare_amount"] = -5.0
df.loc[i[k:2*k], "PULocationID"] = pd.NA
df.loc[i[2*k:3*k], "tpep_dropoff_datetime"] = df.loc[i[2*k:3*k], "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)
df.loc[i[3*k:4*k], "tpep_pickup_datetime"] = pd.Timestamp("2002-12-31 23:00:00")
df.loc[i[4*k:5*k], "trip_distance"] = 9999.0
df.loc[i[5*k:6*k], "tpep_dropoff_datetime"] = df.loc[i[5*k:6*k], "tpep_pickup_datetime"] + pd.Timedelta(hours=30)
dups = df.iloc[i[6*k:7*k]]
df = pd.concat([df, dups], ignore_index=True)
df["PULocationID"] = df["PULocationID"].astype("Int32")
df.to_parquet(out, index=False)
print(f"wrote {len(df)} rows to {out}")
