# Benchmark record (measured, not estimated)

**Scope and honesty note.** These numbers come from a SYNTHETIC dataset
(`scripts/make_synthetic_tlc.py`, 1,206,000 rows, 1 month, seed 42), run with
`local[2]` Spark (2 cores, 3 GB driver) writing to the local-filesystem storage
layout, with `--skip-publish`. They show the job logic works at >1M rows and give
a baseline. They are NOT TLC results, NOT MinIO/S3A, NOT the Docker cluster, and
do not demonstrate production-scale performance.

| Run | Source rows | Accepted | Rejected | Hourly rows | Feature rows | Input | Output | Processing time | Throughput |
|---|---|---|---|---|---|---|---|---|---|
| 1 (cold) | 1,206,000 | 1,164,000 | 42,000 | 195,152 | 195,672 | 28.1 MB | 38.6 MB | 47.2 s | 25.5k rows/s |
| 2 (re-run, same month) | 1,206,000 | 1,164,000 | 42,000 | 195,152 | 195,672 | 28.1 MB | 38.6 MB | 35.4 s | ~34k rows/s |

Run 1 includes JVM/JIT warm-up. After both runs the object-store layer
row counts were identical (cleaned 1,164,000; rejected 42,000; hourly 195,152;
features 195,672), i.e. re-running did not duplicate data.

## Not yet measured (real TLC data, Docker stack)
Run `bash scripts/benchmark.sh` (see README) for several month counts and
`--shuffle-partitions` values, then paste the `ops.batch_runs` rows here.

---

## Real run: NYC TLC Yellow Taxi, January 2024 (Docker stack, Windows laptop)
Source of numbers: `ops.batch_runs` and `scripts/verify_pipeline.py` output captured on 2026-10-25
(Spark mode `local[2]`, 8 shuffle partitions; laptop with 8 GB RAM, Windows + Docker Desktop/WSL 2, WSL limited to 5 GB). Single run, no repetition,
so treat timings as indicative only, not a statistically sound benchmark.

| Metric | Value |
|---|---|
| batch_id | yellow_2024-01_2024-01 |
| Source rows (TLC file `yellow_tripdata_2024-01.parquet`) | 2,964,624 |
| Accepted rows | 2,926,252 |
| Rejected rows | 38,372 (1.29 %) |
| Rejection reasons | negative_fare 37,442; non_positive_duration 870; distance_exceeds_max 25; pickup_outside_window 18; duration_exceeds_max 16; fare_exceeds_max 1 |
| Hourly x zone rows | 77,469 |
| Forecast feature rows | 193,440 (= 260 zones x 744 hours) |
| Input size | 49,961,641 bytes |
| Output size (cleaned + rejected + curated Parquet) | 67,488,291 bytes |
| Spark processing time | 302.0 s |
| Throughput | 9,816 rows/s |
| Verification | 15/15 checks passed (incl. >= 1,000,000 records, source = accepted + rejected, sum(trip_count) = accepted) |

Not yet measured: effect of different shuffle-partition counts and of multi-month inputs (`scripts/benchmark.sh`).
