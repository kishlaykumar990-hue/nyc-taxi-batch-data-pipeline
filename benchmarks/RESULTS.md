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

## Still to measure on your machine (real TLC data, Docker stack)
Run `bash scripts/benchmark.sh` (see README) for several month counts and
`--shuffle-partitions` values, then paste the `ops.batch_runs` rows here.
