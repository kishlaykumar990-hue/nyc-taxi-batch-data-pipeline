"""Spark batch job: raw -> cleaned/rejected -> hourly metrics -> features -> PostgreSQL.

Submitted through scripts/spark_submit.sh (which sets cluster/driver options):

    bash scripts/spark_submit.sh --months 2024-01 [--sample-fraction 0.05]
        [--shuffle-partitions 8] [--skip-publish] [--batch-id my_run]

Re-running the same months is safe: object-store output uses dynamic partition
overwrite and the Postgres publish replaces the batch window in one transaction.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Settings, load_settings
from src.common.months import ZONE_LOOKUP_KEY, batch_id_for, parse_months, raw_key, window_for
from src.common.storage import Storage, get_storage
from src.publish.postgres_publish import publish
from src.spark.transform import (
    aggregate_hourly, build_features, flag_rows, normalize_schema, split_flagged,
)

log = logging.getLogger("batch_job")


def build_spark(app_name: str, s: Settings, shuffle_partitions: int, *, local_storage: bool = False) -> SparkSession:
    use_ssl = str(s.minio_endpoint.startswith("https")).lower()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")                        # timestamp policy: never shift
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")       # idempotent re-runs
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # not legacy INT96
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if not local_storage:   # MinIO through the S3A connector
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", s.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", use_ssl)
        )
    return builder.getOrCreate()


def _jdbc(s: Settings) -> tuple[str, dict[str, str]]:
    return (f"jdbc:postgresql://{s.pg_host}:{s.pg_port}/{s.pg_db}",
            {"user": s.pg_user, "password": s.pg_password, "driver": "org.postgresql.Driver",
             "batchsize": "10000", "truncate": "true"})


def _stage(df: DataFrame, table: str, s: Settings) -> None:
    url, props = _jdbc(s)
    df.write.mode("overwrite").option("truncate", "true").jdbc(url, table, properties=props)


def run(months: list[str], s: Settings, *, sample_fraction: float, shuffle_partitions: int,
        batch_id: str | None, skip_publish: bool) -> dict:
    months = parse_months(months)
    window_start, window_end = window_for(months)
    batch_id = batch_id or batch_id_for(months)
    started_at = datetime.now(timezone.utc)
    t0 = time.time()
    timings: dict[str, float] = {}
    store: Storage = get_storage(s)
    spark = build_spark(f"nyc-taxi-{batch_id}", s, shuffle_partitions, local_storage=store.is_local)

    # ---- 1. read + normalise each month separately (schemas drift between files)
    parts, file_rows, input_bytes = [], {}, 0
    for ym in months:
        key = raw_key(ym)
        raw = spark.read.parquet(store.uri(s.bucket_raw, key))
        file_rows[ym] = raw.count()                                   # Parquet footer metadata, no scan
        input_bytes += store.size(s.bucket_raw, key)
        df = normalize_schema(raw, ym)
        if sample_fraction < 1.0:
            df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
        parts.append(df)
    source = reduce(DataFrame.unionByName, parts)

    # ---- 2. flag every row once (nothing is dropped), persist for reuse
    t = time.time()
    flagged = flag_rows(source, window_start, window_end, s.limits).persist(StorageLevel.MEMORY_AND_DISK)
    counts = {r["reject_reason"]: r["count"] for r in flagged.groupBy("reject_reason").count().collect()}
    source_rows = sum(counts.values())
    accepted_rows = counts.get(None, 0)
    rejected_rows = source_rows - accepted_rows
    reject_breakdown = {k: v for k, v in counts.items() if k is not None}
    timings["flag_and_count_s"] = time.time() - t
    if sample_fraction == 1.0 and source_rows != sum(file_rows.values()):
        raise RuntimeError(f"Row reconciliation failed: processed {source_rows} vs files {sum(file_rows.values())}")
    log.info("source=%s accepted=%s rejected=%s %s", source_rows, accepted_rows, rejected_rows, reject_breakdown)

    accepted, rejected = split_flagged(flagged)

    # ---- 3. persist cleaned + rejected layers (partition overwrite => idempotent)
    t = time.time()
    accepted.write.mode("overwrite").partitionBy("pickup_month").parquet(store.uri(s.bucket_cleaned, "yellow"))
    rejected.write.mode("overwrite").partitionBy("source_month").parquet(store.uri(s.bucket_rejected, "yellow"))
    timings["write_cleaned_rejected_s"] = time.time() - t

    # ---- 4. aggregate + features, reconcile, persist curated layer
    t = time.time()
    hourly = aggregate_hourly(accepted).persist(StorageLevel.MEMORY_AND_DISK)
    hourly_rows = hourly.count()
    aggregated_trips = hourly.agg(F.sum("trip_count")).collect()[0][0] or 0
    if aggregated_trips != accepted_rows:
        raise RuntimeError(f"Aggregation mismatch: sum(trip_count)={aggregated_trips} != accepted={accepted_rows}")
    features = build_features(spark, hourly, window_start, window_end).persist(StorageLevel.MEMORY_AND_DISK)
    feature_rows = features.count()
    with_month = lambda d: d.withColumn("pickup_month", F.date_format("pickup_hour", "yyyy-MM"))  # noqa: E731
    with_month(hourly).write.mode("overwrite").partitionBy("pickup_month") \
        .parquet(store.uri(s.bucket_curated, "hourly_zone_metrics"))
    with_month(features).write.mode("overwrite").partitionBy("pickup_month") \
        .parquet(store.uri(s.bucket_curated, "forecast_features"))
    timings["aggregate_and_write_curated_s"] = time.time() - t

    # ---- 5. sizes + metrics
    out_bytes = 0
    for bucket, prefix in ((s.bucket_cleaned, "yellow/"), (s.bucket_rejected, "yellow/"),
                           (s.bucket_curated, "hourly_zone_metrics/"), (s.bucket_curated, "forecast_features/")):
        out_bytes += store.prefix_size(bucket, prefix)[1]
    processing_s = time.time() - t0
    run_info = {
        "batch_id": batch_id, "months": months, "window_start": window_start, "window_end": window_end,
        "source_rows": source_rows, "accepted_rows": accepted_rows, "rejected_rows": rejected_rows,
        "reject_breakdown": reject_breakdown, "hourly_rows": hourly_rows, "feature_rows": feature_rows,
        "input_bytes": input_bytes, "output_bytes": out_bytes, "processing_s": processing_s,
        "throughput_rows_per_s": source_rows / processing_s if processing_s else None,
        "config": {"spark_master": spark.sparkContext.master, "shuffle_partitions": shuffle_partitions,
                   "sample_fraction": sample_fraction, "executor_memory": spark.conf.get("spark.executor.memory", "default"),
                   "driver_memory": spark.conf.get("spark.driver.memory", "default"),
                   "default_parallelism": spark.sparkContext.defaultParallelism, "stage_timings_s": timings},
        "started_at": started_at.isoformat(),
    }

    # ---- 6. publish
    if skip_publish:
        log.warning("--skip-publish: Postgres not updated")
        run_info["status"] = "processed_not_published"
    else:
        t = time.time()
        zones = (spark.read.option("header", True).csv(store.uri(s.bucket_raw, ZONE_LOOKUP_KEY))
                 .select(F.col("LocationID").cast("int").alias("location_id"), F.col("Borough").alias("borough"),
                         F.col("Zone").alias("zone"), F.col("service_zone")))
        _stage(zones, "staging.taxi_zones", s)
        _stage(hourly.select(*[c for c in hourly.columns if c != "pickup_month"]), "staging.hourly_zone_metrics", s)
        _stage(features.select(*[c for c in features.columns if c != "pickup_month"]), "staging.forecast_features", s)
        run_info["config"]["stage_timings_s"]["stage_to_postgres_s"] = time.time() - t
        run_info["publish_s"] = publish(s, batch_id=batch_id, months=months, window_start=window_start,
                                        window_end=window_end, run=run_info)
        run_info["status"] = "success"

    flagged.unpersist()
    hourly.unpersist()
    features.unpersist()
    run_info["finished_at"] = datetime.now(timezone.utc).isoformat()
    store.put_json(s.bucket_curated, f"_batch_metrics/{batch_id}.json", run_info)
    print("BATCH_METRICS " + json.dumps(run_info, default=str))
    spark.stop()
    return run_info


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--months", help="comma-separated YYYY-MM (contiguous). Default: $PIPELINE_MONTHS")
    p.add_argument("--sample-fraction", type=float, help="0<f<=1 development sample. Default: $SAMPLE_FRACTION")
    p.add_argument("--shuffle-partitions", type=int, help="Default: $SPARK_SHUFFLE_PARTITIONS")
    p.add_argument("--batch-id")
    p.add_argument("--skip-publish", action="store_true")
    a = p.parse_args(argv)

    s = load_settings()
    fraction = a.sample_fraction if a.sample_fraction is not None else s.sample_fraction
    if not 0 < fraction <= 1:
        p.error("--sample-fraction must be in (0, 1]")
    run(parse_months(a.months or s.default_months), s, sample_fraction=fraction,
        shuffle_partitions=a.shuffle_partitions or s.shuffle_partitions, batch_id=a.batch_id,
        skip_publish=a.skip_publish)
    return 0


if __name__ == "__main__":
    sys.exit(main())
