"""Pure DataFrame transformations (no I/O) so they can be unit-tested locally.

Pipeline: normalize_schema -> flag_rows -> split_flagged -> aggregate_hourly
          -> build_features

Timestamp policy: TLC timestamps are NYC local wall-clock time with no offset.
They are never shifted; the Spark session runs in UTC purely so that the naive
values round-trip unchanged. All window bounds are passed as strings and cast
inside Spark (a Python ``datetime`` would be interpreted in the Python
process's own time zone and could shift the window).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import QualityLimits
from src.common.schema import (
    OPTIONAL_RAW_COLUMNS,
    REQUIRED_RAW_COLUMNS,
    SchemaError,
    missing_required,
)

# Precedence order: the first failing rule names the rejection reason.
REJECT_REASONS = (
    "null_pickup_ts",
    "null_dropoff_ts",
    "pickup_outside_window",
    "non_positive_duration",
    "duration_exceeds_max",
    "null_pickup_zone",
    "null_fare",
    "negative_fare",
    "fare_exceeds_max",
    "null_distance",
    "negative_distance",
    "distance_exceeds_max",
    "duplicate",
)

_DEDUP_KEYS = [
    "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id",
    "passenger_count", "trip_distance", "fare_amount", "total_amount",
]
_TYPES = {
    "pickup_ts": "timestamp", "dropoff_ts": "timestamp",
    "pu_location_id": "int", "do_location_id": "int",
    "trip_distance": "double", "fare_amount": "double", "total_amount": "double",
    "vendor_id": "int", "passenger_count": "int",
}


def _ts(literal: str):
    return F.lit(literal).cast("timestamp")


def normalize_schema(df: DataFrame, source_month: str) -> DataFrame:
    """Map raw columns to the canonical schema (case-insensitive, type-safe)."""
    missing = missing_required(df.columns)
    if missing:
        raise SchemaError(f"Raw file for {source_month} is missing required columns: {missing}")
    lookup = {c.lower(): c for c in df.columns}
    cols = []
    for raw, canon in {**REQUIRED_RAW_COLUMNS, **OPTIONAL_RAW_COLUMNS}.items():
        if raw in lookup:
            cols.append(F.col(f"`{lookup[raw]}`").cast(_TYPES[canon]).alias(canon))
        else:  # optional column absent in this year's schema
            cols.append(F.lit(None).cast(_TYPES[canon]).alias(canon))
    return df.select(*cols).withColumn("source_month", F.lit(source_month))


def flag_rows(df: DataFrame, window_start: str, window_end: str, limits: QualityLimits) -> DataFrame:
    """Add ``duration_s`` and ``reject_reason`` (NULL = accepted). Nothing is dropped."""
    duration = F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")
    d = df.withColumn("duration_s", duration)
    reason = (
        F.when(F.col("pickup_ts").isNull(), "null_pickup_ts")
        .when(F.col("dropoff_ts").isNull(), "null_dropoff_ts")
        .when((F.col("pickup_ts") < _ts(window_start)) | (F.col("pickup_ts") >= _ts(window_end)),
              "pickup_outside_window")
        .when(F.col("duration_s") <= 0, "non_positive_duration")
        .when(F.col("duration_s") > limits.max_trip_hours * 3600, "duration_exceeds_max")
        .when(F.col("pu_location_id").isNull(), "null_pickup_zone")
        .when(F.col("fare_amount").isNull(), "null_fare")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when(F.col("fare_amount") > limits.max_fare_amount, "fare_exceeds_max")
        .when(F.col("trip_distance").isNull(), "null_distance")
        .when(F.col("trip_distance") < 0, "negative_distance")
        .when(F.col("trip_distance") > limits.max_trip_distance_miles, "distance_exceeds_max")
    )
    d = d.withColumn("reject_reason", reason)

    # Exact duplicates among otherwise-valid rows: keep one, reject the rest.
    w = Window.partitionBy(F.col("reject_reason").isNull(), *[F.col(k) for k in _DEDUP_KEYS]) \
              .orderBy(F.col("pickup_ts"))
    d = d.withColumn("_rn", F.row_number().over(w))
    d = d.withColumn(
        "reject_reason",
        F.when(F.col("reject_reason").isNull() & (F.col("_rn") > 1), "duplicate")
         .otherwise(F.col("reject_reason")),
    ).drop("_rn")
    return d


def split_flagged(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (accepted, rejected). accepted + rejected == flagged, always."""
    accepted = (
        flagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        .withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_ts")))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
    )
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    return accepted, rejected


def aggregate_hourly(accepted: DataFrame) -> DataFrame:
    """One row per (pickup_hour, pu_location_id) that had at least one trip.

    Metric semantics (see docs/data_quality_rules.md):
      * fare columns use ``fare_amount`` = metered fare only (no tips, tolls,
        surcharges, taxes); ``sum_total_amount`` is all charges except cash tips.
      * p50/p90 are approximate (``percentile_approx``).
    """
    return accepted.groupBy("pickup_hour", "pu_location_id").agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.avg("fare_amount").alias("avg_fare"),
        F.percentile_approx("fare_amount", 0.5).alias("p50_fare"),
        F.percentile_approx("fare_amount", 0.9).alias("p90_fare"),
        F.sum("fare_amount").alias("sum_fare_amount"),
        F.sum("total_amount").alias("sum_total_amount"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        (F.avg("duration_s") / 60.0).alias("avg_duration_min"),
        F.avg("passenger_count").alias("avg_passenger_count"),
    )


def build_features(spark: SparkSession, hourly: DataFrame, window_start: str, window_end: str) -> DataFrame:
    """Forecasting-ready table on a DENSE hourly grid (zero-demand hours filled).

    * ``target_*`` columns are the quantities to predict.
    * Calendar features are known in advance.
    * History features only look at strictly earlier hours (lags / a rolling
      window that excludes the current row) => no target leakage.
    * The first 24 / 168 hours of the window have NULL lags (cold start).
    """
    hours = spark.range(1).select(
        F.explode(F.expr(
            f"sequence(timestamp'{window_start}', timestamp'{window_end}' - interval 1 hour, interval 1 hour)"
        )).alias("pickup_hour")
    )
    zones = hourly.select("pu_location_id").distinct()
    dense = (
        hours.crossJoin(zones)
        .join(hourly.select("pickup_hour", "pu_location_id", "trip_count", "avg_fare"),
              ["pickup_hour", "pu_location_id"], "left")
        .withColumn("trip_count", F.coalesce(F.col("trip_count"), F.lit(0)).cast("long"))
    )
    w = Window.partitionBy("pu_location_id").orderBy("pickup_hour")
    return dense.select(
        "pickup_hour",
        "pu_location_id",
        F.col("trip_count").alias("target_trip_count"),
        F.col("avg_fare").alias("target_avg_fare"),
        F.hour("pickup_hour").alias("hour_of_day"),
        (((F.dayofweek("pickup_hour") + 5) % 7) + 1).alias("day_of_week"),  # ISO 1=Mon..7=Sun
        F.month("pickup_hour").alias("month"),
        (((F.dayofweek("pickup_hour") + 5) % 7) + 1 >= 6).alias("is_weekend"),
        F.lag("trip_count", 1).over(w).alias("lag_1h_trip_count"),
        F.lag("trip_count", 24).over(w).alias("lag_24h_trip_count"),
        F.lag("trip_count", 168).over(w).alias("lag_168h_trip_count"),
        F.avg("trip_count").over(w.rowsBetween(-24, -1)).alias("rolling_mean_24h_trip_count"),
        F.lag("avg_fare", 1).over(w).alias("lag_1h_avg_fare"),
    )
