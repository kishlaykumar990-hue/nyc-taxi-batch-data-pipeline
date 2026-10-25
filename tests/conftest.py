"""Shared fixtures: a local Spark session and a tiny canonical-trip factory."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

CANONICAL_SCHEMA = T.StructType([
    T.StructField("pickup_ts", T.StringType()),
    T.StructField("dropoff_ts", T.StringType()),
    T.StructField("pu_location_id", T.IntegerType()),
    T.StructField("do_location_id", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("vendor_id", T.IntegerType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("source_month", T.StringType()),
])

DEFAULT_TRIP = dict(
    pickup_ts="2024-01-15 10:05:00", dropoff_ts="2024-01-15 10:25:00",
    pu_location_id=161, do_location_id=237, trip_distance=2.5,
    fare_amount=12.0, total_amount=16.0, vendor_id=1, passenger_count=1,
    source_month="2024-01",
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]").appName("unit-tests")
        .config("spark.sql.session.timeZone", "UTC")          # same policy as the job
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def make_trips(spark):
    """Build a canonical DataFrame from dicts overriding DEFAULT_TRIP."""
    def _make(*overrides: dict):
        rows = [tuple({**DEFAULT_TRIP, **o}[f.name] for f in CANONICAL_SCHEMA.fields) for o in overrides]
        df = spark.createDataFrame(rows, CANONICAL_SCHEMA)
        # strings -> timestamps inside Spark (session TZ = UTC): no Python-TZ shifts
        return (df.withColumn("pickup_ts", F.col("pickup_ts").cast("timestamp"))
                  .withColumn("dropoff_ts", F.col("dropoff_ts").cast("timestamp")))
    return _make
