"""Integration tests for the Postgres publish step and role permissions.

Needs a Postgres initialised with sql/ (the Docker stack, or any local server).
Connection comes from PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD (+ ML_DB_USER /
ML_DB_PASSWORD for the read-only role). Skipped automatically if unreachable.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta

import psycopg2
import pytest
from psycopg2 import errors

from src.common.config import QualityLimits, Settings
from src.publish.postgres_publish import FEATURE_COLS, HOURLY_COLS, connect, publish

pytestmark = pytest.mark.integration


def _settings() -> Settings:
    return Settings(
        minio_endpoint="http://unused", minio_access_key="x", minio_secret_key="x",
        bucket_raw="r", bucket_cleaned="c", bucket_rejected="j", bucket_curated="u",
        pg_host=os.environ.get("PG_HOST", "localhost"), pg_port=int(os.environ.get("PG_PORT", "5433")),
        pg_db=os.environ.get("PG_DB", "taxi"), pg_user=os.environ.get("PG_USER", "etl_writer"),
        pg_password=os.environ.get("PG_PASSWORD", ""), tlc_base_url="", zone_lookup_url="",
        shuffle_partitions=2, sample_fraction=1.0, default_months="2031-01", limits=QualityLimits(),
    )


@pytest.fixture(scope="module")
def s() -> Settings:
    settings = _settings()
    try:
        connect(settings).close()
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")
    return settings


class Batch:
    """A unique 1-day window per test so tests cannot interfere with each other."""

    def __init__(self, year: int):
        self.year = year
        self.id = f"it_{year}_{uuid.uuid4().hex[:6]}"
        self.start = f"{year}-01-01 00:00:00"
        self.end = f"{year}-01-02 00:00:00"

    def hourly_rows(self, n_hours: int = 2):
        t0 = datetime(self.year, 1, 1)
        return [(t0 + timedelta(hours=h), 161, 3 + h, 10.0, 10.0, 12.0, 30.0, 42.0, 2.0, 11.0, 1.2)
                for h in range(n_hours)]

    def feature_rows(self):
        t0 = datetime(self.year, 1, 1)
        return [(t0 + timedelta(hours=h), 161, 3, 10.0, h, 1, 1, False, None, None, None, None, None)
                for h in range(24)]

    def run_info(self, hourly=2, features=24):
        return dict(source_rows=10, accepted_rows=8, rejected_rows=2, reject_breakdown={"negative_fare": 2},
                    hourly_rows=hourly, feature_rows=features, input_bytes=1, output_bytes=1,
                    processing_s=1.0, throughput_rows_per_s=10.0, config={"test": True})


def _stage(s: Settings, batch: Batch, hourly=None, features=None):
    hourly = batch.hourly_rows() if hourly is None else hourly
    features = batch.feature_rows() if features is None else features
    conn = connect(s)
    try:
        with conn, conn.cursor() as cur:
            for t in ("hourly_zone_metrics", "forecast_features", "taxi_zones"):
                cur.execute(f"TRUNCATE staging.{t}")
            ph = lambda cols: ", ".join(["%s"] * len(cols))  # noqa: E731
            cur.executemany(f"INSERT INTO staging.hourly_zone_metrics ({', '.join(HOURLY_COLS)}) VALUES ({ph(HOURLY_COLS)})", hourly)
            cur.executemany(f"INSERT INTO staging.forecast_features ({', '.join(FEATURE_COLS)}) VALUES ({ph(FEATURE_COLS)})", features)
            cur.execute("INSERT INTO staging.taxi_zones VALUES (161, 'Manhattan', 'Midtown Center', 'Yellow Zone')")
    finally:
        conn.close()


def _count(s: Settings, sql: str, *params) -> int:
    conn = connect(s)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    finally:
        conn.close()


def _publish(s, b, hourly=2, features=24):
    return publish(s, batch_id=b.id, months=[f"{b.year}-01"], window_start=b.start, window_end=b.end,
                   run=b.run_info(hourly, features))


def test_publish_loads_rows_and_records_the_run(s):
    b = Batch(2031)
    _stage(s, b)
    _publish(s, b)
    in_window = "WHERE pickup_hour >= %s AND pickup_hour < %s"
    assert _count(s, f"SELECT count(*) FROM serving.hourly_zone_metrics {in_window}", b.start, b.end) == 2
    assert _count(s, f"SELECT count(*) FROM serving.forecast_features {in_window}", b.start, b.end) == 24
    assert _count(s, "SELECT count(*) FROM ops.batch_runs WHERE batch_id=%s AND status='success'", b.id) == 1
    assert _count(s, "SELECT count(*) FROM serving.hourly_zone_metrics WHERE batch_id=%s", b.id) == 2


def test_rerun_is_idempotent_no_duplicates(s):
    b = Batch(2032)
    in_window = "WHERE pickup_hour >= %s AND pickup_hour < %s"
    for _ in range(3):
        _stage(s, b)
        _publish(s, b)
    assert _count(s, f"SELECT count(*) FROM serving.hourly_zone_metrics {in_window}", b.start, b.end) == 2
    assert _count(s, f"SELECT count(*) FROM serving.forecast_features {in_window}", b.start, b.end) == 24
    assert _count(s, "SELECT count(*) FROM ops.batch_runs WHERE batch_id=%s", b.id) == 1
    assert _count(s, "SELECT count(*) FROM serving.taxi_zones WHERE location_id=161") == 1


def test_rerun_replaces_stale_rows_inside_window_but_keeps_outside(s):
    b, other = Batch(2033), Batch(2034)
    for x in (b, other):
        _stage(s, x)
        _publish(s, x)
    _stage(s, b, hourly=b.hourly_rows(1))                  # reprocessed result has fewer rows
    _publish(s, b, hourly=1)
    assert _count(s, "SELECT count(*) FROM serving.hourly_zone_metrics WHERE pickup_hour >= %s AND pickup_hour < %s", b.start, b.end) == 1
    assert _count(s, "SELECT count(*) FROM serving.hourly_zone_metrics WHERE pickup_hour >= %s AND pickup_hour < %s", other.start, other.end) == 2


def test_partial_staging_is_refused_and_serving_is_unchanged(s):
    b = Batch(2035)
    _stage(s, b)
    _publish(s, b)
    _stage(s, b, hourly=b.hourly_rows(1))                  # staging has 1 row but the run claims 2
    with pytest.raises(RuntimeError, match="partial load"):
        _publish(s, b, hourly=2)
    assert _count(s, "SELECT count(*) FROM serving.hourly_zone_metrics WHERE pickup_hour >= %s AND pickup_hour < %s", b.start, b.end) == 2


def test_check_constraint_blocks_bad_run_record(s):
    b = Batch(2036)
    _stage(s, b)
    bad = b.run_info()
    bad["rejected_rows"] = 99                              # source != accepted + rejected
    with pytest.raises(psycopg2.errors.CheckViolation):
        publish(s, batch_id=b.id, months=["2036-01"], window_start=b.start, window_end=b.end, run=bad)
    assert _count(s, "SELECT count(*) FROM serving.hourly_zone_metrics WHERE pickup_hour >= %s AND pickup_hour < %s", b.start, b.end) == 0


# ------------------------------------------------------------ least privilege
def _ml_conn(s: Settings):
    user, pw = os.environ.get("ML_DB_USER", "ml_reader"), os.environ.get("ML_DB_PASSWORD")
    if not pw:
        pytest.skip("ML_DB_PASSWORD not set")
    return psycopg2.connect(host=s.pg_host, port=s.pg_port, dbname=s.pg_db, user=user, password=pw)


def test_ml_reader_can_read_serving_but_not_write_or_read_staging(s):
    conn = _ml_conn(s)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM serving.hourly_zone_metrics")
    cur.execute("SELECT count(*) FROM serving.v_hourly_zone_metrics_named")
    for stmt in ("INSERT INTO serving.taxi_zones VALUES (999,'x','x','x')",
                 "DELETE FROM serving.hourly_zone_metrics",
                 "SELECT * FROM staging.hourly_zone_metrics",
                 "SELECT * FROM ops.batch_runs"):
        with pytest.raises(errors.InsufficientPrivilege):
            cur.execute(stmt)
    conn.close()


def test_etl_writer_cannot_alter_schema_of_serving(s):
    conn = connect(s)
    conn.autocommit = True
    with conn.cursor() as cur:
        with pytest.raises(errors.InsufficientPrivilege):
            cur.execute("CREATE TABLE serving.should_not_exist (a int)")
        with pytest.raises(errors.InsufficientPrivilege):
            cur.execute("DROP TABLE serving.taxi_zones")
    conn.close()
