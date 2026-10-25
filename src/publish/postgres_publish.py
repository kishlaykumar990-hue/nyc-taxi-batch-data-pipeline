"""Publish staged Spark output into the serving schema - atomically and idempotently.

Spark writes ``staging.*`` (JDBC, truncate + insert). This module then, inside a
SINGLE transaction, replaces the batch's time window in ``serving.*`` and
records the run in ``ops.batch_runs``. Re-running a batch therefore yields the
same final state (delete window + insert), and a failure rolls everything back.
"""
from __future__ import annotations

import time
from typing import Any

import psycopg2
from psycopg2.extras import Json

from src.common.config import Settings

HOURLY_COLS = [
    "pickup_hour", "pu_location_id", "trip_count", "avg_fare", "p50_fare", "p90_fare",
    "sum_fare_amount", "sum_total_amount", "avg_trip_distance", "avg_duration_min", "avg_passenger_count",
]
FEATURE_COLS = [
    "pickup_hour", "pu_location_id", "target_trip_count", "target_avg_fare", "hour_of_day", "day_of_week",
    "month", "is_weekend", "lag_1h_trip_count", "lag_24h_trip_count", "lag_168h_trip_count",
    "rolling_mean_24h_trip_count", "lag_1h_avg_fare",
]


def connect(s: Settings):
    return psycopg2.connect(host=s.pg_host, port=s.pg_port, dbname=s.pg_db, user=s.pg_user,
                            password=s.pg_password, connect_timeout=10)


def _replace_sql(table: str, cols: list[str]) -> list[tuple[str, tuple]]:
    col_list = ", ".join(cols)
    return [
        (f"DELETE FROM serving.{table} WHERE pickup_hour >= %(ws)s AND pickup_hour < %(we)s", ()),
        (f"INSERT INTO serving.{table} ({col_list}, batch_id) SELECT {col_list}, %(bid)s FROM staging.{table}", ()),
    ]


def publish(s: Settings, *, batch_id: str, months: list[str], window_start: str, window_end: str,
            run: dict[str, Any]) -> float:
    """Replace the batch window in serving.* and upsert the run record. Returns seconds taken.

    ``run`` must carry: source_rows, accepted_rows, rejected_rows, reject_breakdown,
    hourly_rows, feature_rows, and optionally input_bytes, output_bytes, processing_s,
    throughput_rows_per_s, config.
    """
    params = {"ws": window_start, "we": window_end, "bid": batch_id}
    started = time.time()
    conn = connect(s)
    try:
        with conn:  # one transaction: commit on success, rollback on any exception
            with conn.cursor() as cur:
                # Guard: staging must hold exactly what this batch produced.
                for table, expected_key in (("hourly_zone_metrics", "hourly_rows"),
                                            ("forecast_features", "feature_rows")):
                    cur.execute(f"SELECT count(*) FROM staging.{table}")
                    staged = cur.fetchone()[0]
                    if staged != run[expected_key]:
                        raise RuntimeError(
                            f"staging.{table} has {staged} rows but the batch produced {run[expected_key]}; "
                            "refusing to publish a partial load")

                for table, cols in (("hourly_zone_metrics", HOURLY_COLS), ("forecast_features", FEATURE_COLS)):
                    for sql, _ in _replace_sql(table, cols):
                        cur.execute(sql, params)

                cur.execute(
                    """INSERT INTO serving.taxi_zones (location_id, borough, zone, service_zone)
                       SELECT location_id, borough, zone, service_zone FROM staging.taxi_zones
                       ON CONFLICT (location_id) DO UPDATE SET
                         borough = EXCLUDED.borough, zone = EXCLUDED.zone, service_zone = EXCLUDED.service_zone""")

                cur.execute(
                    """INSERT INTO ops.batch_runs
                         (batch_id, months, window_start, window_end, source_rows, accepted_rows, rejected_rows,
                          reject_breakdown, hourly_rows, feature_rows, input_bytes, output_bytes, processing_s,
                          throughput_rows_per_s, config, status, finished_at)
                       VALUES (%(bid)s, %(months)s, %(ws)s, %(we)s, %(source_rows)s, %(accepted_rows)s,
                               %(rejected_rows)s, %(reject_breakdown)s, %(hourly_rows)s, %(feature_rows)s,
                               %(input_bytes)s, %(output_bytes)s, %(processing_s)s, %(throughput)s,
                               %(config)s, 'success', now())
                       ON CONFLICT (batch_id) DO UPDATE SET
                         months = EXCLUDED.months, window_start = EXCLUDED.window_start,
                         window_end = EXCLUDED.window_end, source_rows = EXCLUDED.source_rows,
                         accepted_rows = EXCLUDED.accepted_rows, rejected_rows = EXCLUDED.rejected_rows,
                         reject_breakdown = EXCLUDED.reject_breakdown, hourly_rows = EXCLUDED.hourly_rows,
                         feature_rows = EXCLUDED.feature_rows, input_bytes = EXCLUDED.input_bytes,
                         output_bytes = EXCLUDED.output_bytes, processing_s = EXCLUDED.processing_s,
                         throughput_rows_per_s = EXCLUDED.throughput_rows_per_s, config = EXCLUDED.config,
                         status = EXCLUDED.status, finished_at = EXCLUDED.finished_at, publish_s = NULL""",
                    {**params, "months": months,
                     "source_rows": run["source_rows"], "accepted_rows": run["accepted_rows"],
                     "rejected_rows": run["rejected_rows"], "reject_breakdown": Json(run["reject_breakdown"]),
                     "hourly_rows": run["hourly_rows"], "feature_rows": run["feature_rows"],
                     "input_bytes": run.get("input_bytes"), "output_bytes": run.get("output_bytes"),
                     "processing_s": run.get("processing_s"), "throughput": run.get("throughput_rows_per_s"),
                     "config": Json(run.get("config", {}))})
        elapsed = time.time() - started
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ops.batch_runs SET publish_s = %s WHERE batch_id = %s", (elapsed, batch_id))
        return elapsed
    finally:
        conn.close()
