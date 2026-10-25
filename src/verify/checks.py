"""Reusable end-to-end verification checks (used by the CLI script and the DAG).

Every check returns a CheckResult; nothing is reported as passed unless it was
actually evaluated against the live system.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import requests

from src.common.config import Settings
from src.common.months import manifest_key
from src.common.s3util import get_json, prefix_size, s3_client
from src.publish.postgres_publish import connect

EXPECTED_TABLES = [
    ("serving", "hourly_zone_metrics"), ("serving", "forecast_features"), ("serving", "taxi_zones"),
    ("ops", "batch_runs"), ("staging", "hourly_zone_metrics"), ("staging", "forecast_features"),
    ("staging", "taxi_zones"),
]


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str

    def __str__(self) -> str:
        return f"[{'PASS' if self.ok else 'FAIL'}] {self.name}: {self.detail}"


def check_http(name: str, url: str, timeout: int = 5) -> CheckResult:
    try:
        resp = requests.get(url, timeout=timeout)
        return CheckResult(name, resp.status_code < 400, f"{url} -> HTTP {resp.status_code}")
    except requests.RequestException as exc:
        return CheckResult(name, False, f"{url} unreachable ({type(exc).__name__})")


def check_services(minio_url: str, spark_url: str, airflow_url: str) -> list[CheckResult]:
    return [
        check_http("minio live", f"{minio_url}/minio/health/live"),
        check_http("spark master ui", spark_url),
        check_http("airflow health", f"{airflow_url}/health"),
    ]


def check_database(s: Settings, batch_id: str, min_source_rows: int) -> list[CheckResult]:
    results: list[CheckResult] = []
    try:
        conn = connect(s)
    except Exception as exc:  # noqa: BLE001
        return [CheckResult("postgres reachable", False, str(exc).splitlines()[0])]
    try:
        with conn.cursor() as cur:
            results.append(CheckResult("postgres reachable", True, f"{s.pg_host}:{s.pg_port}/{s.pg_db}"))

            cur.execute("SELECT table_schema, table_name FROM information_schema.tables")
            present = {tuple(r) for r in cur.fetchall()}
            missing = [f"{a}.{b}" for a, b in EXPECTED_TABLES if (a, b) not in present]
            results.append(CheckResult("expected tables exist", not missing, "all present" if not missing else f"missing {missing}"))
            if missing:
                return results

            cur.execute("""SELECT source_rows, accepted_rows, rejected_rows, hourly_rows, feature_rows,
                                  window_start, window_end, status
                           FROM ops.batch_runs WHERE batch_id = %s""", (batch_id,))
            row = cur.fetchone()
            if not row:
                results.append(CheckResult("batch recorded", False, f"no ops.batch_runs row for {batch_id}"))
                return results
            source, accepted, rejected, hourly_rows, feature_rows, w_start, w_end, status = row
            results.append(CheckResult("batch status", status == "success", f"{batch_id}: {status}"))
            results.append(CheckResult("assignment: >= 1,000,000 source records", source >= min_source_rows,
                                       f"source_rows={source} (min {min_source_rows})"))
            results.append(CheckResult("source = accepted + rejected", source == accepted + rejected,
                                       f"{source} vs {accepted}+{rejected}"))

            cur.execute("""SELECT count(*), COALESCE(sum(trip_count), 0) FROM serving.hourly_zone_metrics
                           WHERE pickup_hour >= %s AND pickup_hour < %s""", (w_start, w_end))
            n_hourly, trips = cur.fetchone()
            results.append(CheckResult("aggregates reconcile with accepted rows", trips == accepted,
                                       f"sum(trip_count)={trips} vs accepted={accepted}"))
            results.append(CheckResult("hourly rows published", n_hourly == hourly_rows, f"{n_hourly} vs {hourly_rows}"))

            cur.execute("""SELECT count(*), count(DISTINCT pu_location_id),
                                  count(*) FILTER (WHERE pickup_hour IS NULL OR pu_location_id IS NULL)
                           FROM serving.forecast_features WHERE pickup_hour >= %s AND pickup_hour < %s""", (w_start, w_end))
            n_feat, n_zones, n_null = cur.fetchone()
            hours = int((w_end - w_start).total_seconds() // 3600)
            results.append(CheckResult("feature grid is dense (zones x hours)", n_feat == n_zones * hours == feature_rows,
                                       f"{n_feat} rows = {n_zones} zones x {hours} h"))
            results.append(CheckResult("no null timestamps/zones in features", n_null == 0, f"{n_null} null keys"))

            cur.execute("""SELECT min(pickup_hour), max(pickup_hour) FROM serving.hourly_zone_metrics""")
            lo, hi = cur.fetchone()
            results.append(CheckResult("timestamps within batch window",
                                       lo is not None and lo >= w_start and hi < w_end, f"{lo} .. {hi}"))
    finally:
        conn.close()
    return results


def check_object_store(s: Settings, months: list[str]) -> list[CheckResult]:
    client = s3_client(s)
    results = []
    for ym in months:
        manifest = get_json(client, s.bucket_raw, manifest_key(ym))
        results.append(CheckResult(f"raw manifest {ym}", manifest is not None,
                                   f"{manifest['num_rows']} rows, sha256 {manifest['sha256'][:12]}..." if manifest else "missing"))
    for bucket, prefix in ((s.bucket_cleaned, "yellow/"), (s.bucket_rejected, "yellow/"),
                           (s.bucket_curated, "hourly_zone_metrics/"), (s.bucket_curated, "forecast_features/")):
        count, size = prefix_size(client, bucket, prefix)
        results.append(CheckResult(f"objects in s3://{bucket}/{prefix}", count > 0, f"{count} objects, {size} bytes"))
    return results


def default_urls() -> dict[str, str]:
    return {
        "minio": os.environ.get("VERIFY_MINIO_URL", "http://localhost:9000"),
        "spark": os.environ.get("VERIFY_SPARK_URL", "http://localhost:8080"),
        "airflow": os.environ.get("VERIFY_AIRFLOW_URL", "http://localhost:8081"),
    }
