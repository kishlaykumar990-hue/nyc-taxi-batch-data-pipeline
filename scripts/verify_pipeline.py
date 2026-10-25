#!/usr/bin/env python3
"""End-to-end verification against the RUNNING stack.

Run from the host (service URLs default to the localhost port mappings) or from
inside a container. Exit code 0 only if every check passed.

    # from the host, with the stack up and `.env` loaded into the shell:
    python scripts/verify_pipeline.py --months 2024-01 --minio-endpoint http://localhost:9000 \
        --pg-host localhost --pg-port 5433
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.common.config import load_settings  # noqa: E402
from src.common.months import batch_id_for, parse_months  # noqa: E402
from src.verify.checks import check_database, check_object_store, check_services, default_urls  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--months")
    p.add_argument("--min-rows", type=int, default=int(os.environ.get("MIN_SOURCE_ROWS", "1000000")))
    p.add_argument("--minio-endpoint")
    p.add_argument("--pg-host")
    p.add_argument("--pg-port", type=int)
    p.add_argument("--skip-services", action="store_true", help="skip HTTP health checks")
    a = p.parse_args()

    if a.minio_endpoint:
        os.environ["MINIO_ENDPOINT"] = a.minio_endpoint
    if a.pg_host:
        os.environ["PG_HOST"] = a.pg_host
    if a.pg_port:
        os.environ["PG_PORT"] = str(a.pg_port)

    s = load_settings()
    months = parse_months(a.months or s.default_months)
    results = []
    if not a.skip_services:
        u = default_urls()
        results += check_services(u["minio"], u["spark"], u["airflow"])
    results += check_database(s, batch_id_for(months), a.min_rows)
    try:
        results += check_object_store(s, months)
    except Exception as exc:  # noqa: BLE001
        from src.verify.checks import CheckResult
        results.append(CheckResult("object store reachable", False, f"{type(exc).__name__}: {exc}"))

    for r in results:
        print(r)
    failed = [r for r in results if not r.ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
