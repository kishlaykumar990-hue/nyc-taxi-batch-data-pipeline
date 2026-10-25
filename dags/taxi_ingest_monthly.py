"""Monthly ingestion: official TLC Parquet -> MinIO raw layer.

Schedule: 02:00 on the 15th of every month. TLC publishes with ~2 months delay,
so the run ingests (run month - 2). Manual backfill / demo: trigger with config
{"months": "2024-01,2024-02"}.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from taxi_common import DEFAULT_ARGS, months_from_conf, scheduled_ingest_month


@dag(
    dag_id="taxi_ingest_monthly",
    schedule="0 2 15 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"months": Param("", type="string", description="Optional YYYY-MM list override"),
            "force": Param(False, type="boolean", description="Re-download even if unchanged")},
    tags=["nyc-taxi", "ingestion"],
    doc_md=__doc__,
)
def taxi_ingest_monthly():
    @task
    def resolve_months(params=None, data_interval_end=None) -> list[str]:
        return months_from_conf(params.get("months")) or scheduled_ingest_month(data_interval_end)

    @task
    def check_source_availability(months: list[str]) -> None:
        from src.common.config import load_settings
        from src.ingestion.download_tlc import check_available
        check_available(load_settings(), months)

    @task
    def ingest_zone_lookup() -> None:
        from src.common.config import load_settings
        from src.ingestion.download_tlc import ingest_zone_lookup as run
        run(load_settings())

    @task
    def ingest_months(months: list[str], params=None) -> list[dict]:
        from src.common.config import load_settings
        from src.common.s3util import s3_client
        from src.ingestion.download_tlc import http_session, ingest_month
        s, client, session = load_settings(), None, http_session()
        client = s3_client(s)
        return [{k: v for k, v in ingest_month(s, ym, client=client, session=session,
                                               force=bool(params.get("force"))).items() if k != "columns"}
                for ym in months]

    months = resolve_months()
    available = check_source_availability(months)
    available >> ingest_months(months)
    available >> ingest_zone_lookup()


taxi_ingest_monthly()
