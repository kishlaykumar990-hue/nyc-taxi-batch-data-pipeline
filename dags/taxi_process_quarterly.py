"""Quarterly batch processing + delivery to PostgreSQL (consumed by the ML app).

Schedule: 04:00 on the 15th of Feb/May/Aug/Nov, i.e. once per quarter, after TLC
has published the previous quarter's last month (2-month lag). Manual / demo run:
trigger with config {"months": "2024-01"} (contiguous months only).

Re-runs are safe: object-store partitions are overwritten and the Postgres
publish replaces the batch window inside one transaction.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from taxi_common import DEFAULT_ARGS, months_from_conf, scheduled_quarter_months


@dag(
    dag_id="taxi_process_quarterly",
    schedule="0 4 15 2,5,8,11 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                 # staging tables are shared: never two batches at once
    default_args=DEFAULT_ARGS,
    params={"months": Param("", type="string", description="Optional contiguous YYYY-MM list override"),
            "sample_fraction": Param(1.0, type="number", minimum=0.001, maximum=1.0)},
    tags=["nyc-taxi", "processing"],
    doc_md=__doc__,
)
def taxi_process_quarterly():
    @task
    def resolve_months(params=None, data_interval_end=None) -> list[str]:
        return months_from_conf(params.get("months")) or scheduled_quarter_months(data_interval_end)

    @task(retries=6, retry_delay=timedelta(minutes=10))
    def wait_for_raw_data(months: list[str]) -> None:
        """Raises (=> retried) until every month has a manifest in the raw layer."""
        from src.common.config import load_settings
        from src.common.months import manifest_key
        from src.common.s3util import get_json, s3_client
        s, client = load_settings(), s3_client(load_settings())
        missing = [m for m in months if get_json(client, s.bucket_raw, manifest_key(m)) is None]
        if missing:
            raise RuntimeError(f"Raw data not ingested yet for {missing}; run taxi_ingest_monthly first")

    spark_batch = BashOperator(
        task_id="spark_batch",
        bash_command=("bash /opt/project/scripts/spark_submit.sh "
                      "--months \"{{ ti.xcom_pull(task_ids='resolve_months') | join(',') }}\" "
                      "--sample-fraction {{ params.sample_fraction }}"),
        execution_timeout=timedelta(hours=3),
    )

    @task
    def verify_publication(months: list[str]) -> None:
        """Fail the run if reconciliation checks do not pass."""
        from src.common.config import load_settings
        from src.common.months import batch_id_for
        from src.verify.checks import check_database
        import os
        min_rows = int(os.environ.get("MIN_SOURCE_ROWS", "1000000"))
        results = check_database(load_settings(), batch_id_for(months), min_rows)
        for r in results:
            print(r)
        failed = [r for r in results if not r.ok]
        if failed:
            raise RuntimeError("Verification failed: " + "; ".join(str(r) for r in failed))

    months = resolve_months()
    wait_for_raw_data(months) >> spark_batch >> verify_publication(months)


taxi_process_quarterly()
