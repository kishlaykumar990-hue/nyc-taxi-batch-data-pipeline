"""Shared DAG helpers: default args, failure reporting, month resolution."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from src.common.months import latest_published_month, parse_months, quarter_months_ending

log = logging.getLogger(__name__)


def notify_failure(context) -> None:
    """Failure hook. Logs a structured, greppable record in the task log.

    No SMTP/Slack is configured in this local setup; wire an email/webhook
    callback here for production (documented limitation).
    """
    ti = context["task_instance"]
    log.error("PIPELINE_FAILURE dag=%s task=%s run=%s try=%s/%s exception=%r",
              ti.dag_id, ti.task_id, context.get("run_id"), ti.try_number, ti.max_tries + 1,
              context.get("exception"))


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


def months_from_conf(override: str | None) -> list[str] | None:
    """Manual override via trigger config / params (e.g. '2024-01,2024-02')."""
    override = (override or "").strip()
    return parse_months(override) if override else None


def scheduled_ingest_month(run_date: datetime) -> list[str]:
    """Monthly ingest: the newest month TLC is expected to have published."""
    return [latest_published_month(run_date.date())]


def scheduled_quarter_months(run_date: datetime) -> list[str]:
    """Quarterly processing: the three months ending at the newest published month."""
    return quarter_months_ending(latest_published_month(run_date.date()), 3)
