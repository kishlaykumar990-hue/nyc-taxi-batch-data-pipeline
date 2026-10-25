"""Month / window arithmetic shared by DAGs, ingestion and the Spark job.

Policy: TLC publishes monthly files with roughly a two-month delay, so a run on
(or after) the 15th of month M processes data up to month M-2. Windows are
half-open ``[start, end)`` strings in NYC wall-clock time (no time zone).
"""
from __future__ import annotations

import re
from datetime import date

PUBLICATION_LAG_MONTHS = 2
_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _index(ym: str) -> int:
    year, month = ym.split("-")
    return int(year) * 12 + int(month) - 1


def _from_index(idx: int) -> str:
    return f"{idx // 12:04d}-{idx % 12 + 1:02d}"


def parse_months(value: str | list[str]) -> list[str]:
    """Validate a comma-separated list: format YYYY-MM, unique, sorted, contiguous."""
    items = value.split(",") if isinstance(value, str) else list(value)
    months = [m.strip() for m in items if m and m.strip()]
    if not months:
        raise ValueError("At least one month (YYYY-MM) is required")
    for m in months:
        if not _MONTH_RE.match(m):
            raise ValueError(f"Invalid month '{m}' (expected YYYY-MM)")
    if len(set(months)) != len(months):
        raise ValueError("Duplicate months supplied")
    months = sorted(months)
    idx = [_index(m) for m in months]
    if idx != list(range(idx[0], idx[0] + len(idx))):
        raise ValueError(
            "Months must be contiguous: the hourly feature grid and lag features "
            f"would otherwise span missing data (got {months})"
        )
    return months


def window_for(months: list[str]) -> tuple[str, str]:
    """Half-open window [first day of first month, first day after last month)."""
    months = parse_months(months)
    start = f"{months[0]}-01 00:00:00"
    end = f"{_from_index(_index(months[-1]) + 1)}-01 00:00:00"
    return start, end


def batch_id_for(months: list[str]) -> str:
    """Deterministic id => re-running the same months overwrites, never duplicates."""
    months = parse_months(months)
    return f"yellow_{months[0]}_{months[-1]}"


def latest_published_month(run_date: date) -> str:
    """Newest month expected to be published on ``run_date`` (two-month lag)."""
    return _from_index(run_date.year * 12 + run_date.month - 1 - PUBLICATION_LAG_MONTHS)


def quarter_months_ending(end_month: str, length: int = 3) -> list[str]:
    """The ``length`` months ending at (and including) ``end_month``."""
    end = _index(end_month)
    return [_from_index(i) for i in range(end - length + 1, end + 1)]


def raw_key(ym: str) -> str:
    year, month = ym.split("-")
    return f"yellow/year={year}/month={month}/yellow_tripdata_{ym}.parquet"


def manifest_key(ym: str) -> str:
    return f"_manifests/yellow_tripdata_{ym}.json"


ZONE_LOOKUP_KEY = "reference/taxi_zone_lookup.csv"
