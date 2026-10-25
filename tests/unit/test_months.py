from datetime import date

import pytest

from src.common.months import (
    batch_id_for, latest_published_month, parse_months, quarter_months_ending, raw_key, window_for,
)


def test_parse_sorts_and_validates():
    assert parse_months("2024-02, 2024-01") == ["2024-01", "2024-02"]


@pytest.mark.parametrize("bad", ["", "2024-13", "24-01", "2024-1", "2024-01,2024-01"])
def test_parse_rejects_invalid(bad):
    with pytest.raises(ValueError):
        parse_months(bad)


def test_parse_rejects_gap_between_months():
    with pytest.raises(ValueError, match="contiguous"):
        parse_months("2024-01,2024-03")


def test_window_is_half_open_and_crosses_year_end():
    assert window_for(["2024-12"]) == ("2024-12-01 00:00:00", "2025-01-01 00:00:00")
    assert window_for(["2024-01", "2024-02", "2024-03"]) == ("2024-01-01 00:00:00", "2024-04-01 00:00:00")


def test_batch_id_is_deterministic():
    assert batch_id_for(["2024-02", "2024-01"]) == "yellow_2024-01_2024-02"


def test_latest_published_month_applies_two_month_lag():
    assert latest_published_month(date(2024, 3, 15)) == "2024-01"
    assert latest_published_month(date(2024, 1, 15)) == "2023-11"  # year rollover


def test_quarter_months_ending():
    assert quarter_months_ending("2023-12") == ["2023-10", "2023-11", "2023-12"]
    assert quarter_months_ending("2024-02") == ["2023-12", "2024-01", "2024-02"]


def test_raw_key_layout():
    assert raw_key("2024-01") == "yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet"
