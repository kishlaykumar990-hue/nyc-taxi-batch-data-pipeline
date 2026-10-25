"""Raw-schema contract for the TLC yellow-taxi files.

TLC schemas drift between years (column casing, e.g. ``Airport_fee`` vs
``airport_fee``; new columns such as ``cbd_congestion_fee`` from 2025; integer
vs floating types). The pipeline therefore matches columns case-insensitively,
requires only the columns it actually uses, and ignores everything else.
"""
from __future__ import annotations

from typing import Iterable

# lower-case raw name -> canonical name
REQUIRED_RAW_COLUMNS = {
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "total_amount": "total_amount",
}

OPTIONAL_RAW_COLUMNS = {
    "vendorid": "vendor_id",
    "passenger_count": "passenger_count",
}

ZONE_LOOKUP_REQUIRED = {"locationid", "borough", "zone", "service_zone"}


class SchemaError(ValueError):
    """The raw file does not satisfy the minimum schema contract."""


def missing_required(columns: Iterable[str]) -> list[str]:
    """Return required raw columns absent from ``columns`` (case-insensitive)."""
    present = {c.lower() for c in columns}
    return sorted(c for c in REQUIRED_RAW_COLUMNS if c not in present)
