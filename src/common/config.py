"""Environment-driven configuration (12-factor). No secrets live in code."""
from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    """Raised when a required environment variable is missing."""


@dataclass(frozen=True)
class QualityLimits:
    """Plausibility limits used by the cleaning rules (configurable via env)."""

    max_trip_hours: float = 24.0
    max_trip_distance_miles: float = 500.0
    max_fare_amount: float = 1000.0


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    bucket_raw: str
    bucket_cleaned: str
    bucket_rejected: str
    bucket_curated: str
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    tlc_base_url: str
    zone_lookup_url: str
    shuffle_partitions: int
    sample_fraction: float
    default_months: str
    limits: QualityLimits


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    """Build Settings from the environment; fails fast on missing secrets."""
    return Settings(
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=_required("MINIO_ACCESS_KEY"),
        minio_secret_key=_required("MINIO_SECRET_KEY"),
        bucket_raw=os.environ.get("BUCKET_RAW", "taxi-raw"),
        bucket_cleaned=os.environ.get("BUCKET_CLEANED", "taxi-cleaned"),
        bucket_rejected=os.environ.get("BUCKET_REJECTED", "taxi-rejected"),
        bucket_curated=os.environ.get("BUCKET_CURATED", "taxi-curated"),
        pg_host=os.environ.get("PG_HOST", "postgres"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        pg_db=os.environ.get("PG_DB", "taxi"),
        pg_user=_required("PG_USER"),
        pg_password=_required("PG_PASSWORD"),
        tlc_base_url=os.environ.get(
            "TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
        ).rstrip("/"),
        zone_lookup_url=os.environ.get(
            "TLC_ZONE_LOOKUP_URL",
            "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        ),
        shuffle_partitions=int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16")),
        sample_fraction=float(os.environ.get("SAMPLE_FRACTION", "1.0")),
        default_months=os.environ.get("PIPELINE_MONTHS", "2024-01"),
        limits=QualityLimits(
            max_trip_hours=float(os.environ.get("MAX_TRIP_HOURS", "24")),
            max_trip_distance_miles=float(os.environ.get("MAX_TRIP_DISTANCE_MILES", "500")),
            max_fare_amount=float(os.environ.get("MAX_FARE_AMOUNT", "1000")),
        ),
    )
