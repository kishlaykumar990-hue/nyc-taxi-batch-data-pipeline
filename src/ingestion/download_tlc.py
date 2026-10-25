"""Ingest official TLC yellow-taxi Parquet files into the MinIO *raw* layer.

* Idempotent: a month is skipped when its manifest matches the remote size.
* Verified: size vs Content-Length, Parquet readability, required columns.
* Lineage: a manifest (url, bytes, sha256, rows, columns, timestamp) is stored
  next to the data under ``_manifests/``.

Usage (inside the Airflow scheduler container):
    python -m src.ingestion.download_tlc --months 2024-01,2024-02 [--force]
    python -m src.ingestion.download_tlc --zones-only
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import tempfile
from datetime import datetime, timezone

import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.common.config import Settings, load_settings
from src.common.months import ZONE_LOOKUP_KEY, manifest_key, parse_months, raw_key
from src.common.s3util import get_json, object_exists, put_json, s3_client
from src.common.schema import ZONE_LOOKUP_REQUIRED, SchemaError, missing_required

log = logging.getLogger("ingestion")


def http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "HEAD"))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def source_url(s: Settings, ym: str) -> str:
    return f"{s.tlc_base_url}/yellow_tripdata_{ym}.parquet"


def remote_size(session: requests.Session, url: str) -> int | None:
    """Content-Length of the source file, or None if the server does not say."""
    resp = session.head(url, allow_redirects=True, timeout=30)
    resp.raise_for_status()
    length = resp.headers.get("Content-Length")
    return int(length) if length else None


def check_available(s: Settings, months: list[str], session: requests.Session | None = None) -> dict[str, int | None]:
    """Fail early (before any download) if a requested month is not published."""
    session = session or http_session()
    sizes = {}
    for ym in months:
        try:
            sizes[ym] = remote_size(session, source_url(s, ym))
        except requests.HTTPError as exc:
            raise RuntimeError(f"Month {ym} is not available at {source_url(s, ym)}: {exc}") from exc
    return sizes


def download_to_file(session: requests.Session, url: str, dest: str) -> tuple[int, str]:
    """Stream to disk, returning (bytes, sha256); raises if the transfer is truncated."""
    digest, total = hashlib.sha256(), 0
    with session.get(url, stream=True, timeout=(15, 120)) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                digest.update(chunk)
                total += len(chunk)
        expected = resp.headers.get("Content-Length")
        if expected and int(expected) != total:
            raise IOError(f"Truncated download: expected {expected} bytes, received {total}")
    return total, digest.hexdigest()


def ingest_month(s: Settings, ym: str, *, client=None, session=None, force: bool = False) -> dict:
    client = client or s3_client(s)
    session = session or http_session()
    url, key, mkey = source_url(s, ym), raw_key(ym), manifest_key(ym)

    size = remote_size(session, url)
    existing = get_json(client, s.bucket_raw, mkey)
    if (not force and existing and size is not None and existing.get("bytes") == size
            and object_exists(client, s.bucket_raw, key)):
        log.info("%s: already ingested (%s bytes) - skipping", ym, size)
        return {**existing, "status": "skipped"}

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, f"yellow_tripdata_{ym}.parquet")
        n_bytes, sha = download_to_file(session, url, path)

        parquet = pq.ParquetFile(path)                      # raises if not valid Parquet
        columns = parquet.schema_arrow.names
        missing = missing_required(columns)
        if missing:
            raise SchemaError(f"{ym}: required columns missing from source file: {missing}")
        rows = parquet.metadata.num_rows
        if rows == 0:
            raise ValueError(f"{ym}: source file contains no rows")

        client.upload_file(path, s.bucket_raw, key)

    manifest = {
        "month": ym, "source_url": url, "bucket": s.bucket_raw, "key": key,
        "bytes": n_bytes, "sha256": sha, "num_rows": rows, "columns": columns,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    put_json(client, s.bucket_raw, mkey, manifest)
    log.info("%s: ingested %s rows / %s bytes -> s3://%s/%s", ym, rows, n_bytes, s.bucket_raw, key)
    return {**manifest, "status": "ingested"}


def ingest_zone_lookup(s: Settings, *, client=None, session=None) -> dict:
    client = client or s3_client(s)
    session = session or http_session()
    resp = session.get(s.zone_lookup_url, timeout=60)
    resp.raise_for_status()
    header = resp.text.splitlines()[0].replace('"', "").lower().split(",")
    if not ZONE_LOOKUP_REQUIRED.issubset(set(header)):
        raise SchemaError(f"Unexpected taxi_zone_lookup header: {header}")
    client.put_object(Bucket=s.bucket_raw, Key=ZONE_LOOKUP_KEY, Body=resp.content, ContentType="text/csv")
    log.info("zone lookup stored (%s bytes)", len(resp.content))
    return {"key": ZONE_LOOKUP_KEY, "bytes": len(resp.content), "status": "ingested"}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", help="comma-separated YYYY-MM (contiguous). Default: $PIPELINE_MONTHS")
    parser.add_argument("--force", action="store_true", help="re-download even if the manifest matches")
    parser.add_argument("--zones-only", action="store_true")
    args = parser.parse_args()

    s = load_settings()
    client, session = s3_client(s), http_session()
    ingest_zone_lookup(s, client=client, session=session)
    if args.zones_only:
        return
    months = parse_months(args.months or s.default_months)
    check_available(s, months, session)
    for ym in months:
        ingest_month(s, ym, client=client, session=session, force=args.force)


if __name__ == "__main__":
    main()
