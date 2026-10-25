"""Thin boto3 helpers for MinIO (S3-compatible)."""
from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.common.config import Settings


def s3_client(s: Settings):
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_access_key,
        aws_secret_access_key=s.minio_secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 5, "mode": "standard"}),
    )


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def object_size(client, bucket: str, key: str) -> int:
    return int(client.head_object(Bucket=bucket, Key=key)["ContentLength"])


def put_json(client, bucket: str, key: str, payload: dict[str, Any]) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload, indent=2, default=str).encode(),
                      ContentType="application/json")


def get_json(client, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return json.loads(body)


def prefix_size(client, bucket: str, prefix: str) -> tuple[int, int]:
    """Return (object_count, total_bytes) under a prefix."""
    count = total = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            count += 1
            total += obj["Size"]
    return count, total
