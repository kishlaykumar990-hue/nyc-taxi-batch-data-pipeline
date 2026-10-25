#!/usr/bin/env python3
"""Wait for the S3-compatible object store, then create the four data-layer buckets.

Image-agnostic (no `mc`, `curl` or shell needed in the MinIO image). Idempotent.
Env: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY.
"""
import os
import sys
import time

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

BUCKETS = ["taxi-raw", "taxi-cleaned", "taxi-rejected", "taxi-curated"]

s3 = boto3.client(
    "s3", endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"], aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                          connect_timeout=3, retries={"max_attempts": 1}))

for attempt in range(1, 61):                       # up to ~3 minutes
    try:
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        break
    except (BotoCoreError, ClientError) as exc:
        print(f"object store not ready (try {attempt}/60): {type(exc).__name__}", flush=True)
        time.sleep(3)
else:
    sys.exit("object store never became ready")

for name in BUCKETS:
    if name not in existing:
        s3.create_bucket(Bucket=name)
        print("created bucket", name)
    else:
        print("bucket exists", name)
print("object-store bootstrap complete")
