"""Object-storage abstraction used by the Spark job.

Production/Docker: ``S3Storage`` (MinIO via S3A).
Development & tests without Docker: set ``LOCAL_STORAGE_ROOT=/some/dir`` and the
same bucket/key layout is used on the local filesystem (``<root>/<bucket>/<key>``).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from src.common.config import Settings
from src.common.s3util import prefix_size as s3_prefix_size
from src.common.s3util import put_json as s3_put_json
from src.common.s3util import s3_client


class Storage:
    is_local = False

    def uri(self, bucket: str, *parts: str) -> str:               # pragma: no cover - interface
        raise NotImplementedError

    def size(self, bucket: str, key: str) -> int:                  # pragma: no cover
        raise NotImplementedError

    def prefix_size(self, bucket: str, prefix: str) -> tuple[int, int]:   # pragma: no cover
        raise NotImplementedError

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None:   # pragma: no cover
        raise NotImplementedError


class S3Storage(Storage):
    def __init__(self, settings: Settings):
        self.client = s3_client(settings)

    def uri(self, bucket: str, *parts: str) -> str:
        return "s3a://" + "/".join([bucket, *parts])

    def size(self, bucket: str, key: str) -> int:
        return int(self.client.head_object(Bucket=bucket, Key=key)["ContentLength"])

    def prefix_size(self, bucket: str, prefix: str) -> tuple[int, int]:
        return s3_prefix_size(self.client, bucket, prefix)

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None:
        s3_put_json(self.client, bucket, key, payload)


class LocalStorage(Storage):
    is_local = True

    def __init__(self, root: str):
        self.root = Path(root).resolve()

    def uri(self, bucket: str, *parts: str) -> str:
        return "file://" + str(self.root.joinpath(bucket, *parts))

    def size(self, bucket: str, key: str) -> int:
        return (self.root / bucket / key).stat().st_size

    def prefix_size(self, bucket: str, prefix: str) -> tuple[int, int]:
        base = self.root / bucket / prefix
        files = [p for p in base.rglob("*") if p.is_file()] if base.exists() else []
        return len(files), sum(p.stat().st_size for p in files)

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None:
        target = self.root / bucket / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, default=str))


def get_storage(settings: Settings) -> Storage:
    root = os.environ.get("LOCAL_STORAGE_ROOT")
    return LocalStorage(root) if root else S3Storage(settings)
