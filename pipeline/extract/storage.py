"""Where raw JSON lands. Local disk by default, S3 when STORAGE_BACKEND=s3.

Both stores share the same tiny interface, so the rest of the pipeline does not
care which one is in use. Keys look like:

    raw/players/dt=2026-11-18/1174180_20261118T140501Z.json
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Protocol

from pipeline.config import Settings


class RawStore(Protocol):
    def write_json(self, key: str, obj: dict[str, Any]) -> str: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def read_json(self, key: str) -> dict[str, Any]: ...
    def materialize_locally(self, prefix: str, local_dir: Path) -> Path: ...


def _dumps(obj: dict[str, Any]) -> str:
    # One JSON object per line: Spark's default JSON reader expects this.
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n"


class LocalStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def write_json(self, key: str, obj: dict[str, Any]) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_dumps(obj), encoding="utf-8")
        return str(path)

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*.json"))

    def read_json(self, key: str) -> dict[str, Any]:
        return json.loads((self.root / key).read_text(encoding="utf-8"))

    def materialize_locally(self, prefix: str, local_dir: Path) -> Path:
        # Already on disk - Spark can read it where it is.
        return self.root / prefix


class S3Store:
    def __init__(self, bucket: str, prefix: str = "", region: str | None = None, client=None) -> None:
        if not bucket:
            raise ValueError("S3_BUCKET must be set when STORAGE_BACKEND=s3")
        import boto3  # imported here so local runs don't need AWS set up

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        # Credentials come from the normal AWS chain (env vars, ~/.aws, or an IAM role).
        self.s3 = client or boto3.client("s3", region_name=region)

    def _full(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def write_json(self, key: str, obj: dict[str, Any]) -> str:
        full = self._full(key)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=full,
            Body=_dumps(obj).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{full}"

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        strip = len(self.prefix) + 1 if self.prefix else 0
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._full(prefix)):
            for item in page.get("Contents", []):
                if item["Key"].endswith(".json"):
                    keys.append(item["Key"][strip:])
        return sorted(keys)

    def read_json(self, key: str) -> dict[str, Any]:
        body = self.s3.get_object(Bucket=self.bucket, Key=self._full(key))["Body"].read()
        return json.loads(body)

    def materialize_locally(self, prefix: str, local_dir: Path) -> Path:
        """Copy one partition down so local Spark can read it.

        Fine for a few dozen small files a day. Reading s3a:// directly from Spark
        (hadoop-aws) is on the roadmap.
        """
        target = Path(local_dir) / prefix
        if target.exists():
            shutil.rmtree(target)
        for key in self.list_keys(prefix):
            dest = Path(local_dir) / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.bucket, self._full(key), str(dest))
        return target


def get_store(settings: Settings) -> RawStore:
    if settings.storage_backend == "s3":
        return S3Store(settings.s3_bucket, settings.s3_prefix, settings.aws_region)
    if settings.storage_backend == "local":
        return LocalStore(settings.data_dir)
    raise ValueError(f"Unknown STORAGE_BACKEND: {settings.storage_backend!r}")
