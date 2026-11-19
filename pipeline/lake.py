"""Daily export of the warehouse to the S3 data lake, checked with Athena.

Design (see docs/adr/0005-aws-data-lake.md):

* **Daily batch, D-1.** Once a day, each fact table's rows for the previous (complete) UTC
  day are written from Postgres to ONE Parquet file and uploaded. About 7 S3 PUTs a day,
  so storage and request costs round to zero. The hourly runs don't otherwise touch AWS.
* **The lake is a second destination, not a dependency.** Collection, quality checks and
  the Postgres load all happen first; if AWS is unavailable only this step fails.
* **Idempotent.** A day is written to a fixed key (``part-0.parquet``) and then marked done
  with a small marker object; re-running overwrites the same object and never duplicates.
* **Schema-locked.** Column names and Athena types come from ``pipeline/lake_schema.json``,
  generated from the Spark job by ``infra/aws/build_template.py`` alongside the Glue
  table definitions, so Parquet files, Glue and Spark always agree.

Layout (Hive-style date partitions, read by Athena with partition projection):

    s3://<bucket>/<prefix>/clean/<table>/snapshot_date=YYYY-MM-DD/part-0.parquet
    s3://<bucket>/<prefix>/_exports/YYYY-MM-DD.done

Usage:
    python -m pipeline.lake export [--since YYYY-MM-DD]   # every missing complete day
    python -m pipeline.lake verify  --date YYYY-MM-DD      # Athena reads that day back
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from pipeline.config import PROJECT_ROOT, Settings, get_settings

log = logging.getLogger(__name__)

# Fact tables partitioned by snapshot_date: the ones Athena reads. Small dimensions
# (apps, names) and derived tables (forecasts, rollups) stay in Postgres.
LAKE_TABLES = (
    "player_count_snapshots",
    "player_counts_daily",
    "app_prices",
    "review_summary",
    "review_themes",
    "twitch_viewer_snapshots",
    "market_snapshots",
)

SCHEMA_FILE = PROJECT_ROOT / "pipeline" / "lake_schema.json"
FIRST_DAY = date(2026, 11, 18)  # first day of collection; nothing to export before it
MAX_DAYS_PER_RUN = 14  # bound a catch-up after a long outage


def load_lake_schema(path: Path = SCHEMA_FILE) -> dict[str, list[tuple[str, str]]]:
    """{table: [(column, athena_type), ...]} - excludes the snapshot_date partition column."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {table: [(c["name"], c["type"]) for c in cols] for table, cols in raw["tables"].items()}


def arrow_type(hive: str) -> Any:
    import pyarrow as pa

    simple = {
        "int": pa.int32(),
        "bigint": pa.int64(),
        "smallint": pa.int16(),
        "tinyint": pa.int8(),
        "string": pa.string(),
        "boolean": pa.bool_(),
        "double": pa.float64(),
        "float": pa.float32(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us", tz="UTC"),
    }
    if hive in simple:
        return simple[hive]
    if hive.startswith("decimal("):
        precision, scale = (int(x) for x in hive[8:-1].split(","))
        return pa.decimal128(precision, scale)
    raise ValueError(f"unsupported Athena type {hive}")


def _coerce(value: Any, hive: str) -> Any:
    """Postgres values -> what the Parquet column type expects (e.g. NUMERIC -> float for double)."""
    if value is None:
        return None
    if hive in ("int", "bigint", "smallint", "tinyint"):
        return int(value)
    if hive in ("double", "float"):
        return float(value)
    if hive.startswith("decimal("):
        scale = int(hive[8:-1].split(",")[1])
        return Decimal(value).quantize(Decimal(1).scaleb(-scale))
    if hive == "timestamp" and isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def table_to_parquet(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([pa.field(name, arrow_type(hive)) for name, hive in columns])
    data = {name: [_coerce(r.get(name), hive) for r in rows] for name, hive in columns}
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pydict(data, schema=schema), buf, compression="snappy")
    return buf.getvalue()


@dataclass
class ExportReport:
    days: list[date] = field(default_factory=list)
    files: int = 0
    rows: int = 0
    bytes: int = 0


def _client(service: str, settings: Settings) -> Any:
    import boto3  # only needed when the lake is used

    return boto3.client(service, region_name=settings.aws_region or None)


def _done_days(s3: Any, bucket: str, base: str) -> set[date]:
    done = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{base}/_exports/"):
        for item in page.get("Contents", []):
            stem = item["Key"].rsplit("/", 1)[-1].removesuffix(".done")
            try:
                done.add(date.fromisoformat(stem))
            except ValueError:
                continue
    return done


def days_to_export(done: set[date], today: date, since: date = FIRST_DAY) -> list[date]:
    """Complete days (before today, UTC) not exported yet, oldest first, capped per run."""
    candidates = [since + timedelta(days=i) for i in range((today - since).days)]
    return [d for d in candidates if d not in done][:MAX_DAYS_PER_RUN]


def export_day(engine: Engine, s3: Any, settings: Settings, day: date, schema: dict) -> tuple[int, int, int]:
    """Write every lake table's rows for `day` to S3, then the done-marker. Returns (files, rows, bytes)."""
    base = settings.s3_prefix.strip("/")
    files = rows_total = size = 0
    with engine.connect() as conn:
        for table, columns in schema.items():
            cols = ", ".join(name for name, _ in columns)  # names come from our own schema file
            result = conn.execute(text(f"SELECT {cols} FROM {table} WHERE snapshot_date = :d"), {"d": day})
            rows = [dict(r._mapping) for r in result]
            if not rows:
                continue  # no file at all: Athena treats a missing partition as zero rows
            body = table_to_parquet(rows, columns)
            key = f"{base}/clean/{table}/snapshot_date={day.isoformat()}/part-0.parquet"
            s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType="application/vnd.apache.parquet")
            files, rows_total, size = files + 1, rows_total + len(rows), size + len(body)
    marker = json.dumps(
        {
            "day": day.isoformat(),
            "files": files,
            "rows": rows_total,
            "exported_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=f"{base}/_exports/{day.isoformat()}.done",
        Body=marker.encode(),
        ContentType="application/json",
    )
    return files, rows_total, size


def export_missing(
    engine: Engine, settings: Settings, today: date | None = None, since: date = FIRST_DAY, s3: Any | None = None
) -> ExportReport:
    if not settings.s3_bucket:
        raise ValueError("S3_BUCKET is not set")
    s3 = s3 or _client("s3", settings)
    today = today or datetime.now(timezone.utc).date()
    schema = load_lake_schema()
    report = ExportReport()
    for day in days_to_export(_done_days(s3, settings.s3_bucket, settings.s3_prefix.strip("/")), today, since):
        files, rows, size = export_day(engine, s3, settings, day, schema)
        report.days.append(day)
        report.files, report.rows, report.bytes = report.files + files, report.rows + rows, report.bytes + size
        log.info("Lake: exported %s - %d tables, %d rows, %.1f KB", day, files, rows, size / 1024)
    if not report.days:
        log.info("Lake: nothing to export (every complete day is already in S3)")
    return report


VERIFY_SQL = """
SELECT COUNT(*) AS snapshots, COUNT(DISTINCT app_id) AS games
FROM player_count_snapshots
WHERE snapshot_date = DATE '{day}'
"""


def run_athena(settings: Settings, sql: str, athena: Any | None = None, timeout_s: int = 120) -> list[list[str]]:
    """Run one query in the project's workgroup and return the result rows (header excluded)."""
    athena = athena or _client("athena", settings)
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": settings.athena_database},
        WorkGroup=settings.athena_workgroup,
    )["QueryExecutionId"]
    deadline = time.monotonic() + timeout_s
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"Athena query {qid} still {status['State']} after {timeout_s}s")
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {qid} {status['State']}: {status.get('StateChangeReason', '')}")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows[1:]]


def verify_day(settings: Settings, day: date, athena: Any | None = None) -> tuple[int, int]:
    """End-to-end check that Athena can read a day back from the lake."""
    snapshots, games = (int(x) for x in run_athena(settings, VERIFY_SQL.format(day=day.isoformat()), athena)[0])
    if snapshots == 0:
        raise RuntimeError(f"Athena found no player snapshots for {day} in the lake")
    log.info("Lake verified with Athena: %d snapshots for %d games on %s", snapshots, games, day)
    return snapshots, games


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="python -m pipeline.lake", description="Daily export to the S3 data lake.")
    p.add_argument("action", choices=["export", "verify"])
    p.add_argument("--since", type=date.fromisoformat, default=FIRST_DAY)
    p.add_argument("--date", type=date.fromisoformat, help="verify: which day")
    args = p.parse_args(argv)
    settings = get_settings()
    if args.action == "export":
        from pipeline.load.postgres_loader import get_engine

        report = export_missing(get_engine(settings.database_url), settings, since=args.since)
        if report.days:  # one tiny Athena query per run that exported something
            verify_day(settings, report.days[-1])
        return 0
    if not args.date:
        p.error("verify needs --date")
    verify_day(settings, args.date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
