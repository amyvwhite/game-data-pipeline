"""Load step: upsert the clean tables into PostgreSQL with SQLAlchemy.

Upserts (INSERT ... ON CONFLICT DO UPDATE) make the load idempotent: running the
same day twice leaves exactly one copy of the data. Everything for one run goes
in a single transaction, so a failure leaves the database as it was.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, create_engine, text

from pipeline.config import PROJECT_ROOT

log = logging.getLogger(__name__)

SCHEMA_FILE = PROJECT_ROOT / "sql" / "schema.sql"

# Values to use when a row predates a newer column (e.g. data saved before is_purchasable existed).
COLUMN_DEFAULTS = {"is_purchasable": True}

# table -> (primary key columns, all columns we load). Order matters: apps first.
TABLES: dict[str, tuple[list[str], list[str]]] = {
    "apps": (
        ["app_id"],
        ["app_id", "name", "app_type", "developers", "publishers", "release_date_text", "coming_soon"],
    ),
    "player_count_snapshots": (
        ["app_id", "captured_at"],
        ["app_id", "captured_at", "snapshot_date", "player_count"],
    ),
    "player_counts_daily": (
        ["app_id", "snapshot_date"],
        ["app_id", "snapshot_date", "min_players", "max_players", "avg_players", "snapshot_count"],
    ),
    "app_prices": (
        ["app_id", "snapshot_date"],
        [
            "app_id",
            "snapshot_date",
            "currency",
            "initial_price_cents",
            "final_price_cents",
            "discount_percent",
            "is_free",
            "is_purchasable",
        ],
    ),
    "twitch_viewer_snapshots": (
        ["app_id", "captured_at"],
        ["app_id", "captured_at", "snapshot_date", "viewer_count", "stream_count", "truncated"],
    ),
    "review_summary": (
        ["app_id", "snapshot_date"],
        [
            "app_id",
            "snapshot_date",
            "review_score",
            "review_score_desc",
            "total_positive",
            "total_negative",
            "total_reviews",
            "positive_ratio",
        ],
    ),
}


def get_engine(database_url: str) -> Engine:
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return create_engine(database_url, pool_pre_ping=True)


def init_schema(engine: Engine) -> None:
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def build_upsert(table: str) -> str:
    """Build the upsert statement for one table. Table/column names come only
    from the TABLES dict above, never from user input."""
    key, cols = TABLES[table]
    col_list = ", ".join(cols)
    params = ", ".join(f":{c}" for c in cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
    if table == "apps":
        updates += ", updated_at = now()"
    return f"INSERT INTO {table} ({col_list}) VALUES ({params}) ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}"


def load_tables(engine: Engine, tables: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.begin() as conn:
        for table, (_, cols) in TABLES.items():
            rows = tables.get(table, [])
            if not rows:
                counts[table] = 0
                continue
            payload = [{c: row.get(c, COLUMN_DEFAULTS.get(c)) for c in cols} for row in rows]
            conn.execute(text(build_upsert(table)), payload)
            counts[table] = len(payload)
            log.info("Loaded %d rows into %s", len(payload), table)
    return counts


def record_run(
    engine: Engine, run_date: date, started_at: datetime, status: str, rows_loaded: int, report: list[dict]
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_runs (run_date, started_at, status, rows_loaded, quality_report) "
                "VALUES (:run_date, :started_at, :status, :rows_loaded, CAST(:report AS JSONB))"
            ),
            {
                "run_date": run_date,
                "started_at": started_at,
                "status": status,
                "rows_loaded": rows_loaded,
                "report": json.dumps(report),
            },
        )


def fetch_loaded_dates(
    engine: Engine, run_date: date, table: str = "player_counts_daily", lookback_days: int = 30
) -> list[date]:
    """Distinct dates with data in the window ending at run_date (inclusive)."""
    if table not in TABLES:
        raise ValueError(f"Unknown table {table}")
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT DISTINCT snapshot_date FROM {table} "
                "WHERE snapshot_date BETWEEN CAST(:run_date AS DATE) - CAST(:days AS INTEGER) "
                "AND CAST(:run_date AS DATE) ORDER BY 1"
            ),
            {"run_date": run_date, "days": lookback_days},
        )
        return [r[0] for r in rows]
