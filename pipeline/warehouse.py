"""Save the database to CSV files in the repo, and load it back.

This is what lets the pipeline run "live" on GitHub Actions for free: each hourly run
starts a fresh Postgres, restores the history from data/warehouse/*.csv, runs the
pipeline, dumps the tables back out, and commits the CSVs. Git becomes the storage
layer (with full history of every change), and no always-on database is needed.

    python -m pipeline.warehouse dump      # Postgres -> data/warehouse/*.csv
    python -m pipeline.warehouse restore   # data/warehouse/*.csv -> Postgres (empty tables only)

Rows are written sorted by primary key so a commit's diff shows exactly what changed.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, cast

from sqlalchemy import Engine, text

from pipeline.config import PROJECT_ROOT, get_settings

log = logging.getLogger(__name__)

WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"

# table -> (columns, order by). pipeline_runs leaves out its JSON quality report to keep files small.
WAREHOUSE_TABLES: dict[str, tuple[list[str], str]] = {
    "apps": (
        ["app_id", "name", "app_type", "developers", "publishers", "release_date_text", "coming_soon", "updated_at"],
        "app_id",
    ),
    "player_count_snapshots": (["app_id", "captured_at", "snapshot_date", "player_count"], "app_id, captured_at"),
    "player_counts_daily": (
        ["app_id", "snapshot_date", "min_players", "max_players", "avg_players", "snapshot_count"],
        "app_id, snapshot_date",
    ),
    "app_prices": (
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
        "app_id, snapshot_date",
    ),
    "review_summary": (
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
        "app_id, snapshot_date",
    ),
    "twitch_viewer_snapshots": (
        ["app_id", "captured_at", "snapshot_date", "viewer_count", "stream_count", "truncated"],
        "app_id, captured_at",
    ),
    "player_anomalies": (
        [
            "app_id",
            "captured_at",
            "player_count",
            "baseline_avg",
            "baseline_std",
            "baseline_points",
            "z_score",
            "pct_vs_baseline",
            "direction",
            "detected_at",
        ],
        "app_id, captured_at",
    ),
    "market_snapshots": (
        ["app_id", "captured_at", "snapshot_date", "rank", "last_week_rank", "peak_in_game", "current_players"],
        "app_id, captured_at",
    ),
    "market_daily": (
        ["app_id", "snapshot_date", "best_rank", "avg_players", "max_players", "snapshot_count"],
        "app_id, snapshot_date",
    ),
    "steam_app_names": (["app_id", "name", "first_seen_at"], "app_id"),
    "review_themes": (
        ["app_id", "snapshot_date", "theme", "theme_version", "reviews_sampled", "mentions", "negative_mentions"],
        "app_id, snapshot_date, theme",
    ),
    "player_forecasts": (["app_id", "target_hour", "model", "predicted", "made_at"], "app_id, target_hour, model"),
    "pipeline_runs": (["run_date", "started_at", "finished_at", "status", "rows_loaded"], "started_at"),
}


def dump(engine: Engine, out_dir: Path = WAREHOUSE_DIR) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    raw = engine.raw_connection()
    try:
        with cast(Any, raw.cursor()) as cur:  # psycopg cursor; SQLAlchemy types it as plain DB-API
            cur.execute("SET TIME ZONE 'UTC'")
            for table, (cols, order_by) in WAREHOUSE_TABLES.items():
                path = out_dir / f"{table}.csv"
                sql = f"COPY (SELECT {', '.join(cols)} FROM {table} ORDER BY {order_by}) TO STDOUT WITH CSV HEADER"
                with path.open("wb") as fh, cur.copy(sql) as copy:
                    for chunk in copy:
                        fh.write(chunk)
                counts[table] = max(0, sum(1 for _ in path.open(encoding="utf-8")) - 1)
        raw.commit()
    finally:
        raw.close()
    log.info("Dumped warehouse to %s: %s", out_dir, counts)
    return counts


def restore(engine: Engine, in_dir: Path = WAREHOUSE_DIR) -> dict[str, int]:
    """Load the CSVs into empty tables. Refuses to touch a table that already has rows."""
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for table in WAREHOUSE_TABLES:
            if conn.execute(text(f"SELECT 1 FROM {table} LIMIT 1")).first():
                raise RuntimeError(f"{table} already has rows; restore only works on an empty database")
    raw = engine.raw_connection()
    try:
        with cast(Any, raw.cursor()) as cur:  # psycopg cursor; SQLAlchemy types it as plain DB-API
            cur.execute("SET TIME ZONE 'UTC'")
            for table, (cols, _) in WAREHOUSE_TABLES.items():
                path = in_dir / f"{table}.csv"
                if not path.exists():
                    counts[table] = 0
                    continue
                with (
                    path.open("rb") as fh,
                    cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER") as copy,
                ):
                    while chunk := fh.read(1 << 16):
                        copy.write(chunk)
                counts[table] = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        raw.commit()
    finally:
        raw.close()
    log.info("Restored warehouse from %s: %s", in_dir, counts)
    return counts


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Dump/restore the Postgres tables to/from CSV files")
    p.add_argument("action", choices=["dump", "restore"])
    p.add_argument("--dir", type=Path, default=WAREHOUSE_DIR)
    args = p.parse_args(argv)

    from pipeline.load.postgres_loader import get_engine, init_schema

    engine = get_engine(get_settings().database_url)
    init_schema(engine)
    if args.action == "dump":
        dump(engine, args.dir)
    else:
        restore(engine, args.dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
