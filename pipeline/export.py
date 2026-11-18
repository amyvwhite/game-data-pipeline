"""Export a small JSON summary from Postgres for the static dashboard."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from pipeline.analysis import read_query
from pipeline.config import AppConfig

log = logging.getLogger(__name__)

DAILY_PLAYERS_SQL = """
SELECT d.app_id, COALESCE(a.name, d.app_id::text) AS name, d.snapshot_date,
       d.avg_players, d.max_players
FROM player_counts_daily d
LEFT JOIN apps a USING (app_id)
WHERE d.snapshot_date BETWEEN CAST(:run_date AS DATE) - 29 AND CAST(:run_date AS DATE)
ORDER BY d.app_id, d.snapshot_date
"""

# Latest price per app, plus the previous price so the dashboard can flag changes.
LATEST_PRICES_SQL = """
WITH ordered AS (
    SELECT p.*,
           LAG(final_price_cents) OVER (PARTITION BY app_id ORDER BY snapshot_date) AS prev_final_cents,
           ROW_NUMBER()           OVER (PARTITION BY app_id ORDER BY snapshot_date DESC) AS rn
    FROM app_prices p
    WHERE snapshot_date <= CAST(:run_date AS DATE)
)
SELECT o.app_id, COALESCE(a.name, o.app_id::text) AS name, o.snapshot_date, o.currency,
       o.initial_price_cents, o.final_price_cents, o.discount_percent, o.is_free, o.is_purchasable, o.prev_final_cents
FROM ordered o
LEFT JOIN apps a USING (app_id)
WHERE o.rn = 1
ORDER BY name
"""

LATEST_REVIEWS_SQL = """
SELECT DISTINCT ON (r.app_id)
       r.app_id, COALESCE(a.name, r.app_id::text) AS name, r.snapshot_date,
       r.review_score_desc, r.total_reviews, r.positive_ratio
FROM review_summary r
LEFT JOIN apps a USING (app_id)
WHERE r.snapshot_date <= CAST(:run_date AS DATE)
ORDER BY r.app_id, r.snapshot_date DESC
"""


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _query(engine: Engine, sql: str, run_date: date) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"run_date": run_date})
        return [{k: _jsonable(v) for k, v in row._mapping.items()} for row in result]


# Average players by hour of day (Toronto time) over the last 14 days: the daily rhythm.
HOURLY_PROFILE_SQL = """
SELECT app_id,
       EXTRACT(HOUR FROM captured_at AT TIME ZONE 'America/Toronto')::int AS local_hour,
       ROUND(AVG(player_count)) AS avg_players,
       COUNT(*) AS samples
FROM player_count_snapshots
WHERE captured_at > (SELECT MAX(captured_at) FROM player_count_snapshots) - INTERVAL '14 days'
GROUP BY app_id, local_hour
ORDER BY app_id, local_hour
"""

RECENT_ANOMALIES_SQL = """
SELECT a.app_id, COALESCE(t.name, a.app_id::text) AS name, a.captured_at, a.player_count,
       a.baseline_avg, a.z_score, a.pct_vs_baseline, a.direction
FROM player_anomalies a
LEFT JOIN tracked_apps t USING (app_id)
ORDER BY a.captured_at DESC
LIMIT 15
"""

TWITCH_DAILY_SQL = """
SELECT app_id, snapshot_date, ROUND(AVG(viewer_count)) AS avg_viewers, MAX(viewer_count) AS max_viewers
FROM twitch_viewer_snapshots
WHERE snapshot_date BETWEEN CAST(:run_date AS DATE) - 29 AND CAST(:run_date AS DATE)
GROUP BY app_id, snapshot_date
ORDER BY app_id, snapshot_date
"""

RECENT_RUNS_SQL = """
SELECT finished_at, status, rows_loaded
FROM pipeline_runs
ORDER BY finished_at DESC
LIMIT 48
"""

COVERAGE_SQL = """
SELECT COUNT(*) AS snapshots,
       MIN(captured_at) AS first_snapshot,
       MAX(captured_at) AS last_snapshot,
       COUNT(DISTINCT snapshot_date) AS days_collected
FROM player_count_snapshots
"""


def export_dashboard_summary(
    engine: Engine,
    run_date: date,
    out_path: Path,
    is_sample: bool = False,
    apps: list[AppConfig] | None = None,
) -> Path:
    coverage = _query(engine, COVERAGE_SQL, run_date)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_date": run_date.isoformat(),
        "is_sample": is_sample,
        "groups": {str(a.app_id): a.publisher_group for a in (apps or [])},
        "coverage": coverage[0] if coverage else {},
        "daily_players": _query(engine, DAILY_PLAYERS_SQL, run_date),
        "latest_prices": _query(engine, LATEST_PRICES_SQL, run_date),
        "latest_reviews": _query(engine, LATEST_REVIEWS_SQL, run_date),
        "market_share": _query(engine, read_query("10_market_share.sql"), run_date),
        "hourly_profile": _query(engine, HOURLY_PROFILE_SQL, run_date),
        "peak_hours": _query(engine, read_query("05_peak_hours.sql"), run_date),
        "sale_impact": _query(engine, read_query("07_sale_impact.sql"), run_date),
        "anomalies": _query(engine, RECENT_ANOMALIES_SQL, run_date),
        "twitch_daily": _query(engine, TWITCH_DAILY_SQL, run_date),
        "twitch_vs_players": _query(engine, read_query("11_twitch_vs_players.sql"), run_date),
        "recent_runs": _query(engine, RECENT_RUNS_SQL, run_date),
        "launch_watch": _query(engine, read_query("12_gta6_launch_watch.sql"), run_date),
    }
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("Wrote dashboard summary to %s", out_path)
    return out_path
