"""Post-load analysis that runs inside Postgres.

* sync_tracked_apps     - keep the tracked_apps table in step with config/apps.json
* refresh_player_daily  - rebuild daily player stats from *all* stored snapshots, so an
                          hourly job (which only sees its own snapshot in raw storage)
                          still produces correct daily min/max/avg
* detect_anomalies      - run sql/queries/09_player_anomalies.sql and save new hits;
                          returns only the new ones so each anomaly alerts once
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date, timezone
from typing import Any

from sqlalchemy import Engine, text

from pipeline.config import PROJECT_ROOT, AppConfig

log = logging.getLogger(__name__)

QUERIES_DIR = PROJECT_ROOT / "sql" / "queries"


def read_query(name: str) -> str:
    """Load a query from sql/queries without its trailing semicolon, so it can be nested."""
    return (QUERIES_DIR / name).read_text(encoding="utf-8").strip().rstrip(";")


def sync_tracked_apps(engine: Engine, apps: Iterable[AppConfig]) -> None:
    rows = [
        {"app_id": a.app_id, "name": a.name, "publisher_group": a.publisher_group, "twitch_game": a.twitch_game}
        for a in apps
    ]
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO tracked_apps (app_id, name, publisher_group, twitch_game) "
                "VALUES (:app_id, :name, :publisher_group, :twitch_game) "
                "ON CONFLICT (app_id) DO UPDATE SET name = EXCLUDED.name, "
                "publisher_group = EXCLUDED.publisher_group, twitch_game = EXCLUDED.twitch_game"
            ),
            rows,
        )


REFRESH_DAILY_SQL = """
INSERT INTO player_counts_daily (app_id, snapshot_date, min_players, max_players, avg_players, snapshot_count)
SELECT app_id, snapshot_date,
       MIN(player_count), MAX(player_count), ROUND(AVG(player_count), 1), COUNT(*)
FROM player_count_snapshots
WHERE snapshot_date = :run_date
GROUP BY app_id, snapshot_date
ON CONFLICT (app_id, snapshot_date) DO UPDATE SET
    min_players = EXCLUDED.min_players,
    max_players = EXCLUDED.max_players,
    avg_players = EXCLUDED.avg_players,
    snapshot_count = EXCLUDED.snapshot_count
"""


def refresh_player_daily(engine: Engine, run_date: date) -> None:
    with engine.begin() as conn:
        conn.execute(text(REFRESH_DAILY_SQL), {"run_date": run_date})


def detect_anomalies(engine: Engine) -> list[dict[str, Any]]:
    """Find unusual player counts in the latest snapshots and store the new ones."""
    candidates = read_query("09_player_anomalies.sql")
    sql = f"""
    WITH found AS ({candidates})
    INSERT INTO player_anomalies (app_id, captured_at, player_count, baseline_avg, baseline_std,
                                  baseline_points, z_score, pct_vs_baseline, direction)
    SELECT app_id, captured_at, player_count, baseline_avg, baseline_std,
           baseline_points, z_score, pct_vs_baseline, direction
    FROM found
    ON CONFLICT (app_id, captured_at) DO NOTHING
    RETURNING app_id, captured_at, player_count, baseline_avg, z_score, pct_vs_baseline, direction
    """
    with engine.begin() as conn:
        rows = [dict(r._mapping) for r in conn.execute(text(sql))]
        if rows:
            names = dict(conn.execute(text("SELECT app_id, name FROM tracked_apps")).all())
            for r in rows:
                r["name"] = names.get(r["app_id"], str(r["app_id"]))
    for r in rows:
        log.warning("Anomaly: %s", describe_anomaly(r))
    return rows


def describe_anomaly(a: dict[str, Any]) -> str:
    at = a["captured_at"]
    when = at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if hasattr(at, "astimezone") else at
    pct = float(a["pct_vs_baseline"])
    word = "above" if pct >= 0 else "below"
    return (
        f"{a.get('name', a['app_id'])}: {int(a['player_count']):,} players at {when}, "
        f"{abs(pct):.0f}% {word} normal ({float(a['baseline_avg']):,.0f}; z={float(a['z_score']):+.1f}). "
        "Possible update, sale, event or outage."
    )


def format_anomaly_alert(anomalies: list[dict[str, Any]]) -> str:
    lines = [f"game-data-pipeline: {len(anomalies)} unusual player count(s)"]
    lines += [f"- {describe_anomaly(a)}" for a in anomalies]
    return "\n".join(lines)
