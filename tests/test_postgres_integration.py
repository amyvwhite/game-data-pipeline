"""Integration tests against a real PostgreSQL.

Skipped unless TEST_DATABASE_URL is set, e.g.
    TEST_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/games_test pytest
CI starts a Postgres service container and sets it.

WARNING: these tests DROP and recreate the pipeline tables in that database.
"""

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

from pipeline.analysis import detect_anomalies, refresh_player_daily, sync_tracked_apps
from pipeline.config import AppConfig
from pipeline.export import export_dashboard_summary
from pipeline.load.postgres_loader import (
    TABLES,
    fetch_loaded_dates,
    get_engine,
    init_schema,
    load_tables,
    record_run,
)

DB_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not DB_URL, reason="TEST_DATABASE_URL not set")]

QUERIES_DIR = Path(__file__).resolve().parent.parent / "sql" / "queries"
START = date(2026, 11, 1)


@pytest.fixture
def engine():
    eng = get_engine(DB_URL)
    with eng.begin() as conn:
        for t in [*TABLES, "pipeline_runs", "player_anomalies", "tracked_apps"]:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {t} CASCADE")
    init_schema(eng)
    yield eng
    eng.dispose()


def day_tables(d: date, price: int, players: int) -> dict:
    snaps = [
        {
            "app_id": 1,
            "captured_at": datetime(d.year, d.month, d.day, h, tzinfo=timezone.utc),
            "snapshot_date": d,
            "player_count": players + h * 10,
        }
        for h in (2, 14, 20)
    ]
    return {
        "apps": [
            {
                "app_id": 1,
                "name": "Red Dead Redemption 2",
                "app_type": "game",
                "developers": "Rockstar",
                "publishers": "Rockstar Games",
                "release_date_text": "5 Dec, 2019",
                "coming_soon": False,
            }
        ],
        "player_count_snapshots": snaps,
        "twitch_viewer_snapshots": [
            {
                **{k: s[k] for k in ("app_id", "captured_at", "snapshot_date")},
                "viewer_count": s["player_count"] // 20,
                "stream_count": 40,
                "truncated": False,
            }
            for s in snaps
        ],
        "player_counts_daily": [
            {
                "app_id": 1,
                "snapshot_date": d,
                "min_players": players + 20,
                "max_players": players + 200,
                "avg_players": players + 113.3,
                "snapshot_count": 3,
            }
        ],
        "app_prices": [
            {
                "app_id": 1,
                "snapshot_date": d,
                "currency": "CAD",
                "initial_price_cents": 7999,
                "final_price_cents": price,
                "discount_percent": round(100 - price * 100 / 7999),
                "is_free": False,
            }
        ],
        "review_summary": [
            {
                "app_id": 1,
                "snapshot_date": d,
                "review_score": 8,
                "review_score_desc": "Very Positive",
                "total_positive": 900 + d.day,
                "total_negative": 100,
                "total_reviews": 1000 + d.day,
                "positive_ratio": round((900 + d.day) / (1000 + d.day), 4),
            }
        ],
    }


TRACKED = [AppConfig(1, "Red Dead Redemption 2", "Take-Two", "Red Dead Redemption 2")]


def load_history(engine, days=10, spike=True):
    sync_tracked_apps(engine, TRACKED)
    for i in range(days):
        d = START + timedelta(days=i)
        price = 2639 if 3 <= i <= 5 else 7999  # a sale in the middle
        load_tables(engine, day_tables(d, price, 30000 + i * 500))
    if spike:
        # Last day, 21:00 UTC: double the usual evening count -> should be flagged as an anomaly.
        last = START + timedelta(days=days - 1)
        at = datetime(last.year, last.month, last.day, 21, tzinfo=timezone.utc)
        load_tables(
            engine,
            {
                "player_count_snapshots": [
                    {
                        "app_id": 1,
                        "captured_at": at,
                        "snapshot_date": last,
                        "player_count": 2 * (30000 + (days - 1) * 500),
                    }
                ]
            },
        )


def test_load_is_idempotent(engine):
    tables = day_tables(START, 7999, 30000)
    load_tables(engine, tables)
    load_tables(engine, tables)
    with engine.connect() as conn:
        for t in TABLES:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one()
            assert n == len(tables.get(t, [])), t


def test_upsert_updates_values(engine):
    load_tables(engine, day_tables(START, 7999, 30000))
    load_tables(engine, day_tables(START, 2639, 30000))
    with engine.connect() as conn:
        assert conn.execute(text("SELECT final_price_cents FROM app_prices")).scalar_one() == 2639


def test_record_run_and_loaded_dates(engine):
    load_history(engine, days=3)
    record_run(engine, START, datetime.now(timezone.utc), "success", 10, [{"name": "x"}])
    assert fetch_loaded_dates(engine, START + timedelta(days=2)) == [START + timedelta(days=i) for i in range(3)]


@pytest.mark.parametrize("sql_file", sorted(QUERIES_DIR.glob("*.sql")), ids=lambda p: p.name)
def test_analytical_queries_run(engine, sql_file):
    """Every query in sql/queries must at least run against realistic data."""
    load_history(engine)
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(sql_file.read_text()).fetchall()
    assert rows, f"{sql_file.name} returned no rows on sample data"


def test_price_change_query_finds_the_sale(engine):
    load_history(engine)
    with engine.connect() as conn:
        rows = conn.exec_driver_sql((QUERIES_DIR / "04_price_changes.sql").read_text()).mappings().all()
    kinds = [r["change_type"] for r in rows]
    assert kinds == ["sale started", "sale ended"]


def test_dashboard_export(engine, tmp_path):
    load_history(engine)
    out = export_dashboard_summary(engine, START + timedelta(days=9), tmp_path / "summary.json")
    import json

    data = json.loads(out.read_text())
    assert len(data["daily_players"]) == 10
    assert data["latest_prices"][0]["final_price_cents"] == 7999
    assert data["latest_prices"][0]["prev_final_cents"] == 7999


def test_anomaly_detection_flags_the_spike_once(engine):
    load_history(engine)
    found = detect_anomalies(engine)
    assert [(a["app_id"], a["direction"]) for a in found] == [(1, "spike")]
    assert float(found[0]["pct_vs_baseline"]) > 50
    assert detect_anomalies(engine) == []  # already stored: no second alert


def test_no_anomaly_on_normal_data(engine):
    load_history(engine, spike=False)
    assert detect_anomalies(engine) == []


def test_refresh_player_daily_uses_all_stored_snapshots(engine):
    load_history(engine, days=2, spike=False)
    d = START + timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE player_counts_daily SET snapshot_count = 1, max_players = 0 WHERE snapshot_date = :d"),
            {"d": d},
        )
    refresh_player_daily(engine, d)
    with engine.connect() as conn:
        n, mx = conn.execute(
            text("SELECT snapshot_count, max_players FROM player_counts_daily WHERE snapshot_date = :d"), {"d": d}
        ).one()
    assert n == 3 and mx == 30500 + 200


def test_warehouse_round_trip(engine, tmp_path):
    from pipeline.warehouse import WAREHOUSE_TABLES, dump, restore

    load_history(engine)
    detect_anomalies(engine)
    record_run(engine, START, datetime.now(timezone.utc), "success", 10, [])
    before = dump(engine, tmp_path)
    assert before["player_count_snapshots"] == 31 and before["player_anomalies"] == 1
    with engine.begin() as conn:
        for t in WAREHOUSE_TABLES:
            conn.exec_driver_sql(f"TRUNCATE {t}")
    restore(engine, tmp_path)
    after = dump(engine, tmp_path / "again")
    assert after == before
    for t in WAREHOUSE_TABLES:
        assert (tmp_path / f"{t}.csv").read_text() == (tmp_path / "again" / f"{t}.csv").read_text(), t


def test_restore_refuses_a_non_empty_database(engine, tmp_path):
    from pipeline.warehouse import dump, restore

    load_history(engine, days=1, spike=False)
    dump(engine, tmp_path)
    with pytest.raises(RuntimeError, match="already has rows"):
        restore(engine, tmp_path)


def test_auto_sources(engine):
    from pipeline.config import Settings
    from pipeline.run import resolve_sources

    no_twitch = Settings(apps=TRACKED)
    with_twitch = Settings(apps=TRACKED, twitch_client_id="id", twitch_client_secret="secret")
    # Nothing loaded today yet -> everything.
    assert resolve_sources("auto", with_twitch, engine, START) == ("players", "appdetails", "reviews", "twitch")
    load_history(engine, days=1, spike=False)
    # Prices for START are in -> hourly mode only.
    assert resolve_sources("auto", with_twitch, engine, START) == ("players", "twitch")
    assert resolve_sources("auto", no_twitch, engine, START) == ("players",)
    # Re-processing saved raw data doesn't need Twitch credentials.
    assert "twitch" in resolve_sources("players,twitch", no_twitch, engine, START, extracting=False)
