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

from pipeline.analysis import (
    apply_retention,
    detect_anomalies,
    refresh_market_daily,
    refresh_player_daily,
    sync_tracked_apps,
)
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
        for t in [*TABLES, "pipeline_runs", "player_anomalies", "tracked_apps", "player_forecasts", "market_daily"]:
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
        "market_snapshots": [
            row
            for s in snaps
            for row in (
                {
                    **{k: s[k] for k in ("captured_at", "snapshot_date")},
                    "app_id": 730,
                    "rank": 1,
                    "last_week_rank": 1,
                    "peak_in_game": 1_400_000,
                    "current_players": 1_000_000,
                },
                {
                    **{k: s[k] for k in ("captured_at", "snapshot_date")},
                    "app_id": 1,
                    "rank": 7,
                    "last_week_rank": 8,
                    "peak_in_game": players + 300,
                    "current_players": s["player_count"],
                },
            )
        ],
        "steam_app_names": [{"app_id": 730, "name": "Counter-Strike 2", "first_seen_at": snaps[0]["captured_at"]}],
        "review_themes": [
            {
                "app_id": 1,
                "snapshot_date": d,
                "theme": theme,
                "theme_version": "test",
                "reviews_sampled": 100,
                "mentions": m,
                "negative_mentions": n,
            }
            for theme, m, n in (("performance", 30, 25), ("content_story", 40, 5))
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
        refresh_market_daily(engine, d)
    # Stored forecasts for every snapshot hour so the accuracy query has something to score.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO player_forecasts (app_id, target_hour, model, predicted, made_at) "
                "SELECT app_id, date_trunc('hour', captured_at, 'UTC'), m.model, player_count * m.factor, "
                "captured_at - INTERVAL '24 hours' FROM player_count_snapshots "
                "CROSS JOIN (VALUES ('seasonal_profile', 1.05), ('naive_24h', 1.20)) AS m(model, factor) "
                "ON CONFLICT DO NOTHING"
            )
        )
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
    assert resolve_sources("auto", with_twitch, engine, START) == (
        "players",
        "appdetails",
        "reviews",
        "twitch",
        "market",
        "review_text",
    )
    load_history(engine, days=1, spike=False)
    # Prices for START are in -> hourly mode only.
    assert resolve_sources("auto", with_twitch, engine, START) == ("players", "twitch", "market")
    assert resolve_sources("auto", no_twitch, engine, START) == ("players", "market")
    # A daily source whose table is still empty for today is fetched on its own.
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM review_themes"))
    assert resolve_sources("auto", no_twitch, engine, START) == ("players", "market", "review_text")
    # Re-processing saved raw data doesn't need Twitch credentials.
    assert "twitch" in resolve_sources("players,twitch", no_twitch, engine, START, extracting=False)


# ---- new in 0.2: forecasts, sale lift, market, review themes ----------------


def test_forecast_accuracy_scores_model_against_baseline(engine):
    load_history(engine, spike=False)
    with engine.connect() as conn:
        row = conn.exec_driver_sql((QUERIES_DIR / "13_forecast_accuracy.sql").read_text()).mappings().one()
    assert row["champion"] == "seasonal_profile"
    assert float(row["champion_mape_pct"]) == pytest.approx(5.0, abs=0.1)
    assert float(row["baseline_mape_pct"]) == pytest.approx(20.0, abs=0.1)
    assert float(row["skill"]) == pytest.approx(0.75, abs=0.01)


def test_write_forecasts_once_per_target_hour(engine):
    from pipeline.forecast import write_forecasts

    sync_tracked_apps(engine, TRACKED)
    end = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
    snaps = [
        {
            "app_id": 1,
            "captured_at": end - timedelta(hours=h),
            "snapshot_date": (end - timedelta(hours=h)).date(),
            "player_count": 30000 + 5000 * ((end - timedelta(hours=h)).hour >= 18),
        }
        for h in range(0, 24 * 4)
    ]
    load_tables(engine, {"player_count_snapshots": snaps})
    write_forecasts(engine, now=end)
    write_forecasts(engine, now=end + timedelta(minutes=30))  # same hours again: nothing new
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT model, COUNT(*) FROM player_forecasts GROUP BY model ORDER BY model")).all()
        evening = conn.execute(
            text("SELECT predicted FROM player_forecasts WHERE model = 'seasonal_profile' AND target_hour = :t"),
            {"t": datetime(2026, 11, 10, 20, tzinfo=timezone.utc)},
        ).scalar_one()
    assert rows == [("ensemble", 24), ("naive_24h", 24), ("seasonal_profile", 24)]
    assert float(evening) == pytest.approx(35000, rel=0.01)


def test_sale_lift_uses_a_control_group(engine):
    # Game 1 goes on sale on days 3-5 (see load_history). Game 2 never does and is the control.
    load_history(engine, spike=False)
    sync_tracked_apps(engine, [*TRACKED, AppConfig(2, "Civilization VI", "Take-Two")])
    rows = []
    for i in range(10):
        d = START + timedelta(days=i)
        rows.append(
            {
                "app_id": 2,
                "snapshot_date": d,
                "min_players": 1,
                "max_players": 1,
                "avg_players": 10000 * (1.1 if 3 <= i <= 5 else 1.0),
                "snapshot_count": 1,
            }
        )
    load_tables(
        engine,
        {
            "player_counts_daily": rows,
            "app_prices": [
                {
                    "app_id": 2,
                    "snapshot_date": r["snapshot_date"],
                    "currency": "CAD",
                    "initial_price_cents": 5999,
                    "final_price_cents": 5999,
                    "discount_percent": 0,
                    "is_free": False,
                }
                for r in rows
            ],
        },
    )
    with engine.connect() as conn:
        row = conn.exec_driver_sql((QUERIES_DIR / "14_sale_lift.sql").read_text()).mappings().one()
    assert row["app_id"] == 1 and row["sale_start"] == START + timedelta(days=3)
    assert float(row["control_change_pct"]) == pytest.approx(10.0, abs=0.1)
    assert float(row["lift_pct_points"]) == pytest.approx(float(row["raw_change_pct"]) - 10.0, abs=0.2)


def test_market_rollup_and_retention(engine):
    load_history(engine, spike=False)
    with engine.connect() as conn:
        top = conn.exec_driver_sql((QUERIES_DIR / "15_steam_top100.sql").read_text()).mappings().all()
        daily = conn.execute(text("SELECT COUNT(*) FROM market_daily")).scalar_one()
    assert [r["name"] for r in top] == ["Counter-Strike 2", "Red Dead Redemption 2"]
    assert top[1]["publisher_group"] == "Take-Two" and top[0]["publisher_group"] == "Other"
    assert daily == 20  # 2 games x 10 days
    deleted = apply_retention(engine, START + timedelta(days=40))
    assert deleted == 60  # every hourly row is past the 30-day window...
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM market_daily")).scalar_one() == 20  # ...rollup kept


def test_dashboard_export_has_new_sections(engine, tmp_path):
    import json

    load_history(engine, spike=False)
    data = json.loads(export_dashboard_summary(engine, START + timedelta(days=9), tmp_path / "s.json").read_text())
    for key in ("forecast_series", "forecast_accuracy", "sale_lift", "top100", "top100_share", "review_themes"):
        assert key in data, key
    assert data["top100"] and data["review_themes"] and data["forecast_accuracy"]
