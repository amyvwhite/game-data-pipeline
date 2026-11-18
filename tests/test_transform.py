"""Spark transform tests. Skipped automatically if PySpark/Java are missing."""

from datetime import datetime, timedelta, timezone

from pipeline.extract.extract import run_extract
from pipeline.transform.spark_job import run_transform
from tests.conftest import CIV6, FETCHED_AT, RDR2, RUN_DATE, FakeSteamClient, load_fixture, requires_spark

pytestmark = requires_spark


def _by_app(rows):
    return {r["app_id"]: r for r in rows}


def test_transform_builds_all_tables(spark, raw_store, tmp_path):
    tables = run_transform(spark, raw_store.root / "raw", RUN_DATE, clean_root=tmp_path / "clean")

    apps = _by_app(tables["apps"])
    assert apps[RDR2.app_id]["name"] == "Red Dead Redemption 2"
    assert apps[CIV6.app_id]["publishers"] == "2K, Aspyr (Mac), Aspyr (Linux)"

    prices = _by_app(tables["app_prices"])
    assert prices[RDR2.app_id]["final_price_cents"] == 2639
    assert prices[RDR2.app_id]["discount_percent"] == 67
    assert prices[CIV6.app_id]["discount_percent"] == 0
    assert prices[CIV6.app_id]["currency"] == "CAD"

    daily = _by_app(tables["player_counts_daily"])
    assert daily[RDR2.app_id]["max_players"] == 31245
    assert daily[RDR2.app_id]["snapshot_count"] == 1

    reviews = _by_app(tables["review_summary"])
    assert reviews[RDR2.app_id]["total_reviews"] == 670221
    assert reviews[RDR2.app_id]["positive_ratio"] == round(612340 / 670221, 4)

    # Parquet clean zone written, partitioned by date
    assert (tmp_path / "clean" / "app_prices" / "snapshot_date=2026-11-18").exists()


def test_several_snapshots_per_day_aggregate(spark, raw_store):
    later = FETCHED_AT + timedelta(hours=3)
    client = FakeSteamClient(overrides={("players", RDR2.app_id): {"response": {"player_count": 40000, "result": 1}}})
    run_extract(client, raw_store, [RDR2], RUN_DATE, sources=("players",), now=later)

    tables = run_transform(spark, raw_store.root / "raw", RUN_DATE)
    daily = _by_app(tables["player_counts_daily"])[RDR2.app_id]
    assert (daily["min_players"], daily["max_players"], daily["snapshot_count"]) == (31245, 40000, 2)
    assert float(daily["avg_players"]) == round((31245 + 40000) / 2, 1)


def test_latest_appdetails_wins_when_fetched_twice(spark, raw_store):
    newer = load_fixture("appdetails_1174180.json")
    newer["1174180"]["data"]["price_overview"].update(final=7999, discount_percent=0)
    client = FakeSteamClient(overrides={("appdetails", RDR2.app_id): newer})
    run_extract(client, raw_store, [RDR2], RUN_DATE, sources=("appdetails",), now=FETCHED_AT + timedelta(hours=1))

    prices = _by_app(run_transform(spark, raw_store.root / "raw", RUN_DATE)["app_prices"])
    assert prices[RDR2.app_id]["final_price_cents"] == 7999
    assert len(prices) == 2


def test_failed_and_empty_responses_are_dropped(spark, tmp_path):
    from pipeline.extract.storage import LocalStore

    store = LocalStore(tmp_path)
    client = FakeSteamClient(
        overrides={
            ("players", RDR2.app_id): load_fixture("players_no_data.json"),
            ("appdetails", RDR2.app_id): {"1174180": {"success": False}},
        }
    )
    run_extract(
        client,
        store,
        [RDR2],
        RUN_DATE,
        sources=("players", "appdetails"),
        now=datetime(2026, 11, 18, tzinfo=timezone.utc),
    )
    tables = run_transform(spark, store.root / "raw", RUN_DATE)
    assert tables["player_count_snapshots"] == []
    assert tables["app_prices"] == []
    assert tables["review_summary"] == []  # no reviews folder at all -> empty, not a crash


def test_free_game_gets_zero_price(spark, tmp_path):
    from pipeline.extract.storage import LocalStore

    free = {"5": {"success": True, "data": {"type": "game", "name": "Free Game", "is_free": True}}}
    store = LocalStore(tmp_path)
    app = type(RDR2)(5, "Free Game")
    run_extract(
        FakeSteamClient(overrides={("appdetails", 5): free}),
        store,
        [app],
        RUN_DATE,
        sources=("appdetails",),
        now=FETCHED_AT,
    )
    row = run_transform(spark, store.root / "raw", RUN_DATE)["app_prices"][0]
    assert row["is_free"] is True
    assert row["final_price_cents"] == 0 and row["currency"] is None


def test_paid_game_not_sold_separately_has_null_price(spark, tmp_path):
    """Real case: GTA V Legacy (271590) is paid but Steam returns no price_overview."""
    from pipeline.extract.storage import LocalStore

    unsold = {"9": {"success": True, "data": {"type": "game", "name": "Bundle-only Game", "is_free": False}}}
    store = LocalStore(tmp_path)
    app = type(RDR2)(9, "Bundle-only Game")
    run_extract(
        FakeSteamClient(overrides={("appdetails", 9): unsold}),
        store,
        [app],
        RUN_DATE,
        sources=("appdetails",),
        now=FETCHED_AT,
    )
    row = run_transform(spark, store.root / "raw", RUN_DATE)["app_prices"][0]
    assert row["is_free"] is False
    assert row["is_purchasable"] is False
    assert row["final_price_cents"] is None


def test_timestamps_come_back_as_utc_whatever_the_machine_timezone(spark, raw_store):
    tables = run_transform(spark, raw_store.root / "raw", RUN_DATE)
    for row in tables["player_count_snapshots"]:
        assert row["captured_at"].tzinfo is not None
        assert row["captured_at"].utcoffset() == timedelta(0)
    assert FETCHED_AT in {r["captured_at"] for r in tables["player_count_snapshots"]}
