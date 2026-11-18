"""Tests for the orchestration in pipeline.run (Spark and Postgres are faked)."""

from datetime import date
from types import SimpleNamespace

import pytest

from pipeline import run as run_mod
from pipeline.config import AppConfig, Settings


def _args(**kw):
    base = {
        "date": date(2026, 11, 18),
        "sources": "players,appdetails,reviews",
        "skip_extract": True,
        "skip_load": True,
        "no_parquet": True,
    }
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.fixture
def settings(tmp_path):
    return Settings(data_dir=tmp_path, apps=[AppConfig(1, "A")])


@pytest.fixture
def fake_spark(monkeypatch):
    """Replace the Spark step with a function returning whatever tables the test sets."""
    import pipeline.transform.spark_job as sj

    state = {"tables": {}}
    monkeypatch.setattr(sj, "get_spark", lambda: SimpleNamespace(stop=lambda: None))
    monkeypatch.setattr(sj, "run_transform", lambda *a, **k: state["tables"])
    return state


def test_refuses_to_extract_for_a_past_date(settings):
    with pytest.raises(ValueError, match="only returns current data"):
        run_mod.run(_args(skip_extract=False, date=date(2020, 1, 1)), settings)


def test_unknown_source_rejected(settings):
    with pytest.raises(ValueError, match="Unknown sources"):
        run_mod.run(_args(sources="players,tweets"), settings)


def test_critical_failure_stops_before_load(settings, fake_spark, caplog):
    fake_spark["tables"] = {}  # nothing produced -> row_count checks fail
    with pytest.raises(run_mod.CriticalQualityError):
        run_mod.run(_args(), settings)
    assert "ALERT" in caplog.text


def test_main_returns_exit_code_1_on_quality_failure(monkeypatch, settings, fake_spark):
    monkeypatch.setattr(run_mod, "get_settings", lambda: settings)
    assert run_mod.main(["--date", "2026-11-18", "--skip-extract", "--skip-load"]) == 1


def test_players_only_run_checks_only_player_tables(settings, fake_spark):
    d = date(2026, 11, 18)
    fake_spark["tables"] = {
        "player_count_snapshots": [{"app_id": 1, "captured_at": "t", "snapshot_date": d, "player_count": 3}],
        "player_counts_daily": [{"app_id": 1, "snapshot_date": d, "max_players": 3}],
        "app_prices": [],  # would be critical if it were checked
    }
    assert run_mod.run(_args(sources="players"), settings) == 0
