"""Shared test helpers.

Fixture JSON in tests/fixtures/ copies the *shape* of real Steam responses
(field names and nesting); the numbers are made up for testing.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from pipeline.config import AppConfig
from pipeline.extract.extract import run_extract
from pipeline.extract.storage import LocalStore

FIXTURES = Path(__file__).parent / "fixtures"
RUN_DATE = date(2026, 11, 18)
FETCHED_AT = datetime(2026, 11, 18, 14, 0, 0, tzinfo=timezone.utc)

RDR2 = AppConfig(1174180, "Red Dead Redemption 2", "Take-Two")
CIV6 = AppConfig(289070, "Sid Meier's Civilization VI", "Take-Two")


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class FakeSteamClient:
    """Returns fixture payloads instead of calling Steam."""

    def __init__(self, overrides: dict | None = None) -> None:
        self.overrides = overrides or {}

    def _get(self, source: str, app_id: int) -> dict:
        if (source, app_id) in self.overrides:
            value = self.overrides[(source, app_id)]
            if isinstance(value, Exception):
                raise value
            return value
        return load_fixture(f"{source}_{app_id}.json")

    def get_current_players(self, app_id: int) -> dict:
        return self._get("players", app_id)

    def get_app_details(self, app_id: int, country: str = "ca") -> dict:
        return self._get("appdetails", app_id)

    def get_review_summary(self, app_id: int) -> dict:
        return self._get("reviews", app_id)


@pytest.fixture
def raw_store(tmp_path: Path) -> LocalStore:
    """A local raw store already filled with one day of fixture data for RDR2 and Civ VI."""
    store = LocalStore(tmp_path)
    run_extract(FakeSteamClient(), store, [RDR2, CIV6], RUN_DATE, now=FETCHED_AT)
    return store


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


requires_spark = pytest.mark.skipif(not _spark_available(), reason="PySpark or Java not available")


@pytest.fixture(scope="session")
def spark():
    if not _spark_available():
        pytest.skip("PySpark or Java not available")
    from pipeline.transform.spark_job import get_spark

    session = get_spark("tests", master="local[1]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
