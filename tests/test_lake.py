"""Data lake export: unit tests plus an S3 round trip against moto (fake AWS, no cost)."""

from __future__ import annotations

import io
import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from pipeline.lake import (
    FIRST_DAY,
    LAKE_TABLES,
    MAX_DAYS_PER_RUN,
    days_to_export,
    load_lake_schema,
    table_to_parquet,
    verify_day,
)

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def test_schema_file_covers_every_lake_table():
    schema = load_lake_schema()
    assert set(schema) == set(LAKE_TABLES)
    assert all(cols and all(name != "snapshot_date" for name, _ in cols) for cols in schema.values())


def test_days_to_export_only_complete_missing_days():
    done = {date(2026, 11, 18)}
    assert days_to_export(done, today=date(2026, 11, 21)) == [date(2026, 11, 19), date(2026, 11, 20)]
    assert days_to_export(set(), today=FIRST_DAY) == []  # today is never complete


def test_days_to_export_is_capped_after_an_outage():
    assert len(days_to_export(set(), today=date(2027, 3, 1))) == MAX_DAYS_PER_RUN


def test_parquet_types_match_the_athena_schema():
    cols = [("app_id", "int"), ("captured_at", "timestamp"), ("avg", "double"), ("ok", "boolean"), ("name", "string")]
    rows = [
        {"app_id": 1, "captured_at": datetime(2026, 11, 19, 13, 0), "avg": Decimal("1.5"), "ok": True, "name": "x"},
        {"app_id": 2, "captured_at": None, "avg": None, "ok": None, "name": None},
    ]
    table = pq.read_table(io.BytesIO(table_to_parquet(rows, cols)))
    assert [str(t) for t in table.schema.types] == ["int32", "timestamp[us, tz=UTC]", "double", "bool", "string"]
    first = table.to_pylist()[0]
    assert first["avg"] == 1.5 and first["captured_at"] == datetime(2026, 11, 19, 13, tzinfo=timezone.utc)


class FakeAthena:
    def __init__(self, rows, state="SUCCEEDED"):
        self.rows, self.state, self.sql = rows, state, None

    def start_query_execution(self, QueryString, QueryExecutionContext, WorkGroup):
        self.sql = QueryString
        assert QueryExecutionContext == {"Database": "game_data"} and WorkGroup == "game-data"
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self.state, "StateChangeReason": "boom"}}}

    def get_query_results(self, QueryExecutionId):
        header = {"Data": [{"VarCharValue": "snapshots"}, {"VarCharValue": "games"}]}
        return {"ResultSet": {"Rows": [header] + [{"Data": [{"VarCharValue": v} for v in r]} for r in self.rows]}}


def _settings(**kw):
    from pipeline.config import Settings

    return Settings(s3_bucket="lake-test", aws_region="ca-central-1", **kw)


def test_verify_reads_the_day_back():
    athena = FakeAthena([["24", "11"]])
    assert verify_day(_settings(), date(2026, 11, 19), athena) == (24, 11)
    assert "DATE '2026-11-19'" in athena.sql


def test_verify_fails_on_an_empty_day_or_failed_query():
    with pytest.raises(RuntimeError, match="no player snapshots"):
        verify_day(_settings(), date(2026, 11, 19), FakeAthena([["0", "0"]]))
    with pytest.raises(RuntimeError, match="FAILED"):
        verify_day(_settings(), date(2026, 11, 19), FakeAthena([], state="FAILED"))


# ---- integration: Postgres -> Parquet -> (fake) S3 ---------------------------


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL"), reason="needs TEST_DATABASE_URL")
def test_export_round_trip_is_idempotent(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    from pipeline.lake import export_missing
    from pipeline.load.postgres_loader import get_engine, init_schema
    from tests.test_postgres_integration import START, load_history

    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(var, "testing")
    engine = get_engine(os.environ["TEST_DATABASE_URL"])
    with engine.begin() as conn:
        for t in [
            *LAKE_TABLES,
            "steam_app_names",
            "apps",
            "pipeline_runs",
            "player_anomalies",
            "tracked_apps",
            "player_forecasts",
            "market_daily",
        ]:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {t} CASCADE")
    init_schema(engine)
    load_history(engine, days=3, spike=False)

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="ca-central-1")
        s3.create_bucket(Bucket="lake-test", CreateBucketConfiguration={"LocationConstraint": "ca-central-1"})
        settings = _settings()
        today = START.replace(day=START.day + 3)

        first = export_missing(engine, settings, today=today, since=START, s3=s3)
        assert first.days == [START, START.replace(day=START.day + 1), START.replace(day=START.day + 2)]
        key = f"game-data-pipeline/clean/player_count_snapshots/snapshot_date={START.isoformat()}/part-0.parquet"
        table = pq.read_table(io.BytesIO(s3.get_object(Bucket="lake-test", Key=key)["Body"].read()))
        assert table.num_rows == 3 and table.column_names == ["app_id", "captured_at", "player_count"]

        again = export_missing(engine, settings, today=today, since=START, s3=s3)
        assert again.days == []  # markers say it's done: no new uploads, no duplicates
        objects = s3.list_objects_v2(Bucket="lake-test")["KeyCount"]
        assert objects == first.files + len(first.days)  # one file per table-day + one marker per day
