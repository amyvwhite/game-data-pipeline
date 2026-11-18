import boto3
import pytest
from moto import mock_aws

from pipeline.extract.extract import raw_key, run_extract
from pipeline.extract.steam_client import SteamAPIError
from pipeline.extract.storage import LocalStore, S3Store
from tests.conftest import CIV6, FETCHED_AT, RDR2, RUN_DATE, FakeSteamClient


def test_raw_key_layout():
    key = raw_key("players", RUN_DATE, 1174180, FETCHED_AT)
    assert key == "raw/players/dt=2026-11-18/1174180_20261118T140000Z.json"


def test_extract_writes_envelopes(raw_store):
    keys = raw_store.list_keys("raw")
    assert len(keys) == 9  # 4 per-app sources x 2 apps + 1 market snapshot (no Twitch client given)
    env = raw_store.read_json("raw/players/dt=2026-11-18/1174180_20261118T140000Z.json")
    assert env["app_id"] == 1174180
    assert env["source"] == "players"
    assert env["snapshot_date"] == "2026-11-18"
    assert env["payload"] == {"response": {"player_count": 31245, "result": 1}}


def test_one_failing_app_does_not_stop_the_rest(tmp_path):
    client = FakeSteamClient(overrides={("players", RDR2.app_id): SteamAPIError("boom")})
    result = run_extract(client, LocalStore(tmp_path), [RDR2, CIV6], RUN_DATE, sources=("players",), now=FETCHED_AT)
    assert len(result.written) == 1
    assert result.failures == [("players", RDR2.app_id, "boom")]


def test_local_store_round_trip(tmp_path):
    store = LocalStore(tmp_path)
    store.write_json("raw/x/a.json", {"a": 1})
    assert store.list_keys("raw/x") == ["raw/x/a.json"]
    assert store.read_json("raw/x/a.json") == {"a": 1}
    assert store.list_keys("raw/missing") == []


@mock_aws
def test_s3_store_round_trip_and_materialize(tmp_path):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    store = S3Store("test-bucket", prefix="pipeline", client=s3)

    uri = store.write_json("raw/players/dt=2026-11-18/1_x.json", {"n": 1})
    assert uri == "s3://test-bucket/pipeline/raw/players/dt=2026-11-18/1_x.json"
    assert store.list_keys("raw/players") == ["raw/players/dt=2026-11-18/1_x.json"]
    assert store.read_json("raw/players/dt=2026-11-18/1_x.json") == {"n": 1}

    local = store.materialize_locally("raw/players/dt=2026-11-18", tmp_path)
    assert (local / "1_x.json").exists()


def test_s3_store_needs_bucket():
    with pytest.raises(ValueError, match="S3_BUCKET"):
        S3Store("")


def test_twitch_only_game_gets_no_steam_calls(tmp_path):
    """GTA VI isn't on Steam: only the twitch source should fetch it."""
    from datetime import date as _date

    from pipeline.config import AppConfig as _App
    from pipeline.extract.extract import run_extract as _run
    from pipeline.extract.storage import LocalStore as _Store

    class Steam:
        def __init__(self):
            self.calls = []

        def get_current_players(self, app_id):
            self.calls.append(app_id)
            return {"response": {"player_count": 1, "result": 1}}

        def get_app_details(self, app_id, country="ca"):
            raise AssertionError("not called in this test")

        get_review_summary = get_app_details

    class Twitch:
        def get_game_viewers(self, name):
            return {"twitch_game": name, "found": True, "viewer_count": 5, "stream_count": 1}

    steam = Steam()
    apps = [
        _App(1, "On Steam", "Take-Two", "On Steam"),
        _App(-6, "Grand Theft Auto VI", "Take-Two", "Grand Theft Auto VI", steam=False),
    ]
    result = _run(steam, _Store(tmp_path), apps, _date(2026, 11, 18), sources=("players", "twitch"), twitch=Twitch())
    assert steam.calls == [1]
    assert sum("raw/twitch/" in w.replace("\\", "/") for w in result.written) == 2


def test_market_extract_collects_players_and_new_names_only(tmp_path):
    store = LocalStore(tmp_path)
    result = run_extract(
        FakeSteamClient(),
        store,
        [RDR2, CIV6],
        RUN_DATE,
        sources=("market",),
        now=FETCHED_AT,
        known_names={RDR2.app_id, CIV6.app_id},
    )
    assert result.failures == []
    (key,) = store.list_keys("raw/market")
    env = store.read_json(key)
    ranks = {r["appid"]: r for r in env["payload"]["ranks"]}
    assert ranks[730]["current_players"] == 1201278
    assert ranks[730]["name"] == "Counter-Strike 2"
    assert ranks[RDR2.app_id]["name"] is None  # already known: no extra request


def test_market_chart_failure_is_reported_not_raised(tmp_path):
    client = FakeSteamClient(overrides={("market", 0): SteamAPIError("down")})
    result = run_extract(client, LocalStore(tmp_path), [RDR2], RUN_DATE, sources=("market",), now=FETCHED_AT)
    assert result.written == [] and result.failures == [("market", 0, "down")]
