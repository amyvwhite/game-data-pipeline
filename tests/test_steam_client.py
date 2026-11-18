import json
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses

from pipeline.extract.steam_client import (
    APPDETAILS_URL,
    MOST_PLAYED_URL,
    PLAYERS_URL,
    REVIEWS_URL,
    SteamAPIError,
    SteamClient,
)
from tests.conftest import load_fixture


@pytest.fixture
def sleeps():
    return []


@pytest.fixture
def client(sleeps):
    # No real sleeping in tests; record what the client *would* have slept.
    return SteamClient(timeout_s=5, max_retries=3, backoff_base_s=1, min_interval_s=0, sleep=sleeps.append)


@responses.activate
def test_current_players_calls_right_url(client):
    responses.get(PLAYERS_URL, json=load_fixture("players_1174180.json"))
    data = client.get_current_players(1174180)
    assert data["response"]["player_count"] == 31245
    assert parse_qs(urlparse(responses.calls[0].request.url).query) == {"appid": ["1174180"]}


@responses.activate
def test_app_details_sends_country(client):
    responses.get(APPDETAILS_URL, json=load_fixture("appdetails_1174180.json"))
    data = client.get_app_details(1174180, country="ca")
    assert data["1174180"]["data"]["price_overview"]["currency"] == "CAD"
    qs = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert qs["appids"] == ["1174180"] and qs["cc"] == ["ca"]


@responses.activate
def test_reviews_uses_input_json(client):
    responses.get(REVIEWS_URL, json=load_fixture("reviews_1174180.json"))
    client.get_review_summary(1174180)
    qs = parse_qs(urlparse(responses.calls[0].request.url).query)
    params = json.loads(qs["input_json"][0])
    assert params["appid"] == 1174180
    assert params["review_type"] == 0  # needed for query_summary totals


@responses.activate
def test_retries_on_429_and_honours_retry_after(client, sleeps):
    responses.get(PLAYERS_URL, status=429, headers={"Retry-After": "7"})
    responses.get(PLAYERS_URL, json={"response": {"player_count": 5, "result": 1}})
    assert client.get_current_players(1)["response"]["player_count"] == 5
    assert sleeps == [7.0]


@responses.activate
def test_retries_server_errors_with_exponential_backoff(client, sleeps):
    responses.get(PLAYERS_URL, status=502)
    responses.get(PLAYERS_URL, status=503)
    responses.get(PLAYERS_URL, json={"response": {"player_count": 5, "result": 1}})
    client.get_current_players(1)
    assert sleeps == [1, 2]


@responses.activate
def test_retries_timeouts(client):
    responses.get(PLAYERS_URL, body=requests.Timeout("slow"))
    responses.get(PLAYERS_URL, json={"response": {"player_count": 5, "result": 1}})
    assert client.get_current_players(1)["response"]["player_count"] == 5


@responses.activate
def test_gives_up_after_max_retries(client):
    responses.get(PLAYERS_URL, status=500)
    with pytest.raises(SteamAPIError, match="after 4 attempts"):
        client.get_current_players(1)
    assert len(responses.calls) == 4


@responses.activate
def test_does_not_retry_client_errors(client):
    responses.get(PLAYERS_URL, status=404)
    with pytest.raises(SteamAPIError, match="not retrying"):
        client.get_current_players(1)
    assert len(responses.calls) == 1


@responses.activate
def test_invalid_json_is_retried_then_fails(client):
    responses.get(APPDETAILS_URL, body="<html>oops</html>", status=200)
    with pytest.raises(SteamAPIError, match="invalid JSON"):
        client.get_app_details(1)


def test_throttle_spaces_out_requests():
    slept = []
    clock = iter([0.0, 0.2]).__next__  # second request comes 0.2s after the first
    c = SteamClient(min_interval_s=1.5, sleep=slept.append, clock=clock)
    c._last_request_at = clock()
    c._throttle()
    assert slept == [pytest.approx(1.3)]


@responses.activate
def test_recent_reviews_asks_for_recent_text(client):
    responses.get(REVIEWS_URL, json=load_fixture("review_text_1174180.json"))
    data = client.get_recent_reviews(1174180, count=500)
    sent = json.loads(parse_qs(urlparse(responses.calls[0].request.url).query)["input_json"][0])
    assert sent["filter"] == 1 and sent["num_per_page"] == 100  # Recent; capped at Steam's max
    assert sent["purchase_type"] == 1 and sent["languages"] == ["english"]
    assert len(data["reviews"]) == 5


@responses.activate
def test_most_played_and_app_name(client):
    responses.get(MOST_PLAYED_URL, json=load_fixture("market_chart.json"))
    responses.get(APPDETAILS_URL, json=load_fixture("appname_730.json"))
    responses.get(APPDETAILS_URL, json={"999": {"success": False}})
    assert len(client.get_most_played()["response"]["ranks"]) == 3
    assert client.get_app_name(730) == "Counter-Strike 2"
    assert parse_qs(urlparse(responses.calls[1].request.url).query)["filters"] == ["basic"]
    assert client.get_app_name(999) is None
