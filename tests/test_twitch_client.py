import pytest
import responses

from pipeline.extract.twitch_client import GAMES_URL, STREAMS_URL, TOKEN_URL, TwitchAPIError, TwitchClient


def client(**kw):
    return TwitchClient("cid", "secret", sleep=lambda s: None, **kw)


def add_token(token="tok"):
    responses.add(
        responses.POST, TOKEN_URL, json={"access_token": token, "expires_in": 5000000, "token_type": "bearer"}
    )


def stream_page(viewers, cursor=None):
    body = {"data": [{"viewer_count": v} for v in viewers], "pagination": {}}
    if cursor:
        body["pagination"]["cursor"] = cursor
    return body


@responses.activate
def test_sums_viewers_across_pages_and_sends_auth_headers():
    add_token()
    responses.add(responses.GET, GAMES_URL, json={"data": [{"id": "493959", "name": "Red Dead Redemption 2"}]})
    responses.add(responses.GET, STREAMS_URL, json=stream_page([500, 300], cursor="c1"))
    responses.add(responses.GET, STREAMS_URL, json=stream_page([20]))

    result = client().get_game_viewers("Red Dead Redemption 2")

    assert result == {
        "twitch_game": "Red Dead Redemption 2",
        "twitch_game_id": "493959",
        "found": True,
        "viewer_count": 820,
        "stream_count": 3,
        "pages": 2,
        "truncated": False,
    }
    stream_call = responses.calls[2].request
    assert stream_call.headers["Client-Id"] == "cid"
    assert stream_call.headers["Authorization"] == "Bearer tok"
    assert "after=c1" in responses.calls[3].request.url


@responses.activate
def test_stops_after_max_pages_and_says_so():
    add_token()
    responses.add(responses.GET, GAMES_URL, json={"data": [{"id": "1"}]})
    for i in range(5):
        responses.add(responses.GET, STREAMS_URL, json=stream_page([10], cursor=f"c{i}"))
    result = client(max_pages=2).get_game_viewers("Busy Game")
    assert result["pages"] == 2 and result["truncated"] is True and result["viewer_count"] == 20


@responses.activate
def test_unknown_category_is_not_an_error():
    add_token()
    responses.add(responses.GET, GAMES_URL, json={"data": []})
    result = client().get_game_viewers("Not A Real Game")
    assert result["found"] is False and result["viewer_count"] is None


@responses.activate
def test_expired_token_is_refreshed_once():
    add_token("old")
    add_token("new")
    responses.add(responses.GET, GAMES_URL, status=401)
    responses.add(responses.GET, GAMES_URL, json={"data": [{"id": "7"}]})
    assert client().get_game_id("X") == "7"
    assert responses.calls[-1].request.headers["Authorization"] == "Bearer new"


@responses.activate
def test_server_errors_are_retried_then_raise():
    add_token()
    for _ in range(4):
        responses.add(responses.GET, GAMES_URL, status=503)
    with pytest.raises(TwitchAPIError):
        client(max_retries=3).get_game_id("X")


def test_requires_credentials():
    with pytest.raises(ValueError):
        TwitchClient("", "")
