"""Extract step: call Steam for each configured app and save the raw responses.

Each raw file is an "envelope": a bit of metadata (which app, when we fetched it)
plus the untouched API payload. Keeping the payload untouched means I can
re-run transforms later without calling the API again.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pipeline.config import AppConfig
from pipeline.extract.steam_client import SteamAPIError, SteamClient
from pipeline.extract.storage import RawStore
from pipeline.extract.twitch_client import TwitchAPIError, TwitchClient

log = logging.getLogger(__name__)

SOURCES = ("players", "appdetails", "reviews", "twitch", "market", "review_text")

# Sources that make one request per tracked Steam app. "market" and "twitch" are handled separately.
PER_APP_SOURCES = ("players", "appdetails", "reviews", "review_text")
MARKET_KEY_ID = 0  # the market snapshot is one file per fetch, not per app


@dataclass
class ExtractResult:
    written: list[str] = field(default_factory=list)
    failures: list[tuple[str, int, str]] = field(default_factory=list)  # (source, app_id, error)


def raw_key(source: str, snapshot_date: date, app_id: int, fetched_at: datetime) -> str:
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    return f"raw/{source}/dt={snapshot_date.isoformat()}/{app_id}_{stamp}.json"


def run_extract(
    client: SteamClient,
    store: RawStore,
    apps: list[AppConfig],
    snapshot_date: date,
    sources: tuple[str, ...] = SOURCES,
    country: str = "ca",
    now: datetime | None = None,
    twitch: TwitchClient | None = None,
    known_names: set[int] | None = None,
    max_new_names: int = 25,
    review_sample: int = 100,
) -> ExtractResult:
    """Fetch every (source, app) pair. One failing app does not stop the others;
    failures are returned so the quality step can decide how bad that is."""
    result = ExtractResult()
    fetchers = {
        "players": client.get_current_players,
        "appdetails": lambda app_id: client.get_app_details(app_id, country=country),
        "reviews": client.get_review_summary,
        "review_text": lambda app_id: client.get_recent_reviews(app_id, count=review_sample),
    }
    for source in sources:
        if source == "market":
            _extract_market(client, store, snapshot_date, result, now, known_names or set(), max_new_names)
            continue
        source_apps = [a for a in apps if a.steam]
        if source == "twitch":
            if twitch is None:
                log.info("Twitch credentials not set; skipping the twitch source")
                continue
            source_apps = [a for a in apps if a.twitch_game]  # includes games that aren't on Steam
            fetchers["twitch"] = _twitch_fetcher(twitch, {a.app_id: a.twitch_game for a in source_apps})
        for app in source_apps:
            fetched_at = now or datetime.now(timezone.utc)
            try:
                payload = fetchers[source](app.app_id)
            except (SteamAPIError, TwitchAPIError) as exc:
                log.error("Extract failed for %s app %s: %s", source, app.app_id, exc)
                result.failures.append((source, app.app_id, str(exc)))
                continue
            result.written.append(_write(store, source, app.app_id, snapshot_date, fetched_at, payload))
    log.info("Extract wrote %d files, %d failures", len(result.written), len(result.failures))
    return result


def _write(store: RawStore, source: str, app_id: int, snapshot_date: date, fetched_at: datetime, payload) -> str:
    envelope = {
        "source": source,
        "app_id": app_id,
        "snapshot_date": snapshot_date.isoformat(),
        "fetched_at": fetched_at.isoformat(),
        "payload": payload,
    }
    return store.write_json(raw_key(source, snapshot_date, app_id, fetched_at), envelope)


def parse_most_played(payload: dict) -> list[dict]:
    """Pull the ranks list out of a GetMostPlayedGames response, tolerating a missing wrapper."""
    body = payload.get("response", payload) if isinstance(payload, dict) else {}
    ranks = body.get("ranks") or []
    return [r for r in ranks if isinstance(r, dict) and r.get("appid") is not None]


def _extract_market(
    client: SteamClient,
    store: RawStore,
    snapshot_date: date,
    result: ExtractResult,
    now: datetime | None,
    known_names: set[int],
    max_new_names: int,
) -> None:
    """One market snapshot: Steam's top-100 chart, plus the live player count of each game
    and a name for any game we haven't named before (capped per run to stay polite)."""
    fetched_at = now or datetime.now(timezone.utc)
    try:
        chart = client.get_most_played()
    except SteamAPIError as exc:
        log.error("Extract failed for market chart: %s", exc)
        result.failures.append(("market", MARKET_KEY_ID, str(exc)))
        return
    ranks = parse_most_played(chart)
    if not ranks:
        result.failures.append(("market", MARKET_KEY_ID, "chart response had no ranks"))
        return

    entries, new_names = [], 0
    for r in ranks:
        app_id = int(r["appid"])
        entry = {
            "rank": r.get("rank"),
            "appid": app_id,
            "last_week_rank": r.get("last_week_rank"),
            "peak_in_game": r.get("peak_in_game"),
            "current_players": None,
            "name": None,
        }
        try:
            players = client.get_current_players(app_id).get("response", {})
            if players.get("result") == 1:
                entry["current_players"] = players.get("player_count")
        except SteamAPIError as exc:
            log.warning("Market: no player count for %s: %s", app_id, exc)
        if app_id not in known_names and new_names < max_new_names:
            try:
                entry["name"] = client.get_app_name(app_id)
                new_names += 1
            except SteamAPIError as exc:
                log.warning("Market: no name for %s: %s", app_id, exc)
        entries.append(entry)

    body = chart.get("response", chart)
    payload = {"rollup_date": body.get("rollup_date"), "ranks": entries}
    result.written.append(_write(store, "market", MARKET_KEY_ID, snapshot_date, fetched_at, payload))
    missing = sum(1 for e in entries if e["current_players"] is None)
    if missing > len(entries) // 5:  # a few gaps are normal; alert only if >20% are missing
        result.failures.append(("market", MARKET_KEY_ID, f"{missing} of {len(entries)} games had no player count"))


def _twitch_fetcher(twitch: TwitchClient, names: dict[int, str]):
    def fetch(app_id: int) -> dict:
        return twitch.get_game_viewers(names[app_id])

    return fetch
