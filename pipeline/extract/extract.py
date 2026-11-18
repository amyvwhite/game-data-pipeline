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

SOURCES = ("players", "appdetails", "reviews", "twitch")


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
) -> ExtractResult:
    """Fetch every (source, app) pair. One failing app does not stop the others;
    failures are returned so the quality step can decide how bad that is."""
    result = ExtractResult()
    fetchers = {
        "players": client.get_current_players,
        "appdetails": lambda app_id: client.get_app_details(app_id, country=country),
        "reviews": client.get_review_summary,
    }
    for source in sources:
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
            envelope = {
                "source": source,
                "app_id": app.app_id,
                "snapshot_date": snapshot_date.isoformat(),
                "fetched_at": fetched_at.isoformat(),
                "payload": payload,
            }
            location = store.write_json(raw_key(source, snapshot_date, app.app_id, fetched_at), envelope)
            result.written.append(location)
    log.info("Extract wrote %d files, %d failures", len(result.written), len(result.failures))
    return result


def _twitch_fetcher(twitch: TwitchClient, names: dict[int, str]):
    def fetch(app_id: int) -> dict:
        return twitch.get_game_viewers(names[app_id])

    return fetch
