"""Small client for the Twitch Helix API: how many people are watching each game right now.

Endpoints (Twitch API docs, Nov 2026):

* App token  - POST https://id.twitch.tv/oauth2/token  (grant_type=client_credentials)
  A "client credentials" token is the server-to-server kind; it's enough for public data.
* Game ID    - GET https://api.twitch.tv/helix/games?name=<category name>
* Streams    - GET https://api.twitch.tv/helix/streams?game_id=<id>&first=100&after=<cursor>
  Returns live streams with viewer_count, 100 per page, newest cursor in pagination.cursor.

Twitch has no "total viewers for a game" endpoint, so I page through the live streams
and add up viewer_count. Streams come back sorted by viewers, so the first few pages
hold almost everyone; I stop after `max_pages` and record whether I stopped early
("truncated") so the number is honest about what it covers.

Every call sends both the Client-Id header and the Bearer token, as Helix requires.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import requests

log = logging.getLogger(__name__)

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
GAMES_URL = "https://api.twitch.tv/helix/games"
STREAMS_URL = "https://api.twitch.tv/helix/streams"

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class TwitchAPIError(RuntimeError):
    """A Twitch request still failed after retries, or failed in a way retrying won't fix."""


class TwitchClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        timeout_s: float = 10.0,
        max_retries: int = 3,
        max_pages: int = 10,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not client_id or not client_secret:
            raise ValueError("Twitch client id and secret are both required")
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.max_pages = max_pages
        self.session = session or requests.Session()
        self._sleep = sleep
        self._token: str | None = None
        self._game_ids: dict[str, str | None] = {}

    # ---- auth -----------------------------------------------------------------

    def _get_token(self) -> str:
        if self._token is None:
            resp = self.session.post(
                TOKEN_URL,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
                timeout=self.timeout_s,
            )
            if resp.status_code != 200:
                raise TwitchAPIError(f"Token request failed: HTTP {resp.status_code}")
            self._token = resp.json()["access_token"]
        return self._token

    # ---- requests ---------------------------------------------------------------

    def _get(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        refreshed = False
        for attempt in range(self.max_retries + 1):
            headers = {"Client-Id": self.client_id, "Authorization": f"Bearer {self._get_token()}"}
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout_s)
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise TwitchAPIError(f"{url}: {exc}") from exc
                self._sleep(2**attempt)
                continue
            if resp.status_code == 401 and not refreshed:
                # Token expired or revoked: get a new one once, then retry.
                self._token = None
                refreshed = True
                continue
            if resp.status_code in RETRYABLE_STATUS and attempt < self.max_retries:
                reset = resp.headers.get("Ratelimit-Reset")
                wait = max(1.0, float(reset) - time.time()) if reset and resp.status_code == 429 else 2**attempt
                self._sleep(min(wait, 60))
                continue
            if resp.status_code != 200:
                raise TwitchAPIError(f"{url}: HTTP {resp.status_code}")
            return resp.json()
        raise TwitchAPIError(f"{url}: gave up after {self.max_retries} retries")

    def get_game_id(self, name: str) -> str | None:
        """Twitch category id for an exact category name, or None if Twitch doesn't know it."""
        if name not in self._game_ids:
            data = self._get(GAMES_URL, {"name": name}).get("data", [])
            self._game_ids[name] = data[0]["id"] if data else None
        return self._game_ids[name]

    def get_game_viewers(self, name: str) -> dict[str, Any]:
        """Total live viewers and streams for one Twitch category, summed over up to max_pages pages."""
        game_id = self.get_game_id(name)
        if game_id is None:
            return {"twitch_game": name, "found": False, "viewer_count": None, "stream_count": None}
        viewers = streams = pages = 0
        cursor: str | None = None
        truncated = False
        while True:
            params: dict[str, Any] = {"game_id": game_id, "first": 100}
            if cursor:
                params["after"] = cursor
            body = self._get(STREAMS_URL, params)
            page = body.get("data", [])
            pages += 1
            streams += len(page)
            viewers += sum(int(s.get("viewer_count") or 0) for s in page)
            cursor = (body.get("pagination") or {}).get("cursor")
            if not page or not cursor:
                break
            if pages >= self.max_pages:
                truncated = True
                break
        return {
            "twitch_game": name,
            "twitch_game_id": game_id,
            "found": True,
            "viewer_count": viewers,
            "stream_count": streams,
            "pages": pages,
            "truncated": truncated,
        }
