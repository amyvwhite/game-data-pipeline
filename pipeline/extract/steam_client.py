"""Small client for the public Steam endpoints this project uses.

Endpoints (checked against Valve's Steamworks docs and community docs, Nov 2026):

* Current players  - GET https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=<id>
  No API key needed. Returns {"response": {"player_count": <int>, "result": 1}}.
* App details      - GET https://store.steampowered.com/api/appdetails?appids=<id>&cc=<country>
  Unofficial but widely used store endpoint. Returns {"<id>": {"success": bool, "data": {...}}}.
  Prices are in cents. Only one app id per request works now, and it is rate limited
  (roughly 200 requests per 5 minutes), so we throttle.
* Review summary   - GET https://api.steampowered.com/IUserReviewsService/GetAppReviews/v1/?input_json={...}
  The replacement for the deprecated store.steampowered.com/appreviews/<id>?json=1 endpoint.
  query_summary (total_positive, total_negative, ...) only comes back on the first page.

The client handles timeouts, retries with exponential backoff, HTTP 429 (honours
Retry-After), and spaces requests out so we stay polite.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

import requests

log = logging.getLogger(__name__)

PLAYERS_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
REVIEWS_URL = "https://api.steampowered.com/IUserReviewsService/GetAppReviews/v1/"

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SteamAPIError(RuntimeError):
    """Raised when a request still fails after all retries (or fails in a non-retryable way)."""


class SteamClient:
    def __init__(
        self,
        timeout_s: float = 10.0,
        max_retries: int = 4,
        backoff_base_s: float = 1.0,
        min_interval_s: float = 1.5,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.min_interval_s = min_interval_s
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "game-data-pipeline/0.1 (portfolio project)")
        # sleep/clock are injectable so tests run instantly.
        self._sleep = sleep
        self._clock = clock
        self._last_request_at: float | None = None

    # ---- public methods -------------------------------------------------

    def get_current_players(self, app_id: int) -> dict[str, Any]:
        return self._get_json(PLAYERS_URL, {"appid": app_id})

    def get_app_details(self, app_id: int, country: str = "ca") -> dict[str, Any]:
        return self._get_json(APPDETAILS_URL, {"appids": app_id, "cc": country, "l": "english"})

    def get_review_summary(self, app_id: int) -> dict[str, Any]:
        # We only want the summary numbers, so ask for the smallest page possible.
        # languages=["all"] counts reviews in every language (default is English only).
        params = {
            "appid": app_id,
            "num_per_page": 1,
            "review_type": 0,  # all reviews - required for query_summary totals
            "languages": ["all"],
        }
        return self._get_json(REVIEWS_URL, {"input_json": json.dumps(params)})

    # ---- internals ------------------------------------------------------

    def _throttle(self) -> None:
        if self._last_request_at is None:
            return
        wait = self.min_interval_s - (self._clock() - self._last_request_at)
        if wait > 0:
            self._sleep(wait)

    def _backoff_seconds(self, attempt: int, response: requests.Response | None) -> float:
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                return float(retry_after)
        return self.backoff_base_s * (2**attempt)

    def _get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        last_error: str = ""
        for attempt in range(self.max_retries + 1):
            self._throttle()
            response: requests.Response | None = None
            try:
                self._last_request_at = self._clock()
                response = self.session.get(url, params=params, timeout=self.timeout_s)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError as exc:
                        # Steam sometimes returns an HTML error page or "null".
                        last_error = f"invalid JSON: {exc}"
                elif response.status_code in RETRYABLE_STATUS:
                    last_error = f"HTTP {response.status_code}"
                else:
                    raise SteamAPIError(f"{url} returned HTTP {response.status_code} (not retrying)")

            if attempt < self.max_retries:
                delay = self._backoff_seconds(attempt, response)
                log.warning(
                    "Request to %s failed (%s). Retry %d/%d in %.1fs",
                    url,
                    last_error,
                    attempt + 1,
                    self.max_retries,
                    delay,
                )
                self._sleep(delay)

        raise SteamAPIError(f"{url} failed after {self.max_retries + 1} attempts: {last_error}")
