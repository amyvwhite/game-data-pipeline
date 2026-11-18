"""Player-count forecasts, 24 hours ahead, with honest scoring.

Three models are written side by side (champion / challenger): the dashboard shows,
per game, whichever had the lowest error over the last 7 days, and the accuracy table
shows all of them, so the "smart" model has to earn its place.

* ``seasonal_profile`` - each game has a daily rhythm (quiet at 09:00 UTC, busy in the
  evening). Learn it from the last 7 days as "this hour is X times the surrounding 24-hour
  average" (a centred rolling window, so a half-finished day can't skew it), estimate the
  current level from the last few hours with the rhythm removed, and multiply back:
  forecast = level x profile[hour].
* ``naive_24h`` - "same as this hour yesterday". The standard baseline any forecast has to
  beat.
* ``ensemble`` - the average of the two. Averaging models with different errors is a
  cheap, well-established way to reduce error; on the demo data it beats both.

Every forecast row is written once, the first time its target hour comes within the
24-hour horizon (``INSERT ... ON CONFLICT DO NOTHING``), so stored forecasts are never
revised after the fact. ``sql/queries/13_forecast_accuracy.sql`` scores them against what
actually happened (MAPE) once the hour has passed.

The model functions are pure (lists in, lists out) so they're unit-tested without a database.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta, timezone
from statistics import mean, median

from sqlalchemy import Engine, text

log = logging.getLogger(__name__)

HORIZON_HOURS = 24
PROFILE_DAYS = 7
MIN_HISTORY_POINTS = 48  # two days of hourly data before we forecast at all
MIN_WINDOW_POINTS = 20  # a centred 24h window needs most of its hours to give a fair average
LEVEL_HOURS = 3  # how much recent history sets the level (chosen by backtest, see docs/adr/0003)

Series = Sequence[tuple[datetime, float]]


def hour_floor(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return ts.replace(minute=0, second=0, microsecond=0)


def target_hours(now: datetime, horizon: int = HORIZON_HOURS) -> list[datetime]:
    """The next `horizon` whole hours after `now` (UTC)."""
    start = hour_floor(now) + timedelta(hours=1)
    return [start + timedelta(hours=i) for i in range(horizon)]


def daily_profile(history: Series, days: int = PROFILE_DAYS) -> dict[int, float]:
    """Hour-of-day multipliers: median of (value / centred 24h average) per UTC hour, normalised to mean 1."""
    if not history:
        return {}
    by_hour = {hour_floor(t): v for t, v in history}
    cutoff = max(by_hour) - timedelta(days=days)
    ratios: dict[int, list[float]] = defaultdict(list)
    for t, v in by_hour.items():
        if t <= cutoff:
            continue
        window = [by_hour.get(t + timedelta(hours=k)) for k in range(-12, 12)]
        present = [w for w in window if w is not None]
        if len(present) < MIN_WINDOW_POINTS:
            continue
        local_mean = mean(present)
        if local_mean > 0:
            ratios[t.hour].append(v / local_mean)
    profile = {h: median(r) for h, r in ratios.items()}
    if not profile:
        return {}
    scale = mean(profile.values())
    return {h: p / scale for h, p in profile.items()} if scale > 0 else {}


def seasonal_profile_forecast(history: Series, targets: Iterable[datetime]) -> list[tuple[datetime, float]]:
    """Forecast each target hour as level x profile[hour]. Empty if there isn't enough history."""
    if len(history) < MIN_HISTORY_POINTS:
        return []
    profile = daily_profile(history)
    if not profile:
        return []
    last = max(t for t, _ in history)
    recent = [(t, v) for t, v in history if t > last - timedelta(hours=LEVEL_HOURS)]
    deseasonalised = [v / profile.get(t.hour, 1.0) for t, v in recent if profile.get(t.hour, 1.0) > 0]
    if not deseasonalised:
        return []
    level = mean(deseasonalised)
    return [(t, round(max(0.0, level * profile.get(t.hour, 1.0)), 1)) for t in targets]


def naive_24h_forecast(history: Series, targets: Iterable[datetime]) -> list[tuple[datetime, float]]:
    """Same hour yesterday. Skips targets whose 'yesterday' hour has no data."""
    by_hour = {hour_floor(t): v for t, v in history}
    out = []
    for t in targets:
        prev = by_hour.get(t - timedelta(hours=24))
        if prev is not None:
            out.append((t, round(float(prev), 1)))
    return out


def ensemble_forecast(history: Series, targets: Iterable[datetime]) -> list[tuple[datetime, float]]:
    """Average of the seasonal and naive forecasts, for hours where both exist."""
    targets = list(targets)
    seasonal = dict(seasonal_profile_forecast(history, targets))
    naive = dict(naive_24h_forecast(history, targets))
    return [(t, round((seasonal[t] + naive[t]) / 2, 1)) for t in targets if t in seasonal and t in naive]


MODELS = {
    "seasonal_profile": seasonal_profile_forecast,
    "naive_24h": naive_24h_forecast,
    "ensemble": ensemble_forecast,
}

HOURLY_HISTORY_SQL = """
SELECT s.app_id,
       date_trunc('hour', s.captured_at, 'UTC') AS hour,
       AVG(s.player_count)::float              AS players
FROM player_count_snapshots s
JOIN tracked_apps t USING (app_id)
WHERE s.captured_at >= :since
  AND s.captured_at <= :now  -- point-in-time: never let a forecast see the future
GROUP BY s.app_id, hour
ORDER BY s.app_id, hour
"""

INSERT_SQL = """
INSERT INTO player_forecasts (app_id, target_hour, model, predicted, made_at)
VALUES (:app_id, :target_hour, :model, :predicted, :made_at)
ON CONFLICT (app_id, target_hour, model) DO NOTHING
"""


def write_forecasts(engine: Engine, now: datetime | None = None) -> int:
    """Forecast the next 24 hours for every tracked Steam game. Returns rows inserted."""
    now = now or datetime.now(timezone.utc)
    since = hour_floor(now) - timedelta(days=PROFILE_DAYS + 1)
    with engine.connect() as conn:
        rows = conn.execute(text(HOURLY_HISTORY_SQL), {"since": since, "now": now}).all()
    history: dict[int, list[tuple[datetime, float]]] = defaultdict(list)
    for app_id, hour, players in rows:
        history[int(app_id)].append((hour_floor(hour), float(players)))

    targets = target_hours(now)
    payload = []
    for app_id, series in history.items():
        for model, fn in MODELS.items():
            for target, predicted in fn(series, targets):
                payload.append(
                    {"app_id": app_id, "target_hour": target, "model": model, "predicted": predicted, "made_at": now}
                )
    if not payload:
        log.info("Forecast: not enough history yet (need %d hourly points per game)", MIN_HISTORY_POINTS)
        return 0
    with engine.begin() as conn:
        inserted = max(conn.execute(text(INSERT_SQL), payload).rowcount or 0, 0)
    log.info("Forecast: %d new forecast rows (%d games)", inserted or 0, len(history))
    return inserted or 0


def backfill_forecasts(engine: Engine, start: datetime, end: datetime) -> int:
    """Replay write_forecasts hour by hour over past data (each step only sees data up to
    that hour), e.g. after loading synthetic history for the demo."""
    total, at = 0, hour_floor(start)
    while at <= end:
        total += write_forecasts(engine, now=at + timedelta(minutes=30))
        at += timedelta(hours=1)
    return total
