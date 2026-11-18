"""Unit tests for the forecast models (pure functions, no database)."""

from datetime import datetime, timedelta, timezone

import pytest

from pipeline.forecast import (
    MIN_HISTORY_POINTS,
    daily_profile,
    hour_floor,
    naive_24h_forecast,
    seasonal_profile_forecast,
    target_hours,
)

NOW = datetime(2026, 11, 18, 15, 37, tzinfo=timezone.utc)


def rhythm(hour: int) -> float:
    """A simple daily shape: quiet mornings, busy evenings (UTC)."""
    return 2.0 if 18 <= hour <= 23 else (0.5 if 6 <= hour <= 11 else 1.0)


def make_history(days: int = 7, level: float = 10_000, end: datetime = NOW) -> list[tuple[datetime, float]]:
    last = hour_floor(end)
    return [(last - timedelta(hours=h), level * rhythm((last - timedelta(hours=h)).hour)) for h in range(days * 24)]


def test_target_hours_are_the_next_24_whole_hours():
    hours = target_hours(NOW)
    assert len(hours) == 24
    assert hours[0] == datetime(2026, 11, 18, 16, tzinfo=timezone.utc)
    assert hours[-1] == datetime(2026, 11, 19, 15, tzinfo=timezone.utc)


def test_profile_learns_the_daily_rhythm_and_is_normalised():
    profile = daily_profile(make_history())
    assert sum(profile.values()) / len(profile) == pytest.approx(1.0)
    assert profile[20] / profile[9] == pytest.approx(4.0, rel=0.01)  # evening is 4x the morning


def test_seasonal_forecast_recovers_a_perfect_pattern():
    history = make_history()
    forecast = dict(seasonal_profile_forecast(history, target_hours(NOW)))
    for t, predicted in forecast.items():
        assert predicted == pytest.approx(10_000 * rhythm(t.hour), rel=0.01)


def test_seasonal_forecast_follows_a_level_shift():
    # Same rhythm, but the last day is 50% busier (e.g. a free weekend): forecast should follow.
    history = [(t, v * (1.5 if t > hour_floor(NOW) - timedelta(hours=24) else 1.0)) for t, v in make_history()]
    forecast = dict(seasonal_profile_forecast(history, target_hours(NOW)))
    evening = datetime(2026, 11, 18, 20, tzinfo=timezone.utc)
    assert forecast[evening] == pytest.approx(15_000 * rhythm(20), rel=0.05)


def test_no_forecast_without_enough_history():
    short = make_history(days=1)[: MIN_HISTORY_POINTS - 1]
    assert seasonal_profile_forecast(short, target_hours(NOW)) == []


def test_naive_is_same_hour_yesterday_and_skips_gaps():
    history = make_history(days=2)
    target = datetime(2026, 11, 18, 20, tzinfo=timezone.utc)
    gap = datetime(2026, 11, 18, 21, tzinfo=timezone.utc)
    history = [(t, v) for t, v in history if t != gap - timedelta(hours=24)]
    out = dict(naive_24h_forecast(history, [target, gap]))
    assert out == {target: 10_000 * rhythm(20)}


def test_forecasts_are_never_negative():
    history = [(t, 0.0) for t, _ in make_history()]
    assert all(v >= 0 for _, v in seasonal_profile_forecast(history, target_hours(NOW)))
