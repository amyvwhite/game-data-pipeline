# 0003. Three forecast models, champion picked by out-of-sample error

**Status:** Accepted (Nov 2026)

## Context
A forecast is only worth showing if it beats something simple, and it must be scored on
data it did not see.

## Decision
- Every run writes 24-hour-ahead forecasts for three models: `seasonal_profile` (hour-of-day
  rhythm from a centred 24-hour rolling window x current level), `naive_24h` (same hour
  yesterday, the baseline) and `ensemble` (their average).
- Forecasts are point-in-time: the history query is capped at the forecast time, and each
  (game, hour, model) row is inserted once (`ON CONFLICT DO NOTHING`), so it is never
  revised after the fact.
- `sql/queries/13_forecast_accuracy.sql` scores the last 7 days (MAPE) and picks a
  champion per game with `DISTINCT ON`. The dashboard draws the champion and shows all three.

## Backtest that set the parameters (demo data, 1,848 scored hours)
| Variant | MAPE |
|---|---|
| Naive (same hour yesterday) | 7.38% |
| Seasonal, calendar-day profile, 24h level | 8.81% |
| Seasonal, rolling-window profile, 3h level | 7.39% |
| Ensemble (rolling seasonal + naive) | **7.21%** |

The first seasonal version lost to the baseline because the latest, partial UTC day skewed
its daily average; the rolling window fixed that, and averaging the two won.

## Consequences
- The dashboard can say "baseline wins" for a game, and that is reported, not hidden.
- Weekly seasonality (weekday vs weekend) is not modelled yet; with 2+ weeks of real data
  a `naive_168h` (same hour last week) challenger is the next model to add.
