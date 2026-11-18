-- 13. How good are the player-count forecasts?
--
-- Question: over the last 7 days, how far off were the 24-hour-ahead forecasts, which
-- model wins for each game (the "champion"), and does it beat the "same hour yesterday"
-- baseline?
-- Concepts: bucketing timestamps to the hour (date_trunc with a time zone), joining
-- predictions to actuals, MAPE (mean absolute percentage error), and a skill score
-- (1 - champion error / baseline error; above 0 means the model adds value), and picking
-- a winner per group with DISTINCT ON.
--
-- Only hours that have already happened are scored, and each forecast was written
-- before its hour (see pipeline/forecast.py), so this is a fair out-of-sample test.

WITH actual AS (
    SELECT app_id,
           date_trunc('hour', captured_at, 'UTC') AS target_hour,
           AVG(player_count)                      AS actual
    FROM player_count_snapshots
    WHERE captured_at >= (SELECT MAX(captured_at) FROM player_count_snapshots) - INTERVAL '7 days'
    GROUP BY app_id, target_hour
),
per_model AS (
    SELECT f.app_id, f.model,
           COUNT(*)                                                         AS hours_scored,
           ROUND((100 * AVG(ABS(f.predicted - a.actual) / a.actual))::numeric, 1) AS mape_pct
    FROM player_forecasts f
    JOIN actual a USING (app_id, target_hour)
    WHERE a.actual > 0
    GROUP BY f.app_id, f.model
),
champion AS (
    SELECT DISTINCT ON (app_id) app_id, model, mape_pct, hours_scored
    FROM per_model
    ORDER BY app_id, mape_pct, model
)
SELECT c.app_id,
       COALESCE(t.name, c.app_id::text)                                     AS name,
       c.hours_scored,
       c.model                                                              AS champion,
       c.mape_pct                                                           AS champion_mape_pct,
       MAX(p.mape_pct) FILTER (WHERE p.model = 'seasonal_profile')          AS seasonal_mape_pct,
       MAX(p.mape_pct) FILTER (WHERE p.model = 'ensemble')                  AS ensemble_mape_pct,
       MAX(p.mape_pct) FILTER (WHERE p.model = 'naive_24h')                 AS baseline_mape_pct,
       ROUND(1 - c.mape_pct / NULLIF(MAX(p.mape_pct) FILTER (WHERE p.model = 'naive_24h'), 0), 2) AS skill
FROM champion c
JOIN per_model p ON p.app_id = c.app_id
LEFT JOIN tracked_apps t ON t.app_id = c.app_id
GROUP BY c.app_id, t.name, c.hours_scored, c.model, c.mape_pct
ORDER BY c.mape_pct;
