-- 09. Is anything unusual happening right now?
--
-- For each player-count snapshot from the last 3 hours (relative to the newest
-- snapshot in the table), compare it with that game's normal level and flag it
-- when it is far outside the usual range.
--
-- "Normal" is chosen in two steps:
--   1. Same time of day: snapshots from the previous 14 days within +/- 1 hour of
--      this one. Games have a strong daily rhythm, so 9pm should be compared with
--      other 9pms, not with 4am. Used when there are at least 5 such points.
--   2. Fallback for new data: every snapshot from the previous 72 hours (needs 12+).
--
-- A snapshot is an anomaly when it is at least 3 standard deviations from the
-- baseline AND at least 20 percent away from it (so tiny, steady games don't alert on
-- noise). The pipeline runs this after every load and saves new hits into
-- player_anomalies, which triggers an alert.
--
-- Concepts: LATERAL joins (a correlated subquery per row), STDDEV_SAMP, CASE to
-- pick between two baselines, NULLIF to avoid dividing by zero, and a z-score.

WITH anchor AS (
    SELECT MAX(captured_at) AS latest FROM player_count_snapshots
),
recent AS (
    SELECT s.app_id, s.captured_at, s.player_count
    FROM player_count_snapshots s, anchor
    WHERE s.captured_at > anchor.latest - INTERVAL '3 hours'
),
scored AS (
    SELECT
        r.app_id,
        r.captured_at,
        r.player_count,
        CASE WHEN same_hour.n >= 5 THEN same_hour.avg_players ELSE rolling.avg_players END AS baseline_avg,
        CASE WHEN same_hour.n >= 5 THEN same_hour.std_players ELSE rolling.std_players END AS baseline_std,
        CASE WHEN same_hour.n >= 5 THEN same_hour.n           ELSE rolling.n           END AS baseline_points,
        CASE WHEN same_hour.n >= 5 THEN 'same hour, last 14 days' ELSE 'last 72 hours' END AS baseline_kind,
        (same_hour.n >= 5 OR rolling.n >= 12)                                               AS enough_history
    FROM recent r
    CROSS JOIN LATERAL (
        SELECT AVG(h.player_count) AS avg_players, STDDEV_SAMP(h.player_count) AS std_players, COUNT(*) AS n
        FROM player_count_snapshots h
        WHERE h.app_id = r.app_id
          AND h.captured_at <  r.captured_at - INTERVAL '30 minutes'
          AND h.captured_at >= r.captured_at - INTERVAL '14 days'
          -- within an hour of the same time of day (wrapping around midnight)
          AND LEAST(
                ABS(EXTRACT(EPOCH FROM (h.captured_at::time - r.captured_at::time))),
                86400 - ABS(EXTRACT(EPOCH FROM (h.captured_at::time - r.captured_at::time)))
              ) <= 3600
    ) same_hour
    CROSS JOIN LATERAL (
        SELECT AVG(w.player_count) AS avg_players, STDDEV_SAMP(w.player_count) AS std_players, COUNT(*) AS n
        FROM player_count_snapshots w
        WHERE w.app_id = r.app_id
          AND w.captured_at <  r.captured_at - INTERVAL '30 minutes'
          AND w.captured_at >= r.captured_at - INTERVAL '72 hours'
    ) rolling
)
SELECT
    s.app_id,
    COALESCE(a.name, s.app_id::text)                                           AS name,
    s.captured_at,
    s.player_count,
    ROUND(s.baseline_avg, 1)                                                   AS baseline_avg,
    ROUND(s.baseline_std, 1)                                                   AS baseline_std,
    s.baseline_points,
    s.baseline_kind,
    ROUND((s.player_count - s.baseline_avg) / NULLIF(s.baseline_std, 0), 2)    AS z_score,
    ROUND(100.0 * (s.player_count / NULLIF(s.baseline_avg, 0) - 1), 1)         AS pct_vs_baseline,
    CASE WHEN s.player_count > s.baseline_avg THEN 'spike' ELSE 'drop' END     AS direction
FROM scored s
LEFT JOIN apps a USING (app_id)
WHERE s.enough_history
  AND s.baseline_std > 0
  AND ABS(s.player_count - s.baseline_avg) / s.baseline_std >= 3
  AND ABS(s.player_count / NULLIF(s.baseline_avg, 0) - 1) >= 0.20
ORDER BY ABS((s.player_count - s.baseline_avg) / NULLIF(s.baseline_std, 0)) DESC
