-- 02. 7-day rolling average and spike detection
--
-- Question: which days were unusually busy (or quiet) compared with the past week?
-- Concepts: window frame (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), COUNT() over
-- the same frame to spot incomplete windows, a CTE feeding a second CTE.
--
-- I compare each day with the average of the 7 days *before* it (1-7 PRECEDING),
-- so today's value doesn't water down its own baseline.

WITH windowed AS (
    SELECT
        app_id,
        snapshot_date,
        avg_players,
        ROUND(AVG(avg_players) OVER w7, 1)                         AS rolling_7d_avg,
        ROUND(AVG(avg_players) OVER prior7, 1)                     AS prior_7d_avg,
        COUNT(*)              OVER prior7                          AS prior_days_in_window
    FROM player_counts_daily
    WINDOW
        w7     AS (PARTITION BY app_id ORDER BY snapshot_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        prior7 AS (PARTITION BY app_id ORDER BY snapshot_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
),
flagged AS (
    SELECT
        *,
        ROUND(100.0 * (avg_players - prior_7d_avg) / NULLIF(prior_7d_avg, 0), 1) AS pct_vs_prior_week
    FROM windowed
)
SELECT
    f.app_id,
    COALESCE(a.name, f.app_id::text) AS name,
    f.snapshot_date,
    f.avg_players,
    f.rolling_7d_avg,
    f.prior_7d_avg,
    f.pct_vs_prior_week,
    CASE
        WHEN f.prior_days_in_window < 3          THEN 'not enough history'
        WHEN f.pct_vs_prior_week >=  20          THEN 'spike'
        WHEN f.pct_vs_prior_week <= -20          THEN 'drop'
        ELSE 'normal'
    END AS signal
FROM flagged f
LEFT JOIN apps a USING (app_id)
ORDER BY f.app_id, f.snapshot_date;
