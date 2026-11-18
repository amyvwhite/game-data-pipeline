-- 10. Take-Two vs competitors: share of Steam players over time.
--
-- For each day, what share of the players across all tracked games were playing
-- a Take-Two title? Also ranks the publisher groups by day.
-- Concepts: joining a dimension table (tracked_apps) for grouping, SUM() OVER
-- (PARTITION BY ...) to get the daily total without a second query, and a
-- percentage of total.

WITH daily_group AS (
    SELECT
        d.snapshot_date,
        t.publisher_group,
        SUM(d.avg_players) AS avg_players
    FROM player_counts_daily d
    JOIN tracked_apps t USING (app_id)
    GROUP BY d.snapshot_date, t.publisher_group
)
SELECT
    snapshot_date,
    publisher_group,
    ROUND(avg_players)                                                          AS avg_players,
    ROUND(100.0 * avg_players / NULLIF(SUM(avg_players) OVER (PARTITION BY snapshot_date), 0), 1)
                                                                                AS share_pct,
    RANK() OVER (PARTITION BY snapshot_date ORDER BY avg_players DESC)          AS rank_that_day
FROM daily_group
ORDER BY snapshot_date DESC, rank_that_day
