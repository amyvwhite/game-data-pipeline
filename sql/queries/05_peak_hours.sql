-- 05. Peak hours: what time of day (Toronto time) is each game busiest?
--
-- Needs intra-day snapshots, so schedule the players extract hourly:
--     python -m pipeline.run --sources players
-- Concepts: time-zone conversion (AT TIME ZONE), EXTRACT, scalar subquery for
-- "last 30 days of data we have", ROW_NUMBER() to pick the top row per group.

WITH recent AS (
    SELECT
        app_id,
        EXTRACT(HOUR FROM captured_at AT TIME ZONE 'America/Toronto')::int AS local_hour,
        player_count
    FROM player_count_snapshots
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM player_count_snapshots) - 30
),
by_hour AS (
    SELECT
        app_id,
        local_hour,
        ROUND(AVG(player_count)) AS avg_players,
        COUNT(*)                 AS samples
    FROM recent
    GROUP BY app_id, local_hour
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY app_id ORDER BY avg_players DESC) AS busiest_rank,
        ROW_NUMBER() OVER (PARTITION BY app_id ORDER BY avg_players ASC)  AS quietest_rank
    FROM by_hour
)
SELECT
    r.app_id,
    COALESCE(a.name, r.app_id::text)                                AS name,
    MAX(r.local_hour)  FILTER (WHERE r.busiest_rank  = 1)           AS peak_hour_toronto,
    MAX(r.avg_players) FILTER (WHERE r.busiest_rank  = 1)           AS peak_avg_players,
    MAX(r.local_hour)  FILTER (WHERE r.quietest_rank = 1)           AS quiet_hour_toronto,
    MAX(r.avg_players) FILTER (WHERE r.quietest_rank = 1)           AS quiet_avg_players,
    SUM(r.samples)                                                  AS snapshots_used
FROM ranked r
LEFT JOIN apps a USING (app_id)
GROUP BY r.app_id, a.name
ORDER BY peak_avg_players DESC;
