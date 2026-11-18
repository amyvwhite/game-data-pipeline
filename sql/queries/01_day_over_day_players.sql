-- 01. Day-over-day change in players, per game
--
-- Question: did each game gain or lose players compared with the day before?
-- Concepts: CTE, LAG() window function, NULLIF to avoid dividing by zero.
--
-- Gotcha: LAG() gives the previous *row*, not the previous *calendar day*.
-- If the pipeline missed a day, the "previous row" could be two days back,
-- so I also pull the previous date and only report a change when it really
-- is yesterday.

WITH daily AS (
    SELECT
        d.app_id,
        COALESCE(a.name, d.app_id::text)                                       AS name,
        d.snapshot_date,
        d.avg_players,
        d.max_players,
        LAG(d.avg_players)   OVER (PARTITION BY d.app_id ORDER BY d.snapshot_date) AS prev_avg_players,
        LAG(d.snapshot_date) OVER (PARTITION BY d.app_id ORDER BY d.snapshot_date) AS prev_date
    FROM player_counts_daily d
    LEFT JOIN apps a USING (app_id)
)
SELECT
    app_id,
    name,
    snapshot_date,
    avg_players,
    max_players,
    CASE WHEN prev_date = snapshot_date - 1 THEN prev_avg_players END            AS yesterday_avg,
    CASE WHEN prev_date = snapshot_date - 1
         THEN avg_players - prev_avg_players END                                 AS change,
    CASE WHEN prev_date = snapshot_date - 1
         THEN ROUND(100.0 * (avg_players - prev_avg_players) / NULLIF(prev_avg_players, 0), 1)
    END                                                                          AS pct_change
FROM daily
ORDER BY app_id, snapshot_date;
