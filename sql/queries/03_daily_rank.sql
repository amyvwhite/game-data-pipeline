-- 03. Rank the games by players each day, and how their rank moved
--
-- Question: which tracked game was #1 each day, and who climbed or fell?
-- Concepts: RANK() vs DENSE_RANK(), PARTITION BY date, LAG() over a computed rank
-- (window function on top of a window function needs a CTE in between).
--
-- RANK() leaves gaps after ties (1, 1, 3); DENSE_RANK() doesn't (1, 1, 2).
--
-- Performance note: PARTITION BY snapshot_date means Postgres scans whole days
-- across all apps; idx_player_daily_date (see schema.sql) helps when you add
-- a date filter. Check with EXPLAIN ANALYZE once there's real volume.

WITH ranked AS (
    SELECT
        app_id,
        snapshot_date,
        avg_players,
        RANK()       OVER (PARTITION BY snapshot_date ORDER BY avg_players DESC) AS player_rank,
        DENSE_RANK() OVER (PARTITION BY snapshot_date ORDER BY avg_players DESC) AS player_dense_rank
    FROM player_counts_daily
),
with_movement AS (
    SELECT
        *,
        LAG(player_rank) OVER (PARTITION BY app_id ORDER BY snapshot_date) AS prev_rank
    FROM ranked
)
SELECT
    m.snapshot_date,
    m.player_rank,
    m.player_dense_rank,
    COALESCE(a.name, m.app_id::text) AS name,
    m.avg_players,
    m.prev_rank,
    m.prev_rank - m.player_rank      AS places_moved   -- positive = climbed
FROM with_movement m
LEFT JOIN apps a USING (app_id)
ORDER BY m.snapshot_date DESC, m.player_rank;
