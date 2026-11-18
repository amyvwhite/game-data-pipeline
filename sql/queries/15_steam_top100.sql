-- 15. Where do the tracked games sit in the whole Steam market?
--
-- Question: in the latest top-100 snapshot, what rank is each game, how many players
-- does it have, and what share of all top-100 players do Take-Two titles hold?
-- Concepts: latest-snapshot filtering with a scalar subquery, LEFT JOINs to two
-- dimension tables (names + publisher groups), and a window SUM for share of total.

WITH latest AS (
    SELECT * FROM market_snapshots
    WHERE captured_at = (SELECT MAX(captured_at) FROM market_snapshots)
)
SELECT l.rank,
       l.app_id,
       COALESCE(t.name, n.name, a.name, 'App ' || l.app_id)                 AS name,
       COALESCE(NULLIF(t.publisher_group, ''), 'Other')                     AS publisher_group,
       l.current_players,
       l.peak_in_game                                                       AS daily_peak,
       l.last_week_rank,
       ROUND(100.0 * l.current_players / NULLIF(SUM(l.current_players) OVER (), 0), 2) AS share_of_top100_pct,
       l.captured_at
FROM latest l
LEFT JOIN tracked_apps t    ON t.app_id = l.app_id
LEFT JOIN steam_app_names n ON n.app_id = l.app_id
LEFT JOIN apps a            ON a.app_id = l.app_id
ORDER BY l.rank;
