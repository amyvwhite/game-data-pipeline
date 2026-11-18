-- 11. Does Twitch hype line up with players?
--
-- Matches each Twitch reading with the Steam player snapshot taken in the same
-- run (closest in time, within 15 minutes), then reports viewers per player and
-- the correlation between the two over the last 14 days.
-- Concepts: LATERAL + ORDER BY ... LIMIT 1 for a nearest-time join, CORR()
-- (Pearson correlation) as an aggregate, and FILTER.

WITH paired AS (
    SELECT
        tv.app_id,
        tv.captured_at,
        tv.viewer_count,
        p.player_count
    FROM twitch_viewer_snapshots tv
    CROSS JOIN LATERAL (
        SELECT s.player_count
        FROM player_count_snapshots s
        WHERE s.app_id = tv.app_id
          AND s.captured_at BETWEEN tv.captured_at - INTERVAL '15 minutes' AND tv.captured_at + INTERVAL '15 minutes'
        ORDER BY ABS(EXTRACT(EPOCH FROM (s.captured_at - tv.captured_at)))
        LIMIT 1
    ) p
    WHERE tv.captured_at > (SELECT MAX(captured_at) FROM twitch_viewer_snapshots) - INTERVAL '14 days'
)
SELECT
    pr.app_id,
    COALESCE(t.name, pr.app_id::text)                          AS name,
    COUNT(*)                                                   AS readings,
    ROUND(AVG(pr.viewer_count))                                AS avg_viewers,
    ROUND(AVG(pr.player_count))                                AS avg_players,
    ROUND(AVG(pr.viewer_count)::numeric / NULLIF(AVG(pr.player_count), 0), 3)
                                                               AS viewers_per_player,
    ROUND(CORR(pr.viewer_count, pr.player_count)::numeric, 2)  AS correlation
FROM paired pr
LEFT JOIN tracked_apps t USING (app_id)
GROUP BY pr.app_id, t.name
ORDER BY avg_viewers DESC
