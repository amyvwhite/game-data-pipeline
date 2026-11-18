-- 12. GTA VI launch watch: does a new console release pull players away from the old games?
--
-- GTA VI launches on PS5 / Xbox on 2026-11-19 (no PC or Steam version announced), so it
-- can't be measured on Steam. Instead this compares, day by day:
--   * each Take-Two game's Steam players vs its own average in the 7 days before launch
--   * GTA VI's Twitch viewers that day (how much attention the launch is getting)
-- A falling GTA V line while GTA VI viewers are high would suggest players moving over;
-- it's a correlation, so check it against sales, updates and holidays.
--
-- Concepts: a one-row "params" CTE so the launch date lives in one place, a baseline
-- window relative to that date, LEFT JOINs so days without Twitch data still show.

WITH params AS (
    SELECT DATE '2026-11-19' AS launch_date, 'Grand Theft Auto VI'::text AS new_game
),
new_game_viewers AS (
    SELECT tv.snapshot_date, ROUND(AVG(tv.viewer_count)) AS new_game_avg_viewers
    FROM twitch_viewer_snapshots tv
    JOIN tracked_apps t USING (app_id)
    CROSS JOIN params
    WHERE t.twitch_game = params.new_game
    GROUP BY tv.snapshot_date
),
pre_launch AS (
    SELECT d.app_id, AVG(d.avg_players) AS pre_launch_avg
    FROM player_counts_daily d
    CROSS JOIN params
    WHERE d.snapshot_date BETWEEN params.launch_date - 7 AND params.launch_date - 1
    GROUP BY d.app_id
)
SELECT
    d.snapshot_date,
    d.app_id,
    t.name,
    d.snapshot_date >= params.launch_date                                    AS after_launch,
    ROUND(d.avg_players)                                                     AS avg_players,
    ROUND(p.pre_launch_avg)                                                  AS pre_launch_avg,
    ROUND(100.0 * (d.avg_players / NULLIF(p.pre_launch_avg, 0) - 1), 1)      AS change_vs_pre_launch_pct,
    v.new_game_avg_viewers                                                   AS gta6_twitch_viewers
FROM player_counts_daily d
JOIN tracked_apps t USING (app_id)
CROSS JOIN params
LEFT JOIN pre_launch p USING (app_id)
LEFT JOIN new_game_viewers v ON v.snapshot_date = d.snapshot_date
WHERE t.publisher_group = 'Take-Two'
ORDER BY d.snapshot_date DESC, d.avg_players DESC
