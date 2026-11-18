-- 08. Data coverage: which days are missing for which game?
--
-- A data-quality query in SQL. Build a full calendar with generate_series,
-- CROSS JOIN it with every game (all app/day combinations that *should* exist),
-- then LEFT JOIN the real data and keep the holes. This is the "anti-join"
-- pattern; NOT EXISTS would work too.
-- Concepts: generate_series, CROSS JOIN, LEFT JOIN ... IS NULL, STRING_AGG,
-- and a CTE that is referenced more than once.

WITH bounds AS (
    SELECT MIN(snapshot_date) AS first_day, MAX(snapshot_date) AS last_day
    FROM player_counts_daily
),
calendar AS (
    SELECT gs::date AS day
    FROM bounds, generate_series(bounds.first_day, bounds.last_day, interval '1 day') AS gs
),
tracked_apps AS (
    SELECT DISTINCT app_id FROM player_counts_daily
),
expected AS (
    SELECT t.app_id, c.day
    FROM tracked_apps t
    CROSS JOIN calendar c
)
SELECT
    e.app_id,
    COALESCE(a.name, e.app_id::text)                                          AS name,
    COUNT(*)                                                                  AS expected_days,
    COUNT(d.snapshot_date)                                                    AS days_with_data,
    COUNT(*) - COUNT(d.snapshot_date)                                         AS missing_days,
    STRING_AGG(e.day::text, ', ' ORDER BY e.day) FILTER (WHERE d.snapshot_date IS NULL)
                                                                              AS missing_dates
FROM expected e
LEFT JOIN player_counts_daily d
       ON d.app_id = e.app_id AND d.snapshot_date = e.day
LEFT JOIN apps a ON a.app_id = e.app_id
GROUP BY e.app_id, a.name
ORDER BY missing_days DESC, e.app_id;
