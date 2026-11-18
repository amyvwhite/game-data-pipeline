-- 14. Did a sale actually bring players back? (difference-in-differences)
--
-- Question: when a game goes on sale, how much do its players change compared with the
-- other tracked games that were NOT on sale over the same days?
-- Concepts: gaps-and-islands (turning daily discount flags into sale "episodes" with
-- ROW_NUMBER differences), pre/during windows, and a control group. Subtracting the
-- control group's change removes things that hit every game at once (weekends,
-- holidays, a Steam-wide sale), which query 07's simple average can't.
--
-- Caveat: still observational - a sale that lines up with a big update will look
-- bigger than the discount alone. Episodes need 3+ days of pre-sale data to be scored.

WITH flags AS (
    SELECT app_id, snapshot_date, discount_percent,
           snapshot_date - (ROW_NUMBER() OVER (PARTITION BY app_id ORDER BY snapshot_date))::int AS grp
    FROM app_prices
    WHERE discount_percent > 0
),
episodes AS (                       -- one row per continuous run of discounted days
    SELECT app_id,
           MIN(snapshot_date)     AS sale_start,
           MAX(snapshot_date)     AS sale_end,
           MAX(discount_percent)  AS discount_pct
    FROM flags
    GROUP BY app_id, grp
),
windows AS (
    SELECT e.*, e.sale_start - 7 AS pre_start, e.sale_start - 1 AS pre_end
    FROM episodes e
),
treated AS (
    SELECT w.app_id, w.sale_start, w.sale_end, w.discount_pct,
           AVG(d.avg_players) FILTER (WHERE d.snapshot_date BETWEEN w.pre_start AND w.pre_end)   AS pre_avg,
           COUNT(*)           FILTER (WHERE d.snapshot_date BETWEEN w.pre_start AND w.pre_end)   AS pre_days,
           AVG(d.avg_players) FILTER (WHERE d.snapshot_date BETWEEN w.sale_start AND w.sale_end) AS during_avg
    FROM windows w
    JOIN player_counts_daily d ON d.app_id = w.app_id
    GROUP BY w.app_id, w.sale_start, w.sale_end, w.discount_pct
),
control AS (                        -- other tracked games with no discount anywhere in the window
    SELECT w.app_id AS treated_app, w.sale_start,
           AVG(c.during / NULLIF(c.pre, 0) - 1) AS control_change
    FROM windows w
    JOIN LATERAL (
        SELECT d.app_id,
               AVG(d.avg_players) FILTER (WHERE d.snapshot_date BETWEEN w.pre_start AND w.pre_end)   AS pre,
               AVG(d.avg_players) FILTER (WHERE d.snapshot_date BETWEEN w.sale_start AND w.sale_end) AS during
        FROM player_counts_daily d
        WHERE d.app_id <> w.app_id
          AND d.snapshot_date BETWEEN w.pre_start AND w.sale_end
          AND NOT EXISTS (
              SELECT 1 FROM app_prices p
              WHERE p.app_id = d.app_id AND p.discount_percent > 0
                AND p.snapshot_date BETWEEN w.pre_start AND w.sale_end)
        GROUP BY d.app_id
    ) c ON TRUE
    WHERE c.pre IS NOT NULL AND c.during IS NOT NULL
    GROUP BY w.app_id, w.sale_start
)
SELECT t.app_id,
       COALESCE(a.name, t.app_id::text)                                     AS name,
       t.sale_start, t.sale_end, t.discount_pct,
       ROUND(t.pre_avg)                                                     AS pre_avg_players,
       ROUND(t.during_avg)                                                  AS sale_avg_players,
       ROUND(100 * (t.during_avg / NULLIF(t.pre_avg, 0) - 1), 1)            AS raw_change_pct,
       ROUND(100 * c.control_change, 1)                                     AS control_change_pct,
       ROUND(100 * ((t.during_avg / NULLIF(t.pre_avg, 0) - 1) - c.control_change), 1)
                                                                            AS lift_pct_points
FROM treated t
LEFT JOIN control c ON c.treated_app = t.app_id AND c.sale_start = t.sale_start
LEFT JOIN tracked_apps a ON a.app_id = t.app_id
WHERE t.pre_days >= 3 AND t.during_avg IS NOT NULL
ORDER BY t.sale_start DESC, lift_pct_points DESC NULLS LAST;
