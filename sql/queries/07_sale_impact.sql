-- 07. Do sales bring players back?
--
-- Question: per game, average players on sale days vs full-price days.
-- Concepts: joining two fact tables on (app, date) in a CTE, conditional
-- aggregation with FILTER (Postgres; the portable version is AVG(CASE WHEN ...)),
-- and HAVING to skip games that never went on sale.
--
-- Caveat for interviews: this is correlation, not causation - sales often line
-- up with updates, holidays and free weekends.

WITH joined AS (
    SELECT
        p.app_id,
        p.snapshot_date,
        p.discount_percent,
        d.avg_players
    FROM app_prices p
    JOIN player_counts_daily d
      ON d.app_id = p.app_id
     AND d.snapshot_date = p.snapshot_date
)
SELECT
    j.app_id,
    COALESCE(a.name, j.app_id::text)                                       AS name,
    COUNT(*) FILTER (WHERE j.discount_percent > 0)                         AS sale_days,
    COUNT(*) FILTER (WHERE j.discount_percent = 0)                         AS full_price_days,
    MAX(j.discount_percent)                                                AS max_discount,
    ROUND(AVG(j.avg_players) FILTER (WHERE j.discount_percent > 0))        AS avg_players_on_sale,
    ROUND(AVG(j.avg_players) FILTER (WHERE j.discount_percent = 0))        AS avg_players_full_price,
    ROUND(100.0 * (
        AVG(j.avg_players) FILTER (WHERE j.discount_percent > 0)
      / NULLIF(AVG(j.avg_players) FILTER (WHERE j.discount_percent = 0), 0) - 1), 1)
                                                                           AS sale_uplift_pct
FROM joined j
LEFT JOIN apps a USING (app_id)
GROUP BY j.app_id, a.name
HAVING COUNT(*) FILTER (WHERE j.discount_percent > 0) > 0
ORDER BY sale_uplift_pct DESC NULLS LAST;
