-- 06. Review momentum: new reviews per day and recent vs all-time sentiment
--
-- Question: is a game's recent reception better or worse than its all-time score?
-- Steam only gives running totals, so new reviews per day = today's total minus
-- yesterday's (LAG). Then a 7-day rolling SUM gives "recent" sentiment.
-- Concepts: LAG() deltas, chained CTEs, SUM() OVER a rolling frame, NULLIF.

WITH deltas AS (
    SELECT
        app_id,
        snapshot_date,
        total_reviews,
        positive_ratio                                                              AS all_time_ratio,
        total_reviews  - LAG(total_reviews)  OVER (PARTITION BY app_id ORDER BY snapshot_date) AS new_reviews,
        total_positive - LAG(total_positive) OVER (PARTITION BY app_id ORDER BY snapshot_date) AS new_positive
    FROM review_summary
),
rolling AS (
    SELECT
        *,
        SUM(new_reviews)  OVER w AS new_reviews_7d,
        SUM(new_positive) OVER w AS new_positive_7d
    FROM deltas
    WINDOW w AS (PARTITION BY app_id ORDER BY snapshot_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    r.app_id,
    COALESCE(a.name, r.app_id::text)                                    AS name,
    r.snapshot_date,
    r.total_reviews,
    r.new_reviews,
    r.new_reviews_7d,
    r.all_time_ratio,
    ROUND(r.new_positive_7d::numeric / NULLIF(r.new_reviews_7d, 0), 4)  AS recent_7d_ratio,
    ROUND(r.new_positive_7d::numeric / NULLIF(r.new_reviews_7d, 0) - r.all_time_ratio, 4)
                                                                        AS recent_minus_all_time
FROM rolling r
LEFT JOIN apps a USING (app_id)
ORDER BY r.app_id, r.snapshot_date;
