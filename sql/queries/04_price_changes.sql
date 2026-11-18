-- 04. Price-change detection: when did sales start and end?
--
-- Question: list every day a game's price or discount changed, and what kind of change it was.
-- Concepts: LAG() on several columns, CASE classification, filtering on a window
-- result (needs a CTE, because WHERE runs before window functions).

WITH priced AS (
    SELECT
        app_id,
        snapshot_date,
        currency,
        initial_price_cents,
        final_price_cents,
        discount_percent,
        LAG(final_price_cents)   OVER w AS prev_final_cents,
        LAG(initial_price_cents) OVER w AS prev_initial_cents,
        LAG(discount_percent)    OVER w AS prev_discount
    FROM app_prices
    WINDOW w AS (PARTITION BY app_id ORDER BY snapshot_date)
),
changes AS (
    SELECT
        *,
        CASE
            WHEN prev_discount = 0 AND discount_percent > 0           THEN 'sale started'
            WHEN prev_discount > 0 AND discount_percent = 0           THEN 'sale ended'
            WHEN discount_percent <> prev_discount                     THEN 'discount changed'
            WHEN initial_price_cents > prev_initial_cents              THEN 'base price increased'
            WHEN initial_price_cents < prev_initial_cents              THEN 'base price decreased'
        END AS change_type
    FROM priced
    WHERE prev_final_cents IS NOT NULL            -- first day for a game has nothing to compare to
      AND (final_price_cents   <> prev_final_cents
        OR initial_price_cents <> prev_initial_cents
        OR discount_percent    <> prev_discount)
)
SELECT
    c.app_id,
    COALESCE(a.name, c.app_id::text)                 AS name,
    c.snapshot_date,
    c.change_type,
    c.currency,
    ROUND(c.prev_final_cents / 100.0, 2)             AS old_price,
    ROUND(c.final_price_cents / 100.0, 2)            AS new_price,
    c.prev_discount,
    c.discount_percent
FROM changes c
LEFT JOIN apps a USING (app_id)
ORDER BY c.app_id, c.snapshot_date;
