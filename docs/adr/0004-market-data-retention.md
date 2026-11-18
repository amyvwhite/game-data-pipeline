# 0004. Keep hourly market rows 30 days, daily rollup forever

**Status:** Accepted (Nov 2026)

## Context
The top-100 market snapshot adds about 2,400 rows a day (~150 KB of CSV). Kept forever,
that is ~55 MB a year in one Git-tracked file, which slows every restore and approaches
GitHub's file-size warnings (ADR 0001).

## Decision
After each load, `refresh_market_daily` rolls the day's hourly rows into `market_daily`
(best rank, average and max players per game per day), and `apply_retention` deletes
`market_snapshots` rows older than `MARKET_RETENTION_DAYS` (30).

## Consequences
- Hourly detail for the last month (enough for intraday questions); daily history forever.
- `market_snapshots` stays around 72,000 rows (~4.5 MB).
- The retention window is a single constant, easy to raise if storage moves off Git.
