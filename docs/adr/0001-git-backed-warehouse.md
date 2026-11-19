# 0001. Run on GitHub Actions with the warehouse stored as CSV in Git

**Status:** Accepted (Nov 2026)

## Context
The pipeline has to run hourly and publish a public dashboard, at zero cost, with no
server to maintain. Steam only reports *current* numbers, so a missed hour is lost forever.

## Decision
Each hourly GitHub Actions run starts a throwaway PostgreSQL service container, restores
history from `data/warehouse/*.csv` (`python -m pipeline.warehouse restore`), runs the
pipeline, dumps the tables back to CSV sorted by primary key, commits them with
`[skip ci]`, and deploys `dashboard/` to GitHub Pages.

## Consequences
- Free, and every change to the data is versioned: `git log -p data/warehouse` is a full audit trail.
- Restore refuses to load into a non-empty database, so a run can never double-load history.
- A `concurrency` group stops two runs from writing at once.
- GitHub's cron is best-effort: on the first night only 2 of ~10 hourly runs fired. The
  schedule asks three times an hour (`7,27,47 * * * *`) so at least one lands each hour;
  extra runs are harmless because every load is an idempotent upsert and forecasts are
  insert-once. A dedicated scheduler (cron server, Airflow) would remove the gaps entirely.
- Not a production pattern for large data: restore/dump time grows with history. ADR 0004
  caps the fastest-growing table. The same code runs against a permanent Postgres (or a
  cloud warehouse) by setting `DATABASE_URL` and dropping the restore/dump steps.
