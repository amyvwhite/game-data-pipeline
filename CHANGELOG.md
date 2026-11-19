# Changelog

All notable changes to this project. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

## [0.3.0] - 2026-11-19

### Added
- **AWS data lake** (ADR 0005): daily D-1 export of the 7 fact tables from Postgres to
  Parquet on S3 (`pipeline/lake.py`), verified end to end with an Athena query.
- **Infrastructure as code**: `infra/aws/stack.yml` (CloudFormation) creates the encrypted
  bucket, Glue catalog with partition projection, Athena workgroup with cost limits, a
  GitHub OIDC role (no stored AWS keys, least privilege) and a $1/month budget alert.
  Generated from the Spark schemas by `infra/aws/build_template.py`; CI fails on drift
  and lints the template with cfn-lint.
- `docs/AWS_SETUP.md`; moto-based tests for the export (no real AWS in tests).

### Changed
- `run_transform` split into `build_tables` + `run_transform` so schemas can be read
  without running a job.

## [0.2.1] - 2026-11-19

### Changed
- Live schedule runs three times an hour (`7,27,47 * * * *`) because GitHub drops scheduled
  runs under load; see ADR 0001.

## [0.2.0] - 2026-11-18

### Added
- **Steam top-100 market snapshot** (`market` source, hourly): Steam's most-played chart plus
  a live player count for all 100 games (~2,400 rows/day through Spark), names for new games,
  `market_daily` rollup, and query 15 (Take-Two rank and share of the whole market).
- **Review themes** (`review_text` source, daily): 100 recent reviews per game tagged in Spark
  against versioned keyword rules (`config/review_themes.json`); only counts are stored (ADR 0002).
- **24-hour-ahead forecasts**: seasonal, naive and ensemble models, written once per target
  hour, scored out-of-sample with a per-game champion (query 13, ADR 0003).
- **Sale lift with a control group**: gaps-and-islands sale episodes and a
  difference-in-differences estimate (query 14).
- Data quality rules for every new table; retention policy for hourly market rows (ADR 0004).
- Dashboard cards: forecast vs actual with an accuracy table, top-100 market, review-theme
  heatmap, sale lift.
- Engineering: mypy type checks and an 85% coverage gate in CI, pre-commit hooks,
  architecture decision records, data dictionary, this changelog.

### Changed
- `--sources auto` now also collects `market` hourly and `review_text` daily.
- Forecast history queries are capped at the forecast time (no lookahead).
- Auto mode checks each daily source against its own table, so a new or previously failed
  daily source catches up the same day.

## [0.1.0] - 2026-11-18

### Added
- Extract (Steam players, prices, reviews; Twitch viewers), PySpark transform, data quality
  checks with Slack/email alerts, idempotent Postgres load, anomaly detection, 12 SQL analyses.
- Hourly GitHub Actions run with a Git-backed warehouse and a GitHub Pages dashboard.
- GTA VI launch watch (Twitch-only placeholder app).
