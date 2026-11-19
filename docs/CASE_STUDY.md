# Case study: a live data pipeline for the GTA VI launch

**Amy V White** · Python, PySpark, PostgreSQL, GitHub Actions · [Live dashboard](https://amyvwhite.github.io/game-data-pipeline/) · [Code](https://github.com/amyvwhite/game-data-pipeline)

## The problem

Take-Two's biggest launch in a decade, Grand Theft Auto VI, ships on November 19, 2026, on
consoles only. Publishers watch questions like these closely in a launch week: does the new
game pull players away from the back catalogue (GTA V, Red Dead Redemption 2)? Do sales still
work when attention is elsewhere? How does Take-Two's share of PC players move against
competitors?

None of that is answerable after the fact from public data: Steam only reports *current*
player counts. So the data has to be collected, every hour, starting before launch.

## What I built

An hourly pipeline that runs unattended on GitHub Actions and publishes a live dashboard.

| Stage | What it does |
|---|---|
| Extract | Steam (players, prices, reviews, review text, the top-100 chart) and Twitch, with retries, backoff, `Retry-After` handling and rate limiting |
| Transform | PySpark with explicit schemas: 9 clean tables, ~3,500 new rows a day |
| Quality | Rules per table; critical failures stop the load and alert, warnings alert and continue |
| Load | Idempotent upserts into PostgreSQL in one transaction |
| Analyse | Anomaly detection, 24-hour forecasts from three models, sale lift against a control group, review themes |
| Publish | Static dashboard on GitHub Pages; the warehouse is versioned in Git |
| Data lake | Daily Parquet export to S3, Glue catalog, Athena; deployed with CloudFormation, GitHub OIDC (no stored keys) |

It costs nothing to run: each hour a fresh Postgres container restores the history from
CSV files in the repo, the pipeline runs, and the updated tables are committed back
([ADR 0001](adr/0001-git-backed-warehouse.md)).

## Decisions I'd defend in an interview

- **A forecast has to beat "same as yesterday".** I score every forecast out-of-sample
  against a naive baseline. My first seasonal model *lost* (8.8% vs 7.4% error in the
  backtest), because a half-finished UTC day skewed its daily average. A rolling 24-hour
  window fixed that, and an ensemble of the two won (7.2%). The dashboard shows the
  winner per game, and it says so when the baseline wins.
  ([ADR 0003](adr/0003-forecast-models-and-scoring.md))
- **A sale's effect needs a control group.** Comparing "players on sale days vs other days"
  credits weekends and Steam-wide sales to the discount. Query 14 finds each sale period
  with a gaps-and-islands pattern and subtracts the change in games that weren't on sale
  over the same days (difference-in-differences). On test data with a known +20% effect
  it recovers +20 to +24 points.
- **Collect only what you need.** Review themes come from 100 recent reviews per game, but
  only theme counts are stored, never review text or reviewer ids, because the repo is
  public. ([ADR 0002](adr/0002-review-text-data-minimisation.md))
- **Fail loudly, but not over everything.** The first real run stopped itself: GTA V
  Legacy is no longer sold on its own, so Steam sent no price. I modelled "not sold
  separately" explicitly, and a missing price on a game that *is* for sale still fails.
  A thin top-100 chart only warns, because it must never block the core player counts.
- **Cloud without the bill.** The AWS lake is a daily batch (~7 files a day) behind a $1
  budget alert, so it costs effectively nothing. The first deploy was refused because GitHub
  now sends an ID-based identity; I found the exact value in CloudTrail and pinned the role
  to it, which is also safer than trusting a repo name. ([ADR 0005](adr/0005-aws-data-lake.md))
- **Plan for growth.** The top-100 snapshot is the fastest-growing table, so hourly rows
  are kept 30 days and rolled up daily forever. ([ADR 0004](adr/0004-market-data-retention.md))

## Findings so far

*Collection began November 18, 2026. This section is updated as history builds.*

- On the first full day of real data, Take-Two titles held **49.6%** of the players across
  the 11 tracked games, and Call of Duty was the single most-played tracked game.
- *Launch week (Nov 19 to 26):* GTA VI Twitch viewership vs GTA V and RDR2 Steam players,
  compared with the week before launch. To be added.
- *Steam Autumn Sale:* sale lift per game against the control group. To be added.
- *Forecast accuracy on real data:* which model wins for which games. To be added.

## Engineering standards

101 tests at 88% coverage (unit, Spark, and integration against a real Postgres, including
every SQL query) · ruff lint and format, mypy type checks, coverage gate and Docker build in
CI · pre-commit hooks · data dictionary · decision records · changelog · semantic versioning ·
no secrets in code (GitHub Actions secrets only).

## What I'd do next

Weekly seasonality in the forecasts once there are two weeks of data; an ML or LLM classifier
for review themes behind the same table contract; dbt models on Athena; and moving the system of
record from Git to a hosted database, orchestrated by Airflow, as history grows.
