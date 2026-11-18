# Next steps: finish and deploy (about 3–4 weeks)

A practical checklist. Tick things off as you go, and commit often with clear messages.
Your Git history is part of the portfolio.

## Week 1: get it running for real

- [ ] Create a GitHub repo and push. Check that `.env` and `data/` are **not** committed (`git status`).
- [ ] Install Python 3.12, Java 17 (Temurin) and Docker Desktop. Run `make install`.
- [ ] Run `make lint` and `make test`. Everything should pass, apart from the Postgres tests, which skip.
- [ ] Run `docker compose up -d db`, then `make test-all`. Fix anything that breaks.
- [ ] Run `python -m pipeline.run` against **real Steam**. Then:
  - [ ] Open a few files in `data/raw/`. Do the real responses match `tests/fixtures/`?
        In particular, is the reviews payload wrapped in `{"response": ...}`?
        If anything is different, update the fixtures and the transform, and add a test.
  - [ ] Check prices are in CAD and that free and unreleased games behave.
- [ ] Run `docker compose up --build` and check the image builds and runs end to end.
- [ ] Push and check the GitHub Actions run goes green.

## Week 2: AWS (free tier)

- [ ] Create an AWS account. **First:** turn on MFA for the root user, create a budget
      alert (e.g. $5), and stop using the root user after that.
- [ ] Create an S3 bucket in `ca-central-1` (block all public access, versioning on).
- [ ] Create an IAM user (or better, a role for the task) with **least privilege**: only
      `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on that one bucket. Example policy:
      ```json
      {
        "Version": "2012-10-17",
        "Statement": [
          {"Effect": "Allow", "Action": ["s3:ListBucket"],
           "Resource": "arn:aws:s3:::YOUR-BUCKET"},
          {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
           "Resource": "arn:aws:s3:::YOUR-BUCKET/game-data-pipeline/*"}
        ]
      }
      ```
- [ ] Locally, run `aws configure` with that user, set `STORAGE_BACKEND=s3` in `.env`, and run the pipeline.
      Check the files appear in S3.
- [ ] Choose a database: Amazon RDS Postgres (free tier, `db.t4g.micro`), or keep Postgres in Docker on EC2.

## Week 3: run it on a schedule

Pick **one**:

- **Simplest:** a `t4g.micro`/`t3.micro` EC2 instance with Docker, an IAM role for S3, and a cron entry:
  `15 6 * * * cd ~/game-data-pipeline && docker compose run --rm pipeline >> ~/pipeline.log 2>&1`.
  Add a second hourly cron with `--sources players` for the peak-hours data.
- **More "cloud-native":** push the image to ECR, create an ECS Fargate task definition
  (task role = the S3 policy above, env vars from SSM Parameter Store / Secrets Manager),
  and schedule it with an EventBridge Scheduler rule.
- (Lambda is a poor fit because of Spark + Java image size and the 15-minute limit. It's
  worth being able to explain *why* in an interview.)

Then:

- [ ] Set GitHub repository secrets (`Settings → Secrets and variables → Actions`) for anything CI needs
      later (e.g. `AWS_ROLE_ARN` for OIDC, preferred over long-lived keys). Never put keys in the workflow file.
- [ ] Add a CD job that builds and pushes the image to ECR on `main`.
- [ ] Set up a Slack incoming webhook and put it in `SLACK_WEBHOOK_URL`. Break a raw file on purpose and
      check that the alert arrives.

## Week 4: polish and present

- [ ] Let it collect real data for at least a week. Then run every query in `sql/queries/` on real data
      and note one or two interesting findings (e.g. "RDR2 players went up X% during the sale").
- [ ] Replace the sample `dashboard/data/summary.json` with a real export, and host the dashboard
      (GitHub Pages or an S3 static site).
- [ ] Replace `docs/dashboard.png` (currently synthetic data) with a real screenshot, and record a 20–30 second demo GIF
      (e.g. with ScreenToGif, Kap or LICEcap) of `python -m pipeline.run` plus the dashboard.
      Add both to the README.
- [ ] Update the README sections "What works today" and "Roadmap" so they say exactly what's true.
- [ ] Practise a 2-minute walkthrough: the problem, the architecture, one design decision
      (idempotent upserts, or stopping on critical checks), one thing you'd do differently at scale.

## SQL concepts to practise for the interview

The posting asks for advanced SQL, so be ready to write these from scratch:

- [ ] **CTEs**, including chaining several, and when a CTE vs a subquery vs a temp table makes sense.
- [ ] **Recursive CTEs** (e.g. generating dates, walking a hierarchy).
- [ ] **Window functions:** `ROW_NUMBER`, `RANK`, `DENSE_RANK` (know the difference with ties),
      `LAG`/`LEAD`, `FIRST_VALUE`/`LAST_VALUE`, `NTILE`, running totals with `SUM() OVER`.
- [ ] **Window frames:** `ROWS` vs `RANGE`, `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`.
- [ ] **Top-N per group** (latest row per key), and **deduplication** with `ROW_NUMBER()`.
- [ ] **Gaps and islands** (consecutive days on sale, streaks, missing dates).
- [ ] **Joins:** inner/left/full, anti-joins (`LEFT JOIN ... IS NULL` / `NOT EXISTS`), semi-joins (`EXISTS`),
      and what happens to row counts with duplicate keys (fan-out).
- [ ] **Aggregation:** `GROUP BY` vs window functions, `HAVING`, conditional aggregation
      (`SUM(CASE WHEN ...)`, `FILTER`), `GROUPING SETS`/`ROLLUP`.
- [ ] **NULL handling:** `COALESCE`, `NULLIF`, NULLs in `NOT IN` (a classic trap), NULLs in joins.
- [ ] **Dates and time zones:** `date_trunc`, intervals, `AT TIME ZONE`, `generate_series`.
- [ ] **Upserts and idempotency:** `INSERT ... ON CONFLICT`, `MERGE`.
- [ ] **Performance:** reading `EXPLAIN ANALYZE`, B-tree and composite indexes (column order matters),
      when an index isn't used, sargable predicates (don't wrap indexed columns in functions),
      partitioning large tables by date, avoiding `SELECT *`.
- [ ] **Spark SQL / PySpark equivalents** of the above (`Window.partitionBy().orderBy()`), plus
      shuffles, partitions, `cache()`, broadcast joins, and why `collect()` is dangerous on big data.

Good practice sources: the queries in this repo (rewrite them without looking), LeetCode's
SQL 50, DataLemur, and the PostgreSQL docs on window functions.
