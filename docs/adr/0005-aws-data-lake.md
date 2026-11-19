# 0005. AWS data lake: daily Parquet export to S3, queried with Athena

**Status:** Accepted (Nov 2026)

## Context
The project should show production AWS skills (S3, IAM, Athena, infrastructure as code)
without becoming a dependency that can lose data, and at a cost that rounds to zero:
the account is on AWS's free plan and the budget is effectively $0.

## Decision
- **Daily batch (D-1).** Each live run calls `python -m pipeline.lake export`. It lists
  the `_exports/` markers (one cheap request) and, only for complete UTC days not yet
  exported, writes each fact table's rows for that day from Postgres to one Parquet file.
  That is ~7 PUTs a day; almost every hourly run exports nothing.
- **Second destination, not a dependency.** The export runs after the data is committed
  to Git and only if the pipeline passed its quality checks. If AWS fails, collection
  continues and the run is marked failed so the problem is visible.
- **Idempotent.** Fixed object keys (`part-0.parquet`) plus a done-marker per day: a re-run
  overwrites, never duplicates.
- **Schema-locked.** `infra/aws/build_template.py` reads every table's schema from the Spark
  job and writes both the Glue table definitions (`infra/aws/stack.yml`) and
  `pipeline/lake_schema.json`, which the exporter uses. CI fails if either file drifts
  (`--check`) and lints the template with `cfn-lint`.
- **Partition projection** on `snapshot_date`: new days are queryable immediately, with no
  Glue crawler or `MSCK REPAIR` (which would cost money and time).
- **No stored AWS keys.** GitHub Actions signs in with OIDC to a role that trusts only this
  repository's `main` branch, matched on GitHub's immutable subject
  (`repo:owner@<id>/repo@<id>`). The first deploy used the name-only form and was refused;
  CloudTrail's failed `AssumeRoleWithWebIdentity` event showed the real subject. IDs also
  stop a deleted-and-recreated repo with the same name from inheriting access, with least-privilege permissions (no delete, one bucket, one
  workgroup, read-only catalog).
- **Cost guardrails.** Athena workgroup enforced with a 1 GB per-query scan limit; query
  results expire after 7 days; a $1/month budget emails at 50% and 100%.
- **Region us-east-2.** The free plan allows one region; the data is public, so residency
  doesn't matter.

## Estimated cost (us-east-2, list prices)
| Item | Volume | Monthly |
|---|---|---|
| S3 PUT | ~7/day + markers, ~250/month | < $0.01 |
| S3 LIST/GET | ~2,200 LIST (one per hourly run) | ~$0.01 |
| S3 storage | a few MB | < $0.01 |
| Athena | one verify query/day, billed at the 10 MB minimum | < $0.01 |

## Consequences
- The lake is one day behind (D-1). Fine for analytics; the dashboard still reads
  Postgres hourly.
- Postgres stays the system of record; the lake is a published, columnar copy for
  SQL-on-S3 and future tools (QuickSight, Spark on EMR, dbt-athena).
- Rebuilding the account is one template upload: everything is in code.
