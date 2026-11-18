# Architecture decision records

Short notes on the decisions that shape this project: the context, what was chosen, and
what it costs. Format follows Michael Nygard's ADR template. New decisions get the next
number; superseded ones are marked, not deleted.

| # | Decision | Status |
|---|---|---|
| [0001](0001-git-backed-warehouse.md) | Run on GitHub Actions with the warehouse stored as CSV in Git | Accepted |
| [0002](0002-review-text-data-minimisation.md) | Store review theme counts, never review text | Accepted |
| [0003](0003-forecast-models-and-scoring.md) | Three forecast models, champion picked by out-of-sample error | Accepted |
| [0004](0004-market-data-retention.md) | Keep hourly market rows 30 days, daily rollup forever | Accepted |
