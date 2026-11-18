# 0002. Store review theme counts, never review text

**Status:** Accepted (Nov 2026)

## Context
The review-theme analysis reads the 100 most recent English reviews per game each day.
Review text is user-generated content tied to a Steam account id, and this repository and
its data files are public.

## Decision
Spark tags each review against the versioned keyword rules in `config/review_themes.json`
and emits only aggregates: per game, day and theme, `reviews_sampled`, `mentions` and
`negative_mentions`, plus the `theme_version` that produced them. Raw responses stay in the
raw zone (`data/raw`, git-ignored) and are never loaded into Postgres or committed.

## Consequences
- Nothing personal or copyrighted is published; the public CSV is just counts.
- Rules are reviewable code: changing a pattern bumps `version`, so any row can be traced
  to the rules that produced it.
- A keyword model misses sarcasm and context. It is a transparent baseline; an ML or LLM
  classifier can replace `transform_review_themes` without changing the table contract.
