# Data dictionary

All tables live in PostgreSQL (`sql/schema.sql`) and are mirrored to `data/warehouse/*.csv`.
Times are UTC. "Grain" is what one row means. Every table has a primary key, and loads are
idempotent upserts on it.

## Facts

| Table | Grain | Key columns | Source | Refresh |
|---|---|---|---|---|
| `player_count_snapshots` | one game, one fetch | `app_id, captured_at` | Steam GetNumberOfCurrentPlayers | hourly |
| `player_counts_daily` | one game, one day | `app_id, snapshot_date` | rollup of snapshots | hourly (rebuilt) |
| `app_prices` | one game, one day | `app_id, snapshot_date` | Steam appdetails (CAD) | daily |
| `review_summary` | one game, one day | `app_id, snapshot_date` | Steam GetAppReviews totals | daily |
| `review_themes` | one game, one day, one theme | `app_id, snapshot_date, theme` | 100 recent reviews, Spark rlike tagging | daily |
| `twitch_viewer_snapshots` | one game, one fetch | `app_id, captured_at` | Twitch Helix streams | hourly (if keys set) |
| `market_snapshots` | one top-100 game, one fetch | `app_id, captured_at` | Steam GetMostPlayedGames + live players | hourly, kept 30 days |
| `market_daily` | one game, one day | `app_id, snapshot_date` | rollup of market_snapshots | hourly (rebuilt), kept forever |
| `player_forecasts` | one game, one future hour, one model | `app_id, target_hour, model` | `pipeline/forecast.py` | hourly, insert-once |
| `player_anomalies` | one unusual snapshot | `app_id, captured_at` | query 09 (z-score) | hourly, insert-once |
| `pipeline_runs` | one run | `run_id` | the pipeline itself | every run |

## Dimensions

| Table | Grain | Notes |
|---|---|---|
| `tracked_apps` | one tracked game | synced from `config/apps.json` each run: name, `publisher_group` (Take-Two / Competitor), Twitch category |
| `apps` | one tracked Steam game | store metadata from appdetails |
| `steam_app_names` | one game seen in the top 100 | name looked up once, `first_seen_at` kept |

## Column notes

- **Prices** are integer cents (`*_price_cents`), as Steam returns them: no float rounding.
  `final_price_cents` is null when a paid game isn't sold on its own (`is_purchasable = false`).
- **`positive_ratio`** = `total_positive / total_reviews`, 4 decimal places.
- **`market_snapshots.peak_in_game`** is Steam's chart value (daily peak); `current_players`
  is our own live reading at `captured_at` and can be null if that one lookup failed.
- **`review_themes`**: `mentions` = reviews matching the theme's pattern; `negative_mentions`
  = those that were thumbs-down. Invariant (checked and enforced):
  `negative_mentions <= mentions <= reviews_sampled`. `theme_version` ties each row to a
  version of `config/review_themes.json`.
- **`player_forecasts.model`**: `seasonal_profile`, `naive_24h` or `ensemble` (ADR 0003).
- **GTA VI** uses placeholder `app_id = -6`: it isn't on Steam, so it only appears in Twitch tables.

## Data quality rules (`pipeline/quality/checks.py`)

Critical = stop the run before loading; warning = alert and continue.

| Table | Critical | Warning |
|---|---|---|
| all | primary key unique, required columns not null | |
| daily facts | at least one row, every row dated today | a tracked game missing |
| `player_count_snapshots` | no negative or impossible counts | |
| `app_prices` | discount 0-100, price present when purchasable | |
| `review_summary` | ratio between 0 and 1 | positive + negative = total |
| `market_snapshots` | no negative players | chart has <90 games, rank outside 1-100 |
| `review_themes` | counts consistent | fewer than 20 reviews sampled |
| history (post-load) | | any missing day in the last 7 |
