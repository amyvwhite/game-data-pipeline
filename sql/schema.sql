-- schema.sql: tables the pipeline loads into. Safe to run more than once.
--
-- Design notes
-- * Every fact table has a natural primary key (app + day, or app + timestamp).
--   The loader does INSERT ... ON CONFLICT DO UPDATE on that key, so re-running a
--   day replaces it instead of duplicating it (idempotent loads).
-- * No foreign keys from facts to apps on purpose: if the appdetails call fails
--   for one game, I still want its player counts loaded. A quality check flags
--   the missing app instead.
-- * Prices are stored in cents as integers (that's what Steam returns), so there
--   is no floating-point rounding.

CREATE TABLE IF NOT EXISTS apps (
    app_id             INTEGER PRIMARY KEY,
    name               TEXT        NOT NULL,
    app_type           TEXT,
    developers         TEXT,
    publishers         TEXT,
    release_date_text  TEXT,        -- Steam gives a display string like "5 Dec, 2019"
    coming_soon        BOOLEAN,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The games this project tracks and how they're grouped (Take-Two vs competitors).
-- Synced from config/apps.json at the start of every run.
CREATE TABLE IF NOT EXISTS tracked_apps (
    app_id           INTEGER PRIMARY KEY,
    name             TEXT NOT NULL,
    publisher_group  TEXT NOT NULL DEFAULT '',
    twitch_game      TEXT NOT NULL DEFAULT ''
);

-- Raw-ish grain: one row per fetch. Run the players extract hourly to get
-- intra-day data for the peak-hours query.
CREATE TABLE IF NOT EXISTS player_count_snapshots (
    app_id         INTEGER     NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL,
    snapshot_date  DATE        NOT NULL,
    player_count   INTEGER     NOT NULL CHECK (player_count >= 0),
    PRIMARY KEY (app_id, captured_at)
);

CREATE TABLE IF NOT EXISTS player_counts_daily (
    app_id          INTEGER       NOT NULL,
    snapshot_date   DATE          NOT NULL,
    min_players     INTEGER       NOT NULL,
    max_players     INTEGER       NOT NULL,
    avg_players     NUMERIC(12,1) NOT NULL,
    snapshot_count  INTEGER       NOT NULL,
    PRIMARY KEY (app_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS app_prices (
    app_id               INTEGER  NOT NULL,
    snapshot_date        DATE     NOT NULL,
    currency             CHAR(3),
    initial_price_cents  INTEGER  CHECK (initial_price_cents >= 0),
    final_price_cents    INTEGER  CHECK (final_price_cents >= 0),  -- null when not sold separately
    discount_percent     SMALLINT NOT NULL CHECK (discount_percent BETWEEN 0 AND 100),
    is_free              BOOLEAN  NOT NULL DEFAULT FALSE,
    is_purchasable       BOOLEAN  NOT NULL DEFAULT TRUE,
    PRIMARY KEY (app_id, snapshot_date)
);

-- Migration for databases created before is_purchasable existed.
ALTER TABLE app_prices ADD COLUMN IF NOT EXISTS is_purchasable BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE app_prices ALTER COLUMN final_price_cents DROP NOT NULL;

CREATE TABLE IF NOT EXISTS review_summary (
    app_id             INTEGER      NOT NULL,
    snapshot_date      DATE         NOT NULL,
    review_score       SMALLINT,              -- Steam's 1-9 bucket
    review_score_desc  TEXT,                  -- e.g. "Very Positive"
    total_positive     INTEGER      NOT NULL,
    total_negative     INTEGER      NOT NULL,
    total_reviews      INTEGER      NOT NULL,
    positive_ratio     NUMERIC(5,4) CHECK (positive_ratio BETWEEN 0 AND 1),
    PRIMARY KEY (app_id, snapshot_date)
);

-- Twitch: total live viewers / streams per game at each fetch (optional source).
CREATE TABLE IF NOT EXISTS twitch_viewer_snapshots (
    app_id         INTEGER     NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL,
    snapshot_date  DATE        NOT NULL,
    viewer_count   INTEGER     NOT NULL CHECK (viewer_count >= 0),
    stream_count   INTEGER     NOT NULL CHECK (stream_count >= 0),
    truncated      BOOLEAN     NOT NULL DEFAULT FALSE,  -- true = only the top N pages were counted
    PRIMARY KEY (app_id, captured_at)
);

-- Unusual player counts found after each load (see sql/queries/09_player_anomalies.sql).
CREATE TABLE IF NOT EXISTS player_anomalies (
    app_id           INTEGER       NOT NULL,
    captured_at      TIMESTAMPTZ   NOT NULL,
    player_count     INTEGER       NOT NULL,
    baseline_avg     NUMERIC(12,1) NOT NULL,
    baseline_std     NUMERIC(12,1) NOT NULL,
    baseline_points  INTEGER       NOT NULL,
    z_score          NUMERIC(8,2)  NOT NULL,
    pct_vs_baseline  NUMERIC(8,1)  NOT NULL,
    direction        TEXT          NOT NULL CHECK (direction IN ('spike', 'drop')),
    detected_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, captured_at)
);

-- Steam's top-100 most played chart, with each game's live player count at fetch time.
-- ~100 rows per hourly run: the "scale" table that gives Spark real volume.
CREATE TABLE IF NOT EXISTS market_snapshots (
    app_id           INTEGER     NOT NULL,
    captured_at      TIMESTAMPTZ NOT NULL,
    snapshot_date    DATE        NOT NULL,
    rank             SMALLINT    NOT NULL CHECK (rank BETWEEN 1 AND 1000),
    last_week_rank   SMALLINT,
    peak_in_game     INTEGER     CHECK (peak_in_game >= 0),     -- Steam's daily peak for the chart
    current_players  INTEGER     CHECK (current_players >= 0),  -- null if that one lookup failed
    PRIMARY KEY (app_id, captured_at)
);

-- Daily rollup of market_snapshots, kept forever. Hourly market rows are only kept for
-- MARKET_RETENTION_DAYS (see pipeline/analysis.py) to keep the warehouse small.
CREATE TABLE IF NOT EXISTS market_daily (
    app_id          INTEGER       NOT NULL,
    snapshot_date   DATE          NOT NULL,
    best_rank       SMALLINT      NOT NULL,
    avg_players     NUMERIC(12,1),
    max_players     INTEGER,
    snapshot_count  INTEGER       NOT NULL,
    PRIMARY KEY (app_id, snapshot_date)
);

-- Names for games that appear in the chart (the chart itself only gives ids).
CREATE TABLE IF NOT EXISTS steam_app_names (
    app_id         INTEGER     PRIMARY KEY,
    name           TEXT        NOT NULL,
    first_seen_at  TIMESTAMPTZ NOT NULL
);

-- Review themes: of the most recent N reviews, how many mention each theme.
-- Only counts are stored - never review text or reviewer ids.
CREATE TABLE IF NOT EXISTS review_themes (
    app_id             INTEGER  NOT NULL,
    snapshot_date      DATE     NOT NULL,
    theme              TEXT     NOT NULL,
    theme_version      TEXT     NOT NULL,   -- which version of config/review_themes.json produced it
    reviews_sampled    INTEGER  NOT NULL CHECK (reviews_sampled >= 0),
    mentions           INTEGER  NOT NULL CHECK (mentions >= 0),
    negative_mentions  INTEGER  NOT NULL CHECK (negative_mentions >= 0),
    PRIMARY KEY (app_id, snapshot_date, theme),
    CHECK (mentions <= reviews_sampled AND negative_mentions <= mentions)
);

-- Player-count forecasts. A forecast for each future hour is written once, the first time
-- that hour comes within the 24-hour horizon, so every row is a genuine ~24h-ahead
-- prediction made before the actual was known. Scored by sql/queries/13_forecast_accuracy.sql.
CREATE TABLE IF NOT EXISTS player_forecasts (
    app_id        INTEGER       NOT NULL,
    target_hour   TIMESTAMPTZ   NOT NULL,
    model         TEXT          NOT NULL CHECK (model IN ('seasonal_profile', 'naive_24h', 'ensemble')),
    predicted     NUMERIC(12,1) NOT NULL CHECK (predicted >= 0),
    made_at       TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (app_id, target_hour, model)
);

-- One row per pipeline run: handy for "when did this last succeed?" questions.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT        NOT NULL CHECK (status IN ('success', 'warning', 'failed')),
    rows_loaded     INTEGER     NOT NULL DEFAULT 0,
    quality_report  JSONB
);

-- The primary keys already index (app_id, date). These help queries that scan
-- one date across all apps (e.g. "rank every game on a given day").
CREATE INDEX IF NOT EXISTS idx_player_daily_date ON player_counts_daily (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_prices_date       ON app_prices (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_date    ON player_count_snapshots (snapshot_date);
-- Migration: databases created before the ensemble model existed.
ALTER TABLE player_forecasts DROP CONSTRAINT IF EXISTS player_forecasts_model_check;
ALTER TABLE player_forecasts ADD CONSTRAINT player_forecasts_model_check
    CHECK (model IN ('seasonal_profile', 'naive_24h', 'ensemble'));

CREATE INDEX IF NOT EXISTS idx_twitch_date      ON twitch_viewer_snapshots (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_market_captured  ON market_snapshots (captured_at);
CREATE INDEX IF NOT EXISTS idx_forecast_target  ON player_forecasts (target_hour);
