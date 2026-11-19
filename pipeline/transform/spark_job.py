"""Transform step: raw JSON envelopes -> clean tables, using PySpark.

Runs in Spark local mode by default (master "local[*]"), so you only need Java
and `pip install pyspark`. The same code would run on EMR / Glue / Databricks
by pointing it at a cluster.

Output tables (one row per app per day unless noted):
    apps                    - latest name / publisher info per app
    player_count_snapshots  - one row per fetch (several per day if run hourly)
    player_counts_daily     - min / max / avg players per app per day
    app_prices              - price and discount per app per day
    review_summary          - review totals and score per app per day
    twitch_viewer_snapshots - Twitch viewers / streams per game per fetch
    market_snapshots        - Steam top-100 chart + live players, one row per game per fetch
    steam_app_names         - names for games first seen in the top-100 chart
    review_themes           - how many recent reviews mention each theme (performance, bugs, ...)

The data here is tiny (a handful of games), so at the end I collect the rows to
the driver and hand plain Python dicts to the quality and load steps. With real
volume the checks would run inside Spark instead (see README roadmap).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger(__name__)

# ---- explicit schemas -------------------------------------------------------
# Giving Spark the schema (instead of inferring it) keeps types stable from day
# to day and means a missing field becomes null instead of breaking the job.

_ENVELOPE_FIELDS = [
    T.StructField("source", T.StringType()),
    T.StructField("app_id", T.LongType()),
    T.StructField("snapshot_date", T.StringType()),
    T.StructField("fetched_at", T.StringType()),
]

PLAYERS_SCHEMA = T.StructType(
    _ENVELOPE_FIELDS
    + [
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField(
                        "response",
                        T.StructType(
                            [
                                T.StructField("player_count", T.LongType()),
                                T.StructField("result", T.IntegerType()),
                            ]
                        ),
                    ),
                ]
            ),
        )
    ]
)

_PRICE_OVERVIEW = T.StructType(
    [
        T.StructField("currency", T.StringType()),
        T.StructField("initial", T.LongType()),
        T.StructField("final", T.LongType()),
        T.StructField("discount_percent", T.IntegerType()),
        T.StructField("initial_formatted", T.StringType()),
        T.StructField("final_formatted", T.StringType()),
    ]
)

_APP_DATA = T.StructType(
    [
        T.StructField("type", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("steam_appid", T.LongType()),
        T.StructField("is_free", T.BooleanType()),
        T.StructField("developers", T.ArrayType(T.StringType())),
        T.StructField("publishers", T.ArrayType(T.StringType())),
        T.StructField("price_overview", _PRICE_OVERVIEW),
        T.StructField(
            "release_date",
            T.StructType(
                [
                    T.StructField("coming_soon", T.BooleanType()),
                    T.StructField("date", T.StringType()),
                ]
            ),
        ),
    ]
)

# appdetails is keyed by the app id as a string: {"1174180": {"success": true, "data": {...}}}
APPDETAILS_SCHEMA = T.StructType(
    _ENVELOPE_FIELDS
    + [
        T.StructField(
            "payload",
            T.MapType(
                T.StringType(),
                T.StructType(
                    [
                        T.StructField("success", T.BooleanType()),
                        T.StructField("data", _APP_DATA),
                    ]
                ),
            ),
        )
    ]
)

_QUERY_SUMMARY = T.StructType(
    [
        T.StructField("num_reviews", T.LongType()),
        T.StructField("review_score", T.IntegerType()),
        T.StructField("review_score_desc", T.StringType()),
        T.StructField("total_positive", T.LongType()),
        T.StructField("total_negative", T.LongType()),
        T.StructField("total_reviews", T.LongType()),
    ]
)

# Steam service methods normally wrap results in {"response": {...}}. I also accept
# an unwrapped query_summary so a small format change doesn't silently drop data.
REVIEWS_SCHEMA = T.StructType(
    _ENVELOPE_FIELDS
    + [
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("response", T.StructType([T.StructField("query_summary", _QUERY_SUMMARY)])),
                    T.StructField("query_summary", _QUERY_SUMMARY),
                ]
            ),
        )
    ]
)

TWITCH_SCHEMA = T.StructType(
    _ENVELOPE_FIELDS
    + [
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("twitch_game", T.StringType()),
                    T.StructField("twitch_game_id", T.StringType()),
                    T.StructField("found", T.BooleanType()),
                    T.StructField("viewer_count", T.LongType()),
                    T.StructField("stream_count", T.LongType()),
                    T.StructField("pages", T.IntegerType()),
                    T.StructField("truncated", T.BooleanType()),
                ]
            ),
        )
    ]
)

MARKET_SCHEMA = T.StructType(
    _ENVELOPE_FIELDS
    + [
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("rollup_date", T.LongType()),
                    T.StructField(
                        "ranks",
                        T.ArrayType(
                            T.StructType(
                                [
                                    T.StructField("rank", T.IntegerType()),
                                    T.StructField("appid", T.LongType()),
                                    T.StructField("last_week_rank", T.IntegerType()),
                                    T.StructField("peak_in_game", T.LongType()),
                                    T.StructField("current_players", T.LongType()),
                                    T.StructField("name", T.StringType()),
                                ]
                            )
                        ),
                    ),
                ]
            ),
        )
    ]
)

_REVIEW_ITEM = T.ArrayType(
    T.StructType(
        [
            T.StructField("recommendationid", T.StringType()),
            T.StructField("language", T.StringType()),
            T.StructField("review", T.StringType()),
            T.StructField("timestamp_created", T.LongType()),
            T.StructField("voted_up", T.BooleanType()),
        ]
    )
)

# Review text comes from the same Steam endpoint as the summary; like the summary, the
# list may sit at the top level or under "response", so both are declared.
REVIEW_TEXT_SCHEMA = T.StructType(
    _ENVELOPE_FIELDS
    + [
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("reviews", _REVIEW_ITEM),
                    T.StructField("response", T.StructType([T.StructField("reviews", _REVIEW_ITEM)])),
                ]
            ),
        )
    ]
)

SCHEMAS = {
    "players": PLAYERS_SCHEMA,
    "market": MARKET_SCHEMA,
    "review_text": REVIEW_TEXT_SCHEMA,
    "appdetails": APPDETAILS_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
    "twitch": TWITCH_SCHEMA,
}

CLEAN_TABLES = (
    "apps",
    "player_count_snapshots",
    "player_counts_daily",
    "app_prices",
    "review_summary",
    "twitch_viewer_snapshots",
    "market_snapshots",
    "steam_app_names",
    "review_themes",
)


# ---- spark session ----------------------------------------------------------


def get_spark(app_name: str = "game-data-pipeline", master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")  # tiny data; default 200 is overkill
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ---- readers ----------------------------------------------------------------


def read_source(spark: SparkSession, raw_root: Path, source: str, snapshot_date: date) -> DataFrame:
    """Read one day of one source. Returns an empty DataFrame if nothing is there."""
    path = Path(raw_root) / source / f"dt={snapshot_date.isoformat()}"
    schema = SCHEMAS[source]
    if not path.exists() or not any(path.glob("*.json")):
        log.warning("No raw files for %s on %s at %s", source, snapshot_date, path)
        return spark.createDataFrame([], schema)
    return spark.read.schema(schema).json(str(path / "*.json"))


def _with_common_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("app_id", F.col("app_id").cast("int"))
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
        .withColumn("captured_at", F.to_timestamp("fetched_at"))
    )


def _latest_per_app_day(df: DataFrame) -> DataFrame:
    """If the job ran more than once in a day, keep only the most recent fetch."""
    w = Window.partitionBy("app_id", "snapshot_date").orderBy(F.col("captured_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---- transforms (pure DataFrame -> DataFrame, easy to unit test) -------------


def transform_player_snapshots(players_raw: DataFrame) -> DataFrame:
    df = _with_common_columns(players_raw)
    return (
        df.filter(F.col("payload.response.result") == 1)  # result != 1 means Steam had no data
        .select(
            "app_id",
            "captured_at",
            "snapshot_date",
            F.col("payload.response.player_count").cast("int").alias("player_count"),
        )
        .dropDuplicates(["app_id", "captured_at"])
    )


def transform_twitch_snapshots(twitch_raw: DataFrame) -> DataFrame:
    """One row per (game, fetch): total live viewers and streams on Twitch at that moment."""
    df = _with_common_columns(twitch_raw)
    return (
        df.filter(F.col("payload.found"))  # found=false: Twitch has no category by that name
        .select(
            "app_id",
            "captured_at",
            "snapshot_date",
            F.col("payload.viewer_count").cast("int").alias("viewer_count"),
            F.col("payload.stream_count").cast("int").alias("stream_count"),
            F.coalesce(F.col("payload.truncated"), F.lit(False)).alias("truncated"),
        )
        .dropDuplicates(["app_id", "captured_at"])
    )


def transform_player_daily(snapshots: DataFrame) -> DataFrame:
    return snapshots.groupBy("app_id", "snapshot_date").agg(
        F.min("player_count").alias("min_players"),
        F.max("player_count").alias("max_players"),
        F.round(F.avg("player_count"), 1).alias("avg_players"),
        F.count("*").cast("int").alias("snapshot_count"),
    )


def _appdetails_entries(appdetails_raw: DataFrame) -> DataFrame:
    df = _with_common_columns(appdetails_raw)
    # Pull this app's entry out of the {"<app_id>": {...}} map.
    entry = F.element_at(F.col("payload"), F.col("app_id").cast("string"))
    return (
        df.withColumn("entry", entry)
        .filter(F.col("entry.success"))  # success=false is a normal Steam "no data" answer
        .select("app_id", "snapshot_date", "captured_at", F.col("entry.data").alias("d"))
    )


def transform_app_prices(appdetails_raw: DataFrame) -> DataFrame:
    entries = _latest_per_app_day(_appdetails_entries(appdetails_raw))
    is_free = F.coalesce(F.col("d.is_free"), F.lit(False))
    # Some paid games are no longer sold on their own (e.g. only in a bundle), so Steam
    # sends no price_overview. That's a real answer, not bad data: price stays null and
    # is_purchasable is false.
    has_price = F.col("d.price_overview").isNotNull()
    return entries.select(
        "app_id",
        "snapshot_date",
        F.col("d.price_overview.currency").alias("currency"),
        # Free games have no price_overview; store 0 rather than null.
        F.when(is_free, F.lit(0)).otherwise(F.col("d.price_overview.initial")).cast("int").alias("initial_price_cents"),
        F.when(is_free, F.lit(0)).otherwise(F.col("d.price_overview.final")).cast("int").alias("final_price_cents"),
        F.coalesce(F.col("d.price_overview.discount_percent"), F.lit(0)).cast("int").alias("discount_percent"),
        is_free.alias("is_free"),
        (is_free | has_price).alias("is_purchasable"),
    )


def transform_apps(appdetails_raw: DataFrame) -> DataFrame:
    entries = _latest_per_app_day(_appdetails_entries(appdetails_raw))
    return entries.select(
        "app_id",
        F.col("d.name").alias("name"),
        F.col("d.type").alias("app_type"),
        F.array_join(F.col("d.developers"), ", ").alias("developers"),
        F.array_join(F.col("d.publishers"), ", ").alias("publishers"),
        F.col("d.release_date.date").alias("release_date_text"),
        F.col("d.release_date.coming_soon").alias("coming_soon"),
    ).dropDuplicates(["app_id"])


def transform_review_summary(reviews_raw: DataFrame) -> DataFrame:
    df = _latest_per_app_day(_with_common_columns(reviews_raw))

    def qs(field: str):
        return F.coalesce(F.col(f"payload.response.query_summary.{field}"), F.col(f"payload.query_summary.{field}"))

    out = df.select(
        "app_id",
        "snapshot_date",
        qs("review_score").cast("int").alias("review_score"),
        qs("review_score_desc").alias("review_score_desc"),
        qs("total_positive").cast("int").alias("total_positive"),
        qs("total_negative").cast("int").alias("total_negative"),
        qs("total_reviews").cast("int").alias("total_reviews"),
    ).filter(F.col("total_reviews").isNotNull())
    return out.withColumn(
        "positive_ratio",
        F.when(F.col("total_reviews") > 0, F.round(F.col("total_positive") / F.col("total_reviews"), 4)),
    )


def transform_market_snapshots(market_raw: DataFrame) -> DataFrame:
    """Explode each top-100 snapshot into one row per (game, fetch)."""
    df = _with_common_columns(market_raw)
    return (
        df.select("captured_at", "snapshot_date", F.explode("payload.ranks").alias("r"))
        .select(
            F.col("r.appid").cast("int").alias("app_id"),
            "captured_at",
            "snapshot_date",
            F.col("r.rank").alias("rank"),
            F.col("r.last_week_rank").alias("last_week_rank"),
            F.col("r.peak_in_game").cast("int").alias("peak_in_game"),
            F.col("r.current_players").cast("int").alias("current_players"),
        )
        .filter(F.col("app_id").isNotNull() & F.col("rank").isNotNull())
        .dropDuplicates(["app_id", "captured_at"])
    )


def transform_steam_app_names(market_raw: DataFrame) -> DataFrame:
    """Names learned from the market extract (first time a game shows up in the chart)."""
    df = _with_common_columns(market_raw)
    rows = df.select("captured_at", F.explode("payload.ranks").alias("r")).filter(
        F.col("r.name").isNotNull() & (F.length(F.trim(F.col("r.name"))) > 0)
    )
    w = Window.partitionBy("app_id").orderBy(F.col("first_seen_at"))
    return (
        rows.select(
            F.col("r.appid").cast("int").alias("app_id"),
            F.trim(F.col("r.name")).alias("name"),
            F.col("captured_at").alias("first_seen_at"),
        )
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )


def load_review_themes(path: Path | None = None) -> tuple[str, dict[str, str]]:
    """Read the versioned theme dictionary (config/review_themes.json)."""
    import json

    from pipeline.config import PROJECT_ROOT

    cfg = json.loads((path or PROJECT_ROOT / "config" / "review_themes.json").read_text(encoding="utf-8"))
    return str(cfg["version"]), dict(cfg["themes"])


def transform_review_themes(review_text_raw: DataFrame, themes: dict[str, str], version: str) -> DataFrame:
    """Count how many recent reviews mention each theme, split by thumbs up / down.

    Only counts leave this function: review text and reviewer ids never reach the
    database or the public warehouse files (data minimisation).
    """
    latest = _latest_per_app_day(_with_common_columns(review_text_raw))
    reviews = (
        latest.select(
            "app_id",
            "snapshot_date",
            F.explode(F.coalesce(F.col("payload.reviews"), F.col("payload.response.reviews"))).alias("rv"),
        )
        .select(
            "app_id",
            "snapshot_date",
            F.col("rv.recommendationid").alias("review_id"),
            F.lower(F.coalesce(F.col("rv.review"), F.lit(""))).alias("body"),
            F.coalesce(F.col("rv.voted_up"), F.lit(True)).alias("voted_up"),
        )
        .dropDuplicates(["app_id", "snapshot_date", "review_id"])
    )
    sampled = reviews.groupBy("app_id", "snapshot_date").agg(F.count("*").cast("int").alias("reviews_sampled"))
    # One (theme, matched) pair per review per theme, then aggregate: a single pass in Spark.
    tagged = reviews.select(
        "app_id",
        "snapshot_date",
        "voted_up",
        F.explode(
            F.array(
                *[
                    F.struct(F.lit(name).alias("theme"), F.col("body").rlike(f"(?i){pattern}").alias("hit"))
                    for name, pattern in sorted(themes.items())
                ]
            )
        ).alias("t"),
    )
    counts = tagged.groupBy("app_id", "snapshot_date", F.col("t.theme").alias("theme")).agg(
        F.sum(F.col("t.hit").cast("int")).cast("int").alias("mentions"),
        F.sum((F.col("t.hit") & ~F.col("voted_up")).cast("int")).cast("int").alias("negative_mentions"),
    )
    return counts.join(sampled, ["app_id", "snapshot_date"]).withColumn("theme_version", F.lit(version))


# ---- job entry point --------------------------------------------------------


def _utc_row(row: dict[str, Any]) -> dict[str, Any]:
    """PySpark hands timestamps back as naive datetimes in the *machine's* local time zone.
    Make them explicit UTC so they're stored correctly wherever the job runs (a laptop in
    Toronto, a UTC container, a CI runner)."""
    return {
        k: v.astimezone(timezone.utc) if isinstance(v, datetime) and v.tzinfo is None else v for k, v in row.items()
    }


def build_tables(spark: SparkSession, raw_root: Path, snapshot_date: date) -> dict[str, DataFrame]:
    """All clean tables for one day, as (lazy) Spark DataFrames.

    Also used by infra/aws/build_template.py to read each table's schema, so the Athena
    table definitions can't drift from what Spark actually writes.
    """
    players = read_source(spark, raw_root, "players", snapshot_date)
    appdetails = read_source(spark, raw_root, "appdetails", snapshot_date)
    reviews = read_source(spark, raw_root, "reviews", snapshot_date)
    twitch = read_source(spark, raw_root, "twitch", snapshot_date)
    market = read_source(spark, raw_root, "market", snapshot_date)
    review_text = read_source(spark, raw_root, "review_text", snapshot_date)
    theme_version, themes = load_review_themes()

    snapshots = transform_player_snapshots(players).cache()
    return {
        "apps": transform_apps(appdetails),
        "player_count_snapshots": snapshots,
        "player_counts_daily": transform_player_daily(snapshots),
        "app_prices": transform_app_prices(appdetails),
        "review_summary": transform_review_summary(reviews),
        "twitch_viewer_snapshots": transform_twitch_snapshots(twitch),
        "market_snapshots": transform_market_snapshots(market),
        "steam_app_names": transform_steam_app_names(market),
        "review_themes": transform_review_themes(review_text, themes, theme_version),
    }


def run_transform(
    spark: SparkSession,
    raw_root: Path,
    snapshot_date: date,
    clean_root: Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Build all clean tables for one day.

    If clean_root is given, each table is also written as Parquet partitioned by
    snapshot_date (the "clean zone" of the data lake, see pipeline/lake.py).
    """
    tables = build_tables(spark, raw_root, snapshot_date)
    snapshots = tables["player_count_snapshots"]

    if clean_root is not None:
        for name, df in tables.items():
            writer = df.write.mode("overwrite")
            if "snapshot_date" in df.columns:
                writer = writer.partitionBy("snapshot_date")
            writer.parquet(str(Path(clean_root) / name))

    result = {name: [_utc_row(row.asDict()) for row in df.collect()] for name, df in tables.items()}
    snapshots.unpersist()
    for name, rows in result.items():
        log.info("Transform produced %d rows for %s", len(rows), name)
    return result
