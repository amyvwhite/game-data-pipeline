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

SCHEMAS = {
    "players": PLAYERS_SCHEMA,
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


# ---- job entry point --------------------------------------------------------


def _utc_row(row: dict[str, Any]) -> dict[str, Any]:
    """PySpark hands timestamps back as naive datetimes in the *machine's* local time zone.
    Make them explicit UTC so they're stored correctly wherever the job runs (a laptop in
    Toronto, a UTC container, a CI runner)."""
    return {
        k: v.astimezone(timezone.utc) if isinstance(v, datetime) and v.tzinfo is None else v for k, v in row.items()
    }


def run_transform(
    spark: SparkSession,
    raw_root: Path,
    snapshot_date: date,
    clean_root: Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Build all clean tables for one day.

    If clean_root is given, each table is also written as Parquet partitioned by
    snapshot_date (a simple "clean zone" in the data lake).
    """
    players = read_source(spark, raw_root, "players", snapshot_date)
    appdetails = read_source(spark, raw_root, "appdetails", snapshot_date)
    reviews = read_source(spark, raw_root, "reviews", snapshot_date)
    twitch = read_source(spark, raw_root, "twitch", snapshot_date)

    snapshots = transform_player_snapshots(players).cache()
    tables: dict[str, DataFrame] = {
        "apps": transform_apps(appdetails),
        "player_count_snapshots": snapshots,
        "player_counts_daily": transform_player_daily(snapshots),
        "app_prices": transform_app_prices(appdetails),
        "review_summary": transform_review_summary(reviews),
        "twitch_viewer_snapshots": transform_twitch_snapshots(twitch),
    }

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
