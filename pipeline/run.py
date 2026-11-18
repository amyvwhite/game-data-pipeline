"""Single entry point: extract -> transform -> quality -> load -> export.

Examples:
    python -m pipeline.run                          # today (UTC), everything
    python -m pipeline.run --date 2026-11-18 --skip-extract   # re-process saved raw data
    python -m pipeline.run --sources players        # just player counts (e.g. an hourly job)
    python -m pipeline.run --skip-load              # no database needed

Exit codes: 0 = ok (maybe with warnings), 1 = critical data-quality failure, 2 = other error.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

from pipeline.config import Settings, get_settings
from pipeline.extract.extract import SOURCES, run_extract
from pipeline.extract.steam_client import SteamClient
from pipeline.extract.storage import get_store
from pipeline.quality.alerts import format_alert, send_alert
from pipeline.quality.checks import CheckResult, Severity, check_missing_days, failures, run_checks

log = logging.getLogger("pipeline")


TABLES_BY_SOURCE = {
    "players": ["player_count_snapshots", "player_counts_daily"],
    "appdetails": ["apps", "app_prices"],
    "reviews": ["review_summary"],
    "twitch": ["twitch_viewer_snapshots"],
    "market": ["market_snapshots", "steam_app_names"],
    "review_text": ["review_themes"],
}


class CriticalQualityError(RuntimeError):
    pass


def resolve_sources(
    requested: str, settings: Settings, engine, run_date: date, extracting: bool = True
) -> tuple[str, ...]:
    """Turn the --sources argument into the list of sources to fetch this run.

    'auto' is for an hourly schedule: player counts, the top-100 market chart (and Twitch,
    if configured) every hour, and the slower store/review calls only until today's
    prices are loaded.
    """
    if requested.strip() == "auto":
        sources = ["players", "twitch", "market"]
        need_daily = True
        if engine is not None:
            from sqlalchemy import text

            with engine.connect() as conn:
                have = conn.execute(
                    text("SELECT 1 FROM app_prices WHERE snapshot_date = :d LIMIT 1"), {"d": run_date}
                ).first()
            need_daily = have is None
        if need_daily:
            sources += ["appdetails", "reviews", "review_text"]
    else:
        sources = [s.strip() for s in requested.split(",") if s.strip()]
        unknown = set(sources) - set(SOURCES)
        if unknown:
            raise ValueError(f"Unknown sources: {sorted(unknown)}")
    if "twitch" in sources and extracting and not settings.twitch_enabled:
        log.info("TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET not set: skipping Twitch")
        sources.remove("twitch")
    return tuple(s for s in SOURCES if s in sources)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Steam game data pipeline")
    p.add_argument(
        "--date", type=date.fromisoformat, default=None, help="snapshot date YYYY-MM-DD (default: today, UTC)"
    )
    p.add_argument(
        "--sources",
        default=",".join(SOURCES),
        help=f"comma-separated subset of {','.join(SOURCES)}, or 'auto' (hourly mode: players, market and "
        "twitch every run, plus prices and reviews once per day)",
    )
    p.add_argument("--skip-extract", action="store_true", help="use raw data already in storage")
    p.add_argument("--skip-load", action="store_true", help="stop after quality checks (no Postgres)")
    p.add_argument("--no-parquet", action="store_true", help="don't write the Parquet clean zone")
    return p.parse_args(argv)


def _report(results: list[CheckResult]) -> list[dict]:
    return [{**asdict(r), "severity": r.severity.value} for r in results]


def run(args: argparse.Namespace, settings: Settings) -> int:
    started_at = datetime.now(timezone.utc)
    today = started_at.date()
    run_date: date = args.date or today
    store = get_store(settings)
    expected_ids = {a.app_id for a in settings.apps if a.steam}

    engine = None
    if not args.skip_load:
        from pipeline.analysis import sync_tracked_apps
        from pipeline.load.postgres_loader import get_engine, init_schema

        engine = get_engine(settings.database_url)
        init_schema(engine)
        sync_tracked_apps(engine, settings.apps)

    sources = resolve_sources(args.sources, settings, engine, run_date, extracting=not args.skip_extract)
    log.info("Sources for this run: %s", ", ".join(sources))

    # 1. Extract -----------------------------------------------------------
    extract_failures = []
    if not args.skip_extract:
        if run_date != today:
            # Steam only tells us about *now*; writing today's numbers under an old date would be wrong.
            raise ValueError(
                f"Can't extract for {run_date}: Steam only returns current data. "
                "Use --skip-extract to re-process raw data you already have."
            )
        client = SteamClient(
            timeout_s=settings.request_timeout_s,
            max_retries=settings.max_retries,
            min_interval_s=settings.min_request_interval_s,
        )
        twitch = None
        if "twitch" in sources:
            from pipeline.extract.twitch_client import TwitchClient

            twitch = TwitchClient(
                settings.twitch_client_id,
                settings.twitch_client_secret,
                timeout_s=settings.request_timeout_s,
                max_pages=settings.twitch_max_pages,
            )
        known_names: set[int] = set()
        if engine is not None and "market" in sources:
            from sqlalchemy import text

            with engine.connect() as conn:
                known_names = {
                    r[0] for r in conn.execute(text("SELECT app_id FROM steam_app_names UNION SELECT app_id FROM apps"))
                }
        result = run_extract(
            client,
            store,
            settings.apps,
            run_date,
            sources,
            settings.steam_country,
            twitch=twitch,
            known_names=known_names,
        )
        extract_failures = result.failures
        if not result.written:
            raise RuntimeError("Extract wrote nothing - is Steam reachable?")

    # 2. Transform (Spark) -------------------------------------------------
    from pipeline.transform.spark_job import get_spark, run_transform  # Spark import is slow; do it late

    with tempfile.TemporaryDirectory() as staging:
        raw_root = settings.raw_dir
        if settings.storage_backend == "s3":
            for source in SOURCES:
                store.materialize_locally(f"raw/{source}/dt={run_date.isoformat()}", Path(staging))
            raw_root = Path(staging) / "raw"
        spark = get_spark()
        try:
            tables = run_transform(spark, raw_root, run_date, None if args.no_parquet else settings.clean_dir)
        finally:
            spark.stop()

    # Only check (and load) the tables this run's sources produce. E.g. an hourly
    # "--sources players" job shouldn't fail because today's prices aren't there.
    checked = {t for s in sources for t in TABLES_BY_SOURCE[s]}
    tables = {t: tables.get(t, []) for t in checked}

    # 3. Quality -----------------------------------------------------------
    results = [r for r in run_checks(tables, run_date, expected_ids) if r.table in checked]
    for r in results:
        log.info("%s", r)
    failed = failures(results)
    critical = failures(results, Severity.CRITICAL)
    extra = ""
    if extract_failures:
        extra = "Extract failures: " + "; ".join(f"{s} app {a}: {e}" for s, a, e in extract_failures)
    if failed or extract_failures:
        send_alert(settings, format_alert(run_date, failed, extra), critical=bool(critical))

    if critical:
        if engine is not None:
            from pipeline.load.postgres_loader import record_run

            record_run(engine, run_date, started_at, "failed", 0, _report(results))
        raise CriticalQualityError(f"{len(critical)} critical data-quality check(s) failed; nothing loaded")

    if engine is None:
        log.info("--skip-load set: done after quality checks")
        return 0

    # 4. Load --------------------------------------------------------------
    from pipeline.analysis import (
        apply_retention,
        detect_anomalies,
        format_anomaly_alert,
        refresh_market_daily,
        refresh_player_daily,
    )
    from pipeline.export import export_dashboard_summary
    from pipeline.load.postgres_loader import fetch_loaded_dates, load_tables, record_run

    counts = load_tables(engine, tables)
    # Daily stats come from every snapshot stored so far today, not just this run's raw files.
    refresh_player_daily(engine, run_date)
    if "market" in sources:
        refresh_market_daily(engine, run_date)
        apply_retention(engine, run_date)

    # Unusual player counts (each one alerts once).
    anomalies = detect_anomalies(engine) if "players" in sources else []
    if anomalies:
        send_alert(settings, format_anomaly_alert(anomalies), critical=False)

    # 24-hour-ahead forecasts (written once per target hour, scored later by query 13).
    if "players" in sources:
        from pipeline.forecast import write_forecasts

        write_forecasts(engine)

    # Post-load: look for gaps in history (warning only).
    gap = check_missing_days("player_counts_daily", fetch_loaded_dates(engine, run_date), run_date)
    log.info("%s", gap)
    if not gap.passed:
        send_alert(settings, format_alert(run_date, [gap]), critical=False)
        results.append(gap)

    status = "warning" if failures(results) or extract_failures else "success"
    record_run(engine, run_date, started_at, status, sum(counts.values()), _report(results))

    # 5. Export for the dashboard -----------------------------------------
    export_dashboard_summary(engine, run_date, settings.dashboard_export_path, apps=settings.apps)
    log.info("Run for %s finished with status=%s, rows loaded=%s", run_date, status, counts)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    args = parse_args(argv)
    settings = get_settings()
    try:
        return run(args, settings)
    except CriticalQualityError as exc:
        log.error("%s", exc)  # the alert was already sent inside run()
        return 1
    except Exception as exc:
        log.exception("Pipeline failed")
        send_alert(settings, f"game-data-pipeline crashed: {type(exc).__name__}: {exc}", critical=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
