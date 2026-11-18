"""Data-quality checks that run on the clean tables before they are loaded.

Each check returns CheckResult objects. Severity decides what happens:
    CRITICAL -> alert and stop the run (nothing gets loaded)
    WARNING  -> alert, but keep going

The checks work on plain lists of dicts, so they are quick to test and do not
need Spark or a database.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from typing import Any

Rows = list[dict[str, Any]]


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"


@dataclass(frozen=True)
class CheckResult:
    name: str
    table: str
    passed: bool
    severity: Severity
    details: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else f"FAIL ({self.severity.value})"
        return f"[{status}] {self.table}.{self.name}: {self.details}"


# Primary keys and required columns for each clean table.
TABLE_RULES: dict[str, dict[str, list[str]]] = {
    "apps": {"key": ["app_id"], "not_null": ["app_id", "name"]},
    "player_count_snapshots": {
        "key": ["app_id", "captured_at"],
        "not_null": ["app_id", "captured_at", "snapshot_date", "player_count"],
    },
    "player_counts_daily": {
        "key": ["app_id", "snapshot_date"],
        "not_null": ["app_id", "snapshot_date", "max_players"],
    },
    "app_prices": {
        "key": ["app_id", "snapshot_date"],
        "not_null": ["app_id", "snapshot_date", "discount_percent"],
    },
    "review_summary": {
        "key": ["app_id", "snapshot_date"],
        "not_null": ["app_id", "snapshot_date", "total_reviews"],
    },
    "twitch_viewer_snapshots": {
        "key": ["app_id", "captured_at"],
        "not_null": ["app_id", "captured_at", "snapshot_date", "viewer_count", "stream_count"],
    },
}

# Tables that must have rows every day. "apps" is a dimension, so not in this list.
FACT_TABLES = ["player_count_snapshots", "player_counts_daily", "app_prices", "review_summary"]


# ---- generic checks ---------------------------------------------------------


def check_row_count(table: str, rows: Rows, minimum: int = 1) -> CheckResult:
    n = len(rows)
    return CheckResult("row_count", table, n >= minimum, Severity.CRITICAL, f"{n} rows (minimum {minimum})")


def check_not_null(table: str, rows: Rows, columns: list[str]) -> CheckResult:
    bad = {c: sum(1 for r in rows if r.get(c) is None) for c in columns}
    bad = {c: n for c, n in bad.items() if n}
    detail = "no nulls" if not bad else ", ".join(f"{c}: {n} null" for c, n in bad.items())
    return CheckResult("not_null", table, not bad, Severity.CRITICAL, detail)


def check_unique(table: str, rows: Rows, key: list[str]) -> CheckResult:
    counts = Counter(tuple(r.get(k) for k in key) for r in rows)
    dupes = {k: n for k, n in counts.items() if n > 1}
    detail = "no duplicate keys" if not dupes else f"{len(dupes)} duplicate keys, e.g. {next(iter(dupes))}"
    return CheckResult("unique_key", table, not dupes, Severity.CRITICAL, detail)


def check_expected_apps(table: str, rows: Rows, expected_app_ids: set[int]) -> CheckResult:
    """Missing one game usually means one API call failed. Worth knowing, not worth stopping for."""
    missing = sorted(expected_app_ids - {r.get("app_id") for r in rows})
    detail = "all expected apps present" if not missing else f"missing app_ids: {missing}"
    return CheckResult("expected_apps", table, not missing, Severity.WARNING, detail)


def check_freshness(table: str, rows: Rows, run_date: date) -> CheckResult:
    """Every row in today's batch should be for today. Anything else means stale or mixed-up input."""
    wrong = sorted({str(r.get("snapshot_date")) for r in rows if r.get("snapshot_date") != run_date})
    detail = f"all rows dated {run_date}" if not wrong else f"unexpected snapshot_date values: {wrong}"
    return CheckResult("freshness", table, not wrong, Severity.CRITICAL, detail)


# ---- table-specific "impossible value" checks --------------------------------


def check_player_values(rows: Rows) -> list[CheckResult]:
    negative = [r["app_id"] for r in rows if (r.get("player_count") or 0) < 0]
    # GTA V has peaked in the hundreds of thousands; tens of millions is a bug, not a launch.
    absurd = [r["app_id"] for r in rows if (r.get("player_count") or 0) > 20_000_000]
    return [
        CheckResult(
            "non_negative_players",
            "player_count_snapshots",
            not negative,
            Severity.CRITICAL,
            f"negative counts for {negative}" if negative else "ok",
        ),
        CheckResult(
            "plausible_players",
            "player_count_snapshots",
            not absurd,
            Severity.WARNING,
            f"suspiciously high counts for {absurd}" if absurd else "ok",
        ),
    ]


def check_price_values(rows: Rows) -> list[CheckResult]:
    bad_discount = [r["app_id"] for r in rows if not 0 <= (r.get("discount_percent") or 0) <= 100]
    negative = [
        r["app_id"] for r in rows if (r.get("final_price_cents") or 0) < 0 or (r.get("initial_price_cents") or 0) < 0
    ]
    final_above_initial = [
        r["app_id"]
        for r in rows
        if r.get("final_price_cents") is not None
        and r.get("initial_price_cents") is not None
        and r["final_price_cents"] > r["initial_price_cents"]
    ]
    # Apps Steam doesn't sell on their own have no price at all; that's expected.
    purchasable = [r for r in rows if r.get("is_purchasable", True)]
    missing_price = [r["app_id"] for r in purchasable if r.get("final_price_cents") is None]
    paid_without_currency = [r["app_id"] for r in purchasable if not r.get("is_free") and not r.get("currency")]
    return [
        CheckResult(
            "price_present",
            "app_prices",
            not missing_price,
            Severity.CRITICAL,
            f"purchasable app with no price: {missing_price}" if missing_price else "ok",
        ),
        CheckResult(
            "discount_range",
            "app_prices",
            not bad_discount,
            Severity.CRITICAL,
            f"discount outside 0-100 for {bad_discount}" if bad_discount else "ok",
        ),
        CheckResult(
            "non_negative_price",
            "app_prices",
            not negative,
            Severity.CRITICAL,
            f"negative price for {negative}" if negative else "ok",
        ),
        CheckResult(
            "final_not_above_initial",
            "app_prices",
            not final_above_initial,
            Severity.WARNING,
            f"final > initial for {final_above_initial}" if final_above_initial else "ok",
        ),
        CheckResult(
            "currency_present",
            "app_prices",
            not paid_without_currency,
            Severity.WARNING,
            f"paid app with no currency: {paid_without_currency}" if paid_without_currency else "ok",
        ),
    ]


def check_review_values(rows: Rows) -> list[CheckResult]:
    bad_ratio = [
        r["app_id"] for r in rows if r.get("positive_ratio") is not None and not 0 <= float(r["positive_ratio"]) <= 1
    ]
    mismatched = [
        r["app_id"]
        for r in rows
        if None not in (r.get("total_positive"), r.get("total_negative"), r.get("total_reviews"))
        and r["total_positive"] + r["total_negative"] != r["total_reviews"]
    ]
    return [
        CheckResult(
            "ratio_range",
            "review_summary",
            not bad_ratio,
            Severity.CRITICAL,
            f"positive_ratio outside 0-1 for {bad_ratio}" if bad_ratio else "ok",
        ),
        CheckResult(
            "totals_add_up",
            "review_summary",
            not mismatched,
            Severity.WARNING,
            f"positive + negative != total for {mismatched}" if mismatched else "ok",
        ),
    ]


def check_missing_days(table: str, loaded_dates: list[date], run_date: date, lookback_days: int = 7) -> CheckResult:
    """Look back over the last N days and flag any day with no data at all.

    Runs after load, against what is in Postgres. A warning, because a missed
    day in the past can't be fixed by failing today's run.
    """
    start = run_date - timedelta(days=lookback_days - 1)
    have = set(loaded_dates)
    first = min(have) if have else run_date
    expected = [start + timedelta(days=i) for i in range(lookback_days) if start + timedelta(days=i) >= first]
    missing = [d.isoformat() for d in expected if d not in have]
    detail = f"no gaps in last {lookback_days} days" if not missing else f"missing days: {missing}"
    return CheckResult("missing_days", table, not missing, Severity.WARNING, detail)


# ---- run everything ---------------------------------------------------------


def run_checks(tables: dict[str, Rows], run_date: date, expected_app_ids: set[int]) -> list[CheckResult]:
    results: list[CheckResult] = []
    for table, rules in TABLE_RULES.items():
        rows = tables.get(table, [])
        if table in FACT_TABLES:
            results.append(check_row_count(table, rows))
            results.append(check_freshness(table, rows, run_date))
            results.append(check_expected_apps(table, rows, expected_app_ids))
        results.append(check_not_null(table, rows, rules["not_null"]))
        results.append(check_unique(table, rows, rules["key"]))
    results += check_player_values(tables.get("player_count_snapshots", []))
    results += check_price_values(tables.get("app_prices", []))
    results += check_review_values(tables.get("review_summary", []))
    return results


def failures(results: list[CheckResult], severity: Severity | None = None) -> list[CheckResult]:
    return [r for r in results if not r.passed and (severity is None or r.severity == severity)]
