from datetime import date, datetime, timezone

import responses

from pipeline.config import Settings
from pipeline.quality.alerts import format_alert, send_alert
from pipeline.quality.checks import (
    Severity,
    check_expected_apps,
    check_freshness,
    check_missing_days,
    check_not_null,
    check_price_values,
    check_review_values,
    check_unique,
    failures,
    run_checks,
)

D = date(2026, 11, 18)


def good_tables():
    ts = datetime(2026, 11, 18, 14, tzinfo=timezone.utc)
    return {
        "apps": [{"app_id": 1, "name": "A"}, {"app_id": 2, "name": "B"}],
        "player_count_snapshots": [
            {"app_id": 1, "captured_at": ts, "snapshot_date": D, "player_count": 100},
            {"app_id": 2, "captured_at": ts, "snapshot_date": D, "player_count": 50},
        ],
        "player_counts_daily": [
            {"app_id": 1, "snapshot_date": D, "max_players": 100},
            {"app_id": 2, "snapshot_date": D, "max_players": 50},
        ],
        "app_prices": [
            {
                "app_id": 1,
                "snapshot_date": D,
                "currency": "CAD",
                "initial_price_cents": 7999,
                "final_price_cents": 2639,
                "discount_percent": 67,
                "is_free": False,
            },
            {
                "app_id": 2,
                "snapshot_date": D,
                "currency": None,
                "initial_price_cents": 0,
                "final_price_cents": 0,
                "discount_percent": 0,
                "is_free": True,
            },
        ],
        "review_summary": [
            {
                "app_id": 1,
                "snapshot_date": D,
                "total_positive": 9,
                "total_negative": 1,
                "total_reviews": 10,
                "positive_ratio": 0.9,
            },
            {
                "app_id": 2,
                "snapshot_date": D,
                "total_positive": 0,
                "total_negative": 0,
                "total_reviews": 0,
                "positive_ratio": None,
            },
        ],
    }


def test_clean_data_passes_everything():
    results = run_checks(good_tables(), D, {1, 2})
    assert failures(results) == [], [str(r) for r in failures(results)]


def test_empty_fact_table_is_critical():
    tables = good_tables()
    tables["app_prices"] = []
    crit = failures(run_checks(tables, D, {1, 2}), Severity.CRITICAL)
    assert any(r.name == "row_count" and r.table == "app_prices" for r in crit)


def test_missing_app_is_only_a_warning():
    tables = good_tables()
    tables["review_summary"] = tables["review_summary"][:1]
    results = run_checks(tables, D, {1, 2})
    assert failures(results, Severity.CRITICAL) == []
    [warn] = failures(results, Severity.WARNING)
    assert warn.name == "expected_apps" and "[2]" in warn.details


def test_not_null():
    r = check_not_null("t", [{"a": 1, "b": None}, {"a": None, "b": None}], ["a", "b"])
    assert not r.passed and r.details == "a: 1 null, b: 2 null"


def test_unique():
    assert not check_unique("t", [{"k": 1}, {"k": 1}, {"k": 2}], ["k"]).passed
    assert check_unique("t", [{"k": 1}, {"k": 2}], ["k"]).passed


def test_freshness_flags_stale_rows():
    r = check_freshness("t", [{"snapshot_date": D}, {"snapshot_date": date(2026, 11, 17)}], D)
    assert not r.passed and "2026-11-17" in r.details and r.severity == Severity.CRITICAL


def test_expected_apps():
    assert check_expected_apps("t", [{"app_id": 1}], {1}).passed


def test_impossible_prices():
    rows = [
        {
            "app_id": 1,
            "discount_percent": 150,
            "final_price_cents": 10,
            "initial_price_cents": 5,
            "currency": "CAD",
            "is_free": False,
        },
        {
            "app_id": 2,
            "discount_percent": 0,
            "final_price_cents": -1,
            "initial_price_cents": 0,
            "currency": None,
            "is_free": False,
        },
    ]
    by_name = {r.name: r for r in check_price_values(rows)}
    assert not by_name["discount_range"].passed
    assert not by_name["non_negative_price"].passed
    assert not by_name["final_not_above_initial"].passed
    assert not by_name["currency_present"].passed


def test_negative_players_fail_run():
    tables = good_tables()
    tables["player_count_snapshots"][0]["player_count"] = -5
    crit = failures(run_checks(tables, D, {1, 2}), Severity.CRITICAL)
    assert [r.name for r in crit] == ["non_negative_players"]


def test_review_totals_must_add_up():
    rows = [{"app_id": 1, "total_positive": 5, "total_negative": 1, "total_reviews": 10, "positive_ratio": 1.4}]
    by_name = {r.name: r for r in check_review_values(rows)}
    assert not by_name["ratio_range"].passed
    assert not by_name["totals_add_up"].passed


def test_missing_days_in_history():
    have = [date(2026, 11, d) for d in (12, 13, 14, 16, 17, 18)]
    r = check_missing_days("player_counts_daily", have, D, lookback_days=7)
    assert not r.passed and r.details == "missing days: ['2026-11-15']"


def test_missing_days_ignores_days_before_first_load():
    r = check_missing_days("t", [date(2026, 11, 17), D], D, lookback_days=7)
    assert r.passed


def test_alert_logs_only_by_default(caplog):
    sent = send_alert(Settings(), "something broke", critical=True)
    assert sent == ["log"]
    assert "something broke" in caplog.text


@responses.activate
def test_alert_posts_to_slack_when_configured():
    url = "https://hooks.slack.com/services/T000/B000/XXXX"
    responses.post(url, body="ok")
    sent = send_alert(Settings(slack_webhook_url=url), "hello", critical=False)
    assert sent == ["log", "slack"]
    assert b'"text": "hello"' in responses.calls[0].request.body


@responses.activate
def test_broken_webhook_does_not_raise():
    url = "https://hooks.slack.com/services/bad"
    responses.post(url, status=500)
    assert send_alert(Settings(slack_webhook_url=url), "hello", critical=True) == ["log"]


def test_format_alert_counts():
    tables = good_tables()
    tables["app_prices"] = []
    msg = format_alert(D, failures(run_checks(tables, D, {1, 2})))
    assert msg.startswith("game-data-pipeline 2026-11-18: 1 critical, 1 warning")


def test_not_sold_separately_is_not_a_failure():
    rows = [
        {
            "app_id": 9,
            "snapshot_date": D,
            "currency": None,
            "initial_price_cents": None,
            "final_price_cents": None,
            "discount_percent": 0,
            "is_free": False,
            "is_purchasable": False,
        },
    ]
    results = {r.name: r for r in check_price_values(rows)}
    assert results["price_present"].passed
    assert results["currency_present"].passed


def test_purchasable_app_without_price_is_critical():
    rows = [
        {
            "app_id": 1,
            "snapshot_date": D,
            "currency": "CAD",
            "initial_price_cents": None,
            "final_price_cents": None,
            "discount_percent": 0,
            "is_free": False,
            "is_purchasable": True,
        },
    ]
    results = {r.name: r for r in check_price_values(rows)}
    assert not results["price_present"].passed
    assert results["price_present"].severity.value == "critical"


def _market(n=100, **override):
    at = "2026-11-18T14:00:00Z"
    return [{"app_id": i, "captured_at": at, "rank": i, "current_players": 1000, **override} for i in range(1, n + 1)]


def test_full_market_chart_passes():
    from pipeline.quality.checks import check_market_values

    assert all(r.passed for r in check_market_values(_market()))


def test_thin_market_chart_is_only_a_warning():
    from pipeline.quality.checks import check_market_values

    failed = [r for r in check_market_values(_market(40)) if not r.passed]
    assert [(r.name, r.severity) for r in failed] == [("chart_size", Severity.WARNING)]


def test_negative_market_players_are_critical():
    from pipeline.quality.checks import check_market_values

    failed = [r for r in check_market_values(_market(current_players=-1)) if not r.passed]
    assert ("non_negative", Severity.CRITICAL) in [(r.name, r.severity) for r in failed]


def test_theme_counts_must_be_consistent():
    from pipeline.quality.checks import check_theme_values

    good = {"app_id": 1, "theme": "bugs", "reviews_sampled": 100, "mentions": 10, "negative_mentions": 8}
    bad = {**good, "theme": "price", "negative_mentions": 11}
    thin = {**good, "app_id": 2, "reviews_sampled": 10, "mentions": 1, "negative_mentions": 0}
    results = {r.name: r for r in check_theme_values([good, bad, thin])}
    assert not results["counts_consistent"].passed and results["counts_consistent"].severity == Severity.CRITICAL
    assert not results["sample_size"].passed and results["sample_size"].severity == Severity.WARNING
