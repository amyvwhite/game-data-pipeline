"""Generate SYNTHETIC raw data for a range of past days, so the whole pipeline
(and the dashboard) can be demoed without waiting weeks for real history.

Steam only reports *current* numbers, so there's no way to backfill real history.
Everything this script writes is fake and marked "synthetic": true in each raw file.

Usage:
    python scripts/seed_demo_data.py --days 21              # write raw files only
    python scripts/seed_demo_data.py --days 21 --run        # ...and run the pipeline for each day
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import get_settings  # noqa: E402
from pipeline.extract.extract import raw_key  # noqa: E402
from pipeline.extract.storage import LocalStore  # noqa: E402

# Rough, made-up baselines (players, price in cents) - just to make charts look plausible.
BASELINES = {
    3240220: (80_000, 3999),
    271590: (35_000, 3999),
    1174180: (30_000, 7999),
    3472040: (12_000, 9999),
    1285190: (20_000, 9999),
    289070: (28_000, 7999),
    1295660: (14_000, 9999),
    3405690: (45_000, 8999),
    1938090: (60_000, 0),
    3159330: (4_000, 8999),
    1091500: (30_000, 7999),
    -6: (150_000, 0),  # GTA VI: Twitch only (base used for viewer numbers)
}
SPIKE_APP = 3240220  # on the last day this game gets an evening spike, so the anomaly alert has something to find


def synthetic_day(
    store: LocalStore, app_id: int, name: str, day: date, day_index: int, total_days: int, rng: random.Random
) -> None:
    base_players, base_price = BASELINES.get(app_id, (10_000, 5999))
    on_sale = day_index >= total_days - 4 and (app_id // 10) % 2 == 0  # some games on sale for the last 4 days
    discount = 50 if on_sale else 0
    weekend = 1.15 if day.weekday() >= 5 else 1.0
    boost = 1.2 if on_sale else 1.0

    def envelope(source: str, payload: dict, at: datetime) -> None:
        env = {
            "source": source,
            "app_id": app_id,
            "snapshot_date": day.isoformat(),
            "fetched_at": at.isoformat(),
            "synthetic": True,
            "payload": payload,
        }
        store.write_json(raw_key(source, day, app_id, at), env)

    # Players every hour, with a daily wave peaking in the Toronto evening (about 00:00 UTC).
    steam = app_id > 0  # placeholder ids (e.g. GTA VI) are Twitch-only
    for hour in range(24):
        at = datetime(day.year, day.month, day.day, hour, 17, tzinfo=timezone.utc)
        wave = 1 + 0.35 * math.sin((hour - 19) / 24 * 2 * math.pi + math.pi / 2)
        spike = 1.9 if app_id == SPIKE_APP and day_index == total_days - 1 and hour >= 21 else 1.0
        count = int(base_players * weekend * boost * wave * spike * rng.uniform(0.95, 1.05))
        if steam:
            envelope("players", {"response": {"player_count": count, "result": 1}}, at)
        envelope(
            "twitch",
            {
                "twitch_game": name,
                "twitch_game_id": str(app_id),
                "found": True,
                "viewer_count": int(count * rng.uniform(0.03, 0.06)),
                "stream_count": int(count * rng.uniform(0.002, 0.004)) + 1,
                "pages": 3,
                "truncated": False,
            },
            at,
        )

    if not steam:
        return
    noon = datetime(day.year, day.month, day.day, 12, 0, tzinfo=timezone.utc)
    final = base_price * (100 - discount) // 100
    envelope(
        "appdetails",
        {
            str(app_id): {
                "success": True,
                "data": {
                    "type": "game",
                    "name": name,
                    "steam_appid": app_id,
                    "is_free": base_price == 0,
                    "developers": ["(synthetic)"],
                    "publishers": ["(synthetic)"],
                    "price_overview": None
                    if base_price == 0
                    else {
                        "currency": "CAD",
                        "initial": base_price,
                        "final": final,
                        "discount_percent": discount,
                        "initial_formatted": f"CDN$ {base_price / 100:.2f}" if discount else "",
                        "final_formatted": f"CDN$ {final / 100:.2f}",
                    },
                    "release_date": {"coming_soon": False, "date": "(synthetic)"},
                },
            }
        },
        noon,
    )

    total = 50_000 + app_id % 10_000 * 10 + day_index * rng.randint(80, 200)
    positive = int(total * (0.80 + (app_id % 7) / 50))
    envelope(
        "reviews",
        {
            "response": {
                "query_summary": {
                    "num_reviews": 1,
                    "review_score": 8,
                    "review_score_desc": "Very Positive",
                    "total_positive": positive,
                    "total_negative": total - positive,
                    "total_reviews": total,
                },
                "reviews": [],
            }
        },
        noon,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--days", type=int, default=21)
    p.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    p.add_argument("--run", action="store_true", help="run the pipeline (needs DATABASE_URL) for each day")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    settings = get_settings()
    store = LocalStore(settings.data_dir)
    rng = random.Random(args.seed)
    days = [args.end - timedelta(days=args.days - 1 - i) for i in range(args.days)]
    for i, day in enumerate(days):
        for app in settings.apps:
            synthetic_day(store, app.app_id, app.name, day, i, len(days), rng)
    print(f"Wrote synthetic raw data for {len(days)} days ({days[0]} to {days[-1]}) under {settings.raw_dir}")

    if args.run:
        from pipeline.export import export_dashboard_summary
        from pipeline.load.postgres_loader import get_engine
        from pipeline.run import main as run_main

        for day in days:
            code = run_main(["--date", day.isoformat(), "--skip-extract"])
            if code != 0:
                print(f"Pipeline failed for {day} (exit {code})")
                return code
        # Re-export with the sample flag so the dashboard shows a "synthetic data" banner.
        export_dashboard_summary(
            get_engine(settings.database_url),
            days[-1],
            settings.dashboard_export_path,
            is_sample=True,
            apps=settings.apps,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
