"""Settings for the pipeline, read from environment variables (and an optional .env file).

Nothing secret lives in the code. Copy .env.example to .env and fill in what you need.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env if it exists. Real environment variables always win over the file.
load_dotenv(PROJECT_ROOT / ".env", override=False)


@dataclass(frozen=True)
class AppConfig:
    app_id: int
    name: str
    publisher_group: str = ""
    twitch_game: str = ""  # exact Twitch category name; empty = don't track on Twitch
    steam: bool = True  # False = not on Steam (e.g. a console-only game): track Twitch only


@dataclass(frozen=True)
class Settings:
    # Where raw JSON goes: "local" (default) or "s3".
    storage_backend: str = "local"
    data_dir: Path = PROJECT_ROOT / "data"
    s3_bucket: str = ""
    s3_prefix: str = "game-data-pipeline"
    aws_region: str = "ca-central-1"
    # Data lake (optional, see pipeline/lake.py): Glue database + Athena workgroup.
    athena_database: str = "game_data"
    athena_workgroup: str = "game-data"

    # Postgres connection string, e.g. postgresql+psycopg://user:pass@localhost:5432/games
    database_url: str = ""

    # Steam request settings.
    steam_country: str = "ca"  # cc= for prices (CAD for a Toronto-based project)
    request_timeout_s: float = 10.0
    max_retries: int = 4
    min_request_interval_s: float = 1.5  # store API is roughly 200 requests / 5 min

    # Alerting. Empty = just log.
    slack_webhook_url: str = ""
    alert_email_to: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""

    # Twitch (optional). Without both, the twitch source is skipped.
    twitch_client_id: str = ""
    twitch_client_secret: str = ""
    twitch_max_pages: int = 10  # 100 streams per page; top 1,000 streams cover nearly all viewers

    apps_file: Path = PROJECT_ROOT / "config" / "apps.json"
    dashboard_export_path: Path = PROJECT_ROOT / "dashboard" / "data" / "summary.json"

    apps: list[AppConfig] = field(default_factory=list)

    @property
    def twitch_enabled(self) -> bool:
        return bool(self.twitch_client_id and self.twitch_client_secret)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def clean_dir(self) -> Path:
        return self.data_dir / "clean"


def load_apps(path: Path) -> list[AppConfig]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    known = set(AppConfig.__dataclass_fields__)
    return [AppConfig(**{k: v for k, v in app.items() if k in known}) for app in data["apps"]]


def get_settings() -> Settings:
    """Build Settings from the environment. Called once per run."""
    env = os.environ.get
    apps_file = Path(env("APPS_FILE", str(PROJECT_ROOT / "config" / "apps.json")))
    return Settings(
        storage_backend=env("STORAGE_BACKEND", "local").lower(),
        data_dir=Path(env("DATA_DIR", str(PROJECT_ROOT / "data"))),
        s3_bucket=env("S3_BUCKET", ""),
        s3_prefix=env("S3_PREFIX", "game-data-pipeline"),
        aws_region=env("AWS_REGION", "ca-central-1"),
        athena_database=env("ATHENA_DATABASE", "game_data"),
        athena_workgroup=env("ATHENA_WORKGROUP", "game-data"),
        database_url=env("DATABASE_URL", ""),
        steam_country=env("STEAM_COUNTRY", "ca"),
        request_timeout_s=float(env("REQUEST_TIMEOUT_S", "10")),
        max_retries=int(env("MAX_RETRIES", "4")),
        min_request_interval_s=float(env("MIN_REQUEST_INTERVAL_S", "1.5")),
        slack_webhook_url=env("SLACK_WEBHOOK_URL", ""),
        alert_email_to=env("ALERT_EMAIL_TO", ""),
        smtp_host=env("SMTP_HOST", ""),
        smtp_port=int(env("SMTP_PORT", "587")),
        smtp_user=env("SMTP_USER", ""),
        smtp_password=env("SMTP_PASSWORD", ""),
        twitch_client_id=env("TWITCH_CLIENT_ID", ""),
        twitch_client_secret=env("TWITCH_CLIENT_SECRET", ""),
        twitch_max_pages=int(env("TWITCH_MAX_PAGES", "10")),
        apps_file=apps_file,
        dashboard_export_path=Path(
            env("DASHBOARD_EXPORT_PATH", str(PROJECT_ROOT / "dashboard" / "data" / "summary.json"))
        ),
        apps=load_apps(apps_file),
    )
