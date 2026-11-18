"""Alerting hook. Always logs; also posts to Slack and/or sends email if configured.

    SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...   -> Slack message
    ALERT_EMAIL_TO + SMTP_HOST (+ SMTP_USER/SMTP_PASSWORD)    -> email

An alert that fails to send is logged, never raised: a broken webhook should not
hide the real problem it was trying to report.
"""

from __future__ import annotations

import logging
import smtplib
from datetime import date
from email.message import EmailMessage

import requests

from pipeline.config import Settings
from pipeline.quality.checks import CheckResult, Severity

log = logging.getLogger(__name__)


def format_alert(run_date: date, failed: list[CheckResult], extra: str = "") -> str:
    critical = [f for f in failed if f.severity == Severity.CRITICAL]
    header = (
        f"game-data-pipeline {run_date}: {len(critical)} critical, "
        f"{len(failed) - len(critical)} warning check(s) failed"
    )
    lines = [header] + [f"- {f}" for f in failed]
    if extra:
        lines.append(extra)
    return "\n".join(lines)


def send_alert(settings: Settings, message: str, critical: bool) -> list[str]:
    """Send the message everywhere that is configured. Returns the channels used."""
    sent = ["log"]
    (log.error if critical else log.warning)("ALERT:\n%s", message)

    if settings.slack_webhook_url:
        try:
            resp = requests.post(settings.slack_webhook_url, json={"text": message}, timeout=10)
            resp.raise_for_status()
            sent.append("slack")
        except requests.RequestException as exc:
            log.error("Could not post alert to Slack: %s", exc)

    if settings.alert_email_to and settings.smtp_host:
        try:
            msg = EmailMessage()
            msg["Subject"] = message.splitlines()[0]
            msg["From"] = settings.smtp_user or "game-data-pipeline@localhost"
            msg["To"] = settings.alert_email_to
            msg.set_content(message)
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
                smtp.starttls()
                if settings.smtp_user:
                    smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(msg)
            sent.append("email")
        except (smtplib.SMTPException, OSError) as exc:
            log.error("Could not send alert email: %s", exc)

    return sent
