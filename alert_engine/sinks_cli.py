# alert_engine/sinks_cli.py
"""
Step B.1 test harness:
- Accepts the full set of Slack/SMTP flags + env fallbacks.
- Loads a sample alert (from --sample-json or built-in).
- Instantiates sinks in DRY-RUN mode and prints what would be sent.
- Does NOT perform network I/O.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from .sinks import SlackSink, SMTPSink, SinkMetrics, Alert


def _add_slack_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("Slack sink")
    g.add_argument("--slack", action="store_true", help="Enable Slack sink (dry-run in Step B.1)")
    g.add_argument("--slack-webhook", dest="slack_webhook", help="Slack Incoming Webhook URL (or env SLACK_WEBHOOK_URL)")
    g.add_argument("--slack-timeout", dest="slack_timeout", type=float, help="Slack timeout seconds (env SLACK_TIMEOUT_SECS, default 5)")
    g.add_argument("--slack-rate-per-sec", dest="slack_rate_per_sec", type=float, help="Max posts per second (env SLACK_RATE_PER_SEC)")
    g.add_argument("--slack-mention", dest="slack_mention", help="Optional @mention text (env SLACK_MENTION)")


def _add_smtp_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("SMTP sink")
    g.add_argument("--smtp", action="store_true", help="Enable SMTP sink (dry-run in Step B.1)")
    g.add_argument("--smtp-host", dest="smtp_host", help="SMTP host (env SMTP_HOST)")
    g.add_argument("--smtp-port", dest="smtp_port", help="SMTP port (env SMTP_PORT)")
    g.add_argument("--smtp-user", dest="smtp_user", help="SMTP username (env SMTP_USER)")
    g.add_argument("--smtp-pass", dest="smtp_pass", help="SMTP password (env SMTP_PASS)")
    g.add_argument("--smtp-from", dest="smtp_from", help="From address (env SMTP_FROM)")
    g.add_argument("--smtp-to", dest="smtp_to", help="Comma-separated recipients (env SMTP_TO)")
    g.add_argument("--smtp-subject-prefix", dest="smtp_subject_prefix", help="Subject prefix (env SMTP_SUBJECT_PREFIX)")
    g.add_argument("--smtp-timeout", dest="smtp_timeout", type=float, help="Timeout secs (env SMTP_TIMEOUT_SECS)")
    g.add_argument("--smtp-use-ssl", dest="smtp_use_ssl", help="Use SSL (1/true/yes, env SMTP_USE_SSL)")
    g.add_argument("--smtp-use-starttls", dest="smtp_use_starttls", help="Use STARTTLS (1/true/yes, env SMTP_USE_STARTTLS)")


def _parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m alert_engine.sinks_cli",
        description="Supply-Signals sink dry-run harness (no network I/O).",
    )
    _add_slack_args(p)
    _add_smtp_args(p)
    p.add_argument("--sample-json", help="Path to a sample alert JSON; if omitted, use built-in sample.")
    return p.parse_args(argv)


def _load_sample_alert(path: str | None) -> Alert:
    if path:
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    # Built-in sample
    return {
        "issuer_name": "ACME Corp",
        "ticker": "ACME",
        "cik": "0000123456",
        "event_kind": "press_release",
        "score": 4,
        "event_datetime_utc": "2025-10-05T10:00:00Z",
        "title": "ACME announces strategic supply partnership with XYZ",
        "first_url": "https://example.com/acme-xyz",
        "rule_hits": ["supply", "partnership", "capacity"],
    }


def main(argv: List[str] | None = None) -> int:
    args = _parse_args(argv)

    alert = _load_sample_alert(args.sample_json)

    sinks = []
    if args.slack:
        sinks.append(SlackSink.from_args_env(args))
    if args.smtp:
        sinks.append(SMTPSink.from_args_env(args))

    if not sinks:
        print("No sinks enabled. Use --slack and/or --smtp. (This is expected in Step B.1 if you're just checking flags.)")
        return 0

    # Emit to each sink (DRY-RUN prints). Collect and summarize metrics.
    for s in sinks:
        try:
            s.emit(alert)
            s.flush()
        except Exception as e:  # Defensive; shouldn't occur in dry-run
            print(f"[{getattr(s, 'name', 'sink')}][DRY-RUN] ERROR: {e}")
            if hasattr(s, "metrics"):
                s.metrics.errors += 1

    # Summary
    print("\n=== Sink metrics ===")
    for s in sinks:
        m = s.metrics
        print(f"{s.name}: attempted={m.attempted} sent={m.sent} skipped={m.skipped} errors={m.errors}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
