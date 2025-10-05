# alert_engine/__main__.py
"""
Supply-Signals: alert_engine main CLI

Phase-0 compatible behavior preserved:
- Default: print alerts to console from queue/signals/*.signals.jsonl
- --csv: also append CSV rows to queue/alerts/alerts.csv

Milestone 4:
- Step B.2: optional Slack/SMTP sinks behind flags, DRY-RUN by default.
- Step B.3: add --sinks-live to enable real Slack POST + real SMTP send,
            with basic retries and optional rate-limit. Preflight config in live mode
            exits with code 2 on invalid sink configuration.
- Step B.4: per-run sink dedupe (opt-out), and --fail-on-sink-error considers sink metrics.

Notes:
- Signals input lines are JSON objects. If a line contains a top-level list under
  keys like "signals" or "alerts", we iterate those; otherwise the line itself
  is treated as one alert.
- CSV schema is additive and tolerant; missing fields become empty cells.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import unicodedata
from glob import glob
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse, urlunparse

from .sinks import SlackSink, SMTPSink, SinkMetrics, Alert

DEFAULT_SIGNALS_DIR = os.getenv("SIG_QUEUE_DIR", "queue/signals")
DEFAULT_ALERTS_CSV = os.getenv("ALERTS_CSV_PATH", "queue/alerts/alerts.csv")

CSV_COLUMNS = [
    "issuer_name",
    "ticker",
    "cik",
    "event_kind",
    "score",
    "event_datetime_utc",
    "title",
    "first_url",
    "rule_hits",
]


def _iter_alerts_from_file(path: Path) -> Iterable[Alert]:
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[alert_engine] WARN: {path.name}:{ln} invalid JSON: {e}")
                continue

            if isinstance(obj, dict):
                for key in ("signals", "alerts"):
                    if key in obj and isinstance(obj[key], list):
                        for it in obj[key]:
                            if isinstance(it, dict):
                                yield it
                        break
                else:
                    yield obj
            elif isinstance(obj, list):
                for it in obj:
                    if isinstance(it, dict):
                        yield it
            else:
                print(f"[alert_engine] WARN: {path.name}:{ln} unsupported JSON type")


def load_alerts(signals_dir: str) -> List[Alert]:
    alerts: List[Alert] = []
    for fp in sorted(glob(str(Path(signals_dir) / "*.signals.jsonl"))):
        for alert in _iter_alerts_from_file(Path(fp)):
            alerts.append(alert)
    return alerts


def write_csv(alerts: Iterable[Alert], csv_path: str) -> int:
    out_path = Path(csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not out_path.exists() or out_path.stat().st_size == 0

    with out_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        if is_new:
            w.writeheader()
        rows = 0
        for a in alerts:
            row = dict(a)
            rh = a.get("rule_hits")
            if isinstance(rh, list):
                row["rule_hits"] = ", ".join(map(str, rh))
            w.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
            rows += 1
    return rows


def print_console(alerts: Iterable[Alert]) -> int:
    n = 0
    for a in alerts:
        issuer = a.get("issuer_name") or a.get("company") or "Unknown issuer"
        kind = a.get("event_kind") or a.get("kind") or "event"
        score = a.get("score")
        score_s = f" score={score}" if score is not None else ""
        title = a.get("title") or "(no title)"
        url = a.get("first_url") or a.get("url") or "(no url)"
        ts = a.get("event_datetime_utc") or a.get("ts") or "unknown time"
        print(f"{issuer} — {kind}{score_s}\n  {title}\n  {url}\n  {ts}\n")
        n += 1
    return n


# ---- Sink args & toggles ----

def add_sink_args(parser: argparse.ArgumentParser) -> None:
    # Slack
    g = parser.add_argument_group("Slack sink")
    g.add_argument("--slack", action="store_true", help="Enable Slack sink (DRY-RUN by default)")
    g.add_argument("--slack-webhook", dest="slack_webhook", help="Slack Incoming Webhook URL (or env SLACK_WEBHOOK_URL)")
    g.add_argument("--slack-timeout", dest="slack_timeout", type=float, help="Timeout secs (env SLACK_TIMEOUT_SECS)")
    g.add_argument("--slack-rate-per-sec", dest="slack_rate_per_sec", type=float, help="Max posts/sec (env SLACK_RATE_PER_SEC)")
    g.add_argument("--slack-mention", dest="slack_mention", help="Optional @mention text (env SLACK_MENTION)")

    # SMTP
    g = parser.add_argument_group("SMTP sink")
    g.add_argument("--smtp", action="store_true", help="Enable SMTP sink (DRY-RUN by default)")
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

    # Policy toggles
    parser.add_argument(
        "--sinks-live",
        action="store_true",
        help="Enable LIVE sends (Slack POST / SMTP email). Default is DRY-RUN without this flag.",
    )
    parser.add_argument(
        "--fail-on-sink-error",
        action="store_true",
        help="Exit non-zero if any sink errors during send (applies to dry-run or live).",
    )
    parser.add_argument(
        "--sink-dedupe-disable",
        action="store_true",
        help="Disable per-run sink dedupe (by default duplicates are skipped per sink).",
    )


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m alert_engine",
        description="Supply-Signals alert engine (console/CSV + optional Slack/SMTP sinks).",
    )
    p.add_argument("--signals-dir", default=DEFAULT_SIGNALS_DIR, help=f"Signals dir (default: {DEFAULT_SIGNALS_DIR})")
    p.add_argument("--csv", action="store_true", help="Append alerts to CSV sink (queue/alerts/alerts.csv)")
    p.add_argument("--alerts-csv", default=DEFAULT_ALERTS_CSV, help=f"CSV path (default: {DEFAULT_ALERTS_CSV})")
    add_sink_args(p)
    return p.parse_args(argv)


def _build_enabled_sinks(args) -> List:
    sinks = []
    if args.slack:
        sinks.append(SlackSink.from_args_env(args))
    if args.smtp:
        sinks.append(SMTPSink.from_args_env(args))
    return sinks


def _preflight_live_or_die(args, sinks: List) -> None:
    """
    In LIVE mode, fail-fast (exit 2) on invalid sink configuration as per design.
    """
    if not args.sinks_live:
        return
    errs = []
    for s in sinks:
        if getattr(s, "name", "") == "slack":
            if not getattr(s, "webhook_url", None):
                errs.append("Slack: missing webhook URL (use --slack-webhook or SLACK_WEBHOOK_URL).")
        elif getattr(s, "name", "") == "smtp":
            if not getattr(s, "host", None):
                errs.append("SMTP: missing SMTP_HOST.")
            if not getattr(s, "from_addr", None):
                errs.append("SMTP: missing SMTP_FROM.")
            if not getattr(s, "to_addr", None):
                errs.append("SMTP: missing SMTP_TO.")
    if errs:
        print("[alert_engine] LIVE mode preflight failed:")
        for e in errs:
            print(" - " + e)
        raise SystemExit(2)


# ---- Per-run sink dedupe helpers ----

def _canon_str(s: str) -> str:
    return unicodedata.normalize("NFKC", s).casefold().strip()

def _canon_url(u: str) -> str:
    try:
        p = urlparse(u)
        # Lowercase scheme/host, drop default ports, drop trailing slash
        netloc = p.hostname or ""
        if p.port:
            netloc = f"{netloc}:{p.port}"
        path = p.path or ""
        if path.endswith("/") and path != "/":
            path = path[:-1]
        return urlunparse((p.scheme.lower(), netloc.lower(), path, "", p.query, ""))
    except Exception:
        return _canon_str(u)

def _alert_date(alert: Alert) -> str:
    ts = alert.get("event_datetime_utc") or ""
    return ts[:10] if len(ts) >= 10 else ""

def _make_sink_dedupe_key(alert: Alert) -> str:
    issuer = _canon_str(str(alert.get("issuer_name") or alert.get("company") or ""))
    kind = _canon_str(str(alert.get("event_kind") or alert.get("kind") or ""))
    title = _canon_str(str(alert.get("title") or ""))
    url = str(alert.get("first_url") or alert.get("url") or "")
    urlc = _canon_url(url) if url else ""
    d = _alert_date(alert)
    return "|".join([issuer, kind, title, urlc, d])


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    # 1) Load alerts
    alerts = load_alerts(args.signals_dir)
    if not alerts:
        print(f"[alert_engine] No alerts found in {args.signals_dir} (nothing to do).")
        return 0

    # 2) Console
    printed = print_console(alerts)
    print(f"[alert_engine] Printed {printed} alert(s).")

    # 3) CSV (optional)
    if args.csv:
        nrows = write_csv(alerts, args.alerts_csv)
        print(f"[alert_engine] Appended {nrows} row(s) to {args.alerts_csv}.")

    # 4) Sinks
    sinks = _build_enabled_sinks(args)
    _preflight_live_or_die(args, sinks)

    any_errors = False
    if sinks:
        if args.sinks_live:
            print("[alert_engine] LIVE sink mode enabled.")
        else:
            print("[alert_engine] DRY-RUN sink mode (no network).")

        seen: set[str] = set()
        dedupe_enabled = not args.sink_dedupe_disable and os.getenv("SINK_DEDUPE_DISABLE", "0") not in ("1", "true", "yes", "on")

        for a in alerts:
            key = _make_sink_dedupe_key(a) if dedupe_enabled else None
            is_dup = key in seen if key else False
            if key:
                seen.add(key)

            for s in sinks:
                try:
                    if is_dup:
                        # Count as skipped for each sink
                        if hasattr(s, "metrics"):
                            s.metrics.skipped += 1
                        continue
                    ok = s.emit(a)
                    if not ok:
                        # Not necessarily an error; sink tracks metrics
                        pass
                except Exception as e:
                    any_errors = True
                    if hasattr(s, "metrics"):
                        s.metrics.errors += 1
                    print(f"[{getattr(s, 'name', 'sink')}] ERROR: {e}")
        for s in sinks:
            try:
                s.flush()
            except Exception as e:
                any_errors = True
                print(f"[{getattr(s, 'name', 'sink')}] flush ERROR: {e}")

        print("\n=== Sink metrics ===")
        for s in sinks:
            m = s.metrics
            print(f"{s.name}: attempted={m.attempted} sent={m.sent} skipped={m.skipped} errors={m.errors}")
            if m.errors > 0:
                any_errors = True

    if any_errors and args.fail_on_sink_error:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
