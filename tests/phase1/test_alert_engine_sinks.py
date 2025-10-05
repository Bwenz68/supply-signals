# tests/phase1/test_alert_engine_sinks.py
from __future__ import annotations

import os
import io
import sys
import tempfile
from pathlib import Path

import pytest

# We import the alert_engine main entry
from alert_engine.__main__ import main as alert_main


def _write_signals(tmp: Path, rows: list[dict]) -> str:
    sigdir = tmp / "queue" / "signals"
    sigdir.mkdir(parents=True, exist_ok=True)
    fp = sigdir / "sample.signals.jsonl"
    with fp.open("w", encoding="utf-8") as f:
        for r in rows:
            import json
            f.write(json.dumps(r) + "\n")
    return str(sigdir)


def _caprun(argv):
    """Run alert_main(argv) capturing stdout/stderr and returning (code, out)."""
    old_out, old_err = sys.stdout, sys.stderr
    buf = io.StringIO()
    sys.stdout = buf
    sys.stderr = buf
    try:
        rc = 0
        try:
            alert_main(argv)
        except SystemExit as e:
            rc = int(e.code or 0)
        return rc, buf.getvalue()
    finally:
        sys.stdout = old_out
        sys.stderr = old_err


def test_live_preflight_exit_codes():
    rows = [
        {
            "issuer_name":"Contoso Energy",
            "event_kind":"sec_filing",
            "title":"Form 8-K",
            "first_url":"https://example.com/x",
            "event_datetime_utc":"2025-10-05T12:34:56Z",
        }
    ]
    with tempfile.TemporaryDirectory() as td:
        sigdir = _write_signals(Path(td), rows)
        # Slack: missing webhook -> exit 2
        rc, out = _caprun(["--signals-dir", sigdir, "--slack", "--sinks-live"])
        assert rc == 2
        assert "LIVE mode preflight failed" in out

        # SMTP: missing host/from/to -> exit 2
        rc, out = _caprun(["--signals-dir", sigdir, "--smtp", "--sinks-live"])
        assert rc == 2
        assert "SMTP: missing SMTP_HOST" in out


def test_dry_run_dedupe_sends_once_and_skips_rest():
    rows = [
        {
            "issuer_name":"Contoso Energy",
            "event_kind":"press_release",
            "title":"Capacity expansion",
            "first_url":"https://example.com/pr",
            "event_datetime_utc":"2025-10-05T12:00:00Z",
        },
        # Intentional duplicate in same run
        {
            "issuer_name":"Contoso Energy",
            "event_kind":"press_release",
            "title":"Capacity expansion",
            "first_url":"https://example.com/pr",
            "event_datetime_utc":"2025-10-05T12:00:00Z",
        }
    ]
    with tempfile.TemporaryDirectory() as td:
        sigdir = _write_signals(Path(td), rows)
        # Provide minimal config so sinks are enabled in dry-run but won't preflight-fail
        rc, out = _caprun([
            "--signals-dir", sigdir,
            "--slack", "--slack-webhook", "https://hooks.slack.example/ABC",
            "--smtp", "--smtp-from", "a@b", "--smtp-to", "x@y",
        ])
        assert rc == 0
        # Expect exactly one DRY-RUN Slack block and one SMTP DRY-RUN, plus skipped=1 in metrics
        assert "[Slack][DRY-RUN] Would POST" in out
        assert "[SMTP][DRY-RUN] Would send email" in out
        # Metrics should reflect 1 sent, 1 skipped per sink
        lines = [ln.strip() for ln in out.splitlines() if ln.strip().startswith(("slack:", "smtp:"))]
        # Example: slack: attempted=1 sent=1 skipped=1 errors=0
        assert any("slack:" in ln and "sent=1" in ln and "skipped=1" in ln for ln in lines)
        assert any("smtp:" in ln and "sent=1" in ln and "skipped=1" in ln for ln in lines)
