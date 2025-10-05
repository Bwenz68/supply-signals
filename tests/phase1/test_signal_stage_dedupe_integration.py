import json
import os
import sys
from pathlib import Path

def test_signal_detect_dedupes_on_second_run(tmp_path, monkeypatch):
    """
    Run signal_detect twice against the same normalized file and
    assert first run emits >0 lines, second emits 0 due to dedupe.
    Uses a temporary working directory so .state stays isolated.
    """
    # Work inside a temp CWD so .state lives here
    monkeypatch.chdir(tmp_path)

    # Create temp queue dirs under this CWD
    norm_dir = Path("queue/normalized_events")
    sig_dir = Path("queue/signals")
    norm_dir.mkdir(parents=True)
    sig_dir.mkdir(parents=True)
    Path(".state").mkdir(exist_ok=True)

    # Point the stage to our temp dirs and ensure dedupe is enabled (set BEFORE import)
    monkeypatch.setenv("NORM_QUEUE_DIR", str(norm_dir))
    monkeypatch.setenv("SIG_QUEUE_DIR", str(sig_dir))
    monkeypatch.delenv("DEDUPE_DISABLE", raising=False)
    monkeypatch.setenv("DEDUPE_TTL_DAYS", "7")

    # Now import the CLI module (reads env at runtime in main(), but import-time is now safe too)
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from signal_detect import __main__ as sd_main  # noqa

    # Build one normalized input with two items that should score >= 3 by rules
    events = [
        {
            "source": "Reuters",
            "title": "Company announces share repurchase program",
            "urls": ["https://example.com/a"],
            "event_datetime_utc": "2025-10-04T00:00:00Z",
            "event_kind": "PR",
            "event_subtype": "PR",
            "body": ""
        },
        {
            "source": "Reuters",
            "title": "Company raises guidance for FY2025",
            "urls": ["https://example.com/b"],
            "event_datetime_utc": "2025-10-04T00:00:00Z",
            "event_kind": "PR",
            "event_subtype": "PR",
            "body": ""
        },
    ]
    in_fp = norm_dir / "test.norm.jsonl"
    with in_fp.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    # Run 1
    argv_saved = sys.argv[:]
    sys.argv = ["signal_detect", "--threshold", "3"]
    try:
        sd_main.main()
    finally:
        sys.argv = argv_saved

    out_fp = sig_dir / "test.signals.jsonl"
    assert out_fp.exists(), "Signals file should be created on first run"
    lines1 = out_fp.read_text(encoding="utf-8").splitlines()
    assert len(lines1) == 2, f"Expected 2 signals on first run, got {len(lines1)}"

    # Run 2 (same input; should be fully deduped)
    sys.argv = ["signal_detect", "--threshold", "3"]
    try:
        sd_main.main()
    finally:
        sys.argv = argv_saved

    lines2 = out_fp.read_text(encoding="utf-8").splitlines()
    assert len(lines2) == 0, f"Expected 0 signals on second run, got {len(lines2)}"
