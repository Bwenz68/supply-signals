# data_ingest/pr_feed_cli.py
"""
PR RSS/Atom ingest using feedparser.

Writes raw PR events to queue/raw_events/*.jsonl in a Phase-0 compatible, tolerant shape.
Does NOT require network for verification (works with file:// URLs).

Usage examples:
  python -m data_ingest.pr_feed_cli --url file://$PWD/tests/fixtures/pr_sample.xml --issuer-name "Contoso Energy"
  PR_FEED_URL=file://$PWD/tests/fixtures/pr_sample.xml python -m data_ingest.pr_feed_cli --limit 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import feedparser  # type: ignore
except Exception as e:
    print("ERROR: feedparser is required. Install with: pip install feedparser", file=sys.stderr)
    raise

# Queue dir (Phase-0 path)
RAW_QUEUE_DIR = os.getenv("RAW_QUEUE_DIR", "queue/raw_events")


def _pick_datetime_iso(entry: Any) -> Optional[str]:
    """
    Pick an ISO8601-ish string from feed entry (published/updated).
    We keep it tolerant; normalize_enrich will harden later.
    """
    # feedparser builds time tuples for *_parsed
    for key in ("published", "updated"):
        v = entry.get(key)
        if v:
            return str(v)
    # Fallback to struct_time -> UTC-ish ISO (best-effort)
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            try:
                # Format as UTC-like string; normalize_enrich can parse it
                return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)
            except Exception:
                pass
    return None


def _entry_to_raw(entry: Any, feed_title: str, issuer_name_opt: Optional[str]) -> Dict[str, Any]:
    title = entry.get("title") or "(no title)"
    link = entry.get("link") or entry.get("id") or ""
    summary = entry.get("summary") or entry.get("description") or ""
    dt = _pick_datetime_iso(entry)

    # Minimal, tolerant raw shape; downstream will normalize
    raw = {
        "source": "pr_feed",                # used by dedupe key
        "event_kind": "press_release",      # helps signal stage
        "title": title,
        "first_url": link,
        "event_datetime": dt,               # normalizer turns into event_datetime_utc
        "summary": summary,
        "feed_title": feed_title,
    }
    if issuer_name_opt:
        raw["issuer_name"] = issuer_name_opt  # helps watchlist mapping

    return raw


def _write_jsonl(rows: List[Dict[str, Any]], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(rows)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m data_ingest.pr_feed_cli",
        description="Parse a PR RSS/Atom feed and write raw events to queue/raw_events/*.jsonl",
    )
    p.add_argument("--url", default=os.getenv("PR_FEED_URL"), help="RSS/Atom URL (or file:// path). Env: PR_FEED_URL")
    p.add_argument("--limit", type=int, default=50, help="Max entries to emit (default 50)")
    p.add_argument("--issuer-name", default=os.getenv("PR_ISSUER_NAME"), help="Optional issuer_name hint for mapping")
    p.add_argument("--out", default=None, help="Optional explicit output path; otherwise auto-named in queue/raw_events")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.url:
        print("ERROR: --url or PR_FEED_URL is required", file=sys.stderr)
        return 2

    parsed = feedparser.parse(args.url)
    if parsed.bozo:
        print(f"ERROR: feed parse error: {parsed.bozo_exception}", file=sys.stderr)
        return 2

    feed_title = (parsed.feed.get("title") if hasattr(parsed, "feed") else None) or ""
    entries = parsed.entries or []

    rows: List[Dict[str, Any]] = []
    for e in entries[: args.limit]:
        rows.append(_entry_to_raw(e, feed_title, args.issuer_name))

    if not rows:
        print("[pr_feed] No entries found (nothing written).")
        return 0

    if args.out:
        out_path = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d-%H%M%S")
        out_path = Path(RAW_QUEUE_DIR) / f"pr_{ts}.jsonl"

    n = _write_jsonl(rows, out_path)
    print(f"[pr_feed] Wrote {n} raw row(s) to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
