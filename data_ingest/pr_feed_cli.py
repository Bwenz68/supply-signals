# data_ingest/pr_feed_cli.py
"""
Press Release (RSS/Atom) single-feed ingester.

- Parses a single RSS/Atom feed (supports file:// for offline tests).
- Writes Phase-0-compatible raw rows to queue/raw_events/pr_*.jsonl
- Caching: ETag/Last-Modified via shared/http_cache.py (HTTP only).
- Retry/backoff for transient HTTP errors.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    import feedparser  # type: ignore
except Exception:
    print("ERROR: feedparser is required. Install with: pip install feedparser", file=sys.stderr)
    raise

from shared.http_cache import (
    is_http,
    load_cache,
    save_cache,
    compose_conditional_headers,
    extract_http_metadata,
    update_cache_from_parsed,
)

RAW_QUEUE_DIR = os.getenv("RAW_QUEUE_DIR", "queue/raw_events")
DEFAULT_CACHE_FILE = os.getenv("PR_CACHE_FILE", ".state/pr_cache.json")

def _normalize_file_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return u
    p = urlparse(u)
    if p.scheme.lower() != "file":
        return u
    if p.netloc:
        path = p.path
        if not path.startswith("/"):
            path = "/" + path
        return f"file:///{p.netloc}{path}"
    if not u.startswith("file:///"):
        return "file:///" + u[len("file://"):]
    return u

def _pick_iso(entry: Any) -> Optional[str]:
    # common keys across RSS/Atom
    for key in ("updated", "published"):
        v = entry.get(key)
        if v:
            return str(v)
    for key in ("updated_parsed", "published_parsed"):
        t = entry.get(key)
        if t:
            try:
                return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)
            except Exception:
                pass
    return None

def _first_link(entry: Any) -> str:
    if entry.get("link"):
        return str(entry["link"])
    links = entry.get("links") or []
    for l in links:
        href = l.get("href")
        if href:
            return str(href)
    return ""

def _entry_to_raw(entry: Any, issuer_name: Optional[str]) -> Dict[str, Any]:
    title = entry.get("title") or "(no title)"
    link = _first_link(entry)
    dt = _pick_iso(entry)
    summary = entry.get("summary") or entry.get("summary_detail", {}).get("value") or ""
    raw = {
        "source": "press_release",
        "event_kind": "press_release",
        "title": title,
        "first_url": link,
        "event_datetime": dt,
        "summary": summary,
    }
    if issuer_name:
        raw["issuer_name"] = issuer_name
    return raw

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m data_ingest.pr_feed_cli",
                                description="Parse one PR RSS/Atom feed and write raw rows.")
    p.add_argument("--url", required=True, help="Feed URL (supports file:// for offline).")
    p.add_argument("--issuer-name", help="Optional issuer name hint to attach to rows.")
    p.add_argument("--max", type=int, default=100, help="Max entries to emit (default 100).")
    p.add_argument("--out", help="Explicit output path; otherwise auto-named in queue/raw_events/pr_*.jsonl")
    # Cache / HTTP controls
    p.add_argument("--cache-file", default=DEFAULT_CACHE_FILE, help=f"ETag/Last-Modified cache (default {DEFAULT_CACHE_FILE})")
    p.add_argument("--no-cache", action="store_true", help="Disable HTTP conditional requests & cache updates.")
    p.add_argument("--debug-headers", action="store_true", help="Print request/response metadata (HTTP only).")
    p.add_argument("--rate-per-min", type=float, default=float(os.getenv("PR_RATE_PER_MIN", "30")), help="Max HTTP fetches per minute (default 30).")
    p.add_argument("--retries", type=int, default=2, help="Retries on HTTP parse errors (default 2).")
    p.add_argument("--backoff", type=float, default=1.7, help="Exponential backoff multiplier (default 1.7).")
    return p.parse_args(argv)

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    url = _normalize_file_url(args.url)

    cache: Dict[str, Dict[str, str]] = {}
    if is_http(url) and not args.no_cache:
        cache = load_cache(args.cache_file)

    # polite throttling for HTTP
    min_interval = (60.0 / args.rate_per_min) if (args.rate_per_min and args.rate_per_min > 0) else 0.0
    last_req_time = 0.0

    # retry loop for HTTP (no need for file://)
    attempt = 0
    delay = 0.5
    while True:
        if is_http(url) and min_interval > 0 and last_req_time > 0:
            remaining = (last_req_time + min_interval) - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

        request_headers: Dict[str, str] = {}
        if is_http(url) and not args.no_cache:
            cond = compose_conditional_headers(url, cache)
            request_headers.update(cond)
            if args.debug_headers and cond:
                print(f"[pr_feed] Request conditional headers ({url}): {cond}")

        parsed = feedparser.parse(url, request_headers=request_headers)

        if args.debug_headers and is_http(url):
            meta = extract_http_metadata(parsed)
            print(f"[pr_feed] Response meta ({url}): {meta}")

        # success path or 304
        if not getattr(parsed, "bozo", 0):
            break

        # if file:// bozo => real error (bad file path)
        if not is_http(url):
            print(f"ERROR: feed parse error: {parsed.bozo_exception}", file=sys.stderr)
            return 2

        # HTTP bozo: retry a couple of times
        attempt += 1
        if attempt > max(0, args.retries):
            print(f"ERROR: feed parse error after retries: {parsed.bozo_exception}", file=sys.stderr)
            return 2
        time.sleep(delay)
        delay *= max(1.0, args.backoff)

    last_req_time = time.monotonic()

    entries = parsed.entries or []
    rows: List[Dict[str, Any]] = []
    for e in entries:
        if len(rows) >= args.max:
            break
        rows.append(_entry_to_raw(e, args.issuer_name))

    # Cache update (HTTP only)
    if is_http(url) and not args.no_cache:
        try:
            update_cache_from_parsed(url, parsed, cache, now_ts=time.strftime("%Y-%m-%dT%H:%M:%SZ"))
            save_cache(args.cache_file, cache)
        except Exception:
            pass

    if not rows:
        print("[pr_feed] No entries parsed (nothing written).")
        return 0

    # Output path
    if args.out:
        out_path = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d-%H%M%S")
        suffix = (args.issuer_name or "unknown").replace(" ", "_")
        out_path = Path(RAW_QUEUE_DIR) / f"pr_{suffix}_{ts}.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[pr_feed] Wrote {len(rows)} raw row(s) to {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
