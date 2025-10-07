# data_ingest/pr_feeds_cli.py
"""
Press Release multi-feed ingester.

- Accepts --urls "u1;u2;..." or --feeds-file (one URL per line; comments/# allowed).
- Per-run cache (HTTP only): ETag/Last-Modified (shared across feeds).
- Retry/backoff and optional rate limiting across HTTP fetches.
- Writes combined rows to queue/raw_events/pr_multi_*.jsonl unless --out provided.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
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

def _entry_to_raw(entry: Any, issuer_name: Optional[str], tag: Optional[str]) -> Dict[str, Any]:
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
    if tag:
        raw["tag"] = tag
    return raw

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m data_ingest.pr_feeds_cli",
                                description="Parse multiple PR RSS/Atom feeds and write raw rows.")
    p.add_argument("--urls", help="Semicolon-separated list of URLs")
    p.add_argument("--feeds-file", help="File containing one URL per line (comments with #)")
    p.add_argument("--issuer-name", help="Optional issuer name hint for all rows")
    p.add_argument("--tag", help="Optional tag label to attach to all rows")
    p.add_argument("--max", type=int, default=200, help="Max entries to emit across all feeds (default 200)")
    p.add_argument("--out", help="Explicit output path; otherwise auto-named in queue/raw_events/pr_multi_*.jsonl")
    # Cache / HTTP controls
    p.add_argument("--cache-file", default=DEFAULT_CACHE_FILE, help=f"ETag/Last-Modified cache (default {DEFAULT_CACHE_FILE})")
    p.add_argument("--no-cache", action="store_true", help="Disable HTTP conditional requests & cache updates.")
    p.add_argument("--debug-headers", action="store_true", help="Print request/response metadata (HTTP only).")
    p.add_argument("--rate-per-min", type=float, default=float(os.getenv("PR_RATE_PER_MIN", "60")), help="Max HTTP fetches per minute (default 60).")
    p.add_argument("--retries", type=int, default=2, help="Retries on HTTP parse errors per feed (default 2)")
    p.add_argument("--backoff", type=float, default=1.7, help="Exponential backoff multiplier (default 1.7)")
    return p.parse_args(argv)

def _iter_urls(args: argparse.Namespace) -> Iterable[str]:
    urls: List[str] = []
    if args.urls:
        urls.extend([u.strip() for u in args.urls.split(";") if u.strip()])
    if args.feeds_file:
        for line in Path(args.feeds_file).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    if not urls:
        raise SystemExit("ERROR: Provide --urls or --feeds-file")
    return urls

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    cache: Dict[str, Dict[str, str]] = {}
    if not args.no_cache:
        cache = load_cache(args.cache_file)

    rows: List[Dict[str, Any]] = []

    min_interval = (60.0 / args.rate_per_min) if (args.rate_per_min and args.rate_per_min > 0) else 0.0
    last_req_time = 0.0

    for raw_u in _iter_urls(args):
        url = _normalize_file_url(raw_u)

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
                    print(f"[pr_feeds] Request conditional headers ({url}): {cond}")

            parsed = feedparser.parse(url, request_headers=request_headers)

            if args.debug_headers and is_http(url):
                meta = extract_http_metadata(parsed)
                print(f"[pr_feeds] Response meta ({url}): {meta}")

            if not getattr(parsed, "bozo", 0):
                break

            if not is_http(url):
                print(f"[pr_feeds] ERROR parsing file feed: {parsed.bozo_exception}", file=sys.stderr)
                parsed = None
                break

            attempt += 1
            if attempt > max(0, args.retries):
                print(f"[pr_feeds] ERROR after retries: {parsed.bozo_exception}", file=sys.stderr)
                parsed = None
                break
            time.sleep(delay)
            delay *= max(1.0, args.backoff)

        last_req_time = time.monotonic()

        if not parsed:
            continue

        for e in (parsed.entries or []):
            if len(rows) >= args.max:
                break
            rows.append(_entry_to_raw(e, args.issuer_name, args.tag))

        if is_http(url) and not args.no_cache:
            try:
                update_cache_from_parsed(url, parsed, cache, now_ts=time.strftime("%Y-%m-%dT%H%M%SZ"))
            except Exception:
                pass

        # If we hit max, we can stop parsing further feeds
        if len(rows) >= args.max:
            break

    if not args.no_cache:
        try:
            save_cache(args.cache_file, cache)
        except Exception:
            pass

    if not rows:
        print("[pr_feeds] No entries parsed (nothing written).")
        return 0

    if args.out:
        out_path = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d-%H%M%S")
        suffix = (args.tag or args.issuer_name or "multi").replace(" ", "_")
        out_path = Path(RAW_QUEUE_DIR) / f"pr_multi_{suffix}_{ts}.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[pr_feeds] Wrote {len(rows)} raw row(s) to {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
