# data_ingest/pr_feeds_cli.py
"""
Multi-source PR RSS/Atom ingest using feedparser.

- Accepts multiple feeds via --urls or --feeds-file (TSV or JSONL lines).
- Optional per-feed issuer_name and tag overrides.
- Writes raw PR events to queue/raw_events/pr_multi_*.jsonl (Phase-0 compatible).
- Includes simple retry/backoff and optional rate-limit between feeds.

Examples (offline fixtures):
  python -m data_ingest.pr_feeds_cli \
    --urls "file://$PWD/tests/fixtures/pr_sample.xml;file://$PWD/tests/fixtures/pr_sample_b.xml" \
    --issuer-name "Contoso Energy" \
    --tag demo

Using a feeds file (TSV: url [TAB] issuer_name [TAB] tag):
  python -m data_ingest.pr_feeds_cli --feeds-file ref/feeds.tsv
  # or JSONL lines: {"url": "...", "issuer_name": "...", "tag": "..."}

Notes:
- This CLI is additive; pr_feed_cli (single source) remains available.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import feedparser  # type: ignore
except Exception:
    print("ERROR: feedparser is required. Install with: pip install feedparser", file=sys.stderr)
    raise


RAW_QUEUE_DIR = os.getenv("RAW_QUEUE_DIR", "queue/raw_events")


# ------------------------ Models ------------------------

@dataclass
class FeedSpec:
    url: str
    issuer_name: Optional[str] = None
    tag: Optional[str] = None


# ------------------------ Utilities ------------------------

def _split_list(s: Optional[str]) -> List[str]:
    if not s:
        return []
    # allow comma/semicolon separated
    parts = []
    for chunk in s.split(";"):
        for sub in chunk.split(","):
            t = sub.strip()
            if t:
                parts.append(t)
    return parts

def _load_feeds_file(path: str) -> List[FeedSpec]:
    """
    Supports:
    - TSV: url [TAB] issuer_name [TAB] tag  (issuer_name/tag optional)
    - JSONL: {"url": "...", "issuer_name": "...", "tag": "..."}
    - Plain text: url per line
    Lines starting with '#' are comments.
    """
    specs: List[FeedSpec] = []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"feeds file not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("{"):
                try:
                    obj = json.loads(line)
                    url = obj.get("url")
                    if not url:
                        continue
                    specs.append(FeedSpec(url=str(url).strip(),
                                          issuer_name=obj.get("issuer_name"),
                                          tag=obj.get("tag")))
                except Exception as e:
                    print(f"[pr_feeds] WARN {path}:{ln} invalid JSONL: {e}")
                continue
            # Try TSV first
            parts = line.split("\t")
            if len(parts) >= 1:
                url = parts[0].strip()
                if not url:
                    continue
                issuer = parts[1].strip() if len(parts) >= 2 and parts[1].strip() else None
                tag = parts[2].strip() if len(parts) >= 3 and parts[2].strip() else None
                specs.append(FeedSpec(url=url, issuer_name=issuer, tag=tag))
            else:
                # Fallback plain URL
                specs.append(FeedSpec(url=line))
    return specs


def _pick_datetime_iso(entry: Any) -> Optional[str]:
    # Prefer published/updated string; fallback to parsed tuples
    for key in ("published", "updated"):
        v = entry.get(key)
        if v:
            return str(v)
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            try:
                return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)
            except Exception:
                pass
    return None


def _entry_to_raw(entry: Any, feed_title: str, issuer_name_opt: Optional[str], tag_opt: Optional[str]) -> Dict[str, Any]:
    title = entry.get("title") or "(no title)"
    link = entry.get("link") or entry.get("id") or ""
    summary = entry.get("summary") or entry.get("description") or ""
    dt = _pick_datetime_iso(entry)

    raw = {
        "source": "pr_feed",
        "event_kind": "press_release",
        "title": title,
        "first_url": link,
        "event_datetime": dt,
        "summary": summary,
        "feed_title": feed_title,
    }
    if issuer_name_opt:
        raw["issuer_name"] = issuer_name_opt
    if tag_opt:
        raw["source_tag"] = tag_opt
    return raw


def _parse_one(url: str, *, request_headers: Dict[str, str], retries: int, backoff_base: float) -> Any:
    attempts = max(0, retries) + 1
    for i in range(1, attempts + 1):
        parsed = feedparser.parse(url, request_headers=request_headers)
        status = getattr(parsed, "status", None)
        bozo = bool(getattr(parsed, "bozo", False))
        # Accept if no bozo and not a 5xx
        if (status is None or (200 <= int(status) < 500)) and not bozo:
            return parsed
        # Retry on bozo or 5xx
        if i < attempts:
            sleep_s = backoff_base * (2 ** (i - 1))
            time.sleep(sleep_s)
            continue
        return parsed


# ------------------------ CLI ------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m data_ingest.pr_feeds_cli",
        description="Parse multiple PR RSS/Atom feeds and write raw events to queue/raw_events/*.jsonl",
    )
    p.add_argument("--urls", help="Comma/semicolon separated list of feed URLs (supports file://)")
    p.add_argument("--feeds-file", help="Path to feeds list (TSV/JSONL/plain lines). Each line: url [TAB] issuer [TAB] tag")
    p.add_argument("--issuer-name", help="Global issuer_name fallback (used when a feed does not specify one)")
    p.add_argument("--tag", help="Global source_tag fallback (used when a feed does not specify one)")
    p.add_argument("--limit-per-feed", type=int, default=50, help="Max entries to emit per feed (default 50)")
    p.add_argument("--user-agent", default=os.getenv("PR_USER_AGENT"), help="Optional User-Agent for HTTP PR feeds (env PR_USER_AGENT)")
    p.add_argument("--retries", type=int, default=2, help="Retries per feed on transient errors (default 2)")
    p.add_argument("--retry-backoff", type=float, default=0.5, help="Backoff base seconds (default 0.5)")
    p.add_argument("--rate-per-sec", type=float, default=0.0, help="Throttle between feeds (posts per second). 0=disabled")
    p.add_argument("--out", help="Explicit output path; otherwise auto-named under queue/raw_events")
    return p.parse_args(argv)


def _gather_specs(args: argparse.Namespace) -> List[FeedSpec]:
    specs: List[FeedSpec] = []
    if args.urls:
        for u in _split_list(args.urls):
            specs.append(FeedSpec(url=u))
    if args.feeds_file:
        specs.extend(_load_feeds_file(args.feeds_file))
    if not specs:
        raise SystemExit("ERROR: Provide --urls or --feeds-file")
    # Apply global fallbacks where missing
    for s in specs:
        if not s.issuer_name and args.issuer_name:
            s.issuer_name = args.issuer_name
        if not s.tag and args.tag:
            s.tag = args.tag
    return specs


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    specs = _gather_specs(args)

    # Prepare headers and rate limiter
    headers: Dict[str, str] = {}
    if args.user_agent:
        headers["User-Agent"] = args.user_agent

    min_interval = 1.0 / args.rate_per_sec if args.rate_per_sec and args.rate_per_sec > 0 else 0.0
    next_time = 0.0

    rows: List[Dict[str, Any]] = []
    per_feed_counts: List[Tuple[str, int]] = []

    for s in specs:
        # Throttle between feeds if requested
        if min_interval > 0:
            now = time.monotonic()
            if now < next_time:
                time.sleep(next_time - now)
            next_time = max(now, next_time) + min_interval

        parsed = _parse_one(s.url, request_headers=headers, retries=args.retries, backoff_base=args.retry_backoff)

        if getattr(parsed, "bozo", False):
            print(f"[pr_feeds] WARN: parse issue on {s.url}: {getattr(parsed, 'bozo_exception', '')}")

        feed_title = ""
        if hasattr(parsed, "feed"):
            feed_title = parsed.feed.get("title", "") or ""

        entries = (parsed.entries or [])[: args.limit_per_feed]
        cnt = 0
        for e in entries:
            rows.append(_entry_to_raw(e, feed_title, s.issuer_name, s.tag))
            cnt += 1
        per_feed_counts.append((s.url, cnt))

    if not rows:
        print("[pr_feeds] No entries across all feeds (nothing written).")
        return 0

    # Output file
    if args.out:
        out_path = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d-%H%M%S")
        out_path = Path(RAW_QUEUE_DIR) / f"pr_multi_{ts}.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Summary
    print(f"[pr_feeds] Wrote {len(rows)} raw row(s) to {out_path}")
    for url, n in per_feed_counts:
        print(f"[pr_feeds]  - {url}: {n} row(s)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
