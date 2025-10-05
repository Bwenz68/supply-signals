# data_ingest/sec_edgar_cli.py
"""
SEC EDGAR Atom ingest (lightweight).

- Parses SEC Atom feeds (or any Atom file) using feedparser.
- Supports offline verification via file:// URLs.
- Writes Phase-0-compatible raw rows to queue/raw_events/*.jsonl.
- LIVE mode: polite headers + conditional requests with ETag/Last-Modified caching.
- C.5: Paging via <link rel="next">, honoring --pages-max and global --max.

Examples (offline):
  python -m data_ingest.sec_edgar_cli \
    --cik 9876543 \
    --forms 8-K,10-Q \
    --url "file://$PWD/tests/fixtures/sec_atom_sample.xml" \
    --issuer-name "Contoso Energy"

Live (when ready):
  SEC_USER_AGENT="Your Name <you@example.com> <+1-555-0100>" \
  python -m data_ingest.sec_edgar_cli --cik 0000320193 --forms 8-K --max 20
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urljoin

try:
    import feedparser  # type: ignore
except Exception:
    print("ERROR: feedparser is required. Install with: pip install feedparser", file=sys.stderr)
    raise

RAW_QUEUE_DIR = os.getenv("RAW_QUEUE_DIR", "queue/raw_events")
DEFAULT_CACHE_FILE = os.getenv("EDGAR_CACHE_FILE", ".state/edgar_cache.json")


# ---------- small utils ----------

def _zero_pad_cik(cik: str | int) -> str:
    s = re.sub(r"\D", "", str(cik))
    return s.zfill(10) if s else ""

def _csvish_list(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [t.strip() for t in s.replace(";", ",").split(",") if t.strip()]

def _parse_iso(s: str) -> Optional[datetime]:
    try:
        if len(s) == 10:
            return datetime.fromisoformat(s)
        s2 = s[:-1] if s.endswith("Z") else s
        return datetime.fromisoformat(s2)
    except Exception:
        return None

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

def _form_type(entry: Any) -> str:
    cats = entry.get("tags") or entry.get("categories") or entry.get("category")
    if isinstance(cats, list):
        for c in cats:
            term = c.get("term")
            if term:
                return str(term)
    title = entry.get("title") or ""
    m = re.match(r"([0-9A-Za-z\-]+)\s*[-–—]\s*", str(title))
    return m.group(1) if m else ""

def _entry_to_raw(entry: Any, issuer_name: Optional[str], cik_10: Optional[str]) -> Dict[str, Any]:
    title = entry.get("title") or "(no title)"
    link = _first_link(entry)
    dt = _pick_iso(entry)
    form = _form_type(entry)
    summary = entry.get("summary") or entry.get("summary_detail", {}).get("value") or ""

    raw = {
        "source": "sec_edgar",
        "event_kind": "sec_filing",
        "title": title,
        "first_url": link,
        "event_datetime": dt,  # tolerant; normalizer will harden
        "summary": summary,
        "form_type": form,
    }
    if issuer_name:
        raw["issuer_name"] = issuer_name
    if cik_10:
        raw["cik"] = cik_10
    return raw


# ---------- cache helpers ----------

def _is_http(url: str) -> bool:
    sch = urlparse(url).scheme.lower()
    return sch in ("http", "https")

def _load_cache(path: str) -> Dict[str, Dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def _save_cache(path: str, cache: Dict[str, Dict[str, str]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def _compose_conditional_headers(url: str, cache: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    if not _is_http(url):
        return {}
    rec = cache.get(url) or {}
    h: Dict[str, str] = {}
    if rec.get("etag"):
        h["If-None-Match"] = rec["etag"]
    if rec.get("last_modified"):
        h["If-Modified-Since"] = rec["last_modified"]
    return h

def _extract_http_metadata(parsed: Any) -> Dict[str, Optional[str]]:
    etag = getattr(parsed, "etag", None) or (parsed.get("etag") if isinstance(parsed, dict) else None)
    last_mod = getattr(parsed, "modified", None) or (parsed.get("modified") if isinstance(parsed, dict) else None)
    status = getattr(parsed, "status", None) or (parsed.get("status") if isinstance(parsed, dict) else None)
    headers = getattr(parsed, "headers", None) or (parsed.get("headers") if isinstance(parsed, dict) else None)
    ctype = None
    if headers and isinstance(headers, dict):
        ctype = headers.get("content-type") or headers.get("Content-Type")
    return {"etag": etag, "last_modified": last_mod, "status": str(status) if status is not None else None, "content_type": ctype}

def _update_cache_from_parsed(url: str, parsed: Any, cache: Dict[str, Dict[str, str]], *, now_ts: str) -> None:
    meta = _extract_http_metadata(parsed)
    etag = meta.get("etag")
    lm = meta.get("last_modified")
    if not (etag or lm):
        return
    rec = cache.get(url, {})
    if etag:
        rec["etag"] = etag
    if lm:
        rec["last_modified"] = lm
    rec["fetched"] = now_ts
    cache[url] = rec


# ---------- paging helpers ----------

def _find_next_link(parsed: Any, base_url: str) -> Optional[str]:
    """
    Look for feed-level <link rel="next" href="..."> and resolve relative hrefs.
    """
    try:
        feed = parsed.feed if hasattr(parsed, "feed") else None
        links = (feed.get("links") if isinstance(feed, dict) else getattr(feed, "links", None)) or []
        for l in links:
            if (l.get("rel") or "").lower() == "next":
                href = l.get("href")
                if href:
                    return urljoin(base_url, href)
    except Exception:
        pass
    return None


# ---------- CLI args ----------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m data_ingest.sec_edgar_cli",
        description="Parse SEC Atom feed and write raw filings to queue/raw_events/*.jsonl",
    )
    p.add_argument("--cik", help="Company CIK (digits; zero-padded to 10). Required in live mode.")
    p.add_argument("--forms", help="Comma/semicolon separated form types (e.g., '8-K,10-Q').")
    p.add_argument("--max", type=int, default=50, help="Max entries to emit across all pages (default 50).")
    p.add_argument("--pages-max", type=int, default=3, help="Max pages to follow via rel=next (default 3).")
    p.add_argument("--since", help="ISO date/time threshold; only entries with updated/published >= this are kept.")
    p.add_argument("--issuer-name", help="Optional issuer name hint (assists mapping/watchlist).")
    p.add_argument("--url", help="Override URL (including file:// path). If omitted in live mode, we derive from CIK.")
    p.add_argument("--out", help="Explicit output path; otherwise auto-named in queue/raw_events.")
    # Polite live-mode knobs
    p.add_argument("--rate-per-min", type=float, default=float(os.getenv("SEC_RATE_PER_MIN", "6")), help="Throttle HTTP page requests/min (default 6).")
    p.add_argument("--user-agent", default=os.getenv("SEC_USER_AGENT"), help="SEC User-Agent (Name <email> <phone>). Env: SEC_USER_AGENT")
    # Cache
    p.add_argument("--cache-file", default=DEFAULT_CACHE_FILE, help=f"Cache file path for ETag/Last-Modified (default {DEFAULT_CACHE_FILE})")
    p.add_argument("--no-cache", action="store_true", help="Disable conditional requests & cache updates.")
    p.add_argument("--debug-headers", action="store_true", help="Print request and response header metadata.")
    # Escape hatch
    p.add_argument("--allow-missing-ua", action="store_true", help="Allow HTTP requests without SEC_USER_AGENT (not recommended).")
    return p.parse_args(argv)


# ---------- URL derivation ----------

def _derive_url(cik_10: str, forms: List[str]) -> str:
    base = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
    type_q = ""
    if forms and len(forms) == 1:
        type_q = f"&type={forms[0]}"
    return f"{base}&CIK={cik_10}{type_q}&count=100&owner=exclude&output=atom"


# ---------- main ----------

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    forms = _csvish_list(args.forms)
    cik_10 = _zero_pad_cik(args.cik) if args.cik else None

    # Determine URL
    first_url = args.url
    if not first_url:
        if not cik_10:
            print("ERROR: --cik or --url is required.", file=sys.stderr)
            return 2
        first_url = _derive_url(cik_10, forms)

    # Build user-agent & cache
    request_headers_base: Dict[str, str] = {}
    cache: Dict[str, Dict[str, str]] = {}
    if _is_http(first_url):
        ua = args.user_agent or os.getenv("SEC_USER_AGENT")
        if not ua and not args.allow_missing_ua:
            print(
                "ERROR: SEC_USER_AGENT is required for HTTP SEC endpoints.\n"
                "Set it like:\n"
                "  export SEC_USER_AGENT=\"Your Name <you@example.com> <+1-555-0100>\"\n"
                "Or pass --user-agent. (Use --allow-missing-ua to bypass, not recommended.)",
                file=sys.stderr,
            )
            return 2
        if ua:
            request_headers_base["User-Agent"] = ua
        if not args.no_cache:
            cache = _load_cache(args.cache_file)

    # Paging loop
    total_rows: List[Dict[str, Any]] = []
    pages_seen = 0
    next_url: Optional[str] = first_url
    min_interval = (60.0 / args.rate_per_min) if (args.rate_per_min and args.rate_per_min > 0) else 0.0
    last_req_time = 0.0

    while next_url and pages_seen < args.pages_max and len(total_rows) < args.max:
        page_url = next_url
        pages_seen += 1

        # Polite throttle for HTTP pages only
        if _is_http(page_url) and min_interval > 0 and last_req_time > 0:
            now = time.monotonic()
            sleep_s = (last_req_time + min_interval) - now
            if sleep_s > 0:
                time.sleep(sleep_s)

        request_headers = dict(request_headers_base)
        if not args.no_cache:
            cond = _compose_conditional_headers(page_url, cache)
            request_headers.update(cond)
            if args.debug_headers and cond:
                print(f"[sec_edgar] Request conditional headers ({page_url}): {cond}")

        parsed = feedparser.parse(page_url, request_headers=request_headers)

        if args.debug_headers and _is_http(page_url):
            meta = _extract_http_metadata(parsed)
            print(f"[sec_edgar] Response meta ({page_url}): {meta}")

        status = getattr(parsed, "status", None) or (parsed.get("status") if isinstance(parsed, dict) else None)
        if status == 304:
            # No change for this page URL; move to next only if link exists (rare)
            next_url = _find_next_link(parsed, page_url)
            last_req_time = time.monotonic()
            continue

        if parsed.bozo:
            # Helpful diagnostics
            meta = _extract_http_metadata(parsed) if _is_http(page_url) else {}
            ctype = meta.get("content_type")
            hint = ""
            if _is_http(page_url):
                if not (args.user_agent or os.getenv("SEC_USER_AGENT")) and not args.allow_missing_ua:
                    hint = "Likely missing SEC_USER_AGENT; set a descriptive User-Agent."
                elif ctype and "html" in str(ctype).lower():
                    hint = "Server returned HTML (probably an error page). Check SEC_USER_AGENT and rate limits."
            print(f"ERROR: feed parse error: {parsed.bozo_exception}", file=sys.stderr)
            if hint:
                print(f"HINT: {hint}", file=sys.stderr)
            return 2

        # Filter/collect entries (respect global --max)
        entries = parsed.entries or []
        if forms:
            formset = set(forms)
            entries = [e for e in entries if _form_type(e) in formset]

        if args.since:
            t0 = _parse_iso(args.since)
            if t0:
                kept = []
                for e in entries:
                    ts = _pick_iso(e)
                    dt = _parse_iso(ts) if ts else None
                    if dt and dt >= t0:
                        kept.append(e)
                entries = kept

        for e in entries:
            if len(total_rows) >= args.max:
                break
            total_rows.append(_entry_to_raw(e, args.issuer_name, cik_10))

        # Cache update for this page
        if _is_http(page_url) and not args.no_cache:
            try:
                _update_cache_from_parsed(page_url, parsed, cache, now_ts=time.strftime("%Y-%m-%dT%H:%M:%SZ"))
            except Exception:
                pass

        # Find next page URL
        next_found = _find_next_link(parsed, page_url)
        next_url = next_found
        last_req_time = time.monotonic()

        if not next_url:
            break

    # Save cache once at end
    if not args.no_cache and cache:
        try:
            _save_cache(args.cache_file, cache)
        except Exception:
            pass

    if not total_rows:
        print("[sec_edgar] No entries matched filters (nothing written).")
        return 0

    # Output
    if args.out:
        out_path = Path(args.out)
    else:
        ts = time.strftime("%Y%m%d-%H%M%S")
        suffix = f"{cik_10 or 'unknown'}"
        out_path = Path(RAW_QUEUE_DIR) / f"sec_{suffix}_{ts}.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in total_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[sec_edgar] Wrote {len(total_rows)} raw row(s) to {out_path}")
    if pages_seen > 1:
        print(f"[sec_edgar] Pages fetched: {pages_seen}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
