# shared/http_cache.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

# Simple header cache used by PR & EDGAR ingesters.
# Stores per-URL: etag, last_modified, fetched

def is_http(url: str) -> bool:
    try:
        return urlparse(url).scheme.lower() in ("http", "https")
    except Exception:
        return False

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def save_cache(path: str, cache: Dict[str, Dict[str, str]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def compose_conditional_headers(url: str, cache: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    if not is_http(url):
        return {}
    rec = cache.get(url) or {}
    h: Dict[str, str] = {}
    et = rec.get("etag")
    lm = rec.get("last_modified")
    if et:
        h["If-None-Match"] = et
    if lm:
        h["If-Modified-Since"] = lm
    return h

def extract_http_metadata(parsed: Any) -> Dict[str, Optional[str]]:
    # feedparser returns attributes on the result; be defensive
    etag = getattr(parsed, "etag", None) or (parsed.get("etag") if isinstance(parsed, dict) else None)
    last_mod = getattr(parsed, "modified", None) or (parsed.get("modified") if isinstance(parsed, dict) else None)
    status = getattr(parsed, "status", None) or (parsed.get("status") if isinstance(parsed, dict) else None)
    headers = getattr(parsed, "headers", None) or (parsed.get("headers") if isinstance(parsed, dict) else None)
    ctype = None
    if headers and isinstance(headers, dict):
        ctype = headers.get("content-type") or headers.get("Content-Type")
    return {
        "etag": etag,
        "last_modified": last_mod,
        "status": str(status) if status is not None else None,
        "content_type": ctype,
    }

def update_cache_from_parsed(url: str, parsed: Any, cache: Dict[str, Dict[str, str]], now_ts: str) -> None:
    if not is_http(url):
        return
    meta = extract_http_metadata(parsed)
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
