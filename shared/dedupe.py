# shared/dedupe.py
# Phase-1 MVP dedupe/idempotence helper.
# - Hash key: source | title | first_url | YYYY-MM-DD (UTC)
# - Rolling TTL window (default 7 days).
# - Append-only JSONL state at .state/seen_events.jsonl

from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict, Optional, Tuple, Any
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, unquote

# ---- Canonicalization helpers ------------------------------------------------

_TRACKING_PARAMS = {
    "gclid", "fbclid", "igshid", "ref", "ref_src",
}

def _casefold_trim(s: Optional[str]) -> str:
    if not s:
        return ""
    # Unicode normalize (NFKC) + collapse whitespace + casefold
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.casefold()

def _normalize_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        # Percent-decode once to help normalize later
        url = unquote(url)
        parts = urlsplit(url)
        scheme = (parts.scheme or "").lower()
        netloc = (parts.netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # Drop fragment
        fragment = ""

        # Normalize query: drop utm_* and known trackers
        query_pairs = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            k_low = k.casefold()
            if k_low.startswith("utm_") or k_low in _TRACKING_PARAMS:
                continue
            query_pairs.append((k, v))
        query = urlencode(query_pairs, doseq=True)

        # Normalize path: remove trailing slash if path is "/" only
        path = parts.path or ""
        if path == "/":
            path = ""

        return urlunsplit((scheme, netloc, path, query, fragment))
    except Exception:
        # If anything goes wrong, return a safe fallback that still hashes deterministically
        return url.strip()

def _pick_event_date(event: Dict[str, Any]) -> datetime:
    """
    Choose the datetime source in priority order and return an aware UTC datetime.
    Priority: event_datetime_utc -> filing_datetime -> pubDate -> now (fallback).
    """
    raw = (
        event.get("event_datetime_utc")
        or event.get("filing_datetime")
        or event.get("pubDate")
    )
    if raw:
        dt = _parse_datetime_utc(raw)
        if dt is not None:
            return dt
    # Explicit fallback to 'now' if everything missing/unparseable
    return datetime.now(timezone.utc)

_ISO_Z_RE = re.compile(r"Z$", re.IGNORECASE)

def _parse_datetime_utc(s: str) -> Optional[datetime]:
    """
    Tolerant ISO-8601-ish parsing to UTC-aware datetime.
    Assumes inputs are mostly normalized upstream; supports trailing 'Z' or offsets.
    """
    if not s:
        return None
    s = s.strip()
    try:
        # fromisoformat doesn't like 'Z'
        s = _ISO_Z_RE.sub("+00:00", s)
        # Allow space between date/time (e.g., "2025-10-04 12:00:00Z")
        s = s.replace(" ", "T", 1) if " " in s and "T" not in s else s
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        # Very small fallback set of common RFC822-like forms (RSS)
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
    return None

# ---- Public API --------------------------------------------------------------

def make_hash(event: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    """
    Compute the stable event hash and return (hash_hex, key_dict_for_debug).
    Uses canonicalized source, title, first URL, and UTC date-only.
    """
    source = _casefold_trim(event.get("source") or event.get("source_name"))
    title  = _casefold_trim(event.get("title") or event.get("headline"))

    urls = event.get("urls")
    first_url = (
        event.get("first_url")
        or (urls[0] if isinstance(urls, list) and urls else None)
        or event.get("url")
    )
    url_norm = _normalize_url(first_url)

    dt = _pick_event_date(event)
    date_str = dt.date().isoformat()

    key_str = f"{source}|{title}|{url_norm}|{date_str}"
    h = sha256(key_str.encode("utf-8")).hexdigest()
    return h, {"source": source, "title": title, "url": url_norm, "date": date_str}

@dataclass
class SeenRecord:
    hash: str
    first_seen_utc: datetime
    last_seen_utc: datetime
    key: Dict[str, str]

class SeenStore:
    """
    Append-only JSONL dedupe store with an in-memory active window.
    """
    def __init__(self, state_path: Path, ttl_days: int = 7) -> None:
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.ttl = timedelta(days=ttl_days)
        self._active: Dict[str, SeenRecord] = {}
        self._load_active()

    @classmethod
    def from_env(cls, default_path: str = ".state/seen_events.jsonl") -> "SeenStore":
        ttl_env = os.environ.get("DEDUPE_TTL_DAYS")
        try:
            ttl_days = int(ttl_env) if ttl_env is not None else 7
        except ValueError:
            ttl_days = 7
        return cls(Path(default_path), ttl_days=ttl_days)

    def _load_active(self) -> None:
        now = datetime.now(timezone.utc)
        if not self.state_path.exists():
            return
        with self.state_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    h = obj.get("hash")
                    fs = obj.get("first_seen_utc")
                    ls = obj.get("last_seen_utc") or fs
                    key = obj.get("key") or {}
                    if not h or not fs:
                        continue
                    first_seen = _parse_datetime_utc(fs) or now
                    last_seen = _parse_datetime_utc(ls) or first_seen
                    # Consider entry active if NOW - first_seen < TTL
                    if now - first_seen < self.ttl:
                        self._active[h] = SeenRecord(
                            hash=h,
                            first_seen_utc=first_seen,
                            last_seen_utc=last_seen,
                            key=key,
                        )
                except Exception:
                    # Ignore bad lines; store remains usable
                    continue

    def seen(self, h: str) -> bool:
        return h in self._active

    def record(self, h: str, key: Dict[str, str]) -> None:
        now_dt = datetime.now(timezone.utc)
        rec = self._active.get(h)
        if rec is None:
            self._active[h] = SeenRecord(
                hash=h,
                first_seen_utc=now_dt,
                last_seen_utc=now_dt,
                key=key,
            )
        else:
            rec.last_seen_utc = now_dt

        # Append-only write (line-buffered)
        payload = {
            "hash": h,
            "first_seen_utc": self._active[h].first_seen_utc.isoformat(),
            "last_seen_utc": self._active[h].last_seen_utc.isoformat(),
            "key": key,
        }
        with self.state_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def compact(self) -> None:
        """
        Rewrite state file with only active entries. Safe atomic rename.
        """
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        now = datetime.now(timezone.utc)
        with tmp.open("w", encoding="utf-8") as f:
            for rec in self._active.values():
                if now - rec.first_seen_utc < self.ttl:
                    f.write(json.dumps({
                        "hash": rec.hash,
                        "first_seen_utc": rec.first_seen_utc.isoformat(),
                        "last_seen_utc": rec.last_seen_utc.isoformat(),
                        "key": rec.key,
                    }, ensure_ascii=False) + "\n")
        tmp.replace(self.state_path)

def dedupe_disabled() -> bool:
    return os.environ.get("DEDUPE_DISABLE") == "1"
