from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

__all__ = ["parse_to_utc", "to_iso_utc", "STRICT_Z_ISO_PATTERN"]

# Strict pattern for final outputs: YYYY-MM-DDTHH:MM:SSZ
STRICT_Z_ISO_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# Sanity window (inclusive lower bound, exclusive upper bound)
_MIN_DT = datetime(2000, 1, 1, tzinfo=timezone.utc)
_MAX_DT = datetime(2100, 1, 1, tzinfo=timezone.utc)


def _normalize_candidate(s: str) -> str:
    """
    Normalize common date-time quirks without changing semantics.
    Handles:
      - 'YYYY/MM/DD' -> 'YYYY-MM-DD'
      - space between date and time -> 'T'
      - adds ':00' seconds if missing (even if followed by Z/offset)
      - '+HHMM' -> '+HH:MM' (removes stray space before offset too)
      - uppercase trailing 'z' and convert 'Z' to '+00:00' for fromisoformat
    """
    s = s.strip()

    # Normalize lowercase 'z' to 'Z'
    if s.endswith("z"):
        s = s[:-1] + "Z"

    # Convert leading slash date to dashes
    s = re.sub(r"^(\d{4})/(\d{2})/(\d{2})", r"\1-\2-\3", s)

    # Replace the first space between date and time with 'T' (HH:MM or HH:MM:SS)
    s = re.sub(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)", r"\1T\2", s, count=1)

    # If ISO-like with missing seconds, add ':00' even if a TZ/space follows
    # e.g. '...T12:34Z', '...T12:34+0000', '...T12:34 +0000'
    s = re.sub(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})(?=(Z|[+-]\d{2}:?\d{2}|\s|$))", r"\1:00", s, count=1)

    # Canonicalize '+HHMM' or ' +HHMM' to '+HH:MM' and drop the space
    s = re.sub(r"\s*([+-]\d{2})(\d{2})$", r"\1:\2", s)

    # If explicit 'Z', datetime.fromisoformat doesn't accept 'Z' -> use '+00:00'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    return s


def parse_to_utc(dt_str: str, *, naive_tz: str | None = None) -> datetime:
    """
    Parse a datetime string tolerantly and return an aware UTC datetime.
    Tries ISO (after normalization), then RFC-2822 (RSS).
    If result is naive and naive_tz provided, localize then convert to UTC.
    Enforces sanity window [2000-01-01, 2100-01-01).

    Raises ValueError with one of: "missing", "unparseable", "out_of_range".
    """
    if dt_str is None:
        raise ValueError("missing")

    raw = str(dt_str).strip()
    if not raw:
        raise ValueError("missing")

    # Try ISO(-ish)
    s = _normalize_candidate(raw)
    dt = None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # Try RFC-2822/RSS (e.g., "Sun, 05 Oct 2025 06:20:00 GMT")
        try:
            dt = parsedate_to_datetime(raw)  # use raw here to respect 'GMT', etc.
        except Exception:
            raise ValueError("unparseable")

    if dt.tzinfo is None:
        if naive_tz:
            try:
                dt = dt.replace(tzinfo=ZoneInfo(naive_tz))
            except Exception:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)

    dt_utc = dt.astimezone(timezone.utc)

    if not (_MIN_DT <= dt_utc < _MAX_DT):
        raise ValueError("out_of_range")

    return dt_utc


def to_iso_utc(dt: datetime) -> str:
    """
    Convert aware/naive datetime to strict ISO with trailing 'Z' and no fractions.
    Naive input is treated as UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
