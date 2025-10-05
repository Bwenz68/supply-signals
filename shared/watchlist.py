from __future__ import annotations

import os
import re
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple, Set, Dict, Any

_LOG = logging.getLogger(__name__)

_CIK_RE = re.compile(r"^\s*\d+\s*$")
_TICKER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\.\-]{0,9}$")  # simple, permissive


def _canon_ticker(tok: str) -> Optional[str]:
    t = tok.strip().upper()
    if not t:
        return None
    # Keep '.' and '-' literal (e.g., BRK.B, RDS-A)
    if _TICKER_RE.match(t):
        return t
    return None


def _canon_cik(tok: str) -> Optional[str]:
    t = tok.strip()
    if not _CIK_RE.match(t):
        return None
    try:
        return f"{int(t):010d}"
    except Exception:
        return None


def _extract_identifiers(event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (ticker_norm, cik_norm_10digits) from a normalized event.
    Prefers event["issuer"] fields; falls back to top-level "ticker"/"cik".
    Missing/invalid values return None entries.
    """
    ticker = None
    cik = None

    issuer = event.get("issuer") if isinstance(event, dict) else None
    if isinstance(issuer, dict):
        t = issuer.get("ticker")
        c = issuer.get("cik")
        if isinstance(t, str):
            ticker = _canon_ticker(t)
        elif t is not None:
            ticker = _canon_ticker(str(t))

        if isinstance(c, (str, int)):
            cik = _canon_cik(str(c))
    # fallbacks
    if ticker is None:
        t2 = event.get("ticker")
        if t2 is not None:
            ticker = _canon_ticker(str(t2))
    if cik is None:
        c2 = event.get("cik") or event.get("cik_str")
        if c2 is not None:
            cik = _canon_cik(str(c2))

    return ticker, cik


@dataclass
class Watchlist:
    tickers: Set[str] = field(default_factory=set)  # canonical uppercase
    ciks: Set[str] = field(default_factory=set)     # 10-digit strings

    @classmethod
    def from_file(cls, path: Path) -> "Watchlist":
        tickers: Set[str] = set()
        ciks: Set[str] = set()
        invalid: Set[str] = set()

        text = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for raw in text:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            # First try CIK (digits)
            c = _canon_cik(line)
            if c:
                ciks.add(c)
                continue

            # Else try ticker
            t = _canon_ticker(line)
            if t:
                tickers.add(t)
                continue

            invalid.add(line)

        wl = cls(tickers=tickers, ciks=ciks)
        if invalid:
            # Log only once with a compact preview
            preview = ", ".join(list(invalid)[:10])
            more = "" if len(invalid) <= 10 else f" (+{len(invalid)-10} more)"
            _LOG.warning("Watchlist: ignored %d invalid tokens: %s%s", len(invalid), preview, more)
        _LOG.info("Watchlist loaded: %d tickers, %d CIKs", len(tickers), len(ciks))
        return wl

    def allowed(self, event: Dict[str, Any]) -> bool:
        t, c = _extract_identifiers(event)
        if t and t in self.tickers:
            return True
        if c and c in self.ciks:
            return True
        return False


def load_watchlist(path: str) -> Watchlist:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Watchlist file not found: {p}")
    return Watchlist.from_file(p)


def infer_watchlist(cli_path: Optional[str]) -> Optional[Watchlist]:
    """
    Resolution:
      - if WATCHLIST_DISABLE=1 => disabled (return None)
      - else if cli_path is provided => use it (may be None if flag present without value)
      - else if WATCHLIST_FILE env is set => use it
      - else if 'ref/watchlist.txt' exists => use it
      - else => disabled
    Failure policy:
      - If resolved path does not exist => raise FileNotFoundError
    """
    if os.environ.get("WATCHLIST_DISABLE") == "1":
        return None

    path = None
    if cli_path is not None:
        path = cli_path if cli_path else os.environ.get("WATCHLIST_FILE")
        # If user wrote just "--watchlist" with no path and no env, try default
        if not path:
            default = Path("ref/watchlist.txt")
            if default.exists():
                path = str(default)
    else:
        path = os.environ.get("WATCHLIST_FILE")
        if not path:
            default = Path("ref/watchlist.txt")
            if default.exists():
                path = str(default)

    if not path:
        return None  # not enabled

    return load_watchlist(path)
