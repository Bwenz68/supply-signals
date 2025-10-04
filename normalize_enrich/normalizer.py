import os
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict

from .cik_ticker_map import load_map

IN_DIR  = Path(os.getenv("RAW_QUEUE_DIR", "queue/raw_events"))
OUT_DIR = Path(os.getenv("NORM_QUEUE_DIR", "queue/normalized_events"))

def to_iso_utc(ts: str):
    """
    Coerce common timestamp strings -> ISO-8601 UTC.
    Returns None if parsing fails.
    """
    if not ts:
        return None
    try:
        # ISO-like: 2025-10-04T12:00:00Z
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # RFC-822-ish: Sat, 04 Oct 2025 12:30:00 GMT / +0000
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            return datetime.strptime(ts, fmt).astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return None

def normalize_one(d: Dict[str, Any], refmap):
    src  = d.get("source")
    meta = d.get("meta") or {}

    cik   = (meta.get("cik") or "").lstrip("0") or None
    tick  = meta.get("ticker")
    comp  = meta.get("company_name")

    if cik and (cik in refmap):
        tick = tick or refmap[cik]["ticker"]
        comp = comp or refmap[cik]["company"]

    event_kind = "SEC" if src == "SEC" else ("PR" if src == "PR" else "OTHER")
    subtype    = meta.get("doc_type")

    norm = {
        # Phase-0 baseline fields
        "title":  d.get("title"),
        "body":   d.get("body"),
        "source": src,
        "ts":     d.get("ts"),

        # Phase-1 OPTIONAL enrichments
        "canonical_company": comp,
        "canonical_ticker":  tick,
        "canonical_cik":     cik,
        "event_datetime_utc": to_iso_utc(d.get("ts")),
        "event_kind":        event_kind,
        "event_subtype":     subtype,
        "urls":              (meta.get("urls") or []),
    }
    return norm

def main():
    ap = argparse.ArgumentParser(description="Normalize raw events to Phase-0-compatible records with optional enrichments.")
    ap.add_argument("--once", action="store_true", help="Process all NDJSON in IN_DIR once and exit.")
    args = ap.parse_args()

    refmap = load_map()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    in_files = sorted(IN_DIR.glob("*.jsonl"))
    total_out = 0

    for fp in in_files:
        out_fp = OUT_DIR / fp.name.replace(".jsonl", ".norm.jsonl")
        count = 0
        with fp.open("r", encoding="utf-8") as f, out_fp.open("w", encoding="utf-8") as g:
            for line in f:
                raw = json.loads(line)
                norm = normalize_one(raw, refmap)
                g.write(json.dumps(norm, ensure_ascii=False) + "\n")
                count += 1
                total_out += 1
        print(f"[NORMALIZE] {fp.name} -> {out_fp.name} ({count} items)")

    print(f"[NORMALIZE] wrote {total_out} normalized items")

if __name__ == "__main__":
    main()
