from __future__ import annotations

import argparse
import json
import os
import tempfile
from glob import glob
from typing import Dict, Any, Tuple

from shared.datetime_utils import parse_to_utc, to_iso_utc, STRICT_Z_ISO_PATTERN

STRICT_PATTERN = STRICT_Z_ISO_PATTERN


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _source_defaults(source: str) -> str:
    if source == "sec":
        return _env("SEC_DEFAULT_TZ", "America/New_York")
    if source == "pr":
        return _env("PR_DEFAULT_TZ", "UTC")
    return _env("HARDEN_TZ_FALLBACK", "UTC")


def _candidate_fields(source: str) -> Tuple[str, ...]:
    if source == "sec":
        return ("filing_datetime", "acceptance_datetime", "published_at")
    if source == "pr":
        return ("pubDate", "published", "updated", "lastBuildDate")
    return ("published_at", "updated", "pubDate", "published")


def _has_any_candidates(rec: Dict[str, Any], fields: Tuple[str, ...]) -> bool:
    return any(bool(rec.get(f)) for f in fields)


def _already_strict(rec: Dict[str, Any]) -> bool:
    v = rec.get("event_datetime_utc")
    return isinstance(v, str) and bool(STRICT_PATTERN.match(v))


def harden_record(rec: Dict[str, Any], totals: Dict[str, int]) -> Dict[str, Any]:
    """
    Returns a possibly-modified copy of rec with strict UTC field added when possible.
    Does not remove existing fields. Idempotent: respects existing strict field.
    Updates totals dict with metric counters.
    """
    if _already_strict(rec):
        return rec

    out = dict(rec)
    source = (rec.get("source") or "").strip().lower()
    fields = _candidate_fields(source)
    naive_tz = _source_defaults(source)

    last_error = None
    if _has_any_candidates(rec, fields):
        for f in fields:
            val = rec.get(f)
            if not val:
                continue
            try:
                dt_utc = parse_to_utc(str(val), naive_tz=naive_tz)
                out["event_datetime_utc"] = to_iso_utc(dt_utc)
                out["timestamp_source"] = f
                out["timestamp_backfilled"] = False
                totals["timestamps_parsed_ok"] += 1
                return out
            except ValueError as e:
                last_error = str(e)

        totals["timestamp_parse_fail"] += 1
        out["timestamp_error"] = "out_of_range" if last_error == "out_of_range" else "unparseable"

    if "ingested_at_utc" in rec and rec.get("ingested_at_utc"):
        try:
            dt_utc = parse_to_utc(str(rec["ingested_at_utc"]), naive_tz="UTC")
            out["event_datetime_utc"] = to_iso_utc(dt_utc)
            out["timestamp_source"] = "ingested_at_utc"
            out["timestamp_backfilled"] = True
            totals["timestamp_backfilled"] += 1
            return out
        except ValueError:
            totals["timestamp_parse_fail"] += 1
            out["timestamp_error"] = "unparseable"

    if not _has_any_candidates(rec, fields):
        out["timestamp_error"] = "missing"
    totals["timestamp_missing_or_error"] += 1
    return out


def process_dir(queue_dir: str, dry_run: bool = False) -> Dict[str, int]:
    """
    Process all *.norm.jsonl files in queue_dir. In-place (atomic) unless dry_run.
    Returns totals metrics.
    """
    if os.environ.get("TIMESTAMP_HARDEN_DISABLE") == "1":
        print("timestamp hardening disabled via TIMESTAMP_HARDEN_DISABLE=1")
        return {}

    totals = {
        "files": 0,
        "records": 0,
        "timestamps_parsed_ok": 0,
        "timestamp_backfilled": 0,
        "timestamp_parse_fail": 0,
        "timestamp_missing_or_error": 0,
    }

    paths = sorted(glob(os.path.join(queue_dir, "*.norm.jsonl")))
    for path in paths:
        totals["files"] += 1
        fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=os.path.dirname(path))
        outf = os.fdopen(fd, "w", encoding="utf-8")

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    outf.write(line)
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    totals["timestamp_parse_fail"] += 1
                    outf.write(line)
                    continue

                totals["records"] += 1
                new_rec = harden_record(rec, totals)
                outf.write(json.dumps(new_rec, ensure_ascii=False) + "\n")

        outf.close()
        os.replace(tmp, path)

    return totals


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Harden timestamps in normalized event queue in-place.")
    p.add_argument("--dir", default="queue/normalized_events", help="Normalized queue dir (default: queue/normalized_events)")
    p.add_argument("--check", action="store_true", help="Dry-run: do not modify files; just print metrics")
    args = p.parse_args(argv)

    totals = process_dir(args.dir, dry_run=args.check)
    if totals:
        print(
            "hardened: files={files} records={records} ok={timestamps_parsed_ok} "
            "backfilled={timestamp_backfilled} parse_fail={timestamp_parse_fail} "
            "missing_or_error={timestamp_missing_or_error}".format(**totals)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
