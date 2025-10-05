import json
import importlib
from pathlib import Path
import pytest

# Auto-skip the whole module if the hardener was rolled back
try:
    mod = importlib.import_module("normalize_enrich.timestamp_hardener")
except ModuleNotFoundError:
    pytest.skip("timestamp hardener not present (rolled back)", allow_module_level=True)

process_dir = mod.process_dir

SAMPLE = [
    {"source": "sec", "filing_datetime": "2025-10-05 02:20", "title": "10-K"},
    {"source": "pr", "pubDate": "Sun, 05 Oct 2025 06:20:00 GMT", "title": "Press release"},
    {"source": "pr", "title": "No timestamp", "ingested_at_utc": "2025-10-05T06:25:11Z"},
    {"source": "sec", "filing_datetime": "not a date", "title": "Bad"},
]

def write_file(p: Path, records):
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

def read_file(p: Path):
    out = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            out.append(json.loads(line))
    return out

def test_hardening_tmp_dir(tmp_path: Path):
    qdir = tmp_path / "queue" / "normalized_events"
    qdir.mkdir(parents=True)
    fpath = qdir / "sample.norm.jsonl"
    write_file(fpath, SAMPLE)

    totals = process_dir(str(qdir))
    assert totals["records"] == 4
    data = read_file(fpath)

    assert data[0]["event_datetime_utc"] == "2025-10-05T06:20:00Z"
    assert data[0]["timestamp_source"] == "filing_datetime"
    assert data[0]["timestamp_backfilled"] is False

    assert data[1]["event_datetime_utc"] == "2025-10-05T06:20:00Z"
    assert data[1]["timestamp_source"] in ("pubDate", "published")
    assert data[1]["timestamp_backfilled"] is False

    assert data[2]["event_datetime_utc"] == "2025-10-05T06:25:11Z"
    assert data[2]["timestamp_source"] == "ingested_at_utc"
    assert data[2]["timestamp_backfilled"] is True

    assert "event_datetime_utc" not in data[3]
    assert data[3]["timestamp_error"] in ("unparseable", "out_of_range", "missing")
