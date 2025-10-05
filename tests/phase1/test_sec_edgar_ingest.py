# tests/phase1/test_sec_edgar_ingest.py
from __future__ import annotations

import io
import os
import sys
import tempfile
from pathlib import Path
import json

from data_ingest.sec_edgar_cli import main as edgar_main

def _caprun(argv):
    old_out, old_err = sys.stdout, sys.stderr
    buf = io.StringIO()
    sys.stdout = buf
    sys.stderr = buf
    try:
        rc = 0
        try:
            edgar_main(argv)
        except SystemExit as e:
            rc = int(e.code or 0)
        return rc, buf.getvalue()
    finally:
        sys.stdout = old_out
        sys.stderr = old_err

def test_offline_atom_parse_and_filter(tmp_path: Path):
    # Copy fixture path
    repo_root = Path.cwd()
    fixture = repo_root / "tests" / "fixtures" / "sec_atom_sample.xml"
    assert fixture.exists()
    url = "file://" + str(fixture)

    # Filter to 8-K only, limit 1
    rc, out = _caprun([
        "--cik", "9876543",
        "--forms", "8-K",
        "--max", "5",
        "--issuer-name", "Contoso Energy",
        "--url", url,
        "--out", str(tmp_path / "out.jsonl"),
    ])
    assert rc == 0
    assert "Wrote" in out
    # Verify file contains exactly one 8-K row
    rows = [json.loads(l) for l in (tmp_path / "out.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(rows) == 1
    r = rows[0]
    assert r.get("event_kind") == "sec_filing"
    assert r.get("form_type") == "8-K"
    assert r.get("issuer_name") == "Contoso Energy"
    assert r.get("cik") == "0009876543"
    assert r.get("first_url", "").startswith("https://www.sec.gov/Archives/")
