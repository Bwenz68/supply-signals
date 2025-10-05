# tests/phase1/test_sec_paging.py
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

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

def test_paging_two_pages(tmp_path: Path):
    repo = Path.cwd() / "tests" / "fixtures"
    url1 = "file://" + str(repo / "sec_atom_page1.xml")
    out = tmp_path / "out_paged.jsonl"

    rc, out_text = _caprun([
        "--url", url1,
        "--cik", "9876543",
        "--pages-max", "5",
        "--max", "10",
        "--issuer-name", "Contoso Energy",
        "--out", str(out),
    ])
    assert rc == 0
    rows = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
    # page1 has 1 entry (8-K), page2 has 1 entry (10-Q)
    assert len(rows) == 2
    forms = sorted(set(r.get("form_type") for r in rows))
    assert forms == ["10-Q", "8-K"]

def test_paging_respects_global_max(tmp_path: Path):
    repo = Path.cwd() / "tests" / "fixtures"
    url1 = "file://" + str(repo / "sec_atom_page1.xml")
    out = tmp_path / "out_cap1.jsonl"

    rc, out_text = _caprun([
        "--url", url1,
        "--cik", "9876543",
        "--pages-max", "5",
        "--max", "1",  # cap at 1
        "--issuer-name", "Contoso Energy",
        "--out", str(out),
    ])
    assert rc == 0
    rows = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(rows) == 1
