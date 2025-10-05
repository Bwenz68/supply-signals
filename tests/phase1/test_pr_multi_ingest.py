# tests/phase1/test_pr_multi_ingest.py
from __future__ import annotations

import io
import json
import sys
from pathlib import Path
import tempfile

from data_ingest.pr_feeds_cli import main as pr_multi_main

def _caprun(argv):
    old_out, old_err = sys.stdout, sys.stderr
    buf = io.StringIO()
    sys.stdout = buf
    sys.stderr = buf
    try:
        rc = 0
        try:
            pr_multi_main(argv)
        except SystemExit as e:
            rc = int(e.code or 0)
        return rc, buf.getvalue()
    finally:
        sys.stdout = old_out
        sys.stderr = old_err

def test_multi_urls_and_global_tag(tmp_path: Path):
    repo = Path.cwd()
    url1 = "file://" + str(repo / "tests" / "fixtures" / "pr_sample.xml")
    url2 = "file://" + str(repo / "tests" / "fixtures" / "pr_sample_b.xml")
    out = tmp_path / "out.jsonl"

    rc, out_text = _caprun([
        "--urls", f"{url1};{url2}",
        "--issuer-name", "Contoso Energy",
        "--tag", "demo",
        "--limit-per-feed", "10",
        "--out", str(out),
    ])
    assert rc == 0
    rows = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
    # pr_sample has 2 items, pr_sample_b has 1 → total 3
    assert len(rows) == 3
    # Global issuer_name applied (since feed file didn't override)
    assert all(r.get("issuer_name") == "Contoso Energy" for r in rows)
    # Global tag applied
    assert all(r.get("source_tag") == "demo" for r in rows)
    # Basic fields exist
    assert all("title" in r and "first_url" in r for r in rows)

def test_feeds_file_tsv_with_overrides(tmp_path: Path):
    repo = Path.cwd()
    url1 = "file://" + str(repo / "tests" / "fixtures" / "pr_sample.xml")
    url2 = "file://" + str(repo / "tests" / "fixtures" / "pr_sample_b.xml")

    feeds_path = tmp_path / "feeds.tsv"
    feeds_path.write_text(f"{url1}\tContoso Energy\tcontoso\n{url2}\tFabrikam Power\tfabrikam\n", encoding="utf-8")

    out = tmp_path / "out2.jsonl"
    rc, out_text = _caprun([
        "--feeds-file", str(feeds_path),
        "--limit-per-feed", "10",
        "--out", str(out),
    ])
    assert rc == 0
    rows = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
    # 2 + 1 entries
    assert len(rows) == 3
    issuers = sorted(set(r.get("issuer_name") for r in rows))
    assert issuers == ["Contoso Energy", "Fabrikam Power"]
    tags = sorted(set(r.get("source_tag") for r in rows))
    assert tags == ["contoso", "fabrikam"]
