# tests/phase1/test_pr_cache.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple

# We'll monkeypatch feedparser.parse in each CLI module to capture request_headers.

class _DummyParsed:
    def __init__(self, etag: str, modified: str):
        self.bozo = 0
        self.entries = []  # keep empty; we only test header composition & cache update
        self.feed = {"links": []}
        self.etag = etag
        self.modified = modified
        self.status = 200
        self.headers = {"content-type": "application/atom+xml; charset=utf-8"}

def test_pr_feed_cli_uses_cache_headers_and_updates(tmp_path: Path, monkeypatch):
    # Arrange cache with prior metadata
    url = "http://example.com/feed"
    cache_file = tmp_path / "pr_cache.json"
    cache_file.write_text(json.dumps({url: {"etag": 'W/"old"', "last_modified": "Sat, 01 Jan 2022 00:00:00 GMT"}}), encoding="utf-8")

    captured: Dict[str, Dict[str, str]] = {}

    def fake_parse(u: str, request_headers: Dict[str, str] = None):  # type: ignore
        captured["u"] = {"url": u}
        captured["h"] = request_headers or {}
        return _DummyParsed(etag='W/"new"', modified="Sun, 02 Jan 2022 00:00:00 GMT")

    import data_ingest.pr_feed_cli as mod
    monkeypatch.setattr(mod.feedparser, "parse", fake_parse)

    rc, out_text = _caprun_mod(mod, [
        "--url", url,
        "--max", "0",
        "--cache-file", str(cache_file),
        "--debug-headers",
    ])
    assert rc == 0
    # It should have sent conditional headers
    assert captured["h"].get("If-None-Match") == 'W/"old"'
    assert captured["h"].get("If-Modified-Since") == "Sat, 01 Jan 2022 00:00:00 GMT"

    # And cache should have updated to new etag/last_modified
    new_cache = json.loads(cache_file.read_text(encoding="utf-8"))
    assert new_cache[url]["etag"] == 'W/"new"'
    assert new_cache[url]["last_modified"] == "Sun, 02 Jan 2022 00:00:00 GMT"

def test_pr_feeds_cli_skips_cache_for_file_urls(tmp_path: Path, monkeypatch):
    # file:// should not use HTTP conditionals; capture headers to prove empty
    p = tmp_path / "feed.xml"
    p.write_text("""<?xml version="1.0"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Demo</title>
    </feed>""", encoding="utf-8")
    url = "file://" + str(p)

    captured: Dict[str, Dict[str, str]] = {}

    def fake_parse(u: str, request_headers: Dict[str, str] = None):  # type: ignore
        captured["h"] = request_headers or {}
        return _DummyParsed(etag='W/"e"', modified="Mon, 03 Jan 2022 00:00:00 GMT")

    import data_ingest.pr_feeds_cli as mod
    monkeypatch.setattr(mod.feedparser, "parse", fake_parse)

    rc, out_text = _caprun_mod(mod, [
        "--urls", url,
        "--max", "0",
        "--debug-headers",
    ])
    assert rc == 0
    # For file:// there should be no conditional headers sent
    assert captured["h"] == {}

# Helper: run a module's main(argv) and capture output (same pattern as other tests)
def _caprun_mod(mod, argv) -> Tuple[int, str]:
    import io, sys
    buf = io.StringIO()
    old = sys.stdout
    try:
        sys.stdout = buf
        rc = mod.main(argv)
    finally:
        sys.stdout = old
    return rc, buf.getvalue()
