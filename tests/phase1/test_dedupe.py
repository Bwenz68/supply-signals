# tests/phase1/test_dedupe.py
import os
from pathlib import Path
from shared.dedupe import make_hash, SeenStore

def test_make_hash_canonicalization_equivalence():
    e1 = {
        "source": "Reuters",
        "title": "Apple  raises   buyback",
        "urls": ["https://www.reuters.com/article?id=Foo&utm_source=x&utm_medium=y#frag"],
        "event_datetime_utc": "2025-10-04T12:00:00Z",
    }
    e2 = {
        "source": "reuters ",
        "title": "  APPLE raises buyback ",
        "url": "https://reuters.com/article?id=Foo",
        "event_datetime_utc": "2025-10-04 12:00:00+00:00",
    }

    h1, key1 = make_hash(e1)
    h2, key2 = make_hash(e2)

    assert h1 == h2, "Hashes should match after canonicalization"
    assert key1["source"] == "reuters"
    assert key1["title"] == "apple raises buyback"
    assert key1["url"] == "https://reuters.com/article?id=Foo"
    assert key1["date"] == "2025-10-04"
    assert key2["url"] == "https://reuters.com/article?id=Foo"

def test_url_precedence_and_fallback():
    # first_url wins over urls[0] and url
    e_first = {
        "source": "X",
        "title": "Y",
        "first_url": "https://example.com/a?utm_campaign=z",
        "urls": ["https://example.com/b"],
        "url": "https://example.com/c",
        "event_datetime_utc": "2025-10-04T00:00:00Z",
    }
    h_first, key_first = make_hash(e_first)
    assert key_first["url"] == "https://example.com/a"

    # If no first_url, use urls[0]
    e_urls0 = {
        "source": "X",
        "title": "Y",
        "urls": ["https://example.com/b?gclid=123"],
        "event_datetime_utc": "2025-10-04T00:00:00Z",
    }
    h_urls0, key_urls0 = make_hash(e_urls0)
    assert key_urls0["url"] == "https://example.com/b"

    # If neither first_url nor urls list, use url
    e_url = {
        "source": "X",
        "title": "Y",
        "url": "https://example.com/c#frag",
        "event_datetime_utc": "2025-10-04T00:00:00Z",
    }
    h_url, key_url = make_hash(e_url)
    assert key_url["url"] == "https://example.com/c"

def test_seenstore_record_reload_and_ttl(tmp_path: Path):
    state_file = tmp_path / "seen.jsonl"

    # TTL 7 days: record -> seen() True; survives reload
    store = SeenStore(state_file, ttl_days=7)
    e = {"source":"A","title":"B","url":"https://ex.com/x","event_datetime_utc":"2025-10-04T01:02:03Z"}
    h, key = make_hash(e)

    assert store.seen(h) is False
    store.record(h, key)
    assert store.seen(h) is True

    # Reload from disk — should still be active
    store2 = SeenStore(state_file, ttl_days=7)
    assert store2.seen(h) is True

    # TTL 0 days: nothing should be active
    store3 = SeenStore(state_file, ttl_days=0)
    assert store3.seen(h) is False
