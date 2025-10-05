import os
from pathlib import Path

from shared.watchlist import Watchlist, load_watchlist, infer_watchlist

WL_TEXT = """\
# tickers
aapl
BRK.B
# ciks
320193
0000789019
invalid token!
"""

def test_from_file_parses_and_canonicalizes(tmp_path: Path):
    p = tmp_path / "watchlist.txt"
    p.write_text(WL_TEXT, encoding="utf-8")
    wl = load_watchlist(str(p))
    assert "AAPL" in wl.tickers
    assert "BRK.B" in wl.tickers
    assert "0000320193" in wl.ciks
    assert "0000789019" in wl.ciks

def test_allowed_by_ticker_and_cik(tmp_path: Path):
    p = tmp_path / "watchlist.txt"
    p.write_text("AAPL\n0000789019\n", encoding="utf-8")
    wl = load_watchlist(str(p))

    ev_ticker = {"issuer": {"ticker": "aapl"}}
    ev_cik = {"issuer": {"cik": "789019"}}
    ev_both = {"issuer": {"ticker": "AAPL", "cik": 320193}}
    ev_neither = {"issuer": {"ticker": "TSLA", "cik": 1318605}}

    assert wl.allowed(ev_ticker) is True
    assert wl.allowed(ev_cik) is True
    assert wl.allowed(ev_both) is True
    assert wl.allowed(ev_neither) is False

def test_infer_watchlist_env_and_default(tmp_path: Path, monkeypatch):
    # Ensure test is independent of repo's existing ref/watchlist.txt
    monkeypatch.delenv("WATCHLIST_FILE", raising=False)
    monkeypatch.delenv("WATCHLIST_DISABLE", raising=False)
    monkeypatch.chdir(tmp_path)

    # No env, no default in this temp cwd => disabled
    wl = infer_watchlist(None)
    assert wl is None

    # Default file present => used
    default = Path("ref/watchlist.txt")
    default.parent.mkdir(parents=True, exist_ok=True)
    default.write_text("AAPL\n", encoding="utf-8")
    wl2 = infer_watchlist(None)
    assert wl2 is not None and "AAPL" in wl2.tickers

    # Explicit env path beats default
    p = tmp_path / "w.txt"
    p.write_text("MSFT\n", encoding="utf-8")
    monkeypatch.setenv("WATCHLIST_FILE", str(p))
    wl3 = infer_watchlist(None)
    assert wl3 is not None and "MSFT" in wl3.tickers

def test_disable_via_env(monkeypatch, tmp_path: Path):
    p = tmp_path / "w.txt"
    p.write_text("AAPL\n", encoding="utf-8")
    monkeypatch.setenv("WATCHLIST_FILE", str(p))
    monkeypatch.setenv("WATCHLIST_DISABLE", "1")
    wl = infer_watchlist(None)
    assert wl is None
