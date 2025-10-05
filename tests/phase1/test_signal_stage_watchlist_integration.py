import sys
import subprocess

def test_signal_detect_help_mentions_watchlist():
    proc = subprocess.run(
        [sys.executable, "-m", "signal_detect", "--help"],
        capture_output=True, text=True
    )
    assert "--watchlist" in (proc.stdout + proc.stderr)

def test_missing_watchlist_exits_with_code_2(tmp_path):
    missing = str(tmp_path / "nope.txt")
    proc = subprocess.run(
        [sys.executable, "-m", "signal_detect", "--watchlist", missing],
        capture_output=True, text=True
    )
    assert proc.returncode == 2
    assert "file not found" in (proc.stdout + proc.stderr).lower()
