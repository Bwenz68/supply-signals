# Watchlist Feature — Enable/Disable & Rollback

## TL;DR
- **Default:** Disabled.
- **Enable:** `--watchlist [PATH]` or `WATCHLIST_FILE=ref/watchlist.txt`.
- **Disable quickly:** Do **not** pass `--watchlist` and ensure `WATCHLIST_FILE` is unset — or set `WATCHLIST_DISABLE=1`.
- **Fail-fast:** If enabled but the file is missing, process exits with code **2**.

## What it touches
- **No schema changes.** Phase-0 compatible.
- **No state writes** when events are skipped by the watchlist (dedupe `.state/seen_events.jsonl` is unaffected).
- **Deterministic/idempotent.**

## Enable

    # CLI (optional PATH):
    python3 -m signal_detect --watchlist                 # uses WATCHLIST_FILE or ref/watchlist.txt if present
    python3 -m signal_detect --watchlist ref/watchlist.txt

    # ENV:
    WATCHLIST_FILE=ref/watchlist.txt python3 -m signal_detect --threshold 3

## Disable

    # 1) Simply don't pass --watchlist and unset WATCHLIST_FILE:
    unset WATCHLIST_FILE
    python3 -m signal_detect --threshold 3

    # 2) Force disable even if you pass the flag:
    WATCHLIST_DISABLE=1 python3 -m signal_detect --watchlist

**Expected on disable:**

    [WATCHLIST] disabled

## Remove the data file (optional)

    rm -f ref/watchlist.txt

> Safe: the feature remains disabled unless explicitly re-enabled.

## Hard rollback of code (if ever desired)

    # Discard local edits (if using git):
    git checkout -- shared/watchlist.py signal_detect/__main__.py
    # Or revert the commit that added the feature:
    git log --oneline | head -n 10
    git revert <commit-sha>

## Verification

    # Disabled run
    python3 -m signal_detect --threshold 3 | head -n 1

    # Force-disabled even with flag:
    WATCHLIST_DISABLE=1 python3 -m signal_detect --watchlist | head -n 1
