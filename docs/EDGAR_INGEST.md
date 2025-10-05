# EDGAR Ingest (Atom) — Live Usage

This CLI parses SEC Atom feeds and writes raw rows into `queue/raw_events/*.jsonl`.

## Polite Live Mode

- Set a descriptive `SEC_USER_AGENT` — **Name \<email\> \<phone\>** per SEC guidance.
- Default throttle is `SEC_RATE_PER_MIN=6` (we currently make one request per run).
- Conditional requests are on by default: we send `If-None-Match`/`If-Modified-Since` if available.

## Quickstart

```bash
export SEC_USER_AGENT="Your Name <you@example.com> <+1-555-0100>"
python -m data_ingest.sec_edgar_cli --cik 0000320193 --forms 8-K --max 50
