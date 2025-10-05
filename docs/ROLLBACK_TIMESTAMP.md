# Timestamp Hardening — Rollback / Disable

This milestone is **additive** and **idempotent**. No schemas were removed.

## Disable at runtime
- Set `TIMESTAMP_HARDEN_DISABLE=1` to skip the hardening pass entirely.

## Stop running the pass
- Simply do not invoke:
  - `python -m normalize_enrich.timestamp_hardener`

## Revert the code


## Notes
- The pass is idempotent: re-running on already-hardened files produces the same outputs.
- It preserves all original fields and only adds:
  - `event_datetime_utc` (strict ISO Z),
  - `timestamp_source`,
  - `timestamp_backfilled`,
  - `timestamp_error`.
