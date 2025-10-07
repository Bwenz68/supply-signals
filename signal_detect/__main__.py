import os
import sys
import json
import argparse
from pathlib import Path
from shared.watchlist import infer_watchlist

from .rules_sec_pr import hit_tags
from .scorer import score_hits

# Dedupe helpers
from shared.dedupe import make_hash, SeenStore, dedupe_disabled

def main():
    ap = argparse.ArgumentParser(description="Detect signals from normalized events.")
    ap.add_argument("--threshold", type=int, default=3, help="Minimum score to emit a signal")
    # Optional PATH; presence enables watchlist feature
    ap.add_argument(
        "--watchlist",
        nargs="?",
        metavar="PATH",
        help="Enable watchlist filter (optional PATH). If omitted, uses WATCHLIST_FILE env or ref/watchlist.txt."
    )
    args = ap.parse_args()

    # Resolve watchlist ONCE at startup
    WATCHLIST = None
    try:
        WATCHLIST = infer_watchlist(getattr(args, "watchlist", None))
        if WATCHLIST:
            print(f"[WATCHLIST] enabled (tickers={len(WATCHLIST.tickers)}, ciks={len(WATCHLIST.ciks)})")
        else:
            print("[WATCHLIST] disabled")
    except FileNotFoundError as e:
        print(f"[WATCHLIST] enabled but file not found: {e}", file=sys.stderr)
        raise SystemExit(2)

    # IMPORTANT: resolve env paths at runtime (not import time)
    in_dir  = Path(os.getenv("NORM_QUEUE_DIR", "queue/normalized_events"))
    out_dir = Path(os.getenv("SIG_QUEUE_DIR",  "queue/signals"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Initialize dedupe store (respects DEDUPE_TTL_DAYS; can be bypassed via DEDUPE_DISABLE=1)
    store = SeenStore.from_env()
    use_dedupe = not dedupe_disabled()

    total_emitted_global = 0
    total_skipped_dupes_global = 0
    total_skipped_unwatched_global = 0

    for fp in sorted(in_dir.glob("*.norm.jsonl")):
        out_fp = out_dir / fp.name.replace(".norm.jsonl", ".signals.jsonl")
        emitted = 0
        skipped_dupes = 0
        skipped_unwatched = 0

        with fp.open("r", encoding="utf-8") as f_in, out_fp.open("w", encoding="utf-8") as f_out:
            for line in f_in:
                if not line.strip():
                    continue
                d = json.loads(line)

                # --- Watchlist gate (runs BEFORE scoring & dedupe) ---
                if WATCHLIST is not None:
                    if not WATCHLIST.allowed(d):
                        skipped_unwatched += 1
                        continue
                # -----------------------------------------------------

                text = " ".join(filter(None, [d.get("title"), d.get("body")]))
                hits = hit_tags(text)
                s = score_hits(hits, d.get("event_kind"), d.get("event_subtype"))

                if s >= args.threshold and hits:
                    # Compute hash *only* when the item would be emitted
                    h, key = make_hash(d)

                    if use_dedupe and store.seen(h):
                        skipped_dupes += 1
                        continue

                    sig = dict(d)  # Start with all event fields
                    sig["score"] = s
                    sig["rule_hits"] = hits
                    f_out.write(json.dumps(sig, ensure_ascii=False) + "\n")
                    emitted += 1

                    if use_dedupe:
                        store.record(h, key)

        total_emitted_global += emitted
        total_skipped_dupes_global += skipped_dupes
        total_skipped_unwatched_global += skipped_unwatched

        # Build note string
        details = []
        if use_dedupe:
            details.append(f"skipped_dupes={skipped_dupes}")
        else:
            details.append("dedupe=DISABLED")
        details.append(f"skipped_unwatched={skipped_unwatched}")

        print(f"[SIGNALS] {fp.name} -> {out_fp.name} (emitted={emitted} >= {args.threshold}; " + "; ".join(details) + ")")

    # Optional lightweight auto-compaction if state grows large
    try:
        state_path = store.state_path  # attribute exists on SeenStore
        if state_path.exists() and state_path.stat().st_size > 5_000_000:  # ~5 MB
            store.compact()
    except Exception:
        pass

    if use_dedupe:
        print(f"[SIGNALS] totals: emitted={total_emitted_global}, skipped_dupes={total_skipped_dupes_global}, skipped_unwatched={total_skipped_unwatched_global}")
    else:
        print(f"[SIGNALS] totals: emitted={total_emitted_global} (dedupe disabled), skipped_unwatched={total_skipped_unwatched_global}")

if __name__ == "__main__":
    main()
