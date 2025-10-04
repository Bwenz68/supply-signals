import os
import json
import argparse
from pathlib import Path

from .rules_sec_pr import hit_tags
from .scorer import score_hits

IN_DIR  = Path(os.getenv("NORM_QUEUE_DIR", "queue/normalized_events"))
OUT_DIR = Path(os.getenv("SIG_QUEUE_DIR",  "queue/signals"))

def main():
    ap = argparse.ArgumentParser(description="Detect signals from normalized events.")
    ap.add_argument("--threshold", type=int, default=3, help="Minimum score to emit a signal")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for fp in sorted(IN_DIR.glob("*.norm.jsonl")):
        out_fp = OUT_DIR / fp.name.replace(".norm.jsonl", ".signals.jsonl")
        emitted = 0

        with fp.open("r", encoding="utf-8") as f_in, out_fp.open("w", encoding="utf-8") as f_out:
            for line in f_in:
                d = json.loads(line)
                text = " ".join(filter(None, [d.get("title"), d.get("body")]))
                hits = hit_tags(text)
                s = score_hits(hits, d.get("event_kind"), d.get("event_subtype"))

                if s >= args.threshold and hits:
                    sig = {
                        "score": s,
                        "hits": hits,
                        "event": d,
                    }
                    f_out.write(json.dumps(sig, ensure_ascii=False) + "\n")
                    emitted += 1

        print(f"[SIGNALS] {fp.name} -> {out_fp.name} ({emitted} signals >= {args.threshold})")

if __name__ == "__main__":
    main()
