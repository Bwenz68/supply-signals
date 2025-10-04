import os
import json
import argparse
from pathlib import Path

from .formatter import one_line
from .sinks import console, to_csv

IN_DIR = Path(os.getenv("SIG_QUEUE_DIR", "queue/signals"))

def main():
    ap = argparse.ArgumentParser(description="Alert engine: format and output signals")
    ap.add_argument("--csv", action="store_true", help="Append alerts to CSV instead of printing to console")
    ap.add_argument("--csv-path", default="queue/alerts/alerts.csv", help="CSV output path (used with --csv)")
    args = ap.parse_args()

    lines = []
    for fp in sorted(IN_DIR.glob("*.signals.jsonl")):
        with fp.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    sig = json.loads(line)
                except Exception:
                    continue
                lines.append(one_line(sig))

    if args.csv:
        outp = to_csv(lines, path=args.csv_path)
        print(f"[ALERTS] appended {len(lines)} lines to {outp}")
    else:
        console(lines)
        print(f"[ALERTS] printed {len(lines)} lines")

if __name__ == "__main__":
    main()
