import argparse
import sys
from . import sec_edgar_ingestor as sec
from . import press_release_ingestor as pr

def main():
    ap = argparse.ArgumentParser(prog="python -m data_ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sec = sub.add_parser("sec", help="Ingest SEC feed")
    p_sec.add_argument("--url", help="SEC feed URL or file:ref/...", default=None)
    p_sec.add_argument("--user-agent", help="User-Agent header", default=None)

    p_pr = sub.add_parser("pr", help="Ingest Press Release feeds")
    p_pr.add_argument("--list", help="Path to list of RSS/Atom URLs", default=None)
    p_pr.add_argument("--user-agent", help="User-Agent header", default=None)

    args, unknown = ap.parse_known_args()

    # Rebuild sys.argv for the underlying script (so it can parse its own args)
    if args.cmd == "sec":
        sys.argv = [sys.argv[0]]
        if args.url: sys.argv += ["--url", args.url]
        if args.user_agent: sys.argv += ["--user-agent", args.user_agent]
        sec.main()
    elif args.cmd == "pr":
        sys.argv = [sys.argv[0]]
        if args.list: sys.argv += ["--list", args.list]
        if args.user_agent: sys.argv += ["--user-agent", args.user_agent]
        pr.main()

if __name__ == "__main__":
    main()
