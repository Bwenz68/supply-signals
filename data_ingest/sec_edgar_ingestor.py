import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
import urllib.request

OUT_DIR = Path(os.getenv("RAW_QUEUE_DIR", "queue/raw_events"))

def fetch_text(url: str, user_agent: str) -> str:
    if url.startswith("file:"):
        # local TSV for offline dev
        path = url[5:]
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")

def parse_tsv(feed_text: str):
    """
    Very simple TSV format:
    TIMESTAMP\tCIK\tTICKER\tCOMPANY\tDOCTYPE\tURL\tTITLE
    """
    for line in feed_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        ts, cik, ticker, company, doctype, url, title = parts[:7]
        yield {
            "source": "SEC",
            "title": title,
            "body": None,
            "ts": ts,
            "meta": {
                "source_name": "SEC-EDGAR",
                "doc_type": doctype,
                "cik": (cik or None),
                "ticker": (ticker or None),
                "company_name": (company or None),
                "filing_datetime": ts,
                "urls": [url] if url else [],
            },
        }

def write_ndjson(items, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"sec_{now}.jsonl"
    count = 0
    with out_path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
            count += 1
    print(f"[SEC] wrote: {out_path} ({count} items)")
    return out_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("SEC_FEED_URL", "file:ref/sec_feed_mock.tsv"))
    ap.add_argument("--user-agent", default=os.getenv("SEC_USER_AGENT", "supply-signals/phase1 (contact: you@example.com)"))
    args = ap.parse_args()

    text = fetch_text(args.url, args.user_agent)
    items = list(parse_tsv(text))
    write_ndjson(items, OUT_DIR)

if __name__ == "__main__":
    main()
