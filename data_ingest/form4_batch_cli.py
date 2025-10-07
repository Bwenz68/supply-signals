#!/usr/bin/env python3
"""
Batch Form 4 ingestion - fetches all recent Form 4s and filters by universe.

Usage:
    python -m data_ingest.form4_batch_cli --universe ref/universe.tsv --since 2024-09-01
"""
import argparse
import csv
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Set, Dict


def load_universe_ciks(universe_path: Path) -> Set[str]:
    """Load CIKs from universe.tsv, normalized to 10 digits."""
    ciks = set()
    if not universe_path.exists():
        return ciks
    
    with universe_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            cik_raw = row.get("cik", "").strip()
            if cik_raw:
                # Normalize to 10 digits
                cik_normalized = f"{int(cik_raw):010d}"
                ciks.add(cik_normalized)
    
    return ciks


def extract_issuer_cik(title: str) -> str | None:
    """
    Extract issuer CIK from Form 4 title.
    Examples:
        "4 - Reddit, Inc. (0001713445) (Issuer)" -> "0001713445"
        "4 - Newhouse Steven O (0001913168) (Reporting)" -> None
    """
    if "(Issuer)" not in title:
        return None
    
    # Match CIK in parentheses before (Issuer)
    match = re.search(r'\((\d+)\)\s*\(Issuer\)', title)
    if match:
        cik_raw = match.group(1)
        return f"{int(cik_raw):010d}"
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Fetch recent Form 4s and filter by universe CIKs"
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("ref/universe.tsv"),
        help="Path to universe.tsv"
    )
    parser.add_argument(
        "--since",
        help="ISO date to fetch from (e.g., 2024-09-01)"
    )
    parser.add_argument(
        "--max",
        type=int,
        default=100,
        help="Max Form 4s to fetch from SEC feed"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("queue/raw_events"),
        help="Output directory for filtered Form 4s"
    )
    
    args = parser.parse_args()
    
    # Load universe CIKs
    universe_ciks = load_universe_ciks(args.universe)
    if not universe_ciks:
        print("ERROR: No CIKs found in universe file", file=sys.stderr)
        return 1
    
    print(f"[FORM4] Loaded {len(universe_ciks)} CIKs from universe")
    
    # Fetch Form 4s using existing SEC CLI
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as tmp:
        tmp_path = tmp.name
    
    cmd = [
        sys.executable, "-m", "data_ingest.sec_edgar_cli",
        "--url", f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count={args.max}&output=atom",
        "--forms", "4",
        "--max", str(args.max),
        "--out", tmp_path
    ]
    
    if args.since:
        cmd.extend(["--since", args.since])
    
    print(f"[FORM4] Fetching Form 4s from SEC...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"ERROR: SEC fetch failed\n{result.stderr}", file=sys.stderr)
        Path(tmp_path).unlink(missing_ok=True)
        return 1
    
    # Filter by universe CIKs
    matched_filings: Dict[str, list] = {}  # cik -> list of filings
    total_fetched = 0
    total_matched = 0
    
    tmp_file = Path(tmp_path)
    if tmp_file.exists():
        with tmp_file.open("r", encoding="utf-8") as f:
            for line in f:
                total_fetched += 1
                filing = json.loads(line)
                
                # Extract issuer CIK from title
                issuer_cik = extract_issuer_cik(filing.get("title", ""))
                
                if issuer_cik and issuer_cik in universe_ciks:
                    if issuer_cik not in matched_filings:
                        matched_filings[issuer_cik] = []
                    
                    # Add extracted CIK to filing
                    filing["cik"] = issuer_cik
                    matched_filings[issuer_cik].append(filing)
                    total_matched += 1
        
        tmp_file.unlink()
    
    print(f"[FORM4] Fetched {total_fetched} Form 4s, matched {total_matched} to universe")
    
    # Write filtered filings
    if not matched_filings:
        print("[FORM4] No Form 4s matched universe companies")
        return 0
    
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    for cik, filings in matched_filings.items():
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = args.output_dir / f"form4_{cik}_{ts}.jsonl"
        
        with out_path.open("w", encoding="utf-8") as f:
            for filing in filings:
                f.write(json.dumps(filing, ensure_ascii=False) + "\n")
        
        print(f"[FORM4] Wrote {len(filings)} Form 4(s) for CIK {cik} to {out_path.name}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
