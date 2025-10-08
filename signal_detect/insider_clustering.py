"""
Detect insider transaction clustering patterns.

Clustering signals:
- 3+ insiders buying within 30 days = STRONG BULLISH
- 3+ insiders selling within 30 days = BEARISH
- Mixed activity = NEUTRAL
"""
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import defaultdict

from data_ingest.form4_parser import fetch_form4_xml, parse_form4_xml, is_bullish_transaction


def analyze_form4_file(filepath: Path) -> List[Dict[str, Any]]:
    """
    Parse all Form 4s in a JSONL file and extract transaction details.
    
    Returns list of enriched transactions with insider info.
    """
    transactions = []
    
    with filepath.open("r", encoding="utf-8") as f:
        for line in f:
            filing = json.loads(line)
            
            acc_num = filing.get("accession_number")
            cik = filing.get("cik")
            
            if not (acc_num and cik):
                continue
            
            # Fetch and parse XML
            xml = fetch_form4_xml(acc_num, cik)
            if not xml:
                continue
            
            parsed = parse_form4_xml(xml)
            
            # Extract each transaction with insider context
            for txn in parsed["transactions"]:
                transactions.append({
                    "issuer_cik": parsed["issuer_cik"],
                    "issuer_name": parsed["issuer_name"],
                    "issuer_ticker": parsed["issuer_ticker"],
                    "insider_name": parsed["reporting_owner_name"],
                    "insider_cik": parsed["reporting_owner_cik"],
                    "is_director": parsed["is_director"],
                    "is_officer": parsed["is_officer"],
                    "transaction_date": txn["transaction_date"],
                    "transaction_code": txn["transaction_code"],
                    "shares": txn["shares"],
                    "price_per_share": txn["price_per_share"],
                    "acquired_disposed": txn["acquired_disposed"],
                    "is_bullish": is_bullish_transaction(txn),
                    "accession_number": acc_num,
                })
    
    return transactions


def detect_clusters(transactions: List[Dict[str, Any]], window_days: int = 30, min_insiders: int = 3) -> List[Dict[str, Any]]:
    """
    Detect clustering: when min_insiders or more insiders transact within window_days.
    
    Returns list of cluster signals.
    """
    if not transactions:
        return []
    
    # Sort by date
    transactions = sorted(transactions, key=lambda x: x["transaction_date"])
    
    clusters = []
    
    # Group by company
    by_company = defaultdict(list)
    for txn in transactions:
        by_company[txn["issuer_cik"]].append(txn)
    
    # Detect clusters per company
    for cik, company_txns in by_company.items():
        company_txns = sorted(company_txns, key=lambda x: x["transaction_date"])
        
        # Sliding window approach
        for i, anchor_txn in enumerate(company_txns):
            anchor_date = datetime.fromisoformat(anchor_txn["transaction_date"])
            window_end = anchor_date + timedelta(days=window_days)
            
            # Find all transactions within window
            window_txns = []
            unique_insiders = set()
            
            for txn in company_txns[i:]:
                txn_date = datetime.fromisoformat(txn["transaction_date"])
                if txn_date <= window_end:
                    window_txns.append(txn)
                    unique_insiders.add(txn["insider_cik"])
                else:
                    break
            
            # Check if cluster criteria met
            if len(unique_insiders) >= min_insiders:
                # Analyze cluster sentiment
                bullish_count = sum(1 for t in window_txns if t["is_bullish"])
                bearish_count = sum(1 for t in window_txns if not t["is_bullish"] and t["transaction_code"] in ["S", "D"])
                
                total_shares = sum(t["shares"] for t in window_txns)
                
                # Determine cluster signal
                if bullish_count >= min_insiders:
                    signal_type = "STRONG_BULLISH"
                    score = 8
                elif bearish_count >= min_insiders:
                    signal_type = "BEARISH"
                    score = 2
                elif bullish_count > bearish_count:
                    signal_type = "BULLISH"
                    score = 6
                else:
                    signal_type = "MIXED"
                    score = 4
                
                cluster = {
                    "signal_type": "insider_cluster",
                    "issuer_cik": cik,
                    "issuer_name": window_txns[0]["issuer_name"],
                    "issuer_ticker": window_txns[0]["issuer_ticker"],
                    "cluster_start_date": anchor_txn["transaction_date"],
                    "cluster_end_date": window_txns[-1]["transaction_date"],
                    "window_days": window_days,
                    "num_insiders": len(unique_insiders),
                    "num_transactions": len(window_txns),
                    "total_shares": int(total_shares),
                    "bullish_transactions": bullish_count,
                    "bearish_transactions": bearish_count,
                    "sentiment": signal_type,
                    "score": score,
                    "insiders": [
                        {
                            "name": t["insider_name"],
                            "role": "Director" if t["is_director"] else "Officer" if t["is_officer"] else "Other",
                            "transaction_code": t["transaction_code"],
                            "shares": int(t["shares"]),
                            "date": t["transaction_date"],
                        }
                        for t in window_txns
                    ],
                }
                
                clusters.append(cluster)
                
                # Skip ahead to avoid overlapping clusters
                break
    
    return clusters


def main():
    """Process all Form 4 files and detect clustering signals."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Detect insider transaction clustering")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("queue/raw_events"),
        help="Directory containing form4_*.jsonl files"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("queue/signals"),
        help="Directory for cluster signals"
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Clustering window in days"
    )
    parser.add_argument(
        "--min-insiders",
        type=int,
        default=3,
        help="Minimum insiders for cluster"
    )
    
    args = parser.parse_args()
    
    # Ensure SEC_USER_AGENT is set
    if not os.environ.get("SEC_USER_AGENT"):
        print("ERROR: SEC_USER_AGENT environment variable required")
        return 1
    
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all Form 4 files
    form4_files = sorted(args.input_dir.glob("form4_*.jsonl"))
    
    if not form4_files:
        print("[INSIDER_CLUSTER] No Form 4 files found")
        return 0
    
    print(f"[INSIDER_CLUSTER] Processing {len(form4_files)} Form 4 file(s)")
    
    all_transactions = []
    
    # Parse all Form 4s
    for filepath in form4_files:
        print(f"[INSIDER_CLUSTER] Parsing {filepath.name}...")
        transactions = analyze_form4_file(filepath)
        all_transactions.extend(transactions)
        print(f"[INSIDER_CLUSTER]   Found {len(transactions)} transaction(s)")
    
    if not all_transactions:
        print("[INSIDER_CLUSTER] No transactions extracted")
        return 0
    
    # Detect clusters
    print(f"[INSIDER_CLUSTER] Detecting clusters (window={args.window_days}d, min_insiders={args.min_insiders})...")
    clusters = detect_clusters(all_transactions, args.window_days, args.min_insiders)
    
    if not clusters:
        print("[INSIDER_CLUSTER] No clusters detected")
        return 0
    
    # Write signals
    from datetime import datetime as dt
    timestamp = dt.now().strftime("%Y%m%d-%H%M%S")
    output_path = args.output_dir / f"insider_clusters_{timestamp}.jsonl"
    
    with output_path.open("w", encoding="utf-8") as f:
        for cluster in clusters:
            f.write(json.dumps(cluster, ensure_ascii=False) + "\n")
    
    print(f"[INSIDER_CLUSTER] Wrote {len(clusters)} cluster signal(s) to {output_path.name}")
    
    # Summary
    for cluster in clusters:
        print(f"\n🔔 {cluster['sentiment']} CLUSTER: {cluster['issuer_ticker']} ({cluster['issuer_name']})")
        print(f"   {cluster['num_insiders']} insiders, {cluster['num_transactions']} transactions")
        print(f"   Period: {cluster['cluster_start_date']} to {cluster['cluster_end_date']}")
        print(f"   Score: {cluster['score']}/10")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
