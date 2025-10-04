import csv
from pathlib import Path
from typing import Dict, Optional

def load_map(path: Path = Path("ref/cik_tickers.csv")) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Load a simple CIK->(ticker, company) map from CSV with headers:
      CIK,ticker,company_name
    Returns dict keyed by CIK with no leading zeros.
    """
    m: Dict[str, Dict[str, Optional[str]]] = {}
    if not path.exists():
        return m
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            cik_raw = (row.get("CIK") or "").strip()
            if not cik_raw:
                continue
            cik = cik_raw.lstrip("0")  # normalize to no leading zeros
            m[cik] = {
                "ticker": (row.get("ticker") or row.get("Ticker") or None),
                "company": (row.get("company_name") or row.get("Company") or None),
            }
    return m
