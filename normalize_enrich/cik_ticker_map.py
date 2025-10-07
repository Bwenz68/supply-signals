"""
Load company universe from ref/universe.tsv with sector/industry data.
"""
import csv
from pathlib import Path
from typing import Dict, Optional

def load_universe(path: Path = Path("ref/universe.tsv")) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Load company universe from TSV with headers:
      ticker  cik  name  sector  industry
    
    Returns dict keyed by normalized CIK (no leading zeros) with:
      {
        "ticker": str,
        "company": str,
        "sector": str,
        "industry": str
      }
    """
    m: Dict[str, Dict[str, Optional[str]]] = {}
    
    if not path.exists():
        return m
    
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter='\t')
        for row in rdr:
            cik_raw = (row.get("cik") or "").strip()
            if not cik_raw:
                continue
            
            # Normalize CIK (remove leading zeros)
            cik = cik_raw.lstrip("0") or "0"
            
            m[cik] = {
                "ticker": (row.get("ticker") or "").strip() or None,
                "company": (row.get("name") or "").strip() or None,
                "sector": (row.get("sector") or "").strip() or None,
                "industry": (row.get("industry") or "").strip() or None,
            }
    
    return m


def load_map(path: Path = Path("ref/cik_tickers.csv")) -> Dict[str, Dict[str, Optional[str]]]:
    """
    LEGACY: Load from old CSV format for backward compatibility.
    Try universe.tsv first, fall back to cik_tickers.csv.
    """
    # Try new universe format first
    universe_path = Path("ref/universe.tsv")
    if universe_path.exists():
        return load_universe(universe_path)
    
    # Fall back to old CSV format
    m: Dict[str, Dict[str, Optional[str]]] = {}
    if not path.exists():
        return m
    
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            cik_raw = (row.get("CIK") or "").strip()
            if not cik_raw:
                continue
            cik = cik_raw.lstrip("0")
            m[cik] = {
                "ticker": (row.get("ticker") or row.get("Ticker") or None),
                "company": (row.get("company_name") or row.get("Company") or None),
                "sector": None,
                "industry": None,
            }
    return m
