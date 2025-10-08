"""
Parse SEC Form 4 XML files to extract insider transaction details.

Transaction Codes:
- P = Open market purchase (BULLISH signal)
- S = Open market sale
- A = Award/grant (compensation, less meaningful)
- M = Exercise of derivative
- G = Gift
"""
import os
import re
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from xml.etree import ElementTree as ET
import requests


def fetch_form4_xml(accession_number: str, cik: str) -> Optional[str]:
    """
    Fetch Form 4 XML from SEC given accession number.
    Uses SEC_USER_AGENT for polite access.
    """
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua:
        raise ValueError("SEC_USER_AGENT environment variable required")
    
    acc_no_dashes = accession_number.replace("-", "")
    
    # Get index page to find XML link
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dashes}/{accession_number}-index.htm"
    headers = {"User-Agent": ua}
    
    try:
        resp = requests.get(index_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            # Find all wk-form4_*.xml links
            all_xml = re.findall(r'href="([^"]*wk-form4[^"]*\.xml)"', resp.text)
            
            # Prefer non-xslF345X05 paths (raw XML vs transformed HTML)
            raw_xml_paths = [p for p in all_xml if 'xslF345X05' not in p]
            xml_paths_to_try = raw_xml_paths if raw_xml_paths else all_xml
            
            for xml_path in xml_paths_to_try:
                # Handle both relative and absolute paths
                if xml_path.startswith('/'):
                    xml_url = f"https://www.sec.gov{xml_path}"
                else:
                    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dashes}/{xml_path}"
                
                time.sleep(0.15)  # Polite rate limit
                resp = requests.get(xml_url, headers=headers, timeout=10)
                if resp.status_code == 200 and b"<ownershipDocument>" in resp.content:
                    return resp.text
    except Exception as e:
        print(f"Error fetching Form 4 XML: {e}")
    
    return None


def parse_form4_xml(xml_text: str) -> Dict[str, Any]:
    """
    Parse Form 4 XML and extract transaction details.
    
    Returns dict with:
        - issuer_cik
        - issuer_name
        - issuer_ticker
        - reporting_owner_name
        - reporting_owner_cik
        - is_director
        - is_officer
        - transactions: list of transaction dicts
    """
    root = ET.fromstring(xml_text)
    
    # Extract issuer info
    issuer = root.find("issuer")
    issuer_cik = issuer.findtext("issuerCik", "").strip() if issuer is not None else ""
    issuer_name = issuer.findtext("issuerName", "").strip() if issuer is not None else ""
    issuer_ticker = issuer.findtext("issuerTradingSymbol", "").strip() if issuer is not None else ""
    
    # Extract reporting owner
    owner = root.find("reportingOwner")
    owner_name = ""
    owner_cik = ""
    is_director = False
    is_officer = False
    
    if owner is not None:
        owner_id = owner.find("reportingOwnerId")
        if owner_id is not None:
            owner_cik = owner_id.findtext("rptOwnerCik", "").strip()
            owner_name = owner_id.findtext("rptOwnerName", "").strip()
        
        relationship = owner.find("reportingOwnerRelationship")
        if relationship is not None:
            is_director = relationship.findtext("isDirector") == "1"
            is_officer = relationship.findtext("isOfficer") == "1"
    
    # Extract transactions
    transactions: List[Dict[str, Any]] = []
    
    # Non-derivative transactions (common stock)
    non_deriv_table = root.find("nonDerivativeTable")
    if non_deriv_table is not None:
        for txn in non_deriv_table.findall("nonDerivativeTransaction"):
            trans_data = _parse_transaction(txn)
            if trans_data:
                transactions.append(trans_data)
    
    return {
        "issuer_cik": issuer_cik,
        "issuer_name": issuer_name,
        "issuer_ticker": issuer_ticker,
        "reporting_owner_name": owner_name,
        "reporting_owner_cik": owner_cik,
        "is_director": is_director,
        "is_officer": is_officer,
        "transactions": transactions,
    }


def _parse_transaction(txn_elem) -> Optional[Dict[str, Any]]:
    """Parse a single nonDerivativeTransaction element."""
    try:
        security = txn_elem.findtext("securityTitle/value", "").strip()
        date = txn_elem.findtext("transactionDate/value", "").strip()
        code = txn_elem.findtext("transactionCoding/transactionCode", "").strip()
        
        amounts = txn_elem.find("transactionAmounts")
        if amounts is None:
            return None
        
        shares_str = amounts.findtext("transactionShares/value", "0")
        price_str = amounts.findtext("transactionPricePerShare/value", "0")
        acq_disp = amounts.findtext("transactionAcquiredDisposedCode/value", "").strip()
        
        try:
            shares = float(shares_str)
            price = float(price_str)
        except (ValueError, TypeError):
            shares = 0
            price = 0
        
        post_txn = txn_elem.find("postTransactionAmounts")
        shares_owned = 0
        if post_txn is not None:
            owned_str = post_txn.findtext("sharesOwnedFollowingTransaction/value", "0")
            try:
                shares_owned = float(owned_str)
            except (ValueError, TypeError):
                pass
        
        return {
            "security_title": security,
            "transaction_date": date,
            "transaction_code": code,
            "shares": shares,
            "price_per_share": price,
            "acquired_disposed": acq_disp,  # A=acquired, D=disposed
            "shares_owned_after": shares_owned,
        }
    except Exception:
        return None


def transaction_type_description(code: str) -> str:
    """Human-readable transaction type."""
    types = {
        "P": "Open Market Purchase",
        "S": "Open Market Sale",
        "A": "Award/Grant",
        "M": "Exercise of Options",
        "G": "Gift",
        "D": "Disposition",
        "F": "Payment of Exercise Price",
        "I": "Discretionary Transaction",
        "X": "Exercise of In-the-Money Options",
    }
    return types.get(code.upper(), f"Other ({code})")


def is_bullish_transaction(txn: Dict[str, Any]) -> bool:
    """
    Determine if transaction signals bullish sentiment.
    Open market purchases (P) = bullish
    Sales (S) = bearish
    Awards/grants (A) = neutral (compensation)
    """
    code = txn.get("transaction_code", "").upper()
    acq_disp = txn.get("acquired_disposed", "").upper()
    
    # Open market purchase
    if code == "P" and acq_disp == "A":
        return True
    
    # Exercise and hold (not selling immediately)
    if code == "M" and acq_disp == "A":
        return True
    
    return False
