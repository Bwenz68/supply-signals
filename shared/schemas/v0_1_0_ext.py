from typing import Optional, List, Dict
from pydantic import BaseModel, Field
from datetime import datetime

# NOTE:
# - We do NOT import or change your Phase-0 models here.
# - These "Ext" models are OPTIONAL add-ons. You can merge their fields
#   into dicts or use them as helper models during ingest/normalize.
# - Downstream code must treat every field here as optional.

class RawItemExt(BaseModel):
    source_name: Optional[str] = Field(None, description="e.g., 'SEC-EDGAR', 'NewsroomRSS'")
    doc_type: Optional[str] = Field(None, description="e.g., '8-K', '10-Q', 'PR'")
    cik: Optional[str] = None
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    filing_datetime: Optional[datetime] = None
    urls: Optional[List[str]] = None
    extracted: Optional[Dict[str, str]] = None  # e.g., {'headline':..., 'body':...}

class NormalizedItemExt(BaseModel):
    canonical_company: Optional[str] = None
    canonical_ticker: Optional[str] = None
    canonical_cik: Optional[str] = None
    event_datetime_utc: Optional[datetime] = None
    event_kind: Optional[str] = None       # 'SEC' | 'PR'
    event_subtype: Optional[str] = None    # '8-K', 'PR-Guidance', etc.
    urls: Optional[List[str]] = None
