from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class RawEvent(BaseModel):
    schema_version: str = Field(default="rawevent/1")
    source: str
    ts_utc: str
    ticker: Optional[str] = None
    cik: Optional[str] = None
    doc_type: Optional[str] = None
    url: Optional[str] = None
    headline: Optional[str] = None
    body_text: Optional[str] = None

class Fact(BaseModel):
    schema_version: str = Field(default="fact/1")
    event_id: str
    companies: List[str] = []
    entities: List[str] = []
    metrics: Dict[str, float] = {}
    tags: List[str] = []
    confidence: float = 0.0
    embedding_id: Optional[str] = None

class Signal(BaseModel):
    schema_version: str = Field(default="signal/1")
    ticker: Optional[str] = None
    score_components: Dict[str, float]
    score_total: float
    tier: str
    explanation: str
    provenance_event_ids: List[str]
    links: List[str] = []
