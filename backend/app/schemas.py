# backend/app/schemas.py
from pydantic import BaseModel
from typing import Any, Optional, List, Dict
from datetime import datetime

class InsightOut(BaseModel):
    id: str
    kind: str
    severity: str
    confidence: int
    exposure_brl: Optional[float] = None
    title: str
    description_md: str
    pattern: Optional[str] = None
    sources: List[Any] = []
    tags: List[Any] = []
    sample_n: int = 0
    unit_total: float = 0.0
    esfera: Optional[str] = None
    ente: Optional[str] = None
    orgao: Optional[str] = None
    municipio: Optional[str] = None
    uf: Optional[str] = None
    area_tematica: Optional[str] = None
    sus: bool = False
    created_at: datetime

class EvidenceOut(BaseModel):
    id: str
    source: str
    source_kind: str
    captured_at: datetime
    uri: Optional[str] = None
    content_sha256: Optional[str] = None
    excerpt: Any = {}
    pii_redacted: bool = True

class EntityOut(BaseModel):
    id: str
    type: str
    display_name: str
    attributes: Any = {}

class EventOut(BaseModel):
    type: str
    occurred_at: Optional[datetime] = None
    occurred_to: Optional[datetime] = None
    title: Optional[str] = None
    amount_brl: Optional[float] = None
    attributes: Any = {}

class SummaryOut(BaseModel):
    entities: int
    edges: int
    sources: int
    alerts: int
    last_updated: Optional[datetime] = None


class FacetBucketOut(BaseModel):
    value: str
    count: int


class InsightFacetsOut(BaseModel):
    esferas: List[FacetBucketOut] = []
    entes: List[FacetBucketOut] = []
    orgaos: List[FacetBucketOut] = []
    municipios: List[FacetBucketOut] = []
    areas_tematicas: List[FacetBucketOut] = []
    sus: Dict[str, int] = {}
