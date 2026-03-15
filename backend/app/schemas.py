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
    classe_achado: Optional[str] = None
    grau_probatorio: Optional[str] = None
    fonte_primaria: Optional[str] = None
    uso_externo: Optional[str] = None
    inferencia_permitida: Optional[str] = None
    limite_conclusao: Optional[str] = None
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
    cases: int
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
    classes_achado: List[FacetBucketOut] = []
    usos_externos: List[FacetBucketOut] = []
    sus: Dict[str, int] = {}


class OpsCaseOut(BaseModel):
    case_id: str
    family: Optional[str] = None
    title: str
    subtitle: Optional[str] = None
    subject_name: Optional[str] = None
    subject_doc: Optional[str] = None
    esfera: Optional[str] = None
    ente: Optional[str] = None
    orgao: Optional[str] = None
    municipio: Optional[str] = None
    uf: Optional[str] = None
    area_tematica: Optional[str] = None
    severity: Optional[str] = None
    classe_achado: Optional[str] = None
    uso_externo: Optional[str] = None
    estagio_operacional: Optional[str] = None
    status_operacional: Optional[str] = None
    prioridade: Optional[int] = None
    valor_referencia_brl: Optional[float] = None
    source_table: Optional[str] = None
    source_row_ref: Optional[str] = None
    resumo_curto: Optional[str] = None
    proximo_passo: Optional[str] = None
    bundle_path: Optional[str] = None
    bundle_sha256: Optional[str] = None
    artifact_count: int = 0
    updated_at: datetime


class OpsArtifactOut(BaseModel):
    artifact_id: str
    case_id: str
    label: Optional[str] = None
    kind: Optional[str] = None
    path: Optional[str] = None
    exists: bool = False
    sha256: Optional[str] = None
    size_bytes: Optional[int] = None
    metadata_json: Any = {}
    updated_at: datetime


class OpsSummaryOut(BaseModel):
    total_cases: int
    external_ready: int
    document_request_ready: int
    by_stage: Dict[str, int] = {}
    by_family: Dict[str, int] = {}
    last_updated: Optional[datetime] = None
