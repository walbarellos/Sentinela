// frontend/lib/types.ts
export type Severity = "CRITICO" | "ALTO" | "MEDIO" | "BAIXO";

export type Insight = {
  id: string;
  kind: string;
  severity: Severity;
  confidence: number;
  exposure_brl?: number;
  title: string;
  description_md: string;
  pattern?: string;
  sources: string[];
  tags: string[];
  esfera?: string;
  ente?: string;
  orgao?: string;
  municipio?: string;
  uf?: string;
  area_tematica?: string;
  sus?: boolean;
  classe_achado?: string;
  grau_probatorio?: string;
  fonte_primaria?: string;
  uso_externo?: string;
  inferencia_permitida?: string;
  limite_conclusao?: string;
  created_at: string;
};

export type Summary = {
  entities: number;
  edges: number;
  sources: number;
  alerts: number;
  last_updated?: string | null;
};

export type FacetBucket = {
  value: string;
  count: number;
};

export type InsightFacets = {
  esferas: FacetBucket[];
  entes: FacetBucket[];
  orgaos: FacetBucket[];
  municipios: FacetBucket[];
  areas_tematicas: FacetBucket[];
  classes_achado: FacetBucket[];
  usos_externos: FacetBucket[];
  sus: Record<string, number>;
};

export type OpsCase = {
  case_id: string;
  family?: string;
  title: string;
  subtitle?: string;
  subject_name?: string;
  subject_doc?: string;
  esfera?: string;
  ente?: string;
  orgao?: string;
  municipio?: string;
  uf?: string;
  area_tematica?: string;
  severity?: string;
  classe_achado?: string;
  uso_externo?: string;
  estagio_operacional?: string;
  status_operacional?: string;
  prioridade?: number;
  valor_referencia_brl?: number;
  source_table?: string;
  source_row_ref?: string;
  resumo_curto?: string;
  proximo_passo?: string;
  bundle_path?: string;
  bundle_sha256?: string;
  artifact_count: number;
  updated_at: string;
};

export type OpsArtifact = {
  artifact_id: string;
  case_id: string;
  label?: string;
  kind?: string;
  path?: string;
  exists: boolean;
  sha256?: string;
  size_bytes?: number;
  metadata_json?: unknown;
  updated_at: string;
};

export type OpsSummary = {
  total_cases: number;
  external_ready: number;
  document_request_ready: number;
  by_stage: Record<string, number>;
  by_family: Record<string, number>;
  last_updated?: string | null;
};
