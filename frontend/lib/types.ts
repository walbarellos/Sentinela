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
