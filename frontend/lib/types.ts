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
  sus: Record<string, number>;
};
