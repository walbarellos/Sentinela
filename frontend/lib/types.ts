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
  created_at: string;
};

export type Summary = {
  entities: number;
  edges: number;
  sources: number;
  alerts: number;
  last_updated?: string | null;
};
