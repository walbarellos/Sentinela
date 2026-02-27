// frontend/lib/api.ts
import type { Insight, Summary } from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

export async function fetchSummary(): Promise<Summary> {
  const r = await fetch(`${API_BASE}/meta/summary`, { cache: "no-store" });
  if (!r.ok) throw new Error("summary failed");
  return r.json();
}

export async function fetchInsights(params: Record<string, string | number | undefined>): Promise<Insight[]> {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== "") qs.set(k, String(v));
  });
  const r = await fetch(`${API_BASE}/insights?${qs.toString()}`, { cache: "no-store" });
  if (!r.ok) throw new Error("insights failed");
  return (await r.json()) as Insight[];
}

export async function fetchEntityTimeline(entityId: string) {
  const r = await fetch(`${API_BASE}/entities/${encodeURIComponent(entityId)}/timeline`, { cache: "no-store" });
  if (!r.ok) throw new Error("timeline failed");
  return r.json();
}
