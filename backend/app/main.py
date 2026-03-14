# backend/app/main.py
from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from collections import Counter
import logging
import duckdb
import json
import pandas as pd
import requests
from src.core.insight_classification import (
    build_insight_extra_text,
    classify_insight_record,
    classify_probative_record,
    ensure_insight_classification_columns,
)
from src.core.ops_registry import ensure_ops_registry, sync_ops_case_registry
from .schemas import (
    InsightFacetsOut,
    InsightOut,
    SummaryOut,
    EntityOut,
    EventOut,
    OpsCaseOut,
    OpsArtifactOut,
    OpsSummaryOut,
)

app = FastAPI(title="Sentinela API", version="2.0")
log = logging.getLogger("sentinela.api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "./data/sentinela_analytics.duckdb"
CLASSIFICATION_FIELDS = [
    "esfera",
    "ente",
    "orgao",
    "municipio",
    "uf",
    "area_tematica",
    "sus",
]
PROBATIVE_FIELDS = [
    "classe_achado",
    "grau_probatorio",
    "fonte_primaria",
    "uso_externo",
    "inferencia_permitida",
    "limite_conclusao",
]

@app.get("/proxy")
def proxy(url: str):
    try:
        r = requests.get(url, timeout=10)
        # Filtra cabeçalhos que bloqueiam iframe (X-Frame-Options e CSP)
        excluded = ['x-frame-options', 'content-security-policy', 'content-encoding', 'transfer-encoding', 'connection']
        headers = {k: v for k, v in r.headers.items() if k.lower() not in excluded}
        return Response(content=r.content, status_code=r.status_code, headers=headers)
    except Exception as e:
        return Response(content=f"Error: {str(e)}", status_code=500)

def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


def get_rw_con():
    return duckdb.connect(DB_PATH)

def fix_mojibake(text: str) -> str:
    if not text or not isinstance(text, str): return text
    try:
        # Tenta corrigir UTF-8 interpretado como Latin-1 (CearÃ¡ -> Ceará)
        return text.encode('latin-1').decode('utf-8')
    except:
        return text


def normalize_text(text: Any) -> str:
    if text is None:
        return ""
    return " ".join(fix_mojibake(str(text)).upper().split())


def parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return value
    return value


def merge_classification(existing: Dict[str, Any], computed: Dict[str, Any]) -> Dict[str, Any]:
    merged = {}
    for field in CLASSIFICATION_FIELDS:
        current = existing.get(field)
        candidate = computed.get(field)
        if field == "sus":
            merged[field] = bool(current) or bool(candidate)
        else:
            merged[field] = candidate if candidate not in (None, "") else current
    return merged


def merge_probative(existing: Dict[str, Any], computed: Dict[str, Any]) -> Dict[str, Any]:
    merged = {}
    for field in PROBATIVE_FIELDS:
        current = existing.get(field)
        candidate = computed.get(field)
        merged[field] = candidate if candidate not in (None, "") else current
    return merged


def has_canonical_classification(row: Dict[str, Any]) -> bool:
    return bool(row.get("esfera") and row.get("ente") and row.get("uf"))


def hydrate_insight_records(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
) -> list[dict[str, Any]]:
    if df.empty:
        return []

    hydrated_df = df.copy()
    for field in CLASSIFICATION_FIELDS:
        if field not in hydrated_df.columns:
            hydrated_df[field] = False if field == "sus" else None
    for field in PROBATIVE_FIELDS:
        if field not in hydrated_df.columns:
            hydrated_df[field] = None

    extra_text_by_id = build_insight_extra_text(con, hydrated_df["id"].astype(str).tolist())
    records: list[dict[str, Any]] = []

    for row in hydrated_df.to_dict("records"):
        row["sources"] = parse_json_field(row.get("sources")) or []
        row["tags"] = parse_json_field(row.get("tags")) or []
        if has_canonical_classification(row):
            row["sus"] = bool(row.get("sus"))
        else:
            computed = classify_insight_record(
                row,
                extra_text=extra_text_by_id.get(row["id"], ""),
            )
            row.update(merge_classification(row, computed))
        row.update(
            merge_probative(
                row,
                classify_probative_record(
                    row,
                    extra_text=extra_text_by_id.get(row["id"], ""),
                ),
            )
        )
        for key in ["title", "description_md", "ente", "orgao", "municipio", "uf", "area_tematica"]:
            row[key] = fix_mojibake(row.get(key))
        records.append(row)

    return records


def matches_filter(value: Any, expected: str) -> bool:
    return normalize_text(expected) in normalize_text(value)


def filter_insight_records(
    records: list[dict[str, Any]],
    *,
    esfera: Optional[str] = None,
    ente: Optional[str] = None,
    orgao: Optional[str] = None,
    municipio: Optional[str] = None,
    uf: Optional[str] = None,
    area_tematica: Optional[str] = None,
    sus: Optional[bool] = None,
    classe_achado: Optional[str] = None,
    uso_externo: Optional[str] = None,
) -> list[dict[str, Any]]:
    filtered = records

    if esfera:
        filtered = [row for row in filtered if matches_filter(row.get("esfera"), esfera)]
    if ente:
        filtered = [row for row in filtered if matches_filter(row.get("ente"), ente)]
    if orgao:
        filtered = [row for row in filtered if matches_filter(row.get("orgao"), orgao)]
    if municipio:
        filtered = [row for row in filtered if matches_filter(row.get("municipio"), municipio)]
    if uf:
        filtered = [row for row in filtered if matches_filter(row.get("uf"), uf)]
    if area_tematica:
        filtered = [row for row in filtered if matches_filter(row.get("area_tematica"), area_tematica)]
    if sus is not None:
        filtered = [row for row in filtered if bool(row.get("sus")) is sus]
    if classe_achado:
        filtered = [row for row in filtered if matches_filter(row.get("classe_achado"), classe_achado)]
    if uso_externo:
        filtered = [row for row in filtered if matches_filter(row.get("uso_externo"), uso_externo)]

    return filtered


def query_insights_df(
    con: duckdb.DuckDBPyConnection,
    *,
    severity: Optional[str] = None,
    kind: Optional[str] = None,
    q: Optional[str] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM insight WHERE 1=1"
    params = []

    if severity:
        sql += " AND severity = ?"
        params.append(severity)
    if kind:
        sql += " AND kind = ?"
        params.append(kind)
    if q:
        sql += " AND (title ILIKE ? OR description_md ILIKE ?)"
        params.extend([f"%{q}%", f"%{q}%"])

    sql += (
        " ORDER BY CASE severity WHEN 'CRITICO' THEN 3 WHEN 'ALTO' THEN 2 "
        "WHEN 'MEDIO' THEN 1 ELSE 0 END DESC, exposure_brl DESC"
    )
    return con.execute(sql, params).df()


def to_buckets(values: list[str]) -> list[dict[str, Any]]:
    counts = Counter(v for v in values if v)
    return [
        {"value": value, "count": count}
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def sync_insight_classification() -> None:
    con = get_rw_con()
    try:
        ensure_insight_classification_columns(con)
        df = con.execute(
            """
            SELECT id, kind, title, description_md, pattern, sources, tags,
                   esfera, ente, orgao, municipio, uf, area_tematica, sus,
                   classe_achado, grau_probatorio, fonte_primaria,
                   uso_externo, inferencia_permitida, limite_conclusao
            FROM insight
            """
        ).df()
        if df.empty:
            return

        extra_text_by_id = build_insight_extra_text(con, df["id"].astype(str).tolist())
        updates = []
        for row in df.to_dict("records"):
            row["sources"] = parse_json_field(row.get("sources")) or []
            row["tags"] = parse_json_field(row.get("tags")) or []
            if has_canonical_classification(row):
                classification = {
                    "esfera": row.get("esfera"),
                    "ente": row.get("ente"),
                    "orgao": row.get("orgao"),
                    "municipio": row.get("municipio"),
                    "uf": row.get("uf"),
                    "area_tematica": row.get("area_tematica"),
                    "sus": bool(row.get("sus")),
                }
            else:
                classification = merge_classification(
                    row,
                    classify_insight_record(
                        row,
                        extra_text=extra_text_by_id.get(row["id"], ""),
                    ),
                )
            probative = merge_probative(
                row,
                classify_probative_record(
                    row,
                    extra_text=extra_text_by_id.get(row["id"], ""),
                ),
            )
            updates.append(
                [
                    classification["esfera"],
                    classification["ente"],
                    classification["orgao"],
                    classification["municipio"],
                    classification["uf"],
                    classification["area_tematica"],
                    classification["sus"],
                    probative["classe_achado"],
                    probative["grau_probatorio"],
                    probative["fonte_primaria"],
                    probative["uso_externo"],
                    probative["inferencia_permitida"],
                    probative["limite_conclusao"],
                    row["id"],
                ]
            )

        con.executemany(
            """
            UPDATE insight
            SET esfera = ?, ente = ?, orgao = ?, municipio = ?,
                uf = ?, area_tematica = ?, sus = ?,
                classe_achado = ?, grau_probatorio = ?, fonte_primaria = ?,
                uso_externo = ?, inferencia_permitida = ?, limite_conclusao = ?
            WHERE id = ?
            """,
            updates,
        )
    finally:
        con.close()


def sync_operational_registry() -> None:
    con = get_rw_con()
    try:
        ensure_ops_registry(con)
        sync_ops_case_registry(con)
    finally:
        con.close()


@app.on_event("startup")
def startup():
    try:
        sync_insight_classification()
    except Exception as exc:
        log.warning("Falha ao sincronizar classificacao de insights: %s", exc)
    try:
        sync_operational_registry()
    except Exception as exc:
        log.warning("Falha ao sincronizar registry operacional: %s", exc)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/meta/summary", response_model=SummaryOut)
def summary():
    con = get_con()
    res = con.execute("""
        SELECT
          (SELECT COUNT(*) FROM entity)  AS entities,
          (SELECT COUNT(*) FROM edge)    AS edges,
          (SELECT COUNT(DISTINCT source) FROM evidence) AS sources,
          (SELECT COUNT(*) FROM insight WHERE severity IN ('CRITICO','ALTO')) AS alerts,
          (SELECT MAX(captured_at) FROM evidence) AS last_updated;
    """).fetchone()
    con.close()
    return {
        "entities": res[0],
        "edges": res[1],
        "sources": res[2],
        "alerts": res[3],
        "last_updated": res[4]
    }

@app.get("/insights", response_model=List[InsightOut])
def list_insights(
    severity: Optional[str] = None,
    kind: Optional[str] = None,
    q: Optional[str] = None,
    esfera: Optional[str] = None,
    ente: Optional[str] = None,
    orgao: Optional[str] = None,
    municipio: Optional[str] = None,
    uf: Optional[str] = None,
    area_tematica: Optional[str] = None,
    sus: Optional[bool] = None,
    classe_achado: Optional[str] = None,
    uso_externo: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200)
):
    con = get_con()
    try:
        df = query_insights_df(con, severity=severity, kind=kind, q=q)
        records = hydrate_insight_records(con, df)
    finally:
        con.close()

    filtered = filter_insight_records(
        records,
        esfera=esfera,
        ente=ente,
        orgao=orgao,
        municipio=municipio,
        uf=uf,
        area_tematica=area_tematica,
        sus=sus,
        classe_achado=classe_achado,
        uso_externo=uso_externo,
    )
    return filtered[:limit]


@app.get("/meta/facets", response_model=InsightFacetsOut)
def insight_facets(
    severity: Optional[str] = None,
    kind: Optional[str] = None,
    q: Optional[str] = None,
    esfera: Optional[str] = None,
    ente: Optional[str] = None,
    orgao: Optional[str] = None,
    municipio: Optional[str] = None,
    uf: Optional[str] = None,
    area_tematica: Optional[str] = None,
    sus: Optional[bool] = None,
    classe_achado: Optional[str] = None,
    uso_externo: Optional[str] = None,
):
    con = get_con()
    try:
        df = query_insights_df(con, severity=severity, kind=kind, q=q)
        records = hydrate_insight_records(con, df)
    finally:
        con.close()

    filtered = filter_insight_records(
        records,
        esfera=esfera,
        ente=ente,
        orgao=orgao,
        municipio=municipio,
        uf=uf,
        area_tematica=area_tematica,
        sus=sus,
        classe_achado=classe_achado,
        uso_externo=uso_externo,
    )
    return {
        "esferas": to_buckets([row.get("esfera") for row in filtered]),
        "entes": to_buckets([row.get("ente") for row in filtered]),
        "orgaos": to_buckets([row.get("orgao") for row in filtered]),
        "municipios": to_buckets([row.get("municipio") for row in filtered]),
        "areas_tematicas": to_buckets([row.get("area_tematica") for row in filtered]),
        "classes_achado": to_buckets([row.get("classe_achado") for row in filtered]),
        "usos_externos": to_buckets([row.get("uso_externo") for row in filtered]),
        "sus": {
            "true": sum(1 for row in filtered if row.get("sus")),
            "false": sum(1 for row in filtered if not row.get("sus")),
        },
    }


@app.get("/ops/summary", response_model=OpsSummaryOut)
def ops_summary():
    con = get_con()
    try:
        total_cases = con.execute("SELECT COUNT(*) FROM ops_case_registry").fetchone()[0]
        external_ready = con.execute("SELECT COUNT(*) FROM ops_case_registry WHERE uso_externo IN ('APTO_APURACAO', 'PEDIDO_DOCUMENTAL', 'REPRESENTACAO_PRELIMINAR')").fetchone()[0]
        document_request_ready = con.execute("SELECT COUNT(*) FROM ops_case_registry WHERE estagio_operacional = 'APTO_OFICIO_DOCUMENTAL'").fetchone()[0]
        by_stage = dict(con.execute("SELECT estagio_operacional, COUNT(*) FROM ops_case_registry GROUP BY 1").fetchall())
        by_family = dict(con.execute("SELECT family, COUNT(*) FROM ops_case_registry GROUP BY 1").fetchall())
        last_updated = con.execute("SELECT MAX(updated_at) FROM ops_case_registry").fetchone()[0]
    finally:
        con.close()
    return {
        "total_cases": total_cases,
        "external_ready": external_ready,
        "document_request_ready": document_request_ready,
        "by_stage": by_stage,
        "by_family": by_family,
        "last_updated": last_updated,
    }


@app.get("/ops/cases", response_model=List[OpsCaseOut])
def ops_cases(
    family: Optional[str] = None,
    estagio_operacional: Optional[str] = None,
    uso_externo: Optional[str] = None,
    orgao: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(100, ge=1, le=300),
):
    con = get_con()
    try:
        sql = "SELECT * FROM v_ops_case_registry WHERE 1=1"
        params: list[Any] = []
        if family:
            sql += " AND family ILIKE ?"
            params.append(f"%{family}%")
        if estagio_operacional:
            sql += " AND estagio_operacional ILIKE ?"
            params.append(f"%{estagio_operacional}%")
        if uso_externo:
            sql += " AND uso_externo ILIKE ?"
            params.append(f"%{uso_externo}%")
        if orgao:
            sql += " AND orgao ILIKE ?"
            params.append(f"%{orgao}%")
        if q:
            sql += " AND (title ILIKE ? OR subject_name ILIKE ? OR resumo_curto ILIKE ?)"
            params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        sql += " LIMIT ?"
        params.append(limit)
        return json.loads(con.execute(sql, params).fetchdf().to_json(orient="records", force_ascii=False))
    finally:
        con.close()


@app.get("/ops/cases/{case_id}", response_model=OpsCaseOut)
def ops_case_detail(case_id: str):
    con = get_con()
    try:
        df = con.execute("SELECT * FROM ops_case_registry WHERE case_id = ?", [case_id]).fetchdf()
    finally:
        con.close()
    if df.empty:
        raise HTTPException(status_code=404, detail="Case not found")
    return json.loads(df.to_json(orient="records", force_ascii=False))[0]


@app.get("/ops/cases/{case_id}/artifacts", response_model=List[OpsArtifactOut])
def ops_case_artifacts(case_id: str):
    con = get_con()
    try:
        df = con.execute(
            "SELECT * FROM v_ops_case_artifact WHERE case_id = ? ORDER BY kind, label",
            [case_id],
        ).fetchdf()
    finally:
        con.close()
    return json.loads(df.to_json(orient="records", force_ascii=False))

@app.get("/timeline/{entity_id:path}")
def get_timeline(entity_id: str):
    con = get_con()
    
    # Normaliza o ID: remove prefixo se houver
    raw_id = entity_id.replace("rb_matricula:", "")
    
    # 1) Busca nome e valida existência
    row = con.execute("""
        SELECT split_part(servidor, '-', 2) AS nome
        FROM rb_servidores_mass
        WHERE split_part(servidor, '-', 1) = ?
        LIMIT 1
    """, [raw_id]).fetchone()

    if not row:
        con.close()
        raise HTTPException(status_code=404, detail=f"Entidade {raw_id} não localizada")

    nome = row[0]

    # 2) Homônimo gate
    n = con.execute("""
        SELECT COUNT(DISTINCT split_part(servidor, '-', 1)) 
        FROM rb_servidores_mass WHERE split_part(servidor, '-', 2) = ?
    """, [nome]).fetchone()[0]

    include_diarias = (n == 1)

    # 3) Build Timeline
    sql = f"""
        SELECT capturado_em AS occurred_at, 'salario' AS type, salario_liquido AS amount_brl, 
               'Folha (snapshot)' AS title, struct_pack(cargo := cargo, bruto := salario_bruto) AS attributes
        FROM rb_servidores_mass WHERE split_part(servidor, '-', 1) = ?
        { "UNION ALL SELECT data_saida::TIMESTAMP, 'diaria', valor, 'Viagem: ' || destino, struct_pack(motivo := motivo) FROM diarias WHERE servidor_nome = ?" if include_diarias else "" }
        ORDER BY occurred_at DESC
    """
    params = [raw_id, nome] if include_diarias else [raw_id]
    df = con.execute(sql, params).df()
    con.close()

    # Post-process
    df['title'] = df['title'].apply(fix_mojibake)
    for col in ['occurred_at']:
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    return {
        "entity_id": entity_id,
        "nome": fix_mojibake(nome),
        "diarias_included": include_diarias,
        "events": df.to_dict("records")
    }

@app.get("/entities/{entity_id:path}", response_model=EntityOut)
def get_entity(entity_id: str):
    con = get_con()
    res = con.execute("SELECT * FROM entity WHERE id = ?", [entity_id]).fetchone()
    con.close()
    if not res:
        raise HTTPException(status_code=404, detail="Entity not found")
    return {
        "id": res[0], "type": res[1], 
        "display_name": fix_mojibake(res[2]),
        "attributes": json.loads(res[3]) if isinstance(res[3], str) else res[3]
    }
