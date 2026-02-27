# backend/app/main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import duckdb
import json
import pandas as pd
from .schemas import InsightOut, SummaryOut, EntityOut, EventOut

app = FastAPI(title="Sentinela API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "./data/sentinela_analytics.duckdb"

def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

def fix_mojibake(text: str) -> str:
    if not text or not isinstance(text, str): return text
    try:
        # Tenta corrigir UTF-8 interpretado como Latin-1 (CearÃ¡ -> Ceará)
        return text.encode('latin-1').decode('utf-8')
    except:
        return text

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
    limit: int = Query(50, ge=1, le=200)
):
    con = get_con()
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
        
    sql += " ORDER BY CASE severity WHEN 'CRITICO' THEN 3 WHEN 'ALTO' THEN 2 WHEN 'MEDIO' THEN 1 ELSE 0 END DESC, exposure_brl DESC LIMIT ?"
    params.append(limit)
    
    df = con.execute(sql, params).df()
    con.close()
    
    for col in ['sources', 'tags']:
        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    
    df['title'] = df['title'].apply(fix_mojibake)
    df['description_md'] = df['description_md'].apply(fix_mojibake)
    return df.to_dict('records')

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
