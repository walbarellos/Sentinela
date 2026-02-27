# scripts/sync_v2.py
import json, hashlib, uuid, re, unicodedata
import duckdb
import pandas as pd
from datetime import datetime

DB = "./data/sentinela_analytics.duckdb"

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def ulid_like() -> str:
    return str(uuid.uuid4())

def parse_matricula_from_insight_id(insight_id: str) -> str:
    # SAL_537703/1_0 -> 537703/1
    parts = insight_id.split("_")
    if len(parts) > 1 and "/" in parts[1]:
        return parts[1]
    return ""

def parse_mes(mes_id):
    try:
        m = int(mes_id)
        if 1 <= m <= 12:
            return m
    except Exception:
        pass
    return None

def norm_name(s: str) -> str:
    if not s: return ""
    s = s.strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def safe_json(obj):
    def handler(o):
        if hasattr(o, 'isoformat'):
            return o.isoformat()
        if isinstance(o, pd.Timestamp):
            return o.isoformat()
        return str(o)
    return json.dumps(obj, default=handler, ensure_ascii=False)

def ensure_v2_tables(con):
    con.execute(open("v2_core.sql","r",encoding="utf-8").read())

def upsert_entity(con, entity_id, type_, display_name, attributes=None):
    con.execute(
        "INSERT OR REPLACE INTO entity VALUES (?, ?, ?, ?)",
        [entity_id, type_, display_name, safe_json(attributes or {})]
    )

def insert_event(con, event):
    con.execute(
        "INSERT OR REPLACE INTO event VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            event["id"], event["type"], event.get("occurred_at"),
            event.get("occurred_to"), event.get("amount_brl"),
            event.get("title"), safe_json(event.get("attributes", {}))
        ]
    )

def insert_insight(con, ins):
    con.execute(
        "INSERT OR REPLACE INTO insight VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())",
        [
            ins["id"],
            ins.get("kind","salario"),
            ins["severidade"],
            int(ins["confianca"]),
            float(ins.get("exposicao") or 0.0),
            ins["titulo"],
            ins["descricao"],
            ins.get("pattern"),
            safe_json(ins.get("fontes", [])),
            safe_json(ins.get("tags", [])),
            int(ins.get("n_amostra") or 0),
            float(ins.get("total_unidade") or 0.0),
        ]
    )

def link_insight(con, insight_id, entity_id=None, event_id=None):
    link_id = f"il:{insight_id}:{entity_id or 'none'}:{event_id or 'none'}"
    con.execute(
        "INSERT OR REPLACE INTO insight_link VALUES (?, ?, ?, ?)",
        [link_id, insight_id, entity_id, event_id]
    )

def insert_evidence(con, source, source_kind, excerpt_obj, uri=None, payload_ref=None):
    raw = safe_json(excerpt_obj)
    evid_id = ulid_like()
    con.execute(
        "INSERT INTO evidence VALUES (?, ?, ?, now(), ?, ?, ?, ?, TRUE)",
        [evid_id, source, source_kind, uri, sha256_str(raw), payload_ref, raw]
    )
    return evid_id

def link_evidence(con, evidence_id, role, insight_id=None, entity_id=None, event_id=None):
    lid = f"el:{evidence_id}:{role}:{insight_id or 'n'}:{entity_id or 'n'}:{event_id or 'n'}"
    con.execute(
        "INSERT OR REPLACE INTO evidence_link VALUES (?, ?, ?, ?, ?, ?, now())",
        [lid, evidence_id, insight_id, entity_id, event_id, role]
    )

def sync_all():
    con = duckdb.connect(DB)
    ensure_v2_tables(con)
    
    # 1. Obter insights da engine (precisa importar aqui ou passar como param)
    from insights_engine import generate_insights_for_servidores, generate_insights_for_diarias
    
    df_serv = con.execute("SELECT * FROM rb_servidores_mass").df()
    df_dia = con.execute("SELECT * FROM diarias").df()
    
    insights_s = generate_insights_for_servidores(df_serv)
    insights_d = generate_insights_for_diarias(df_dia)
    
    all_insights = [i.to_dict() for i in (insights_s + insights_d)]
    
    print(f"Sincronizando {len(all_insights)} insights...")

    for ins in all_insights:
        # 1) Insight
        insert_insight(con, ins)

        # 2) Entity & Events
        # Para Salários
        if ins["id"].startswith("SAL_"):
            matricula = parse_matricula_from_insight_id(ins["id"])
            entity_id = f"rb_matricula:{matricula}"
            nome = ins["titulo"].replace("Indício: ", "").strip()
            upsert_entity(con, entity_id, "pessoa", nome, {"nome_norm": norm_name(nome)})
            
            # Eventos relacionados a esse insight (evidências)
            for row in ins.get("evidencias", []):
                mes = parse_mes(row.get("mes_id"))
                ano = 2026 # Fallback
                try:
                    # Se ano_id for o ID do portal, precisamos mapear. 
                    # Mas por enquanto vamos usar 2026 se for 2873896
                    if str(row.get("ano_id")) == "2873896": ano = 2026
                except: pass
                
                occurred_at = f"{ano:04d}-{mes:02d}-01 00:00:00" if mes else None
                event_id = f"evento:salario:rb:{matricula}:{row.get('ano_id','')}{row.get('mes_id','')}"
                
                insert_event(con, {
                    "id": event_id,
                    "type": "salario",
                    "occurred_at": occurred_at,
                    "amount_brl": float(row.get("salario_liquido") or 0.0),
                    "title": "Folha de pagamento",
                    "attributes": row
                })
                
                evid_id = insert_evidence(con, ins["fontes"][0], "csv_row", row)
                link_insight(con, ins["id"], entity_id=entity_id, event_id=event_id)
                link_evidence(con, evid_id, "supports", insight_id=ins["id"], entity_id=entity_id, event_id=event_id)

        # Para Diárias
        elif ins["id"].startswith("DIA_"):
            # O ID de diárias costuma ser DIA_AGRUP_...
            # Linkamos os eventos de diárias que compõem o agrupamento
            for row in ins.get("evidencias", []):
                event_id = f"evento:diaria:{row.get('id')}"
                occurred_at = row.get("data_saida")
                if isinstance(occurred_at, pd.Timestamp):
                    occurred_at = occurred_at.to_pydatetime().isoformat()
                
                insert_event(con, {
                    "id": event_id,
                    "type": "diaria",
                    "occurred_at": occurred_at,
                    "occurred_to": row.get("data_retorno"),
                    "amount_brl": float(row.get("valor") or 0.0),
                    "title": f"Diária para {row.get('destino')}",
                    "attributes": row
                })
                
                # Tenta achar a entidade pelo nome
                s_nome = row.get("servidor_nome")
                s_norm = norm_name(s_nome)
                
                # Procura no banco se já existe essa pessoa (da folha)
                entity_res = con.execute("SELECT id FROM entity WHERE attributes->>'$.nome_norm' = ?", [s_norm]).fetchone()
                ent_id = entity_res[0] if entity_res else f"pessoa_unresolved:{s_norm}"
                
                if not entity_res:
                    upsert_entity(con, ent_id, "pessoa", s_nome, {"nome_norm": s_norm, "unresolved": True})

                evid_id = insert_evidence(con, ins["fontes"][0], "csv_row", row)
                link_insight(con, ins["id"], entity_id=ent_id, event_id=event_id)
                link_evidence(con, evid_id, "supports", insight_id=ins["id"], entity_id=ent_id, event_id=event_id)

    con.close()
    print("Sincronização concluída.")

if __name__ == "__main__":
    sync_all()
