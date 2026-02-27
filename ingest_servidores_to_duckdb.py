#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import random
import sys
import time
from datetime import datetime

import duckdb
import pandas as pd
import requests

from src.ingest.riobranco_servidor_list import RioBrancoServidorList
from src.ingest.riobranco_servidor_detail import RioBrancoServidorDetail


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS rb_servidores_meta (
        servidor_id TEXT PRIMARY KEY,
        url TEXT,
        capturado_em TEXT,
        nome TEXT,
        matricula TEXT,
        cpf TEXT,
        cargo TEXT,
        lotacao TEXT,
        vinculo TEXT,
        regime TEXT,
        secretaria TEXT,
        unidade TEXT,
        raw_meta_json TEXT,
        html_hash TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rb_servidores_tabela (
        servidor_id TEXT,
        row_hash TEXT,
        capturado_em TEXT,
        raw_row_json TEXT,
        competencia TEXT,
        mes TEXT,
        ano TEXT,
        descricao TEXT,
        tipo TEXT,
        valor DOUBLE
    );
    """)

    con.execute("""
    CREATE INDEX IF NOT EXISTS idx_rb_servidores_tabela_servidor ON rb_servidores_tabela(servidor_id);
    """)


def already_ingested(con: duckdb.DuckDBPyConnection, servidor_id: str) -> bool:
    r = con.execute(
        "SELECT 1 FROM rb_servidores_meta WHERE servidor_id = ? LIMIT 1",
        [servidor_id]
    ).fetchone()
    return r is not None


def normalize_money_to_float(x) -> float or None:
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if not s: return None
    s = s.replace(".", "").replace(",", ".")
    s = "".join(ch for ch in s if (ch.isdigit() or ch in ".-"))
    try: return float(s)
    except: return None


def extract_competencia_fields(row: dict) -> tuple[str or None, str or None, str or None]:
    comp = None
    mes = None
    ano = None
    for k in ["competencia", "competência", "ref", "referencia", "referência", "mês/ano", "mes/ano"]:
        if k in row and row[k]:
            comp = str(row[k]).strip()
            break
    if comp and ("/" in comp) and len(comp) >= 4:
        parts = comp.split("/")
        if len(parts) >= 2:
            m = parts[0].strip()
            a = parts[1].strip()
            if m.isdigit(): mes = m.zfill(2)
            if a.isdigit(): ano = a
    for k in ["mes", "mês"]:
        if k in row and row[k]:
            mm = str(row[k]).strip()
            if mm.isdigit(): mes = mm.zfill(2)
    for k in ["ano", "exercicio", "exercício"]:
        if k in row and row[k]:
            aa = str(row[k]).strip()
            if aa.isdigit(): ano = aa
    return comp, mes, ano


def pick_value_column(row: dict) -> float or None:
    candidates = ["valor", "valor (r$)", "r$", "provento", "desconto", "líquido", "liquido", "bruto"]
    for c in candidates:
        if c in row and row[c] not in (None, ""):
            v = normalize_money_to_float(row[c])
            if v is not None: return v
    return None


def table_to_rows(df: pd.DataFrame) -> list[dict]:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.fillna("")
    return df.to_dict(orient="records")


def ingest_one(con: duckdb.DuckDBPyConnection, detail: RioBrancoServidorDetail, servidor_id: str, force: bool = False) -> bool:
    if (not force) and already_ingested(con, servidor_id): return False
    payload = detail.fetch(servidor_id)
    meta = payload.get("meta", {})
    df = payload.get("table", None)
    raw_meta_json = json.dumps(meta, ensure_ascii=False)
    html_hash = sha1_text(raw_meta_json)
    
    def g(k): return (meta.get(k) or "").strip() if isinstance(meta.get(k), str) else (meta.get(k) or "")
    row_meta = {
        "servidor_id": str(servidor_id), "url": g("url"), "capturado_em": g("capturado_em") or now_iso(),
        "nome": g("nome"), "matricula": g("matrícula") or g("matricula"), "cpf": g("cpf"),
        "cargo": g("cargo"), "lotacao": g("lotação") or g("lotacao"), "vinculo": g("vínculo") or g("vinculo"),
        "regime": g("regime"), "secretaria": g("secretaria"), "unidade": g("unidade"),
        "raw_meta_json": raw_meta_json, "html_hash": html_hash
    }

    con.execute("""
        INSERT INTO rb_servidores_meta (
            servidor_id, url, capturado_em, nome, matricula, cpf, cargo, lotacao,
            vinculo, regime, secretaria, unidade, raw_meta_json, html_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (servidor_id) DO UPDATE SET
            url=excluded.url, capturado_em=excluded.capturado_em, nome=excluded.nome,
            matricula=excluded.matricula, cpf=excluded.cpf, cargo=excluded.cargo,
            lotacao=excluded.lotacao, vinculo=excluded.vinculo, regime=excluded.regime,
            secretaria=excluded.secretaria, unidade=excluded.unidade,
            raw_meta_json=excluded.raw_meta_json, html_hash=excluded.html_hash
    """, list(row_meta.values()))

    if df is not None and isinstance(df, pd.DataFrame) and df.shape[0] > 0:
        rows = table_to_rows(df)
        capt = row_meta["capturado_em"]
        if force: con.execute("DELETE FROM rb_servidores_tabela WHERE servidor_id = ?", [servidor_id])
        for r in rows:
            comp, mes, ano = extract_competencia_fields(r)
            desc = ""
            for k in ["descricao", "descrição", "evento", "rubrica", "vantagem", "desconto", "item"]:
                if k in r and r[k]: desc = str(r[k]).strip(); break
            tipo = ""
            for k in ["tipo", "natureza", "categoria"]:
                if k in r and r[k]: tipo = str(r[k]).strip(); break
            val = pick_value_column(r)
            raw_row_json = json.dumps(r, ensure_ascii=False)
            row_hash = sha1_text(f"{servidor_id}|{raw_row_json}")
            con.execute("""
                INSERT INTO rb_servidores_tabela (
                    servidor_id, row_hash, capturado_em, raw_row_json,
                    competencia, mes, ano, descricao, tipo, valor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [str(servidor_id), row_hash, capt, raw_row_json, comp, mes, ano, desc, tipo, val])
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./data/sentinela_analytics.duckdb")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    con = duckdb.connect(args.db)
    ensure_schema(con)
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Sentinela/3.0"})
    lister = RioBrancoServidorList(session=sess)
    detail = RioBrancoServidorDetail(session=sess)
    ids = lister.fetch_all_ids()
    if args.limit > 0: ids = ids[:args.limit]
    print(f"[{now_iso()}] Ingestão iniciada: {len(ids)} alvos encontrados.")
    for i, sid in enumerate(ids, start=1):
        try:
            if ingest_one(con, detail, sid, force=args.force):
                print(f"[{i}/{len(ids)}] OK ID={sid}")
            else:
                print(f"[{i}/{len(ids)}] SKIP ID={sid}")
        except Exception as e:
            print(f"FAIL ID={sid} err={e}", file=sys.stderr)
        time.sleep(random.uniform(0.1, 0.3))
    print("
PROCESSO CONCLUÍDO.")

if __name__ == "__main__":
    main()
