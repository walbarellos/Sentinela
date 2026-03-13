from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
TOP_LEADS = {"21813150000194", "36990588000115"}


DDL_MATCH = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_match (
    row_id VARCHAR PRIMARY KEY,
    contrato_cnpj VARCHAR,
    contrato_fornecedor VARCHAR,
    contrato_orgao VARCHAR,
    contrato_unidade_gestora VARCHAR,
    contrato_ano INTEGER,
    contrato_numero VARCHAR,
    contrato_valor_brl DOUBLE,
    contrato_objeto VARCHAR,
    lic_ano INTEGER,
    lic_numero_processo VARCHAR,
    lic_modalidade VARCHAR,
    lic_orgao VARCHAR,
    lic_unidade_gestora VARCHAR,
    lic_valor_estimado DOUBLE,
    lic_valor_real DOUBLE,
    lic_situacao VARCHAR,
    lic_data_abertura VARCHAR,
    lic_objeto VARCHAR,
    lic_fornecedores_json JSON,
    score_total DOUBLE,
    score_orgao DOUBLE,
    score_unidade DOUBLE,
    score_objeto DOUBLE,
    score_fornecedor DOUBLE,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def fix_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(marker in text for marker in ("Ã", "â", "�")):
        for source_encoding in ("latin1", "cp1252"):
            try:
                repaired = text.encode(source_encoding).decode("utf-8")
            except Exception:
                continue
            if repaired and repaired != text:
                return repaired.strip()
    return text


def normalize_text(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", fix_text(value).upper())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip()


def token_set(value: object) -> set[str]:
    text = normalize_text(value)
    parts = re.findall(r"[A-Z0-9]{3,}", text)
    stop = {
        "PARA",
        "COM",
        "UMA",
        "DAS",
        "DOS",
        "DA",
        "DO",
        "DE",
        "POR",
        "NO",
        "NA",
        "NOS",
        "NAS",
        "QUE",
        "AOS",
        "AS",
        "OS",
        "EM",
        "REGIME",
        "CONTINUADO",
        "SERVICO",
        "SERVICOS",
        "CONTRATACAO",
        "PESSOA",
        "JURIDICA",
    }
    return {part for part in parts if part not in stop}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def classify_category(text: object) -> str:
    normalized = normalize_text(text)
    if re.search(r"(LIMPEZA|ASSEIO|CONSERVACAO|CONSERVAÇÃO|COPEIRAGEM|JARDINAGEM|ROCAGEM|ROÇAGEM)", normalized):
        return "limpeza_conservacao"
    if re.search(r"(\bENGENHARIA\b|\bCONSTRUCAO\b|\bCONSTRUÇÃO\b|\bREFORMA\b)", normalized):
        return "engenharia"
    if re.search(r"(\bVIGILANCIA\b|\bVIGILÂNCIA\b|\bPATRIMONIAL\b|\bARMADA\b)", normalized):
        return "vigilancia"
    if re.search(r"(VEICULOS|VEÍCULOS|CAMINHONETE|VIATURAS|PICK-UP|PICKUPS)", normalized):
        return "veiculos"
    if re.search(r"(MATERIAL DE CONSUMO|AQUISICAO|AQUISIÇÃO|GENEROS|GÊNEROS|ALIMENTIC|INSUMOS)", normalized):
        return "material_consumo"
    if re.search(r"(APOIO OPERACIONAL|APOIO ADMINISTRATIVO|DEDICACAO EXCLUSIVA|DEDICAÇÃO EXCLUSIVA|POSTOS DE TRABALHO|FORNECIMENTO DE PESSOAL)", normalized):
        return "apoio_operacional_admin"
    return "outro"


def choose_leads(con: duckdb.DuckDBPyConnection, top_n: int) -> list[str]:
    rows = con.execute(
        """
        SELECT cnpj
        FROM v_trace_norte_rede_resumo
        ORDER BY valor_terceirizacao_brl DESC, total_valor_brl DESC
        LIMIT ?
        """,
        [top_n],
    ).fetchall()
    return [str(row[0]) for row in rows if str(row[0]) in TOP_LEADS]


def org_unit_score(contract_orgao: str, contract_unidade: str, lic_orgao: str, lic_unidade: str) -> tuple[float, float]:
    c_org = normalize_text(contract_orgao)
    c_uni = normalize_text(contract_unidade)
    l_org = normalize_text(lic_orgao)
    l_uni = normalize_text(lic_unidade)

    score_org = 1.0 if c_org and c_org == l_org else 0.0
    score_unit = 0.0
    if c_uni and l_uni:
        if c_uni == l_uni:
            score_unit = 1.0
        elif c_uni in l_uni or l_uni in c_uni:
            score_unit = 0.8
        else:
            score_unit = jaccard(token_set(c_uni), token_set(l_uni))
    return score_org, score_unit


def fornecedor_score(contract_fornecedor: str, fornecedores_json: object) -> float:
    candidate = normalize_text(contract_fornecedor)
    if not candidate or not fornecedores_json:
        return 0.0
    try:
        parsed = json.loads(str(fornecedores_json))
    except Exception:
        parsed = []
    haystack = normalize_text(json.dumps(parsed, ensure_ascii=False))
    if candidate and haystack and candidate in haystack:
        return 1.0
    return 0.0


def collect_matches(con: duckdb.DuckDBPyConnection, lead_cnpjs: list[str]) -> list[tuple]:
    if not lead_cnpjs:
        return []
    placeholders = ",".join("?" for _ in lead_cnpjs)
    contracts = con.execute(
        f"""
        SELECT
            cnpj, fornecedor_nome, orgao, unidade_gestora, ano,
            numero_contrato, valor_brl, objeto
        FROM trace_norte_rede_contratos
        WHERE cnpj IN ({placeholders})
        ORDER BY valor_brl DESC, orgao, numero_contrato
        """,
        lead_cnpjs,
    ).fetchall()
    licitacoes = con.execute(
        """
        SELECT
            ano, numero_processo, modalidade, orgao, unidade_gestora,
            valor_estimado, valor_real, situacao, data_abertura, objeto, fornecedores_json
        FROM estado_ac_licitacoes
        WHERE ano = 2024
        """
    ).fetchall()

    payload = []
    for contract in contracts:
        cnpj, fornecedor, orgao, unidade, ano, numero, valor, objeto = contract
        objeto_tokens = token_set(objeto)
        ranked = []
        for lic in licitacoes:
            l_ano, proc, modalidade, l_orgao, l_unidade, v_est, v_real, situacao, abertura, l_objeto, forn_json = lic
            if int(l_ano or 0) != int(ano or 0):
                continue
            score_org, score_unidade = org_unit_score(orgao, unidade, l_orgao, l_unidade)
            score_objeto = jaccard(objeto_tokens, token_set(l_objeto))
            score_fornecedor = fornecedor_score(fornecedor, forn_json)
            contract_category = classify_category(objeto)
            lic_category = classify_category(l_objeto)
            if contract_category != lic_category:
                continue
            score_total = (score_org * 0.25) + (score_unidade * 0.30) + (score_objeto * 0.35) + (score_fornecedor * 0.10)
            if score_total < 0.33:
                continue
            ranked.append(
                (
                    score_total,
                    score_org,
                    score_unidade,
                    score_objeto,
                    score_fornecedor,
                    lic,
                )
            )
        ranked.sort(key=lambda item: (item[0], item[3], item[2], item[1]), reverse=True)
        for score_total, score_org, score_unidade, score_objeto, score_fornecedor, lic in ranked[:3]:
            (
                l_ano,
                proc,
                modalidade,
                l_orgao,
                l_unidade,
                v_est,
                v_real,
                situacao,
                abertura,
                l_objeto,
                forn_json,
            ) = lic
            evidence = {
                "contrato_tokens_top": Counter(sorted(objeto_tokens)).most_common(15),
                "licitacao_tokens_top": Counter(sorted(token_set(l_objeto))).most_common(15),
                "score_orgao": score_org,
                "score_unidade": score_unidade,
                "score_objeto": score_objeto,
                "score_fornecedor": score_fornecedor,
                "contract_category": contract_category,
                "licitacao_category": lic_category,
            }
            payload.append(
                (
                    row_hash("rede_match", cnpj, numero, proc, l_orgao, l_unidade),
                    cnpj,
                    fornecedor,
                    orgao,
                    unidade,
                    int(ano or 0),
                    numero,
                    float(valor or 0),
                    fix_text(objeto),
                    int(l_ano or 0),
                    fix_text(proc),
                    fix_text(modalidade),
                    fix_text(l_orgao),
                    fix_text(l_unidade),
                    float(v_est or 0),
                    float(v_real or 0),
                    fix_text(situacao),
                    fix_text(abertura),
                    fix_text(l_objeto),
                    json.dumps(json.loads(str(forn_json)) if forn_json else [], ensure_ascii=False),
                    float(score_total),
                    float(score_org),
                    float(score_unidade),
                    float(score_objeto),
                    float(score_fornecedor),
                    json.dumps(evidence, ensure_ascii=False),
                )
            )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Vincula contratos dos leads NORTE a licitações estaduais candidatas.")
    parser.add_argument("--db", default=str(DB_PATH), help="Caminho do DuckDB alvo.")
    parser.add_argument("--top", type=int, default=2, help="Quantidade de empresas-lead prioritarias para matching.")
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    con.execute(DDL_MATCH)
    con.execute("DELETE FROM trace_norte_rede_match")
    lead_cnpjs = choose_leads(con, args.top)
    payload = collect_matches(con, lead_cnpjs)
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_rede_match (
                row_id, contrato_cnpj, contrato_fornecedor, contrato_orgao, contrato_unidade_gestora,
                contrato_ano, contrato_numero, contrato_valor_brl, contrato_objeto, lic_ano,
                lic_numero_processo, lic_modalidade, lic_orgao, lic_unidade_gestora, lic_valor_estimado,
                lic_valor_real, lic_situacao, lic_data_abertura, lic_objeto, lic_fornecedores_json,
                score_total, score_orgao, score_unidade, score_objeto, score_fornecedor, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    con.execute("DROP VIEW IF EXISTS v_trace_norte_rede_match_best")
    con.execute(
        """
        CREATE VIEW v_trace_norte_rede_match_best AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY contrato_cnpj, contrato_numero
                    ORDER BY score_total DESC, score_objeto DESC, score_unidade DESC
                ) AS rn
            FROM trace_norte_rede_match
        )
        WHERE rn = 1
        """
    )
    total = con.execute("SELECT COUNT(*) FROM trace_norte_rede_match").fetchone()[0]
    best = con.execute("SELECT COUNT(*) FROM v_trace_norte_rede_match_best").fetchone()[0]
    con.close()
    print(f"matches={total}")
    print(f"best_matches={best}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
