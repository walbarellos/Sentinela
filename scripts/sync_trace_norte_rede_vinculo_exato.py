from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.transparencia_ac_connector import TransparenciaAcConnector

DDL_LINK = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_vinculo_exato (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    ano_contrato INTEGER,
    numero_contrato VARCHAR,
    valor_brl DOUBLE,
    terceirizacao_pessoal BOOLEAN,
    id_contrato BIGINT,
    id_licitacao BIGINT,
    origem VARCHAR,
    modalidade_contrato VARCHAR,
    data_publicacao_contrato VARCHAR,
    vigencia_inicial VARCHAR,
    vigencia_final VARCHAR,
    possui_aditivo VARCHAR,
    status_contrato VARCHAR,
    lic_ano INTEGER,
    lic_numero VARCHAR,
    lic_processo_adm VARCHAR,
    lic_modalidade VARCHAR,
    lic_abertura VARCHAR,
    lic_status VARCHAR,
    lic_objeto VARCHAR,
    lic_publicacoes_json JSON,
    raw_contrato_json JSON,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_AUDIT = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_vinculo_audit (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    numero_contrato VARCHAR,
    valor_brl DOUBLE,
    heuristic_processo VARCHAR,
    heuristic_modalidade VARCHAR,
    heuristic_unidade_gestora VARCHAR,
    exact_licitacao_ano INTEGER,
    exact_licitacao_numero VARCHAR,
    exact_processo_adm VARCHAR,
    exact_modalidade VARCHAR,
    status VARCHAR,
    motivo VARCHAR,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def normalize_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip()


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def short_vendor_name(name: str) -> str:
    tokens = re.findall(r"[A-Z0-9]{2,}", normalize_text(name))
    return " ".join(tokens[:4])


def load_targets(con: duckdb.DuckDBPyConnection, top_n: int) -> list[tuple]:
    return con.execute(
        """
        WITH top_leads AS (
            SELECT cnpj
            FROM v_trace_norte_rede_resumo
            ORDER BY valor_terceirizacao_brl DESC, total_valor_brl DESC
            LIMIT ?
        )
        SELECT
            c.row_id,
            c.cnpj,
            c.fornecedor_nome,
            c.orgao,
            c.unidade_gestora,
            c.ano,
            c.numero_contrato,
            c.valor_brl,
            c.terceirizacao_pessoal,
            c.objeto
        FROM trace_norte_rede_contratos c
        JOIN top_leads t ON t.cnpj = c.cnpj
        ORDER BY c.valor_brl DESC, c.cnpj, c.numero_contrato
        """,
        [top_n],
    ).fetchall()


def match_contract_row(items: list[dict], numero: str, cnpj: str, unidade: str) -> dict | None:
    numero_norm = normalize_text(numero)
    unidade_norm = normalize_text(unidade)
    cnpj_norm = clean_doc(cnpj)
    for item in items:
        if normalize_text(item.get("numero_contrato")) != numero_norm:
            continue
        if cnpj_norm and clean_doc(item.get("cpf_cnpj")) != cnpj_norm:
            continue
        item_unidade = normalize_text(item.get("entidade"))
        if unidade_norm and item_unidade and unidade_norm != item_unidade:
            continue
        return item
    return None


def fetch_contract_row(
    bot: TransparenciaAcConnector,
    *,
    ano: int,
    unidade_gestora: str,
    fornecedor_nome: str,
    numero_contrato: str,
    cnpj: str,
) -> dict | None:
    attempts = [
        numero_contrato,
        cnpj,
        short_vendor_name(fornecedor_nome),
        fornecedor_nome,
    ]
    seen: set[str] = set()
    for term in attempts:
        term = str(term or "").strip()
        if not term or term in seen:
            continue
        seen.add(term)
        items = bot._portal_list(
            page="contratos",
            extra_payload={
                "ano": str(ano),
                "orgao": unidade_gestora,
                "busca": term,
                "filtro": "",
                "fonte": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "modalidade": "",
            },
            page_size=100,
        )
        match = match_contract_row(items, numero_contrato, cnpj, unidade_gestora)
        if match:
            return match
    return None


def fetch_licitacao_detail(
    bot: TransparenciaAcConnector,
    *,
    id_licitacao: int,
    ano_contrato: int,
) -> tuple[int | None, list[dict]]:
    candidate_years = [ano_contrato, ano_contrato - 1, ano_contrato - 2, ano_contrato + 1]
    tried: set[int] = set()
    for year in candidate_years:
        if year in tried or year < 2020:
            continue
        tried.add(year)
        data = bot._portal_post_json(
            page="licitacoes",
            endpoint="detalhamento-card",
            payload={
                "ano": str(year),
                "id_licitacao": str(id_licitacao),
                "busca": "",
                "busca_card": "",
                "filtro": "",
                "fonte": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "modalidade": "",
                "natureza": "",
                "status": "",
            },
        )
        if data:
            return year, data
    return None, []


def build_link_rows(con: duckdb.DuckDBPyConnection, top_n: int) -> list[tuple]:
    bot = TransparenciaAcConnector(delay_entre_requests=0.05)
    targets = load_targets(con, top_n=top_n)
    rows: list[tuple] = []
    for (
        _row_id,
        cnpj,
        fornecedor_nome,
        orgao,
        unidade_gestora,
        ano,
        numero_contrato,
        valor_brl,
        terceirizacao_pessoal,
        objeto,
    ) in targets:
        raw = fetch_contract_row(
            bot,
            ano=int(ano or 0),
            unidade_gestora=str(unidade_gestora or ""),
            fornecedor_nome=str(fornecedor_nome or ""),
            numero_contrato=str(numero_contrato or ""),
            cnpj=str(cnpj or ""),
        )
        if not raw:
            continue
        lic_ano, lic_publicacoes = fetch_licitacao_detail(
            bot,
            id_licitacao=int(raw.get("id_licitacao") or 0),
            ano_contrato=int(ano or 0),
        )
        lic_base = lic_publicacoes[0] if lic_publicacoes else {}
        evidence = {
            "match_source": "portal_contratos.id_licitacao",
            "contract_search": {
                "ano": int(ano or 0),
                "numero_contrato": numero_contrato,
                "unidade_gestora": unidade_gestora,
                "fornecedor": fornecedor_nome,
                "cnpj": cnpj,
            },
            "contract_portal": {
                "id_contrato": raw.get("id_contrato"),
                "id_licitacao": raw.get("id_licitacao"),
                "origem": raw.get("origem"),
                "modalidade_licitacao": raw.get("modalidade_licitacao"),
            },
            "licitacao_portal": {
                "lic_ano": lic_ano,
                "numero_licitacao": lic_base.get("numero_licitacao"),
                "numero_processo_administrativo": lic_base.get("numero_processo_administrativo"),
                "modalidade": lic_base.get("modalidade"),
                "data_abertura": lic_base.get("data_abertura"),
            },
        }
        rows.append(
            (
                row_hash(
                    "trace_norte_rede_vinculo_exato",
                    cnpj,
                    orgao,
                    ano,
                    numero_contrato,
                    raw.get("id_contrato"),
                    raw.get("id_licitacao"),
                ),
                cnpj,
                fornecedor_nome,
                orgao,
                unidade_gestora,
                int(ano or 0),
                numero_contrato,
                float(valor_brl or 0),
                bool(terceirizacao_pessoal),
                int(raw.get("id_contrato") or 0),
                int(raw.get("id_licitacao") or 0),
                str(raw.get("origem") or ""),
                str(raw.get("modalidade_licitacao") or ""),
                str(raw.get("data_publi") or ""),
                str(raw.get("vigencia_inicial") or ""),
                str(raw.get("vigencia_final") or ""),
                str(raw.get("possui_aditivo") or ""),
                str(raw.get("status_contrato") or ""),
                lic_ano,
                str(lic_base.get("numero_licitacao") or ""),
                str(lic_base.get("numero_processo_administrativo") or ""),
                str(lic_base.get("modalidade") or ""),
                str(lic_base.get("data_abertura") or ""),
                str(lic_base.get("status_licitacao_atual") or ""),
                str(lic_base.get("objeto_licitacao") or objeto or ""),
                json.dumps(lic_publicacoes, ensure_ascii=False),
                json.dumps(raw, ensure_ascii=False),
                json.dumps(evidence, ensure_ascii=False),
            )
        )
    return rows


def upsert_links(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> int:
    con.execute(DDL_LINK)
    con.execute("DELETE FROM trace_norte_rede_vinculo_exato")
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO trace_norte_rede_vinculo_exato (
            row_id, cnpj, fornecedor_nome, orgao, unidade_gestora, ano_contrato,
            numero_contrato, valor_brl, terceirizacao_pessoal, id_contrato, id_licitacao,
            origem, modalidade_contrato, data_publicacao_contrato, vigencia_inicial,
            vigencia_final, possui_aditivo, status_contrato, lic_ano, lic_numero,
            lic_processo_adm, lic_modalidade, lic_abertura, lic_status, lic_objeto,
            lic_publicacoes_json, raw_contrato_json, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_rede_vinculo_exato AS
        SELECT
            cnpj,
            fornecedor_nome,
            orgao,
            unidade_gestora,
            ano_contrato,
            numero_contrato,
            valor_brl,
            terceirizacao_pessoal,
            id_contrato,
            id_licitacao,
            modalidade_contrato,
            data_publicacao_contrato,
            vigencia_inicial,
            vigencia_final,
            possui_aditivo,
            status_contrato,
            lic_ano,
            lic_numero,
            lic_processo_adm,
            lic_modalidade,
            lic_abertura,
            lic_status,
            lic_objeto
        FROM trace_norte_rede_vinculo_exato
        ORDER BY valor_brl DESC, fornecedor_nome, numero_contrato
        """
    )
    return len(rows)


def upsert_audit(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(DDL_AUDIT)
    con.execute("DELETE FROM trace_norte_rede_vinculo_audit")
    rows = con.execute(
        """
        WITH heuristic AS (
            SELECT
                contrato_cnpj AS cnpj,
                contrato_fornecedor AS fornecedor_nome,
                contrato_orgao AS orgao,
                contrato_unidade_gestora AS unidade_gestora,
                contrato_numero AS numero_contrato,
                contrato_valor_brl AS valor_brl,
                lic_numero_processo AS heuristic_processo,
                lic_modalidade AS heuristic_modalidade,
                lic_unidade_gestora AS heuristic_unidade_gestora
            FROM v_trace_norte_rede_match_best
        )
        SELECT
            h.cnpj,
            h.fornecedor_nome,
            h.orgao,
            h.unidade_gestora,
            h.numero_contrato,
            h.valor_brl,
            h.heuristic_processo,
            h.heuristic_modalidade,
            h.heuristic_unidade_gestora,
            e.lic_ano,
            e.lic_numero,
            e.lic_processo_adm,
            e.lic_modalidade
        FROM heuristic h
        LEFT JOIN trace_norte_rede_vinculo_exato e
          ON e.cnpj = h.cnpj
         AND e.numero_contrato = h.numero_contrato
         AND e.orgao = h.orgao
        """
    ).fetchall()
    payload: list[tuple] = []
    for (
        cnpj,
        fornecedor_nome,
        orgao,
        unidade_gestora,
        numero_contrato,
        valor_brl,
        heuristic_processo,
        heuristic_modalidade,
        heuristic_unidade_gestora,
        lic_ano,
        lic_numero,
        lic_processo_adm,
        lic_modalidade,
    ) in rows:
        if not lic_numero:
            status = "SEM_VINCULO_EXATO"
            motivo = "portal_contratos nao devolveu id_licitacao resolvido para este contrato"
        elif normalize_text(str(heuristic_processo)) == normalize_text(str(lic_numero)) and normalize_text(str(heuristic_modalidade)) == normalize_text(str(lic_modalidade)):
            status = "CONFIRMADO"
            motivo = "match heuristico coincide com o vinculo exato do portal"
        else:
            status = "DIVERGENTE"
            motivo = "match heuristico diverge do id_licitacao presente no portal de contratos"
        evidence = {
            "heuristic": {
                "processo": heuristic_processo,
                "modalidade": heuristic_modalidade,
                "unidade_gestora": heuristic_unidade_gestora,
            },
            "exact": {
                "lic_ano": lic_ano,
                "numero_licitacao": lic_numero,
                "processo_administrativo": lic_processo_adm,
                "modalidade": lic_modalidade,
            },
        }
        payload.append(
            (
                row_hash("trace_norte_rede_vinculo_audit", cnpj, numero_contrato, heuristic_processo, lic_numero),
                cnpj,
                fornecedor_nome,
                orgao,
                unidade_gestora,
                numero_contrato,
                float(valor_brl or 0),
                str(heuristic_processo or ""),
                str(heuristic_modalidade or ""),
                str(heuristic_unidade_gestora or ""),
                lic_ano,
                str(lic_numero or ""),
                str(lic_processo_adm or ""),
                str(lic_modalidade or ""),
                status,
                motivo,
                json.dumps(evidence, ensure_ascii=False),
            )
        )
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_rede_vinculo_audit (
                row_id, cnpj, fornecedor_nome, orgao, unidade_gestora, numero_contrato,
                valor_brl, heuristic_processo, heuristic_modalidade, heuristic_unidade_gestora,
                exact_licitacao_ano, exact_licitacao_numero, exact_processo_adm, exact_modalidade,
                status, motivo, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_rede_vinculo_divergencias AS
        SELECT *
        FROM trace_norte_rede_vinculo_audit
        WHERE status = 'DIVERGENTE'
        ORDER BY valor_brl DESC, cnpj, numero_contrato
        """
    )
    return len(payload)


def upsert_insight(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute(
        """
        SELECT
            cnpj,
            fornecedor_nome,
            orgao,
            unidade_gestora,
            lic_ano,
            lic_numero,
            lic_processo_adm,
            lic_modalidade,
            lic_abertura,
            COUNT(*) AS n_contratos,
            SUM(valor_brl) AS total_valor,
            MIN(vigencia_inicial) AS vig_ini,
            MAX(vigencia_inicial) AS vig_max
        FROM trace_norte_rede_vinculo_exato
        WHERE cnpj = '36990588000115'
          AND orgao = 'SEPLANH'
          AND lic_numero = '053'
        GROUP BY ALL
        """
    ).fetchone()
    if not row:
        return 0
    (
        cnpj,
        fornecedor_nome,
        orgao,
        unidade_gestora,
        lic_ano,
        lic_numero,
        lic_processo_adm,
        lic_modalidade,
        lic_abertura,
        n_contratos,
        total_valor,
        vig_ini,
        vig_max,
    ) = row
    insight_id = f"TRACE_NORTE_REDE:vinculo_exato:{cnpj}:{orgao}:{lic_ano}:{lic_numero}"
    description = (
        f"O bloco contratual de **{fornecedor_nome}** em **{unidade_gestora}** "
        f"foi resolvido no próprio portal de contratos para a licitação **{lic_numero}/{lic_ano}** "
        f"(**{lic_modalidade}**, processo **{lic_processo_adm}**), com abertura em **{lic_abertura}**. "
        f"No recorte atual, são **{int(n_contratos)} contrato(s)** somando **R$ {float(total_valor or 0):,.2f}**, "
        f"com vigências iniciais entre **{vig_ini}** e **{vig_max}**. "
        f"Esse vínculo exato substitui o match heurístico anterior com o processo `282/2024`."
    ).replace(",", "X", 1).replace(".", ",").replace("X", ".")
    con.execute("DELETE FROM insight WHERE id = ?", [insight_id])
    con.execute(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl, title, description_md,
            pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
            municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            insight_id,
            "TRACE_NORTE_REDE_VINCULO_EXATO",
            "HIGH",
            98,
            float(total_valor or 0),
            "CENTRAL NORTE / SEPLAN remete a PP 053/2023, nao a PE 282/2024",
            description,
            "TRACE_NORTE_REDE -> CONTRATO_COM_ID_LICITACAO_EXATO_NO_PORTAL",
            json.dumps(
                [
                    {"fonte": "portal_contratos", "cnpj": cnpj, "unidade_gestora": unidade_gestora},
                    {"fonte": "portal_licitacoes", "numero_licitacao": lic_numero, "ano": lic_ano, "processo": lic_processo_adm},
                ],
                ensure_ascii=False,
            ),
            json.dumps(
                [
                    "trace_norte",
                    "rede",
                    "vinculo_exato",
                    "central_norte",
                    "seplanh",
                    f"licitacao:{lic_numero}/{lic_ano}",
                ],
                ensure_ascii=False,
            ),
            int(n_contratos),
            float(total_valor or 0),
            "estadual",
            "AC",
            orgao,
            None,
            "AC",
            "administracao",
            False,
            float(total_valor or 0),
            int(lic_ano or 0),
            "portal_transparencia_acre",
        ],
    )
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve vinculo exato contrato -> licitacao para a rede TRACE_NORTE.")
    parser.add_argument("--top-leads", type=int, default=3, help="Quantidade de empresas-lead a resolver.")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))
    link_rows = build_link_rows(con, top_n=args.top_leads)
    n_links = upsert_links(con, link_rows)
    n_audit = upsert_audit(con)
    n_insights = upsert_insight(con)
    resolved = con.execute("SELECT COUNT(*) FROM trace_norte_rede_vinculo_exato").fetchone()[0]
    diverg = con.execute("SELECT COUNT(*) FROM v_trace_norte_rede_vinculo_divergencias").fetchone()[0]
    con.close()

    print(f"links={n_links}")
    print(f"resolved={resolved}")
    print(f"audit_rows={n_audit}")
    print(f"divergencias={diverg}")
    print(f"insights={n_insights}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
