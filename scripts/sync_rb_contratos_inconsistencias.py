from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

import enrich_rb_contratos_licitacao as bridge
import sync_rb_contratos as rb_contratos_sync

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
CACHE_DIR = ROOT / "data" / "cache" / "rb_licitacao_audit"
KIND_INCONSISTENCIA = "RB_CONTRATO_LICITACAO_INCONSISTENTE"

DDL_AUDIT = """
CREATE TABLE IF NOT EXISTS rb_contratos_item_audit (
    row_id VARCHAR PRIMARY KEY,
    contrato_row_id VARCHAR,
    numero_contrato VARCHAR,
    numero_processo VARCHAR,
    ano INTEGER,
    licitacao_id VARCHAR,
    licitacao_url VARCHAR,
    detail_url VARCHAR,
    source_kind VARCHAR,
    edital_publicacoes VARCHAR,
    item_ordem INTEGER,
    item_descricao VARCHAR,
    item_descricao_norm VARCHAR,
    quantidade DOUBLE,
    valor_unitario DOUBLE,
    valor_total DOUBLE,
    found_in_proposals BOOLEAN,
    found_in_edital BOOLEAN,
    candidate_count INTEGER,
    min_proposal_unit DOUBLE,
    max_proposal_unit DOUBLE,
    nearest_supplier VARCHAR,
    nearest_cnpj VARCHAR,
    nearest_lote VARCHAR,
    nearest_proposal_unit DOUBLE,
    nearest_diff_rel DOUBLE,
    anomaly_kind VARCHAR,
    severity VARCHAR,
    evidence_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CPL_PUBLICACOES: dict[str, list[str]] = {
    "3006": ["1554", "1640"],
}

GENERIC_TERMS = {
    "AC",
    "ARTES",
    "ATENDER",
    "BRANCO",
    "COM",
    "CONFECCAO",
    "CONTRATACAO",
    "DIVERSAS",
    "DO",
    "EM",
    "EMPRESA",
    "ESPECIALIZADA",
    "GRAFICOS",
    "MATERIAL",
    "MUNICIPAL",
    "NO",
    "PARA",
    "PREFEITURA",
    "RIO",
    "SAUDE",
    "SECRETARIA",
    "SERVICOS",
}


def normalize_text(value: object) -> str:
    return bridge.norm_text(str(value or ""))


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Sentinela/3.0",
            "Accept-Encoding": "identity",
        }
    )
    return session


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_AUDIT)
    rb_contratos_sync.ensure_insight_columns(con)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def normalized_terms(description: str) -> list[str]:
    terms: set[str] = set()
    full = normalize_text(description)
    if full:
        terms.add(full)
    for part in re.split(r"[\/,;()\-]+", str(description or "")):
        piece = normalize_text(part)
        if len(piece) >= 5 and piece not in GENERIC_TERMS:
            terms.add(piece)
            for word in piece.split():
                if len(word) >= 7 and word not in GENERIC_TERMS:
                    terms.add(word)
    return sorted(term for term in terms if term)


def item_in_text(description: str, normalized_text_blob: str) -> bool:
    if not normalized_text_blob:
        return False
    return any(term in normalized_text_blob for term in normalized_terms(description))


def fetch_notice_pdf_links(session: requests.Session, publicacao_id: str) -> list[str]:
    url = f"https://cpl.riobranco.ac.gov.br/publicacao/{publicacao_id}"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if href.lower().endswith(".pdf") or "/notices/" in href:
            links.append(href if href.startswith("http") else f"https://cpl.riobranco.ac.gov.br{href}")
    return links


def fetch_notice_text(session: requests.Session, publicacao_id: str) -> tuple[str, list[str]]:
    cache_txt = CACHE_DIR / f"cpl_publicacao_{publicacao_id}.txt"
    if cache_txt.exists():
        return cache_txt.read_text(encoding="utf-8", errors="ignore"), [f"https://cpl.riobranco.ac.gov.br/publicacao/{publicacao_id}"]

    sources = [f"https://cpl.riobranco.ac.gov.br/publicacao/{publicacao_id}"]
    texts: list[str] = []
    for index, pdf_url in enumerate(fetch_notice_pdf_links(session, publicacao_id), start=1):
        pdf_path = CACHE_DIR / f"cpl_publicacao_{publicacao_id}_{index}.pdf"
        if not pdf_path.exists():
            response = session.get(pdf_url, timeout=120)
            response.raise_for_status()
            pdf_path.write_bytes(response.content)
        proc = subprocess.run(
            ["pdftotext", str(pdf_path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
        texts.append(proc.stdout)
        sources.append(pdf_url)

    combined = "\n".join(texts)
    cache_txt.write_text(combined, encoding="utf-8")
    return combined, sources


def fetch_edital_sources(session: requests.Session, numero_processo: str) -> tuple[str, list[str]]:
    publicacoes = CPL_PUBLICACOES.get(numero_processo, [])
    texts: list[str] = []
    sources: list[str] = []
    for publicacao_id in publicacoes:
        text, links = fetch_notice_text(session, publicacao_id)
        if text:
            texts.append(text)
        sources.extend(links)
    return "\n".join(texts), sources


def relative_diff(contract_unit: float, proposal_unit: float) -> float:
    if contract_unit <= 0 or proposal_unit <= 0:
        return 999.0
    return abs(contract_unit - proposal_unit) / max(contract_unit, 0.01)


def audit_item(
    *,
    item: dict,
    item_ordem: int,
    contrato_row: tuple,
    licitacao_id: str,
    proposals: list[bridge.Proposal],
    edital_text_norm: str,
    edital_publicacoes: str,
) -> dict[str, object]:
    (
        contrato_row_id,
        ano,
        numero_contrato,
        numero_processo,
        objeto,
        secretaria,
        valor_referencia_brl,
        detail_url,
    ) = contrato_row

    matches: list[tuple[bridge.Proposal, bridge.ProposalItem]] = []
    for proposal in proposals:
        for _, proposal_item in bridge.iter_candidate_items(item["descricao_norm"], proposal.items):
            matches.append((proposal, proposal_item))

    found_in_proposals = bool(matches)
    found_in_edital = item_in_text(item["descricao"], edital_text_norm)
    positive_units = [proposal_item.valor_unitario for _, proposal_item in matches if proposal_item.valor_unitario > 0]
    min_proposal_unit = min(positive_units) if positive_units else None
    max_proposal_unit = max(positive_units) if positive_units else None

    nearest_supplier = ""
    nearest_cnpj = ""
    nearest_lote = ""
    nearest_proposal_unit = None
    nearest_diff_rel = None
    if matches:
        proposal, proposal_item = min(
            matches,
            key=lambda entry: (
                relative_diff(float(item["valor_unitario"] or 0), entry[1].valor_unitario),
                abs(entry[1].quantidade - float(item["quantidade"] or 0)),
            ),
        )
        nearest_supplier = proposal.fornecedor
        nearest_cnpj = proposal.cnpj
        nearest_lote = proposal_item.lote_numero
        nearest_proposal_unit = proposal_item.valor_unitario
        nearest_diff_rel = relative_diff(float(item["valor_unitario"] or 0), proposal_item.valor_unitario)

    anomaly_kind = ""
    severity = ""
    if edital_text_norm and not found_in_edital and not found_in_proposals:
        anomaly_kind = "ITEM_FORA_EDITAL_E_PROPOSTAS"
        severity = "HIGH"
    elif not found_in_proposals:
        anomaly_kind = "ITEM_SEM_CORRESPONDENCIA_NAS_PROPOSTAS"
        severity = "MEDIUM"
    evidence = {
        "objeto": objeto,
        "secretaria": secretaria,
        "valor_referencia_brl": valor_referencia_brl,
        "terms": normalized_terms(item["descricao"]),
        "candidate_count": len(matches),
        "match_preview": [
            {
                "fornecedor": proposal.fornecedor,
                "cnpj": proposal.cnpj,
                "lote": proposal_item.lote_numero,
                "proposal_item": proposal_item.descricao,
                "proposal_unit": proposal_item.valor_unitario,
                "proposal_qty": proposal_item.quantidade,
            }
            for proposal, proposal_item in matches[:5]
        ],
        "edital_publicacoes": edital_publicacoes,
    }

    return {
        "row_id": rb_contratos_sync.short_id(
            "RBAUD",
            contrato_row_id,
            licitacao_id,
            item_ordem,
            anomaly_kind,
        ),
        "contrato_row_id": contrato_row_id,
        "numero_contrato": numero_contrato,
        "numero_processo": numero_processo,
        "ano": ano,
        "licitacao_id": licitacao_id,
        "licitacao_url": f"{bridge.BASE}/licitacao/ver/{licitacao_id}/",
        "detail_url": detail_url,
        "source_kind": "licitacao_bridge+edital_cpl" if edital_text_norm else "licitacao_bridge",
        "edital_publicacoes": edital_publicacoes,
        "item_ordem": item_ordem,
        "item_descricao": item["descricao"],
        "item_descricao_norm": item["descricao_norm"],
        "quantidade": float(item["quantidade"] or 0),
        "valor_unitario": float(item["valor_unitario"] or 0),
        "valor_total": float(item["valor_total"] or 0),
        "found_in_proposals": found_in_proposals,
        "found_in_edital": found_in_edital,
        "candidate_count": len(matches),
        "min_proposal_unit": min_proposal_unit,
        "max_proposal_unit": max_proposal_unit,
        "nearest_supplier": nearest_supplier,
        "nearest_cnpj": nearest_cnpj,
        "nearest_lote": nearest_lote,
        "nearest_proposal_unit": nearest_proposal_unit,
        "nearest_diff_rel": nearest_diff_rel,
        "anomaly_kind": anomaly_kind,
        "severity": severity,
        "evidence_json": json.dumps(evidence, ensure_ascii=False),
    }


def upsert_audit_rows(con: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0
    contrato_ids = sorted({str(row["contrato_row_id"]) for row in rows})
    con.executemany(
        "DELETE FROM rb_contratos_item_audit WHERE contrato_row_id = ?",
        [(row_id,) for row_id in contrato_ids],
    )
    payload = [
        (
            row["row_id"],
            row["contrato_row_id"],
            row["numero_contrato"],
            row["numero_processo"],
            row["ano"],
            row["licitacao_id"],
            row["licitacao_url"],
            row["detail_url"],
            row["source_kind"],
            row["edital_publicacoes"],
            row["item_ordem"],
            row["item_descricao"],
            row["item_descricao_norm"],
            row["quantidade"],
            row["valor_unitario"],
            row["valor_total"],
            row["found_in_proposals"],
            row["found_in_edital"],
            row["candidate_count"],
            row["min_proposal_unit"],
            row["max_proposal_unit"],
            row["nearest_supplier"],
            row["nearest_cnpj"],
            row["nearest_lote"],
            row["nearest_proposal_unit"],
            row["nearest_diff_rel"],
            row["anomaly_kind"],
            row["severity"],
            row["evidence_json"],
            datetime.now(),
        )
        for row in rows
    ]
    con.executemany(
        """
        INSERT INTO rb_contratos_item_audit (
            row_id, contrato_row_id, numero_contrato, numero_processo, ano, licitacao_id,
            licitacao_url, detail_url, source_kind, edital_publicacoes, item_ordem,
            item_descricao, item_descricao_norm, quantidade, valor_unitario, valor_total,
            found_in_proposals, found_in_edital, candidate_count, min_proposal_unit, max_proposal_unit,
            nearest_supplier, nearest_cnpj, nearest_lote, nearest_proposal_unit, nearest_diff_rel,
            anomaly_kind, severity, evidence_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def build_views(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_contratos_inconsistencias AS
        SELECT
            a.ano,
            a.numero_processo,
            a.numero_contrato,
            any_value(c.objeto) AS objeto,
            any_value(c.secretaria) AS secretaria,
            any_value(c.valor_referencia_brl) AS valor_referencia_brl,
            any_value(a.licitacao_id) AS licitacao_id,
            any_value(a.licitacao_url) AS licitacao_url,
            any_value(a.detail_url) AS detail_url,
            any_value(a.edital_publicacoes) AS edital_publicacoes,
            count(*) FILTER (WHERE a.anomaly_kind <> '') AS n_anomalias,
            string_agg(a.item_descricao || ' [' || a.anomaly_kind || ']', ' | ')
                FILTER (WHERE a.anomaly_kind <> '') AS resumo_itens,
            max(
                CASE a.severity
                    WHEN 'CRITICAL' THEN 3
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 1
                    ELSE 0
                END
            ) AS severity_rank
        FROM rb_contratos_item_audit a
        LEFT JOIN rb_contratos c ON c.row_id = a.contrato_row_id
        GROUP BY a.ano, a.numero_processo, a.numero_contrato
        HAVING count(*) FILTER (WHERE a.anomaly_kind <> '') > 0
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_contratos_triagem_final AS
        SELECT
            t.*,
            coalesce(i.n_anomalias, 0) AS n_inconsistencias_licitacao,
            i.resumo_itens AS inconsistencias_licitacao,
            CASE
                WHEN coalesce(t.sancao_ativa, FALSE) THEN t.fila_investigacao
                WHEN i.numero_contrato IS NOT NULL THEN 'auditoria_documental_licitacao'
                ELSE t.fila_investigacao
            END AS fila_investigacao_final,
            GREATEST(
                coalesce(t.prioridade, 0),
                CASE
                    WHEN coalesce(t.sancao_ativa, FALSE) THEN 100
                    WHEN i.numero_contrato IS NOT NULL THEN 95
                    ELSE 0
                END
            ) AS prioridade_final
        FROM v_rb_contratos_triagem t
        LEFT JOIN v_rb_contratos_inconsistencias i
          ON i.ano = t.ano
         AND i.numero_processo = t.numero_processo
         AND i.numero_contrato = t.numero_contrato
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_contratos_prioritarios AS
        SELECT *
        FROM v_rb_contratos_triagem_final
        WHERE prioridade_final >= 90
        ORDER BY prioridade_final DESC, valor_referencia_brl DESC, numero_contrato
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM v_rb_contratos_inconsistencias").fetchone()[0])


def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DELETE FROM insight WHERE kind = ?", [KIND_INCONSISTENCIA])
    rows = con.execute(
        """
        SELECT
            ano,
            numero_processo,
            numero_contrato,
            objeto,
            secretaria,
            valor_referencia_brl,
            licitacao_id,
            licitacao_url,
            detail_url,
            edital_publicacoes,
            n_anomalias,
            resumo_itens,
            severity_rank
        FROM v_rb_contratos_inconsistencias
        ORDER BY valor_referencia_brl DESC, numero_contrato
        """
    ).fetchall()
    if not rows:
        return 0

    payload = []
    for (
        ano,
        numero_processo,
        numero_contrato,
        objeto,
        secretaria,
        valor_referencia_brl,
        licitacao_id,
        licitacao_url,
        detail_url,
        edital_publicacoes,
        n_anomalias,
        resumo_itens,
        severity_rank,
    ) in rows:
        severity = "HIGH" if severity_rank >= 2 else "MEDIUM"
        publicacao_links = [
            f"https://cpl.riobranco.ac.gov.br/publicacao/{publicacao_id}"
            for publicacao_id in str(edital_publicacoes or "").split(",")
            if publicacao_id
        ]
        sources = [
            "rb_contratos",
            "rb_contratos_item_audit",
            "transparencia.riobranco.ac.gov.br/contrato",
            licitacao_url,
            *publicacao_links,
        ]
        tags = [
            "SUS",
            "SEMSA",
            "RIO_BRANCO",
            "contrato",
            "licitacao",
            "edital",
            "inconsistencia",
        ]
        insight_id = rb_contratos_sync.short_id(
            "INS",
            KIND_INCONSISTENCIA,
            numero_processo,
            numero_contrato,
            ano,
        )
        payload.append(
            (
                insight_id,
                KIND_INCONSISTENCIA,
                severity,
                92 if severity == "HIGH" else 84,
                float(valor_referencia_brl or 0),
                f"RB SUS: contrato {numero_contrato} diverge da licitação mãe do processo {numero_processo}",
                (
                    f"O contrato **{numero_contrato}** do processo **{numero_processo}** (licitação **{licitacao_id}**) "
                    f"apresentou **{int(n_anomalias or 0)} inconsistência(s)** quando comparado às propostas do portal "
                    f"e ao edital oficial da **CPL**. Principais achados: **{resumo_itens}**.\n\n"
                    f"Objeto: **{objeto}**.\n"
                    f"Secretaria: **{secretaria}**.\n"
                    f"Valor de referência: **R$ {float(valor_referencia_brl or 0):,.2f}**."
                ),
                "contrato_divergente_da_licitacao_mae",
                json.dumps(sources, ensure_ascii=False),
                json.dumps(tags, ensure_ascii=False),
                int(n_anomalias or 0),
                float(valor_referencia_brl or 0),
                "municipal",
                "Prefeitura de Rio Branco",
                "SEMSA",
                "Rio Branco",
                "AC",
                "saude",
                True,
                float(valor_referencia_brl or 0),
                int(ano or 0),
                "transparencia.riobranco.ac.gov.br/contrato + CPL Rio Branco",
            )
        )

    con.executemany(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl, title, description_md, pattern,
            sources, tags, sample_n, unit_total, created_at, esfera, ente, orgao,
            municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [row[:12] + (datetime.now(),) + row[12:] for row in payload],
    )
    return len(payload)


def run_audit(con: duckdb.DuckDBPyConnection) -> int:
    session = build_session()
    bridge_session = bridge.build_session()
    rows = con.execute(
        """
        SELECT
            row_id,
            ano,
            numero_contrato,
            numero_processo,
            objeto,
            secretaria,
            valor_referencia_brl,
            detail_url
        FROM rb_contratos
        WHERE sus = TRUE
          AND numero_processo IN ({})
        ORDER BY ano, numero_processo, numero_contrato
        """.format(",".join("?" for _ in bridge.PROCESS_HINTS)),
        list(bridge.PROCESS_HINTS.keys()),
    ).fetchall()
    if not rows:
        return 0

    audit_rows: list[dict[str, object]] = []
    proposal_cache: dict[str, list[bridge.Proposal]] = {}
    edital_cache: dict[str, tuple[str, str]] = {}

    for contrato_row in rows:
        numero_processo = str(contrato_row[3] or "")
        detail_url = str(contrato_row[7] or "")
        hint = bridge.PROCESS_HINTS.get(numero_processo)
        if not hint or not detail_url:
            continue
        licitacao_id = str(hint["licitacao_id"])
        if licitacao_id not in proposal_cache:
            proposal_cache[licitacao_id] = bridge.parse_licitacao_proposals(bridge_session, licitacao_id)
        if numero_processo not in edital_cache:
            edital_text, sources = fetch_edital_sources(session, numero_processo)
            edital_cache[numero_processo] = (normalize_text(edital_text), ",".join(CPL_PUBLICACOES.get(numero_processo, [])))
        edital_text_norm, edital_publicacoes = edital_cache[numero_processo]
        contract_items, source_kind = bridge.fetch_contract_items(bridge_session, detail_url)
        for item_ordem, item in enumerate(contract_items, start=1):
            audit_row = audit_item(
                item=item,
                item_ordem=item_ordem,
                contrato_row=contrato_row,
                licitacao_id=licitacao_id,
                proposals=proposal_cache[licitacao_id],
                edital_text_norm=edital_text_norm,
                edital_publicacoes=edital_publicacoes,
            )
            audit_row["source_kind"] = source_kind if source_kind else audit_row["source_kind"]
            audit_rows.append(audit_row)

    return upsert_audit_rows(con, audit_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita contratos SUS de Rio Branco contra a licitação mãe e o edital oficial da CPL."
    )
    parser.add_argument("--db-path", default=str(DB_PATH), help="Caminho do banco DuckDB.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db_path))
    ensure_tables(con)
    audited = run_audit(con)
    n_views = build_views(con)
    n_insights = build_insights(con)
    print(
        f"audited_rows={audited} | inconsistencias={n_views} | insights={n_insights}"
    )
    con.close()


if __name__ == "__main__":
    main()
