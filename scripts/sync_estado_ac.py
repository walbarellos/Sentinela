from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb

from src.core.insight_classification import ensure_insight_classification_columns
from src.ingest.transparencia_ac_connector import (
    ContratoRow,
    LicitacaoRow,
    PagamentoRow,
    TransparenciaAcConnector,
)

log = logging.getLogger("sync_estado_ac")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
ESFERA = "estadual"
ENTE = "Governo do Estado do Acre"
UF = "AC"
FONTE = "Portal da Transparência do Acre"
SOURCE_TAG = "transparencia.ac.gov.br"

AREA_MAP = {
    "SESACRE": "saude",
    "SEE": "educacao",
    "SEFAZ": "financas",
    "SEJUSP": "seguranca",
    "SEINFRA": "infraestrutura",
    "SEOP": "obras",
    "SEMA": "meio_ambiente",
    "SEAGRO": "agropecuaria",
    "SEAS": "assistencia_social",
    "SECD": "cultura_esporte",
    "SEDET": "desenvolvimento",
    "SEPLANH": "planejamento",
    "CASA CIVIL": "governo",
    "PGE": "juridico",
    "ALEAC": "legislativo",
    "TCE-AC": "controle",
    "MPAC": "ministerio_publico",
    "TJ-AC": "judiciario",
    "GOVERNO_ACRE": "gestao_estadual",
}
SUS_ORGAOS = {"SESACRE"}
ESTADO_KIND_PREFIX = "ESTADO_AC_"

DDL_PAGAMENTOS = """
CREATE TABLE IF NOT EXISTS estado_ac_pagamentos (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    data_movimento VARCHAR,
    numero_empenho VARCHAR,
    credor VARCHAR,
    cnpjcpf VARCHAR,
    natureza_despesa VARCHAR,
    modalidade VARCHAR,
    valor DOUBLE,
    id_empenho BIGINT,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    codigo_unidade VARCHAR,
    esfera VARCHAR DEFAULT 'estadual',
    ente VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf VARCHAR DEFAULT 'AC',
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_CONTRATOS = """
CREATE TABLE IF NOT EXISTS estado_ac_contratos (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    numero VARCHAR,
    tipo VARCHAR,
    data_inicio_vigencia VARCHAR,
    data_fim_vigencia VARCHAR,
    valor DOUBLE,
    credor VARCHAR,
    cnpjcpf VARCHAR,
    objeto VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    esfera VARCHAR DEFAULT 'estadual',
    ente VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf VARCHAR DEFAULT 'AC',
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_LICITACOES = """
CREATE TABLE IF NOT EXISTS estado_ac_licitacoes (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    numero_processo VARCHAR,
    modalidade VARCHAR,
    objeto VARCHAR,
    valor_estimado DOUBLE,
    valor_real DOUBLE,
    situacao VARCHAR,
    data_abertura VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    fornecedores_json JSON,
    esfera VARCHAR DEFAULT 'estadual',
    ente VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf VARCHAR DEFAULT 'AC',
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def ensure_extra_insight_columns(con: duckdb.DuckDBPyConnection) -> None:
    ensure_insight_classification_columns(con)
    existing = {row[1] for row in con.execute("PRAGMA table_info('insight')").fetchall()}
    extra_cols = {
        "valor_referencia": "DOUBLE",
        "ano_referencia": "INTEGER",
        "fonte": "VARCHAR",
    }
    for col, dtype in extra_cols.items():
        if col not in existing:
            con.execute(f"ALTER TABLE insight ADD COLUMN {col} {dtype}")


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def to_json(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def dedupe_payload(rows: list[tuple]) -> list[tuple]:
    unique: dict[object, tuple] = {}
    for row in rows:
        unique[row[0]] = row
    return list(unique.values())


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_PAGAMENTOS)
    con.execute(DDL_CONTRATOS)
    con.execute(DDL_LICITACOES)
    ensure_extra_insight_columns(con)


def upsert_pagamentos(con: duckdb.DuckDBPyConnection, rows: list[PagamentoRow], ano: int) -> int:
    con.execute("DELETE FROM estado_ac_pagamentos WHERE ano = ?", [ano])
    if not rows:
        return 0

    payload = [
        (
            row_hash(ano, row.numero_empenho, row.id_empenho, row.cnpjcpf, row.valor, row.data_movimento),
            ano,
            row.data_movimento,
            row.numero_empenho,
            row.credor,
            row.cnpjcpf,
            row.natureza_despesa,
            row.modalidade_licitacao,
            row.valor,
            row.id_empenho,
            row.orgao,
            row.unidade_gestora,
            row.codigo_unidade,
        )
        for row in rows
    ]
    payload = dedupe_payload(payload)
    con.executemany(
        """
        INSERT INTO estado_ac_pagamentos (
            row_id, ano, data_movimento, numero_empenho, credor, cnpjcpf,
            natureza_despesa, modalidade, valor, id_empenho,
            orgao, unidade_gestora, codigo_unidade
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def upsert_contratos(con: duckdb.DuckDBPyConnection, rows: list[ContratoRow], ano: int) -> int:
    con.execute("DELETE FROM estado_ac_contratos WHERE ano = ?", [ano])
    if not rows:
        return 0

    payload = [
        (
            row_hash(
                ano,
                row.numero,
                row.cnpjcpf,
                row.valor,
                row.data_inicio_vigencia,
                row.data_fim_vigencia,
                row.orgao,
                row.objeto,
            ),
            ano,
            row.numero,
            row.tipo,
            row.data_inicio_vigencia,
            row.data_fim_vigencia,
            row.valor,
            row.credor,
            row.cnpjcpf,
            row.objeto,
            row.orgao,
            row.unidade_gestora,
        )
        for row in rows
    ]
    payload = dedupe_payload(payload)
    con.executemany(
        """
        INSERT INTO estado_ac_contratos (
            row_id, ano, numero, tipo, data_inicio_vigencia, data_fim_vigencia,
            valor, credor, cnpjcpf, objeto, orgao, unidade_gestora
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def upsert_licitacoes(con: duckdb.DuckDBPyConnection, rows: list[LicitacaoRow], ano: int) -> int:
    con.execute("DELETE FROM estado_ac_licitacoes WHERE ano = ?", [ano])
    if not rows:
        return 0

    payload = [
        (
            row_hash(
                ano,
                row.numero_processo,
                row.modalidade,
                row.valor_real,
                row.data_abertura,
                row.orgao,
                row.objeto,
            ),
            ano,
            row.numero_processo,
            row.modalidade,
            row.objeto,
            row.valor_estimado,
            row.valor_real,
            row.situacao,
            row.data_abertura,
            row.orgao,
            row.unidade_gestora,
            to_json(row.fornecedores),
        )
        for row in rows
    ]
    payload = dedupe_payload(payload)
    con.executemany(
        """
        INSERT INTO estado_ac_licitacoes (
            row_id, ano, numero_processo, modalidade, objeto,
            valor_estimado, valor_real, situacao, data_abertura,
            orgao, unidade_gestora, fornecedores_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def severity_for_total(total: float, *, high: float, critical: float) -> str:
    if total >= critical:
        return "CRITICO"
    if total >= high:
        return "ALTO"
    return "MEDIO"


def confidence_for_count(count: int, *, base: int = 65, step: int = 3, cap: int = 95) -> int:
    return min(cap, base + count * step)


def build_insights(con: duckdb.DuckDBPyConnection, ano: int) -> list[dict]:
    now = datetime.now(UTC)
    insights: list[dict] = []

    pagamentos = con.execute(
        """
        SELECT orgao, COUNT(*) AS n_pag, SUM(valor) AS total_val, COUNT(DISTINCT cnpjcpf) AS n_cred
        FROM estado_ac_pagamentos
        WHERE ano = ? AND orgao IS NOT NULL AND orgao <> ''
        GROUP BY orgao
        ORDER BY total_val DESC
        """,
        [ano],
    ).fetchall()
    for orgao, n_pag, total_val, n_cred in pagamentos:
        area = AREA_MAP.get(orgao, "gestao_estadual")
        insights.append(
            {
                "id": f"ESTADO_AC:pagamentos:{ano}:{orgao}",
                "kind": f"{ESTADO_KIND_PREFIX}PAGAMENTO_ORGAO_ANO",
                "severity": severity_for_total(total_val or 0.0, high=10_000_000, critical=50_000_000),
                "confidence": confidence_for_count(int(n_pag), base=68),
                "exposure_brl": float(total_val or 0.0),
                "title": f"{orgao} — Pagamentos {ano}",
                "description_md": (
                    f"O órgão **{orgao}** realizou **{int(n_pag):,} pagamentos** em **{ano}**, "
                    f"totalizando **R$ {float(total_val or 0.0):,.2f}** para **{int(n_cred):,} credores distintos**."
                ),
                "pattern": "ORGAO_ESTADUAL -> PAGAMENTOS_AGREGADOS_POR_ANO",
                "sources": [FONTE],
                "tags": ["estado_ac", orgao, f"ano:{ano}", "pagamentos", area],
                "sample_n": int(n_pag),
                "unit_total": float(total_val or 0.0),
                "esfera": ESFERA,
                "ente": ENTE,
                "orgao": orgao,
                "municipio": "",
                "uf": UF,
                "area_tematica": area,
                "sus": orgao in SUS_ORGAOS,
                "valor_referencia": float(total_val or 0.0),
                "ano_referencia": ano,
                "fonte": SOURCE_TAG,
                "created_at": now,
            }
        )

    contratos = con.execute(
        """
        SELECT orgao, COUNT(*) AS n_contratos, SUM(valor) AS total_val
        FROM estado_ac_contratos
        WHERE ano = ? AND orgao IS NOT NULL AND orgao <> ''
        GROUP BY orgao
        ORDER BY total_val DESC
        """,
        [ano],
    ).fetchall()
    for orgao, n_contratos, total_val in contratos:
        area = AREA_MAP.get(orgao, "gestao_estadual")
        insights.append(
            {
                "id": f"ESTADO_AC:contratos:{ano}:{orgao}",
                "kind": f"{ESTADO_KIND_PREFIX}CONTRATO_ORGAO_ANO",
                "severity": severity_for_total(total_val or 0.0, high=5_000_000, critical=20_000_000),
                "confidence": confidence_for_count(int(n_contratos), base=66),
                "exposure_brl": float(total_val or 0.0),
                "title": f"{orgao} — Contratos {ano}",
                "description_md": (
                    f"O órgão **{orgao}** firmou **{int(n_contratos):,} contratos** em **{ano}**, "
                    f"com valor acumulado de **R$ {float(total_val or 0.0):,.2f}**."
                ),
                "pattern": "ORGAO_ESTADUAL -> CONTRATOS_AGREGADOS_POR_ANO",
                "sources": [FONTE],
                "tags": ["estado_ac", orgao, f"ano:{ano}", "contratos", area],
                "sample_n": int(n_contratos),
                "unit_total": float(total_val or 0.0),
                "esfera": ESFERA,
                "ente": ENTE,
                "orgao": orgao,
                "municipio": "",
                "uf": UF,
                "area_tematica": area,
                "sus": orgao in SUS_ORGAOS,
                "valor_referencia": float(total_val or 0.0),
                "ano_referencia": ano,
                "fonte": SOURCE_TAG,
                "created_at": now,
            }
        )

    licitacoes = con.execute(
        """
        SELECT orgao, modalidade, COUNT(*) AS n_licitacoes, SUM(COALESCE(valor_real, valor_estimado, 0)) AS total_val
        FROM estado_ac_licitacoes
        WHERE ano = ? AND orgao IS NOT NULL AND orgao <> ''
        GROUP BY orgao, modalidade
        HAVING COUNT(*) > 0
        ORDER BY total_val DESC
        LIMIT 100
        """,
        [ano],
    ).fetchall()
    for orgao, modalidade, n_licitacoes, total_val in licitacoes:
        area = AREA_MAP.get(orgao, "gestao_estadual")
        modalidade_norm = (modalidade or "").upper()
        severity = "ALTO" if any(flag in modalidade_norm for flag in ("DISPENSA", "INEXIGIBILIDADE")) else "MEDIO"
        insights.append(
            {
                "id": f"ESTADO_AC:licitacoes:{ano}:{orgao}:{modalidade_norm or 'NI'}",
                "kind": f"{ESTADO_KIND_PREFIX}LICITACAO_ORGAO_MODALIDADE_ANO",
                "severity": severity,
                "confidence": confidence_for_count(int(n_licitacoes), base=62),
                "exposure_brl": float(total_val or 0.0),
                "title": f"{orgao} — Licitações {modalidade or 'Geral'} {ano}",
                "description_md": (
                    f"O órgão **{orgao}** abriu **{int(n_licitacoes):,} licitação(ões)** "
                    f"na modalidade **{modalidade or 'N/I'}** em **{ano}**, "
                    f"somando **R$ {float(total_val or 0.0):,.2f}**."
                ),
                "pattern": "ORGAO_ESTADUAL -> LICITACOES_MODALIDADE_AGREGADAS_POR_ANO",
                "sources": [FONTE],
                "tags": ["estado_ac", orgao, f"ano:{ano}", "licitacoes", area, modalidade or "N/I"],
                "sample_n": int(n_licitacoes),
                "unit_total": float(total_val or 0.0),
                "esfera": ESFERA,
                "ente": ENTE,
                "orgao": orgao,
                "municipio": "",
                "uf": UF,
                "area_tematica": area,
                "sus": orgao in SUS_ORGAOS,
                "valor_referencia": float(total_val or 0.0),
                "ano_referencia": ano,
                "fonte": SOURCE_TAG,
                "created_at": now,
            }
        )

    return insights


def upsert_insights(con: duckdb.DuckDBPyConnection, insights: list[dict], ano: int) -> int:
    con.execute(
        """
        DELETE FROM insight
        WHERE esfera = 'estadual' AND uf = 'AC' AND ano_referencia = ?
          AND kind LIKE ?
        """,
        [ano, f"{ESTADO_KIND_PREFIX}%"],
    )
    if not insights:
        return 0

    payload = [
        (
            insight["id"],
            insight["kind"],
            insight["severity"],
            insight["confidence"],
            insight["exposure_brl"],
            insight["title"],
            insight["description_md"],
            insight["pattern"],
            json.dumps(insight["sources"], ensure_ascii=False),
            json.dumps(insight["tags"], ensure_ascii=False),
            insight["sample_n"],
            insight["unit_total"],
            insight["esfera"],
            insight["ente"],
            insight["orgao"],
            insight["municipio"],
            insight["uf"],
            insight["area_tematica"],
            insight["sus"],
            insight["valor_referencia"],
            insight["ano_referencia"],
            insight["fonte"],
            insight["created_at"],
        )
        for insight in insights
    ]
    con.executemany(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl,
            title, description_md, pattern, sources, tags,
            sample_n, unit_total,
            esfera, ente, orgao, municipio, uf, area_tematica, sus,
            valor_referencia, ano_referencia, fonte, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def log_orgao_preview(rows: list[PagamentoRow], label: str) -> None:
    counter = Counter(row.orgao or "SEM_MAPEAMENTO" for row in rows)
    log.info("%s — top orgaos:", label)
    for orgao, count in counter.most_common(10):
        log.info("  %-20s %d", orgao, count)


def run_sync(anos: list[int], force_rediscover: bool = False, dry_run: bool = False) -> None:
    connector = TransparenciaAcConnector(
        data_dir=str(ROOT / "data" / "transparencia_ac"),
        force=force_rediscover,
    )
    con = duckdb.connect(str(DB_PATH))
    ensure_tables(con)

    total_pag = total_cont = total_lic = total_ins = 0
    try:
        for ano in anos:
            log.info("=== Coletando Governo do Acre %d ===", ano)
            result = connector.run(anos=[ano])
            pagamentos = result["pagamentos"]
            contratos = result["contratos"]
            licitacoes = result["licitacoes"]

            log.info(
                "Coletado %d: %d pagamentos | %d contratos | %d licitacoes",
                ano,
                len(pagamentos),
                len(contratos),
                len(licitacoes),
            )
            log_orgao_preview(pagamentos, f"Pagamentos {ano}")

            if dry_run:
                continue

            total_pag += upsert_pagamentos(con, pagamentos, ano)
            total_cont += upsert_contratos(con, contratos, ano)
            total_lic += upsert_licitacoes(con, licitacoes, ano)

            insights = build_insights(con, ano)
            total_ins += upsert_insights(con, insights, ano)
            log.info("Ano %d gravado com %d insights estaduais", ano, len(insights))
    finally:
        con.close()

    if not dry_run:
        log.info(
            "Concluído: %d pagamentos | %d contratos | %d licitacoes | %d insights",
            total_pag,
            total_cont,
            total_lic,
            total_ins,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Governo do Acre -> DuckDB canônico")
    parser.add_argument("--anos", nargs="+", type=int, default=[2024, 2023])
    parser.add_argument("--force-rediscover", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_sync(args.anos, force_rediscover=args.force_rediscover, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
