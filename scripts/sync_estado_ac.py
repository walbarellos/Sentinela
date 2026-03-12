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
    FornecedorDetalheRow,
    FornecedorResumoRow,
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
FORNECEDOR_ORGAOS_ALVO = {"SESACRE"}
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

DDL_FORNECEDOR_DETALHES = """
CREATE TABLE IF NOT EXISTS estado_ac_fornecedor_detalhes (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    entidade VARCHAR,
    orgao VARCHAR,
    razao_social VARCHAR,
    cnpjcpf VARCHAR,
    numero_empenho VARCHAR,
    ano_empenho VARCHAR,
    data_empenho VARCHAR,
    total_empenho DOUBLE,
    historico VARCHAR,
    despesa_orcamentaria VARCHAR,
    funcao VARCHAR,
    subfuncao VARCHAR,
    fonte_recurso VARCHAR,
    numero_liquidacao VARCHAR,
    data_liquidacao VARCHAR,
    valor_liquidacao DOUBLE,
    numero_pagamento VARCHAR,
    data_pagamento VARCHAR,
    valor_pagamento DOUBLE,
    esfera VARCHAR DEFAULT 'estadual',
    ente VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf VARCHAR DEFAULT 'AC',
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_FORNECEDORES = """
CREATE TABLE IF NOT EXISTS estado_ac_fornecedores (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    orgao VARCHAR,
    razao_social VARCHAR,
    cnpjcpf VARCHAR,
    total_empenhado DOUBLE,
    total_liquidado DOUBLE,
    total_pago DOUBLE,
    n_empenhos INTEGER,
    n_liquidacoes INTEGER,
    n_pagamentos INTEGER,
    entidades_json JSON,
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
    con.execute(DDL_FORNECEDOR_DETALHES)
    con.execute(DDL_FORNECEDORES)
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


def upsert_fornecedor_detalhes(
    con: duckdb.DuckDBPyConnection,
    rows: list[FornecedorDetalheRow],
    ano: int,
) -> int:
    con.execute("DELETE FROM estado_ac_fornecedor_detalhes WHERE ano = ?", [ano])
    if not rows:
        return 0

    payload = [
        (
            row_hash(
                ano,
                row.entidade,
                row.razao_social,
                row.cnpjcpf,
                row.numero_empenho,
                row.ano_empenho,
                row.numero_liquidacao,
                row.numero_pagamento,
                row.valor_liquidacao,
                row.valor_pagamento,
            ),
            ano,
            row.entidade,
            row.orgao,
            row.razao_social,
            row.cnpjcpf,
            row.numero_empenho,
            row.ano_empenho,
            row.data_empenho,
            row.total_empenho,
            row.historico,
            row.despesa_orcamentaria,
            row.funcao,
            row.subfuncao,
            row.fonte_recurso,
            row.numero_liquidacao,
            row.data_liquidacao,
            row.valor_liquidacao,
            row.numero_pagamento,
            row.data_pagamento,
            row.valor_pagamento,
        )
        for row in rows
    ]
    payload = dedupe_payload(payload)
    con.executemany(
        """
        INSERT INTO estado_ac_fornecedor_detalhes (
            row_id, ano, entidade, orgao, razao_social, cnpjcpf,
            numero_empenho, ano_empenho, data_empenho, total_empenho,
            historico, despesa_orcamentaria, funcao, subfuncao, fonte_recurso,
            numero_liquidacao, data_liquidacao, valor_liquidacao,
            numero_pagamento, data_pagamento, valor_pagamento
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def _supplier_identity(razao_social: str, cnpjcpf: str) -> str:
    return (cnpjcpf or razao_social).strip().upper()


def _event_key(*parts: object) -> str:
    return "|".join("" if part is None else str(part).strip() for part in parts)


def aggregate_fornecedores(rows: list[FornecedorDetalheRow]) -> list[dict]:
    buckets: dict[tuple[int, str, str], dict] = {}

    for row in rows:
        fornecedor_key = _supplier_identity(row.razao_social, row.cnpjcpf)
        bucket_key = (row.ano, row.orgao or "GOVERNO_ACRE", fornecedor_key)
        bucket = buckets.setdefault(
            bucket_key,
            {
                "ano": row.ano,
                "orgao": row.orgao or "GOVERNO_ACRE",
                "razao_social": row.razao_social,
                "cnpjcpf": row.cnpjcpf,
                "total_empenhado": 0.0,
                "total_liquidado": 0.0,
                "total_pago": 0.0,
                "n_empenhos": 0,
                "n_liquidacoes": 0,
                "n_pagamentos": 0,
                "entidades": set(),
                "empenhos_seen": set(),
                "liquidacoes_seen": set(),
                "pagamentos_seen": set(),
            },
        )

        if row.cnpjcpf and not bucket["cnpjcpf"]:
            bucket["cnpjcpf"] = row.cnpjcpf
        if row.razao_social and len(row.razao_social) > len(bucket["razao_social"]):
            bucket["razao_social"] = row.razao_social
        if row.entidade:
            bucket["entidades"].add(row.entidade)

        if row.numero_empenho or row.total_empenho:
            empenho_key = _event_key(
                row.entidade,
                row.numero_empenho or row.data_empenho or row.historico,
                row.ano_empenho or row.ano,
                f"{row.total_empenho:.2f}",
            )
            if empenho_key not in bucket["empenhos_seen"]:
                bucket["empenhos_seen"].add(empenho_key)
                bucket["n_empenhos"] += 1
                bucket["total_empenhado"] += float(row.total_empenho or 0.0)

        if row.numero_liquidacao or row.valor_liquidacao:
            liquidacao_key = _event_key(
                row.entidade,
                row.numero_empenho,
                row.numero_liquidacao or row.data_liquidacao,
                f"{row.valor_liquidacao:.2f}",
            )
            if liquidacao_key not in bucket["liquidacoes_seen"]:
                bucket["liquidacoes_seen"].add(liquidacao_key)
                bucket["n_liquidacoes"] += 1
                bucket["total_liquidado"] += float(row.valor_liquidacao or 0.0)

        if row.numero_pagamento or row.valor_pagamento:
            pagamento_key = _event_key(
                row.entidade,
                row.numero_empenho,
                row.numero_pagamento or row.data_pagamento,
                f"{row.valor_pagamento:.2f}",
            )
            if pagamento_key not in bucket["pagamentos_seen"]:
                bucket["pagamentos_seen"].add(pagamento_key)
                bucket["n_pagamentos"] += 1
                bucket["total_pago"] += float(row.valor_pagamento or 0.0)

    aggregated: list[dict] = []
    for bucket in buckets.values():
        aggregated.append(
            {
                "ano": bucket["ano"],
                "orgao": bucket["orgao"],
                "razao_social": bucket["razao_social"],
                "cnpjcpf": bucket["cnpjcpf"],
                "total_empenhado": bucket["total_empenhado"],
                "total_liquidado": bucket["total_liquidado"],
                "total_pago": bucket["total_pago"],
                "n_empenhos": bucket["n_empenhos"],
                "n_liquidacoes": bucket["n_liquidacoes"],
                "n_pagamentos": bucket["n_pagamentos"],
                "entidades_json": to_json(sorted(bucket["entidades"])),
            }
        )
    return aggregated


def aggregate_fornecedor_resumos(rows: list[FornecedorResumoRow]) -> list[dict]:
    buckets: dict[tuple[int, str, str], dict] = {}

    for row in rows:
        fornecedor_key = _supplier_identity(row.razao_social, row.cnpjcpf)
        bucket_key = (row.ano, row.orgao or "GOVERNO_ACRE", fornecedor_key)
        bucket = buckets.setdefault(
            bucket_key,
            {
                "ano": row.ano,
                "orgao": row.orgao or "GOVERNO_ACRE",
                "razao_social": row.razao_social,
                "cnpjcpf": row.cnpjcpf,
                "total_empenhado": 0.0,
                "total_liquidado": 0.0,
                "total_pago": 0.0,
                "n_empenhos": None,
                "n_liquidacoes": None,
                "n_pagamentos": None,
                "entidades": set(),
            },
        )
        if row.cnpjcpf and not bucket["cnpjcpf"]:
            bucket["cnpjcpf"] = row.cnpjcpf
        if row.razao_social and len(row.razao_social) > len(bucket["razao_social"]):
            bucket["razao_social"] = row.razao_social
        if row.entidade:
            bucket["entidades"].add(row.entidade)
        bucket["total_empenhado"] += float(row.empenhado or 0.0)
        bucket["total_liquidado"] += float(row.liquidado or 0.0)
        bucket["total_pago"] += float(row.pago or 0.0)

    aggregated: list[dict] = []
    for bucket in buckets.values():
        aggregated.append(
            {
                "ano": bucket["ano"],
                "orgao": bucket["orgao"],
                "razao_social": bucket["razao_social"],
                "cnpjcpf": bucket["cnpjcpf"],
                "total_empenhado": bucket["total_empenhado"],
                "total_liquidado": bucket["total_liquidado"],
                "total_pago": bucket["total_pago"],
                "n_empenhos": bucket["n_empenhos"],
                "n_liquidacoes": bucket["n_liquidacoes"],
                "n_pagamentos": bucket["n_pagamentos"],
                "entidades_json": to_json(sorted(bucket["entidades"])),
            }
        )
    return aggregated


def upsert_fornecedores(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict],
    ano: int,
) -> int:
    con.execute("DELETE FROM estado_ac_fornecedores WHERE ano = ?", [ano])
    if not rows:
        return 0

    payload = [
        (
            row_hash(
                row["ano"],
                row["orgao"],
                row["cnpjcpf"],
                row["razao_social"],
            ),
            row["ano"],
            row["orgao"],
            row["razao_social"],
            row["cnpjcpf"],
            row["total_empenhado"],
            row["total_liquidado"],
            row["total_pago"],
            row["n_empenhos"],
            row["n_liquidacoes"],
            row["n_pagamentos"],
            row["entidades_json"],
        )
        for row in rows
    ]
    payload = dedupe_payload(payload)
    con.executemany(
        """
        INSERT INTO estado_ac_fornecedores (
            row_id, ano, orgao, razao_social, cnpjcpf,
            total_empenhado, total_liquidado, total_pago,
            n_empenhos, n_liquidacoes, n_pagamentos, entidades_json
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

    fornecedores = con.execute(
        """
        WITH ranked AS (
            SELECT
                orgao,
                razao_social,
                cnpjcpf,
                total_pago,
                total_liquidado,
                total_empenhado,
                n_empenhos,
                n_liquidacoes,
                n_pagamentos,
                ROW_NUMBER() OVER (
                    PARTITION BY orgao
                    ORDER BY total_pago DESC, total_liquidado DESC, total_empenhado DESC, razao_social
                ) AS rn
            FROM estado_ac_fornecedores
            WHERE ano = ? AND orgao IS NOT NULL AND orgao <> '' AND COALESCE(total_pago, 0) > 0
        )
        SELECT
            orgao, razao_social, cnpjcpf, total_pago, total_liquidado, total_empenhado,
            n_empenhos, n_liquidacoes, n_pagamentos
        FROM ranked
        WHERE rn <= 5
        ORDER BY orgao, total_pago DESC, razao_social
        """,
        [ano],
    ).fetchall()
    for (
        orgao,
        razao_social,
        cnpjcpf,
        total_pago,
        total_liquidado,
        total_empenhado,
        n_empenhos,
        n_liquidacoes,
        n_pagamentos,
    ) in fornecedores:
        area = AREA_MAP.get(orgao, "gestao_estadual")
        doc_label = f" ({cnpjcpf})" if cnpjcpf else ""
        supplier_id = cnpjcpf or row_hash(orgao, razao_social)[:12]
        n_pag = int(n_pagamentos or 0)
        n_liq = int(n_liquidacoes or 0)
        n_emp = int(n_empenhos or 0)
        if n_pag or n_liq or n_emp:
            description_md = (
                f"O fornecedor **{razao_social}**"
                f"{f' (`{cnpjcpf}`)' if cnpjcpf else ''} recebeu **R$ {float(total_pago or 0.0):,.2f}** "
                f"do órgão **{orgao}** em **{ano}**, com **{n_pag:,} pagamentos**, "
                f"**{n_liq:,} liquidações** e **{n_emp:,} empenhos** "
                f"identificados no portal estadual."
            )
        else:
            description_md = (
                f"O fornecedor **{razao_social}**"
                f"{f' (`{cnpjcpf}`)' if cnpjcpf else ''} aparece no recorte oficial do órgão **{orgao}** "
                f"em **{ano}**, com total pago de **R$ {float(total_pago or 0.0):,.2f}**, "
                f"total liquidado de **R$ {float(total_liquidado or 0.0):,.2f}** "
                f"e total empenhado de **R$ {float(total_empenhado or 0.0):,.2f}**."
            )
        insights.append(
            {
                "id": f"ESTADO_AC:fornecedor:{ano}:{orgao}:{supplier_id}",
                "kind": f"{ESTADO_KIND_PREFIX}FORNECEDOR_ORGAO_ANO",
                "severity": severity_for_total(total_pago or 0.0, high=1_000_000, critical=10_000_000),
                "confidence": confidence_for_count(
                    max(n_pag, n_emp, 1),
                    base=72,
                    step=2,
                ),
                "exposure_brl": float(total_pago or 0.0),
                "title": f"{orgao} — {razao_social}{doc_label} em {ano}",
                "description_md": description_md,
                "pattern": "ORGAO_ESTADUAL -> FORNECEDOR_CNPJ -> PAGAMENTOS_AGREGADOS_POR_ANO",
                "sources": [FONTE],
                "tags": [
                    "estado_ac",
                    orgao,
                    f"ano:{ano}",
                    "fornecedores",
                    area,
                    *( [f"cnpj:{cnpjcpf}"] if cnpjcpf else [] ),
                ],
                "sample_n": max(n_pag, n_emp, 1),
                "unit_total": float(total_pago or 0.0),
                "esfera": ESFERA,
                "ente": ENTE,
                "orgao": orgao,
                "municipio": "",
                "uf": UF,
                "area_tematica": area,
                "sus": orgao in SUS_ORGAOS,
                "valor_referencia": float(total_pago or 0.0),
                "ano_referencia": ano,
                "fonte": SOURCE_TAG,
                "created_at": now,
            }
        )

    concentracao = con.execute(
        """
        WITH ranked AS (
            SELECT
                orgao,
                razao_social,
                cnpjcpf,
                total_pago,
                SUM(total_pago) OVER (PARTITION BY orgao) AS total_orgao_pago,
                ROW_NUMBER() OVER (
                    PARTITION BY orgao
                    ORDER BY total_pago DESC, razao_social
                ) AS rn
            FROM estado_ac_fornecedores
            WHERE ano = ? AND orgao IS NOT NULL AND orgao <> '' AND COALESCE(total_pago, 0) > 0
        )
        SELECT
            orgao,
            razao_social,
            cnpjcpf,
            total_pago,
            total_orgao_pago,
            CASE
                WHEN total_orgao_pago = 0 THEN 0
                ELSE total_pago / total_orgao_pago
            END AS share_pago
        FROM ranked
        WHERE rn = 1 AND total_orgao_pago >= 1000000 AND total_pago / NULLIF(total_orgao_pago, 0) >= 0.20
        ORDER BY share_pago DESC, total_pago DESC
        """,
        [ano],
    ).fetchall()
    for orgao, razao_social, cnpjcpf, total_pago, total_orgao_pago, share_pago in concentracao:
        area = AREA_MAP.get(orgao, "gestao_estadual")
        share = float(share_pago or 0.0)
        if share >= 0.5 and float(total_pago or 0.0) >= 5_000_000:
            severity = "CRITICO"
        elif share >= 0.3 or float(total_pago or 0.0) >= 5_000_000:
            severity = "ALTO"
        else:
            severity = "MEDIO"
        supplier_id = cnpjcpf or row_hash(orgao, razao_social)[:12]
        insights.append(
            {
                "id": f"ESTADO_AC:concentracao-fornecedor:{ano}:{orgao}:{supplier_id}",
                "kind": f"{ESTADO_KIND_PREFIX}CONCENTRACAO_FORNECEDOR_ORGAO_ANO",
                "severity": severity,
                "confidence": 78,
                "exposure_brl": float(total_pago or 0.0),
                "title": f"{orgao} — concentração em fornecedor líder {ano}",
                "description_md": (
                    f"O principal fornecedor de **{orgao}** em **{ano}** foi **{razao_social}**"
                    f"{f' (`{cnpjcpf}`)' if cnpjcpf else ''}, responsável por **{share:.1%}** "
                    f"do valor pago pelo órgão no ano, equivalente a **R$ {float(total_pago or 0.0):,.2f}** "
                    f"de um total de **R$ {float(total_orgao_pago or 0.0):,.2f}**."
                ),
                "pattern": "ORGAO_ESTADUAL -> CONCENTRACAO_FORNECEDOR_LIDER_POR_ANO",
                "sources": [FONTE],
                "tags": [
                    "estado_ac",
                    orgao,
                    f"ano:{ano}",
                    "concentracao_fornecedor",
                    area,
                    *( [f"cnpj:{cnpjcpf}"] if cnpjcpf else [] ),
                ],
                "sample_n": 1,
                "unit_total": float(total_orgao_pago or 0.0),
                "esfera": ESFERA,
                "ente": ENTE,
                "orgao": orgao,
                "municipio": "",
                "uf": UF,
                "area_tematica": area,
                "sus": orgao in SUS_ORGAOS,
                "valor_referencia": float(total_pago or 0.0),
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


def run_sync(
    anos: list[int],
    force_rediscover: bool = False,
    dry_run: bool = False,
    max_fornecedores_detalhe: int | None = None,
) -> None:
    connector = TransparenciaAcConnector(
        data_dir=str(ROOT / "data" / "transparencia_ac"),
        force=force_rediscover,
    )
    con = duckdb.connect(str(DB_PATH))
    ensure_tables(con)

    total_pag = total_cont = total_lic = total_ins = 0
    total_forn = total_forn_det = 0
    try:
        for ano in anos:
            log.info("=== Coletando Governo do Acre %d ===", ano)
            result = connector.run(anos=[ano])
            pagamentos = result["pagamentos"]
            contratos = result["contratos"]
            licitacoes = result["licitacoes"]
            log.info(
                "Base %d concluída: %d pagamentos | %d contratos | %d licitacoes",
                ano,
                len(pagamentos),
                len(contratos),
                len(licitacoes),
            )
            fornecedores_resumo: list[FornecedorResumoRow] = []
            fornecedor_detalhes: list[FornecedorDetalheRow] = []
            fornecedores_ok = False

            try:
                for orgao_alvo in sorted(FORNECEDOR_ORGAOS_ALVO):
                    fornecedores_resumo.extend(connector.get_fornecedores_por_orgao(ano, orgao_alvo))
                fornecedores_resumo.sort(key=lambda row: row.pago, reverse=True)
                log.info(
                    "Fornecedores %d agregados por órgão-alvo (%s): %d",
                    ano,
                    ", ".join(sorted(FORNECEDOR_ORGAOS_ALVO)),
                    len(fornecedores_resumo),
                )
                fornecedores_ok = bool(fornecedores_resumo)
                if fornecedores_ok and max_fornecedores_detalhe is not None:
                    fornecedor_detalhes = connector.get_fornecedor_detalhes(
                        ano,
                        fornecedores=fornecedores_resumo,
                        max_fornecedores=max_fornecedores_detalhe,
                    )
                if not fornecedores_ok:
                    log.warning(
                        "Fornecedores %d sem linhas úteis no recorte por órgão; preservando dados já gravados para esse eixo",
                        ano,
                    )
            except Exception as exc:
                log.warning(
                    "Coleta de fornecedores do Acre falhou em %d: %s. Pagamentos/contratos/licitações seguem normalmente.",
                    ano,
                    exc,
                )

            log.info(
                "Coletado %d: %d pagamentos | %d contratos | %d licitacoes | %d fornecedores_filtrados | %d detalhes",
                ano,
                len(pagamentos),
                len(contratos),
                len(licitacoes),
                len(fornecedores_resumo),
                len(fornecedor_detalhes),
            )
            log_orgao_preview(pagamentos, f"Pagamentos {ano}")

            if dry_run:
                continue

            total_pag += upsert_pagamentos(con, pagamentos, ano)
            total_cont += upsert_contratos(con, contratos, ano)
            total_lic += upsert_licitacoes(con, licitacoes, ano)
            if fornecedores_ok:
                total_forn += upsert_fornecedores(con, aggregate_fornecedor_resumos(fornecedores_resumo), ano)
                if fornecedor_detalhes:
                    total_forn_det += upsert_fornecedor_detalhes(con, fornecedor_detalhes, ano)

            insights = build_insights(con, ano)
            total_ins += upsert_insights(con, insights, ano)
            log.info("Ano %d gravado com %d insights estaduais", ano, len(insights))
    finally:
        con.close()

    if not dry_run:
        log.info(
            "Concluído: %d pagamentos | %d contratos | %d licitacoes | %d fornecedores | %d fornecedor_detalhes | %d insights",
            total_pag,
            total_cont,
            total_lic,
            total_forn,
            total_forn_det,
            total_ins,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Governo do Acre -> DuckDB canônico")
    parser.add_argument("--anos", nargs="+", type=int, default=[2024, 2023])
    parser.add_argument("--force-rediscover", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--max-fornecedores-detalhe",
        type=int,
        default=None,
        help="Limita entidades detalhadas do órgão-alvo; permitido apenas com --dry-run para validação controlada",
    )
    args = parser.parse_args()
    if args.max_fornecedores_detalhe is not None and not args.dry_run:
        parser.error("--max-fornecedores-detalhe só pode ser usado com --dry-run")
    run_sync(
        args.anos,
        force_rediscover=args.force_rediscover,
        dry_run=args.dry_run,
        max_fornecedores_detalhe=args.max_fornecedores_detalhe,
    )


if __name__ == "__main__":
    main()
