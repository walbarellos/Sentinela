"""
sync_estado_ac.py
-----------------
Persiste pagamentos, contratos e licitações do Governo do Acre no DuckDB
canônico, com os campos de recorte institucional preenchidos:

    esfera     = 'estadual'
    ente       = 'Governo do Estado do Acre'
    orgao      = <canônico, ex: 'SESACRE', 'SEE', 'SEFAZ', ...>
    uf         = 'AC'
    municipio  = ''   (não se aplica para estadual)

Após persistir os dados brutos, gera insights agregados e os grava
na tabela `insight` com a taxonomia canônica completa.

Uso:
    python scripts/sync_estado_ac.py --anos 2023 2024
    python scripts/sync_estado_ac.py --anos 2024 --force-rediscover
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb

from src.ingest.transparencia_ac_connector import (
    TransparenciaAcConnector,
    PagamentoRow,
    ContratoRow,
    LicitacaoRow,
)

log = logging.getLogger("sync_estado_ac")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DUCKDB_PATH = ROOT / "data" / "v2_core.duckdb"
ESFERA = "estadual"
ENTE = "Governo do Estado do Acre"
UF = "AC"


DDL_PAGAMENTOS = """
CREATE TABLE IF NOT EXISTS estado_ac_pagamentos (
    id               INTEGER PRIMARY KEY,
    data_movimento   VARCHAR,
    numero_empenho   VARCHAR,
    credor           VARCHAR,
    cnpjcpf          VARCHAR,
    natureza_despesa VARCHAR,
    modalidade       VARCHAR,
    valor            DOUBLE,
    id_empenho       INTEGER,
    orgao            VARCHAR,
    unidade_gestora  VARCHAR,
    codigo_unidade   VARCHAR,
    esfera           VARCHAR DEFAULT 'estadual',
    ente             VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf               VARCHAR DEFAULT 'AC',
    inserted_at      TIMESTAMP DEFAULT current_timestamp,
    ano              INTEGER
)
"""

DDL_CONTRATOS = """
CREATE TABLE IF NOT EXISTS estado_ac_contratos (
    id                    INTEGER PRIMARY KEY,
    numero                VARCHAR,
    tipo                  VARCHAR,
    data_inicio_vigencia  VARCHAR,
    data_fim_vigencia     VARCHAR,
    valor                 DOUBLE,
    credor                VARCHAR,
    cnpjcpf               VARCHAR,
    objeto                VARCHAR,
    orgao                 VARCHAR,
    unidade_gestora       VARCHAR,
    esfera                VARCHAR DEFAULT 'estadual',
    ente                  VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf                    VARCHAR DEFAULT 'AC',
    inserted_at           TIMESTAMP DEFAULT current_timestamp,
    ano                   INTEGER
)
"""

DDL_LICITACOES = """
CREATE TABLE IF NOT EXISTS estado_ac_licitacoes (
    id               INTEGER PRIMARY KEY,
    numero_processo  VARCHAR,
    modalidade       VARCHAR,
    objeto           VARCHAR,
    valor_estimado   DOUBLE,
    valor_real       DOUBLE,
    situacao         VARCHAR,
    data_abertura    VARCHAR,
    orgao            VARCHAR,
    unidade_gestora  VARCHAR,
    esfera           VARCHAR DEFAULT 'estadual',
    ente             VARCHAR DEFAULT 'Governo do Estado do Acre',
    uf               VARCHAR DEFAULT 'AC',
    inserted_at      TIMESTAMP DEFAULT current_timestamp,
    ano              INTEGER
)
"""


def _upsert_pagamentos(con, rows: list[PagamentoRow], ano: int) -> int:
    if not rows:
        return 0
    con.execute("DELETE FROM estado_ac_pagamentos WHERE ano = ?", [ano])
    con.executemany(
        """
        INSERT INTO estado_ac_pagamentos
            (data_movimento, numero_empenho, credor, cnpjcpf,
             natureza_despesa, modalidade, valor, id_empenho,
             orgao, unidade_gestora, codigo_unidade, ano)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (r.data_movimento, r.numero_empenho, r.credor, r.cnpjcpf,
             r.natureza_despesa, r.modalidade_licitacao, r.valor, r.id_empenho,
             r.orgao, r.unidade_gestora, r.codigo_unidade, ano)
            for r in rows
        ],
    )
    return len(rows)


def _upsert_contratos(con, rows: list[ContratoRow], ano: int) -> int:
    if not rows:
        return 0
    con.execute("DELETE FROM estado_ac_contratos WHERE ano = ?", [ano])
    con.executemany(
        """
        INSERT INTO estado_ac_contratos
            (numero, tipo, data_inicio_vigencia, data_fim_vigencia,
             valor, credor, cnpjcpf, objeto, orgao, unidade_gestora, ano)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (r.numero, r.tipo, r.data_inicio_vigencia, r.data_fim_vigencia,
             r.valor, r.credor, r.cnpjcpf, r.objeto, r.orgao, r.unidade_gestora, ano)
            for r in rows
        ],
    )
    return len(rows)


def _upsert_licitacoes(con, rows: list[LicitacaoRow], ano: int) -> int:
    if not rows:
        return 0
    con.execute("DELETE FROM estado_ac_licitacoes WHERE ano = ?", [ano])
    con.executemany(
        """
        INSERT INTO estado_ac_licitacoes
            (numero_processo, modalidade, objeto, valor_estimado, valor_real,
             situacao, data_abertura, orgao, unidade_gestora, ano)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (r.numero_processo, r.modalidade, r.objeto, r.valor_estimado, r.valor_real,
             r.situacao, r.data_abertura, r.orgao, r.unidade_gestora, ano)
            for r in rows
        ],
    )
    return len(rows)


AREA_MAP = {
    "SESACRE": "saude", "SEE": "educacao", "SEFAZ": "financas",
    "SEJUSP": "seguranca", "SEINFRA": "infraestrutura", "SEOP": "obras",
    "SEMA": "meio_ambiente", "SEAGRO": "agropecuaria", "SEAS": "assistencia_social",
    "SECD": "cultura_esporte", "SEDET": "desenvolvimento", "SEPLANH": "planejamento",
    "CASA CIVIL": "governo", "PGE": "juridico", "ALEAC": "legislativo",
    "TCE-AC": "controle", "MPAC": "ministerio_publico", "TJ-AC": "judiciario",
    "GOVERNO_ACRE": "gestao_estadual",
}
SUS_ORGAOS = {"SESACRE"}


def generate_insights(con, ano: int) -> list[dict]:
    insights = []
    hoje = datetime.now().isoformat()

    for orgao, n_pag, total_val, n_cred in con.execute(
        """SELECT orgao, COUNT(*), SUM(valor), COUNT(DISTINCT cnpjcpf)
           FROM estado_ac_pagamentos WHERE ano=? AND orgao!=''
           GROUP BY orgao ORDER BY SUM(valor) DESC""", [ano]
    ).fetchall():
        insights.append({
            "titulo": f"{orgao} — Pagamentos {ano}",
            "descricao": (
                f"O órgão {orgao} realizou {n_pag:,} pagamentos em {ano}, "
                f"totalizando R$ {total_val:,.2f} para {n_cred} credores distintos."
            ),
            "tipo": "pagamento_orgao_ano", "esfera": ESFERA, "ente": ENTE,
            "orgao": orgao, "municipio": "", "uf": UF,
            "area_tematica": AREA_MAP.get(orgao, "gestao_estadual"),
            "sus": orgao in SUS_ORGAOS,
            "valor_referencia": total_val, "ano_referencia": ano,
            "fonte": "transparencia.ac.gov.br", "created_at": hoje,
        })

    for orgao, n_cont, total_val in con.execute(
        """SELECT orgao, COUNT(*), SUM(valor)
           FROM estado_ac_contratos WHERE ano=? AND orgao!=''
           GROUP BY orgao ORDER BY SUM(valor) DESC""", [ano]
    ).fetchall():
        insights.append({
            "titulo": f"{orgao} — Contratos {ano}",
            "descricao": f"{orgao} firmou {n_cont} contratos em {ano}, totalizando R$ {total_val:,.2f}.",
            "tipo": "contrato_orgao_ano", "esfera": ESFERA, "ente": ENTE,
            "orgao": orgao, "municipio": "", "uf": UF,
            "area_tematica": AREA_MAP.get(orgao, "gestao_estadual"),
            "sus": orgao in SUS_ORGAOS,
            "valor_referencia": total_val, "ano_referencia": ano,
            "fonte": "transparencia.ac.gov.br", "created_at": hoje,
        })

    for orgao, modalidade, n_lic, total_val in con.execute(
        """SELECT orgao, modalidade, COUNT(*), SUM(valor_real)
           FROM estado_ac_licitacoes WHERE ano=? AND orgao!=''
           GROUP BY orgao, modalidade HAVING COUNT(*)>0
           ORDER BY SUM(valor_real) DESC LIMIT 100""", [ano]
    ).fetchall():
        insights.append({
            "titulo": f"{orgao} — Licitações {modalidade or 'Geral'} {ano}",
            "descricao": (
                f"{orgao} abriu {n_lic} licitação(ões) na modalidade "
                f"'{modalidade or 'N/I'}' em {ano}, valor total R$ {total_val:,.2f}."
            ),
            "tipo": "licitacao_orgao_modalidade_ano", "esfera": ESFERA, "ente": ENTE,
            "orgao": orgao, "municipio": "", "uf": UF,
            "area_tematica": AREA_MAP.get(orgao, "gestao_estadual"),
            "sus": orgao in SUS_ORGAOS,
            "valor_referencia": total_val, "ano_referencia": ano,
            "fonte": "transparencia.ac.gov.br", "created_at": hoje,
        })

    return insights


def _ensure_insight_columns(con):
    existing = {row[0] for row in con.execute("PRAGMA table_info(insight)").fetchall()}
    extra_cols = {
        "esfera": "VARCHAR", "ente": "VARCHAR", "orgao": "VARCHAR",
        "municipio": "VARCHAR", "uf": "VARCHAR", "area_tematica": "VARCHAR",
        "sus": "BOOLEAN", "valor_referencia": "DOUBLE",
        "ano_referencia": "INTEGER", "fonte": "VARCHAR",
    }
    for col, dtype in extra_cols.items():
        if col not in existing:
            log.info("Adicionando coluna insight.%s (%s)", col, dtype)
            con.execute(f"ALTER TABLE insight ADD COLUMN {col} {dtype}")


def _upsert_insights(con, insights: list[dict], ano: int) -> int:
    if not insights:
        return 0
    con.execute(
        "DELETE FROM insight WHERE esfera='estadual' AND uf='AC' AND ano_referencia=?",
        [ano],
    )
    con.executemany(
        """INSERT INTO insight
            (titulo, descricao, tipo, esfera, ente, orgao,
             municipio, uf, area_tematica, sus,
             valor_referencia, ano_referencia, fonte, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (i["titulo"], i["descricao"], i["tipo"], i["esfera"], i["ente"], i["orgao"],
             i["municipio"], i["uf"], i["area_tematica"], i["sus"],
             i["valor_referencia"], i["ano_referencia"], i["fonte"], i["created_at"])
            for i in insights
        ],
    )
    return len(insights)


def run(anos: list[int], force_rediscover: bool = False, dry_run: bool = False):
    log.info("=== sync_estado_ac.py — anos=%s ===", anos)
    connector = TransparenciaAcConnector(
        data_dir=str(ROOT / "data" / "transparencia_ac"),
        force=force_rediscover,
    )
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(DDL_PAGAMENTOS)
    con.execute(DDL_CONTRATOS)
    con.execute(DDL_LICITACOES)
    _ensure_insight_columns(con)

    total_pag = total_cont = total_lic = total_ins = 0

    for ano in anos:
        log.info("── Coletando %d ──────────────────────────────", ano)
        result = connector.run(anos=[ano])
        pagamentos, contratos, licitacoes = (
            result["pagamentos"], result["contratos"], result["licitacoes"]
        )
        log.info("Coletado: %d pag | %d cont | %d lic", len(pagamentos), len(contratos), len(licitacoes))

        if dry_run:
            from collections import Counter
            for orgao, cnt in Counter(p.orgao for p in pagamentos).most_common(10):
                log.info("  %-20s  %d pagamentos", orgao, cnt)
            continue

        n_p = _upsert_pagamentos(con, pagamentos, ano)
        n_c = _upsert_contratos(con, contratos, ano)
        n_l = _upsert_licitacoes(con, licitacoes, ano)
        insights = generate_insights(con, ano)
        n_i = _upsert_insights(con, insights, ano)
        log.info("Gravado %d: %d pag | %d cont | %d lic | %d insights", ano, n_p, n_c, n_l, n_i)
        total_pag += n_p; total_cont += n_c; total_lic += n_l; total_ins += n_i

    if not dry_run:
        stats = con.execute(
            """SELECT orgao, COUNT(*), SUM(valor_referencia)
               FROM insight WHERE esfera='estadual' AND uf='AC'
               GROUP BY orgao ORDER BY SUM(valor_referencia) DESC LIMIT 15"""
        ).fetchall()
        log.info("=== Top órgãos no DuckDB ===")
        for orgao, n, total in stats:
            log.info("  %-20s  %3d insights  R$ %,.0f", orgao, n, total or 0)
        log.info("=== Total: %d pag | %d cont | %d lic | %d insights ===",
                 total_pag, total_cont, total_lic, total_ins)
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Sync Governo do Acre → DuckDB canônico")
    parser.add_argument("--anos", nargs="+", type=int, default=[2024, 2023])
    parser.add_argument("--force-rediscover", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.anos, force_rediscover=args.force_rediscover, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
