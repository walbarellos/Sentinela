"""
sync_sancoes_collapsed.py
-------------------------
Colapsa os 614 cruzamentos de estado_ac_fornecedor_sancoes em uma visão
analítica por fornecedor × órgão × status da sanção.

Cria / recria:
  - VIEW  v_sancoes_ativas       — sanções vigentes ou indefinidas (hoje)
  - TABLE sancoes_collapsed       — uma linha por (cnpj × orgao × fonte), com
                                    flag de sanção ativa + valores consolidados
  - INSERTs na insight            — só sanções ativas, kind=SESACRE_SANCAO_ATIVA

Uso:
    .venv/bin/python scripts/sync_sancoes_collapsed.py
    .venv/bin/python scripts/sync_sancoes_collapsed.py --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger("sync_sancoes_collapsed")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DUCKDB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
TODAY       = date.today().isoformat()          # ex: "2026-03-12"
KIND_ATIVA  = "SESACRE_SANCAO_ATIVA"
KIND_PREFIX = "SESACRE_SANCAO"

DDL_COLLAPSED = """
CREATE TABLE IF NOT EXISTS sancoes_collapsed (
    id                  VARCHAR PRIMARY KEY,
    cnpj_cpf            VARCHAR,
    nome_sancionado     VARCHAR,
    fonte               VARCHAR,       -- CEIS | CNEP
    orgao_ac            VARCHAR,       -- órgão do estado que contratou
    n_sancoes_total     INTEGER,       -- qtd de linhas no CEIS/CNEP para esse CNPJ
    n_sancoes_ativas    INTEGER,       -- sanções com data_fim >= hoje ou NULL
    tipos_sancao        VARCHAR,       -- CSV dos tipos distintos
    data_inicio_mais_antiga VARCHAR,
    data_fim_mais_recente   VARCHAR,
    valor_contratado_ac DOUBLE,        -- valor total dos contratos com o estado
    n_contratos_ac      INTEGER,
    ativa               BOOLEAN,       -- TRUE se qualquer sanção ainda vigente
    inserted_at         TIMESTAMP DEFAULT current_timestamp
)
"""


def _sancao_ativa(data_fim: str | None) -> bool:
    if not data_fim or data_fim.strip() in ("", "N/A", "SEM PRAZO"):
        return True
    # Tenta parse DD/MM/YYYY e YYYY-MM-DD
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(data_fim.strip(), fmt).date() >= date.today()
        except ValueError:
            pass
    return True  # não conseguiu parsear → conservador: considera ativa


def build_collapsed(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DELETE FROM sancoes_collapsed")

    # Lê tudo de uma vez (614 linhas, cabe em memória)
    rows = con.execute("""
        SELECT
            cnpj_cpf,
            nome_sancionado,
            fonte,
            orgao_ac,
            tipo_sancao,
            data_inicio_sancao,
            data_fim_sancao,
            valor_pago_ac,
            n_pagamentos_ac
        FROM estado_ac_fornecedor_sancoes
        ORDER BY cnpj_cpf, orgao_ac, fonte
    """).fetchall()

    # Agrupa por (cnpj, orgao, fonte)
    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)
    for row in rows:
        cnpj, nome, fonte, orgao, tipo, dt_ini, dt_fim, valor, n_op = row
        key = (cnpj or "", orgao or "", fonte or "")
        groups[key].append({
            "nome": nome or "",
            "tipo": tipo or "",
            "dt_ini": dt_ini or "",
            "dt_fim": dt_fim or "",
            "valor": float(valor or 0),
            "n_op": int(n_op or 0),
        })

    records = []
    for (cnpj, orgao, fonte), items in groups.items():
        ativas = [i for i in items if _sancao_ativa(i["dt_fim"])]
        tipos  = list({i["tipo"] for i in items if i["tipo"]})
        datas_ini = sorted(i["dt_ini"] for i in items if i["dt_ini"])
        datas_fim = sorted(
            (i["dt_fim"] for i in items if i["dt_fim"] and i["dt_fim"].strip()),
            reverse=True,
        )

        nome_sancionado = max((i["nome"] for i in items), key=len, default="")
        valor_total = max(i["valor"] for i in items)
        n_contratos = max(i["n_op"] for i in items)

        iid = "SC_" + hashlib.md5(f"{cnpj}{orgao}{fonte}".encode()).hexdigest()[:12]

        records.append((
            iid,
            cnpj,
            nome_sancionado,
            fonte,
            orgao,
            len(items),
            len(ativas),
            ", ".join(sorted(tipos))[:500],
            datas_ini[0]  if datas_ini  else "",
            datas_fim[0]  if datas_fim  else "",
            valor_total,
            n_contratos,
            bool(ativas),
        ))

    con.executemany(
        """INSERT OR IGNORE INTO sancoes_collapsed
           (id, cnpj_cpf, nome_sancionado, fonte, orgao_ac,
            n_sancoes_total, n_sancoes_ativas, tipos_sancao,
            data_inicio_mais_antiga, data_fim_mais_recente,
            valor_contratado_ac, n_contratos_ac, ativa)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        records,
    )

    total = len(records)
    ativas = sum(1 for r in records if r[12])
    log.info("sancoes_collapsed: %d linhas (%d com sanção ativa)", total, ativas)
    return total


def build_view(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW v_sancoes_ativas AS
        SELECT
            cnpj_cpf,
            nome_sancionado,
            fonte,
            orgao_ac,
            n_sancoes_ativas,
            tipos_sancao,
            data_inicio_mais_antiga,
            valor_contratado_ac,
            n_contratos_ac
        FROM sancoes_collapsed
        WHERE ativa = TRUE
        ORDER BY valor_contratado_ac DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM v_sancoes_ativas").fetchone()[0]
    log.info("v_sancoes_ativas: %d fornecedores com sanção vigente", n)


def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    # Remove só os insights do kind novo (mantém os antigos CEIS/CNEP genéricos)
    con.execute(f"DELETE FROM insight WHERE kind = '{KIND_ATIVA}'")

    rows = con.execute("""
        SELECT
            cnpj_cpf, nome_sancionado, fonte, orgao_ac,
            n_sancoes_ativas, n_sancoes_total, tipos_sancao,
            data_inicio_mais_antiga, valor_contratado_ac, n_contratos_ac
        FROM sancoes_collapsed
        WHERE ativa = TRUE
        ORDER BY valor_contratado_ac DESC
        LIMIT 500
    """).fetchall()

    if not rows:
        log.info("Sem sanções ativas — nenhum insight ATIVA gerado.")
        return 0

    AREA_MAP = {
        "SESACRE": "saude", "SEE": "educacao", "SEFAZ": "financas",
        "SEJUSP": "seguranca", "SEINFRA": "infraestrutura",
        "GOVERNO_ACRE": "gestao_estadual",
    }

    agora = datetime.now()
    records = []
    for (cnpj, nome, fonte, orgao, n_ativas, n_total,
         tipos, dt_ini, valor, n_cont) in rows:

        iid = "INS_" + hashlib.md5(
            f"{KIND_ATIVA}{cnpj}{orgao}".encode()
        ).hexdigest()[:12]

        title = (
            f"[{fonte}·ATIVA] {(nome or cnpj)[:50]} — "
            f"R$ {float(valor or 0):,.0f} contratados com {orgao}"
        )
        description_md = (
            f"**{nome}** (`{cnpj}`) possui **{n_ativas} sanção(ões) ativa(s)** "
            f"no cadastro **{fonte}** (de {n_total} total).\n\n"
            f"Tipo(s): _{tipos}_. Início mais antigo: {dt_ini or 'N/I'}.\n\n"
            f"Mesmo sob sanção, o órgão **{orgao}** manteve **{n_cont} contrato(s)** "
            f"totalizando **R$ {float(valor or 0):,.2f}**."
        )

        records.append((
            iid,
            KIND_ATIVA,
            "CRITICAL",
            97,
            float(valor or 0),
            title,
            description_md,
            "fornecedor_sancionado_contrato_ativo",
            json.dumps([f"portaldatransparencia.gov.br/{fonte.lower()}"]),
            json.dumps([fonte, orgao or "GOVERNO_ACRE", "sancao", "ativa"]),
            int(n_cont or 0),
            float(valor or 0),
            agora,
            "estadual",
            "Governo do Estado do Acre",
            orgao or "",
            "",
            "AC",
            AREA_MAP.get(orgao or "", "gestao_estadual"),
            (orgao == "SESACRE"),
            float(valor or 0),
            None,
            f"portaldatransparencia.gov.br/{fonte.lower()}",
        ))

    con.executemany(
        """INSERT OR IGNORE INTO insight
           (id, kind, severity, confidence, exposure_brl,
            title, description_md, pattern, sources, tags,
            sample_n, unit_total, created_at,
            esfera, ente, orgao, municipio, uf,
            area_tematica, sus, valor_referencia, ano_referencia, fonte)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        records,
    )
    return len(records)


def print_summary(con: duckdb.DuckDBPyConnection):
    log.info("=== Resumo por órgão (sanções ativas) ===")
    rows = con.execute("""
        SELECT orgao_ac, fonte, COUNT(*) AS fornecedores,
               SUM(valor_contratado_ac) AS valor_total
        FROM sancoes_collapsed
        WHERE ativa = TRUE
        GROUP BY orgao_ac, fonte
        ORDER BY valor_total DESC
        LIMIT 20
    """).fetchall()
    for orgao, fonte, n, valor in rows:
        log.info("  [%s] %-20s %3d fornecedores  R$ %,.0f",
                 fonte, orgao, n, float(valor or 0))

    log.info("=== Top 10 fornecedores SESACRE (ativa) ===")
    rows = con.execute("""
        SELECT nome_sancionado, cnpj_cpf, tipos_sancao,
               valor_contratado_ac, n_contratos_ac
        FROM sancoes_collapsed
        WHERE ativa = TRUE AND orgao_ac = 'SESACRE'
        ORDER BY valor_contratado_ac DESC
        LIMIT 10
    """).fetchall()
    for nome, cnpj, tipos, valor, n in rows:
        log.info("  %-45s | %s | R$ %,.0f (%d contratos)",
                 (nome or "")[:45], cnpj, float(valor or 0), int(n or 0))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        n = con.execute(
            "SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes"
        ).fetchone()[0]
        log.info("[dry-run] estado_ac_fornecedor_sancoes: %d linhas", n)
        con.close()
        return

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(DDL_COLLAPSED)

    build_collapsed(con)
    build_view(con)
    n_ins = build_insights(con)
    log.info("Insights SESACRE_SANCAO_ATIVA gerados: %d", n_ins)
    print_summary(con)
    con.close()


if __name__ == "__main__":
    main()
