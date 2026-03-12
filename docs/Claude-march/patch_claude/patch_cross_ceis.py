"""
patch_cross_ceis.py
-------------------
Corrige dois problemas identificados no diagnóstico:

1. Coluna real em federal_ceis é `cnpj`, não `cnpj_cpf`
   (tabela foi criada pelo sync_sesacre_sancoes.py legado com DDL diferente)

2. O cross_with_estado usava só estado_ac_fornecedores (121 CNPJs).
   Os 6615 contratos em estado_ac_contratos têm cnpjcpf por linha —
   essa é a fonte certa para o cruzamento sancionatório.

Aplique este patch sobre sync_ceis_cnep.py e re-execute.
Ou rode este script diretamente — ele faz só o cruzamento + insights.

Uso:
    .venv/bin/python scripts/patch_cross_ceis.py
    .venv/bin/python scripts/patch_cross_ceis.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger("patch_cross_ceis")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DUCKDB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
SANCAO_KIND_PREFIX = "SESACRE_SANCAO"


def _detect_cnpj_col(con: duckdb.DuckDBPyConnection, table: str) -> str:
    """Retorna o nome real da coluna CNPJ na tabela, seja `cnpj` ou `cnpj_cpf`."""
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    for candidate in ("cnpj_cpf", "cnpj"):
        if candidate in cols:
            return candidate
    raise RuntimeError(f"Coluna CNPJ não encontrada em {table}. Colunas: {cols}")


def rebuild_cross(con: duckdb.DuckDBPyConnection) -> int:
    """
    Reconstrói estado_ac_fornecedor_sancoes usando TODAS as fontes de CNPJ:
      1. estado_ac_contratos.cnpjcpf   (6615 linhas, fonte principal)
      2. estado_ac_fornecedores.cnpjcpf (121 CNPJs)
      3. estado_ac_pagamentos.cnpjcpf  (0 preenchidos agora, mas inclui para o futuro)
    """
    ceis_cnpj = _detect_cnpj_col(con, "federal_ceis")
    cnep_cnpj = _detect_cnpj_col(con, "federal_cnep")
    log.info("Colunas CNPJ detectadas: federal_ceis.%s | federal_cnep.%s",
             ceis_cnpj, cnep_cnpj)

    # Verifica tabelas disponíveis
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    for t in ("federal_ceis", "federal_cnep", "estado_ac_fornecedor_sancoes"):
        if t not in tables:
            raise RuntimeError(f"Tabela {t} não existe — rode sync_ceis_cnep.py primeiro.")

    con.execute("DELETE FROM estado_ac_fornecedor_sancoes")

    # ── CTE que une todos os CNPJs estaduais conhecidos ──────────────────────
    # Fontes em ordem de riqueza de dados:
    #   contratos: tem credor + cnpjcpf + orgao_canonico + valor por linha
    #   fornecedores: tem razao_social + cnpjcpf agregado
    #   pagamentos: cnpjcpf normalmente vazio neste portal, mas inclui por completude

    sources_cte = """
        WITH ac_docs AS (
            -- Contratos (fonte mais rica: CNPJ por credor por orgao)
            SELECT
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS doc,
                credor       AS nome_fornecedor,
                orgao_canonico AS orgao_ac,
                SUM(valor)   AS valor_total,
                COUNT(*)     AS n_operacoes,
                'contrato'   AS origem
            FROM estado_ac_contratos
            WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)
            GROUP BY cnpjcpf, credor, orgao_canonico

            UNION ALL

            -- Fornecedores cadastrados
            SELECT
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS doc,
                razao_social AS nome_fornecedor,
                orgao        AS orgao_ac,
                total_pago   AS valor_total,
                1            AS n_operacoes,
                'fornecedor' AS origem
            FROM estado_ac_fornecedores
            WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)

            UNION ALL

            -- Pagamentos (backup para quando cnpjcpf vier preenchido)
            SELECT
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS doc,
                credor       AS nome_fornecedor,
                orgao_canonico AS orgao_ac,
                SUM(valor)   AS valor_total,
                COUNT(*)     AS n_operacoes,
                'pagamento'  AS origem
            FROM estado_ac_pagamentos
            WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)
            GROUP BY cnpjcpf, credor, orgao_canonico
        )
    """

    for fonte, s_table, s_cnpj in (
        ("CEIS", "federal_ceis", ceis_cnpj),
        ("CNEP", "federal_cnep", cnep_cnpj),
    ):
        con.execute(f"""
            {sources_cte}
            INSERT INTO estado_ac_fornecedor_sancoes
                (id, cnpj_cpf, nome_sancionado, fonte, tipo_sancao,
                 data_inicio_sancao, data_fim_sancao, nome_informante,
                 orgao_ac, valor_pago_ac, n_pagamentos_ac)
            SELECT
                md5(ac.doc || '{fonte}' || s.tipo_sancao || COALESCE(ac.orgao_ac,'')) AS id,
                ac.doc                  AS cnpj_cpf,
                s.nome_sancionado,
                '{fonte}'               AS fonte,
                s.tipo_sancao,
                s.data_inicio_sancao,
                s.data_fim_sancao,
                s.nome_informante,
                ac.orgao_ac,
                ac.valor_total          AS valor_pago_ac,
                ac.n_operacoes          AS n_pagamentos_ac
            FROM ac_docs ac
            JOIN {s_table} s
              ON ac.doc = REGEXP_REPLACE(s.{s_cnpj}, '[^0-9]', '', 'g')
            WHERE LENGTH(ac.doc) IN (11, 14)
        """)
        n = con.execute(
            f"SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes WHERE fonte='{fonte}'"
        ).fetchone()[0]
        log.info("%s: %d matches", fonte, n)

    total = con.execute(
        "SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes"
    ).fetchone()[0]
    return total


def rebuild_insights(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(f"DELETE FROM insight WHERE kind LIKE '{SANCAO_KIND_PREFIX}%'")

    rows = con.execute("""
        SELECT
            cnpj_cpf, nome_sancionado, fonte, tipo_sancao,
            orgao_ac, valor_pago_ac, n_pagamentos_ac,
            data_inicio_sancao, data_fim_sancao, nome_informante
        FROM estado_ac_fornecedor_sancoes
        ORDER BY valor_pago_ac DESC
        LIMIT 500
    """).fetchall()

    if not rows:
        log.info("Sem cruzamentos — nenhum insight gerado.")
        return 0

    AREA_MAP = {
        "SESACRE": "saude", "SEE": "educacao", "SEFAZ": "financas",
        "SEJUSP": "seguranca", "SEINFRA": "infraestrutura",
    }

    import hashlib
    agora = datetime.now()
    records = []
    for (cnpj, nome, fonte, tipo, orgao, valor, n_op,
         dt_ini, dt_fim, informante) in rows:

        kind = f"{SANCAO_KIND_PREFIX}_{fonte}"
        iid  = "INS_" + hashlib.md5(f"{kind}{cnpj}{orgao}".encode()).hexdigest()[:12]
        ativa = not dt_fim or dt_fim.strip() == ""

        records.append((
            iid,
            kind,
            "CRITICAL" if ativa else "HIGH",
            95,
            float(valor or 0),
            f"[{fonte}] Fornecedor sancionado recebeu R$ {float(valor or 0):,.0f} do {orgao or 'Governo do Acre'}",
            (
                f"**{nome}** (CNPJ/CPF `{cnpj}`) consta no cadastro **{fonte}** "
                f"com sanção _{tipo}_ ({dt_ini} a {dt_fim or 'indefinida'}), "
                f"informada por {informante or 'N/I'}.\n\n"
                f"O órgão **{orgao}** realizou **{n_op} operação(ões)** "
                f"totalizando **R$ {float(valor or 0):,.2f}**."
            ),
            "fornecedor_sancionado_ativo",
            json.dumps([f"portaldatransparencia.gov.br/{fonte.lower()}"]),
            json.dumps([fonte, orgao or "GOVERNO_ACRE", "sancao"]),
            int(n_op or 0),
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)

    # Diagnóstico rápido antes de tudo
    for t, col in (("estado_ac_contratos", "cnpjcpf"),
                   ("estado_ac_fornecedores", "cnpjcpf"),
                   ("estado_ac_pagamentos", "cnpjcpf")):
        try:
            n = con.execute(
                f"SELECT COUNT(*) FROM {t} "
                f"WHERE LENGTH(REGEXP_REPLACE({col},'[^0-9]','','g')) IN (11,14)"
            ).fetchone()[0]
            log.info("Docs válidos em %-30s: %d", t, n)
        except Exception as e:
            log.warning("%-30s: %s", t, e)

    if args.dry_run:
        log.info("[dry-run] Encerrando sem gravar.")
        con.close()
        return

    n_cruz = rebuild_cross(con)
    log.info("Total cruzamentos: %d", n_cruz)

    n_ins = rebuild_insights(con)
    log.info("Total insights: %d", n_ins)

    if n_cruz > 0:
        top = con.execute("""
            SELECT nome_sancionado, fonte, orgao_ac, valor_pago_ac
            FROM estado_ac_fornecedor_sancoes
            ORDER BY valor_pago_ac DESC LIMIT 10
        """).fetchall()
        log.info("=== Top matches ===")
        for nome, fonte, orgao, valor in top:
            log.info("  [%s] %-40s | %-15s | R$ %,.0f",
                     fonte, (nome or "")[:40], orgao, valor)
    else:
        # Mostra quantos docs o AC tem para referência
        log.warning(
            "Zero matches. Verifique se os CNPJs dos contratos estaduais "
            "realmente aparecem no CEIS/CNEP nacional — pode ser ausência real."
        )

    con.close()


if __name__ == "__main__":
    main()
