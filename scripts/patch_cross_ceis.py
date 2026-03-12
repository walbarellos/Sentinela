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

DUCKDB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
SANCAO_KIND_PREFIX = "SESACRE_SANCAO_"

log = logging.getLogger("patch_cross_ceis")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


def detect_cnpj_col(con: duckdb.DuckDBPyConnection, table: str) -> str:
    cols = [row[1] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    for candidate in ("cnpj_cpf", "cnpj"):
        if candidate in cols:
            return candidate
    raise RuntimeError(f"Coluna CNPJ nao encontrada em {table}. Colunas: {cols}")


def ensure_cross_columns(con: duckdb.DuckDBPyConnection) -> None:
    cols = {row[1] for row in con.execute("PRAGMA table_info('estado_ac_fornecedor_sancoes')").fetchall()}
    extra = {
        "nome_sancionado": "VARCHAR",
        "n_pagamentos": "INTEGER",
        "origem_dado": "VARCHAR",
    }
    for col, dtype in extra.items():
        if col not in cols:
            con.execute(f"ALTER TABLE estado_ac_fornecedor_sancoes ADD COLUMN {col} {dtype}")


def short_id(prefix: str, *parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{hashlib.md5(raw.encode('utf-8')).hexdigest()[:16]}"


def infer_status(data_fim_sancao: object) -> str:
    text = str(data_fim_sancao or "").strip()
    if not text:
        return "INDEFINIDA"
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d", "%d-%m-%Y"):
        try:
            fim = datetime.strptime(text[:10], fmt).date()
            return "VIGENTE" if fim >= date.today() else "EXPIRADA"
        except ValueError:
            continue
    return "INDEFINIDA"


def log_doc_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in ("estado_ac_contratos", "estado_ac_fornecedores", "estado_ac_pagamentos"):
        try:
            n = con.execute(
                f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)
                """
            ).fetchone()[0]
            counts[table] = n
            log.info("Docs validos em %-24s: %d", table, n)
        except Exception as exc:
            counts[table] = -1
            log.warning("%-24s: %s", table, exc)
    return counts


def rebuild_cross(con: duckdb.DuckDBPyConnection) -> int:
    ensure_cross_columns(con)
    ceis_cnpj = detect_cnpj_col(con, "federal_ceis")
    cnep_cnpj = detect_cnpj_col(con, "federal_cnep")
    log.info("Colunas CNPJ detectadas: federal_ceis.%s | federal_cnep.%s", ceis_cnpj, cnep_cnpj)

    con.execute("DELETE FROM estado_ac_fornecedor_sancoes")

    rows = con.execute(
        f"""
        WITH ac_docs AS (
            SELECT
                MAX(ano) AS ano,
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS doc,
                credor AS nome_fornecedor,
                orgao AS orgao_ac,
                SUM(valor) AS valor_total,
                COUNT(*) AS n_operacoes,
                'contrato' AS origem_dado
            FROM estado_ac_contratos
            WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)
            GROUP BY 2, 3, 4

            UNION ALL

            SELECT
                ano,
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS doc,
                razao_social AS nome_fornecedor,
                orgao AS orgao_ac,
                total_pago AS valor_total,
                COALESCE(n_pagamentos, 1) AS n_operacoes,
                'fornecedor' AS origem_dado
            FROM estado_ac_fornecedores
            WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)

            UNION ALL

            SELECT
                MAX(ano) AS ano,
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS doc,
                credor AS nome_fornecedor,
                orgao AS orgao_ac,
                SUM(valor) AS valor_total,
                COUNT(*) AS n_operacoes,
                'pagamento' AS origem_dado
            FROM estado_ac_pagamentos
            WHERE LENGTH(REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')) IN (11, 14)
            GROUP BY 2, 3, 4
        ),
        sancoes AS (
            SELECT
                'CEIS' AS fonte,
                REGEXP_REPLACE({ceis_cnpj}, '[^0-9]', '', 'g') AS doc,
                nome AS nome_sancionado,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                COALESCE(nome_informante, orgao_sancionador) AS nome_informante,
                fundamentacao_legal,
                NULL::DOUBLE AS multa
            FROM federal_ceis

            UNION ALL

            SELECT
                'CNEP' AS fonte,
                REGEXP_REPLACE({cnep_cnpj}, '[^0-9]', '', 'g') AS doc,
                nome AS nome_sancionado,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                COALESCE(nome_informante, orgao_sancionador) AS nome_informante,
                fundamentacao_legal,
                multa
            FROM federal_cnep
        )
        SELECT
            ac.ano,
            ac.orgao_ac,
            ac.nome_fornecedor,
            ac.doc,
            ac.valor_total,
            ac.n_operacoes,
            ac.origem_dado,
            s.fonte,
            s.nome_sancionado,
            s.tipo_sancao,
            s.data_inicio_sancao,
            s.data_fim_sancao,
            s.nome_informante,
            s.fundamentacao_legal,
            s.multa
        FROM ac_docs ac
        JOIN sancoes s ON ac.doc = s.doc
        WHERE LENGTH(ac.doc) IN (11, 14)
        ORDER BY ac.valor_total DESC, ac.orgao_ac, s.fonte
        """
    ).fetchall()

    if not rows:
        return 0

    payload = []
    for (
        ano,
        orgao_ac,
        nome_fornecedor,
        doc,
        valor_total,
        n_operacoes,
        origem_dado,
        fonte,
        nome_sancionado,
        tipo_sancao,
        data_inicio_sancao,
        data_fim_sancao,
        nome_informante,
        fundamentacao_legal,
        multa,
    ) in rows:
        payload.append(
            (
                short_id(
                    "CRS",
                    ano,
                    orgao_ac,
                    nome_fornecedor,
                    doc,
                    fonte,
                    nome_sancionado,
                    tipo_sancao,
                    data_inicio_sancao,
                    data_fim_sancao,
                    nome_informante,
                    fundamentacao_legal,
                    origem_dado,
                    valor_total,
                    n_operacoes,
                ),
                ano,
                orgao_ac,
                nome_fornecedor,
                doc,
                nome_sancionado,
                float(valor_total or 0.0),
                int(n_operacoes or 0),
                origem_dado,
                fonte,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                infer_status(data_fim_sancao),
                nome_informante,
                fundamentacao_legal,
                float(multa or 0.0) if multa is not None else None,
            )
        )

    deduped = list(dict.fromkeys(payload))

    con.executemany(
        """
        INSERT INTO estado_ac_fornecedor_sancoes (
            row_id, ano, orgao, fornecedor_nome, fornecedor_cnpj, nome_sancionado,
            total_pago, n_pagamentos, origem_dado, fonte, tipo_sancao,
            data_inicio_sancao, data_fim_sancao, status_sancao,
            orgao_sancionador, fundamentacao_legal, multa, capturado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        deduped,
    )

    for fonte in ("CEIS", "CNEP"):
        n = con.execute(
            "SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes WHERE fonte = ?",
            [fonte],
        ).fetchone()[0]
        log.info("%s: %d matches", fonte, n)
    return len(deduped)


def rebuild_insights(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DELETE FROM insight WHERE kind LIKE ?", [f"{SANCAO_KIND_PREFIX}%"])

    rows = con.execute(
        """
        SELECT
            row_id,
            ano,
            fornecedor_cnpj,
            fornecedor_nome,
            nome_sancionado,
            fonte,
            tipo_sancao,
            orgao,
            total_pago,
            n_pagamentos,
            data_inicio_sancao,
            data_fim_sancao,
            origem_dado,
            orgao_sancionador,
            status_sancao
        FROM estado_ac_fornecedor_sancoes
        ORDER BY total_pago DESC
        LIMIT 500
        """
    ).fetchall()

    if not rows:
        log.info("Sem cruzamentos - nenhum insight gerado.")
        return 0

    area_map = {
        "SESACRE": "saude",
        "SEE": "educacao",
        "SEFAZ": "financas",
        "SEJUSP": "seguranca",
        "SEINFRA": "infraestrutura",
    }

    now = datetime.now()
    payload = []
    for (
        row_id,
        ano,
        cnpj,
        fornecedor_nome,
        nome_sancionado,
        fonte,
        tipo_sancao,
        orgao,
        valor,
        n_op,
        dt_ini,
        dt_fim,
        origem_dado,
        informante,
        status_sancao,
    ) in rows:
        kind = f"{SANCAO_KIND_PREFIX}{fonte}"
        iid = f"INS_{row_id}"
        title = f"[{fonte}] Fornecedor sancionado recebeu R$ {float(valor or 0):,.0f} do {orgao or 'Governo do Acre'}"
        description = (
            f"**{nome_sancionado or fornecedor_nome}** (CNPJ/CPF `{cnpj}`) consta no cadastro **{fonte}** "
            f"com sancao _{tipo_sancao or 'N/I'}_ ({dt_ini or 'N/I'} a {dt_fim or 'indefinida'}), "
            f"informada por {informante or 'N/I'}.\n\n"
            f"O orgao **{orgao or 'Governo do Acre'}** realizou **{int(n_op or 0)} operacao(oes)** "
            f"via origem **{origem_dado}** totalizando **R$ {float(valor or 0):,.2f}**."
        )
        payload.append(
            (
                iid,
                kind,
                "CRITICO" if status_sancao in {"VIGENTE", "INDEFINIDA"} else "ALTO",
                95,
                float(valor or 0.0),
                title,
                description,
                "fornecedor_sancionado_ativo",
                json.dumps([f"portaldatransparencia.gov.br/{fonte.lower()}"], ensure_ascii=False),
                json.dumps([fonte, orgao or "GOVERNO_ACRE", "sancao", origem_dado], ensure_ascii=False),
                int(n_op or 0),
                float(valor or 0.0),
                now,
                "estadual",
                "Governo do Estado do Acre",
                orgao or "",
                "",
                "AC",
                area_map.get(orgao or "", "gestao_estadual"),
                orgao == "SESACRE",
                float(valor or 0.0),
                int(ano) if ano is not None else None,
                f"portaldatransparencia.gov.br/{fonte.lower()}",
            )
        )

    deduped = list(dict.fromkeys(payload))

    con.executemany(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl,
            title, description_md, pattern, sources, tags,
            sample_n, unit_total, created_at,
            esfera, ente, orgao, municipio, uf,
            area_tematica, sus, valor_referencia, ano_referencia, fonte
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        deduped,
    )
    return len(deduped)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)
    try:
        counts = log_doc_counts(con)
        if args.dry_run:
            log.info("[dry-run] Encerrando sem gravar.")
            return

        if counts.get("estado_ac_contratos", 0) <= 0:
            log.warning("estado_ac_contratos sem docs validos; o bloqueio parece ser de fonte, nao de JOIN.")

        n_cross = rebuild_cross(con)
        log.info("Total cruzamentos: %d", n_cross)
        n_ins = rebuild_insights(con)
        log.info("Total insights: %d", n_ins)

        if n_cross > 0:
            top = con.execute(
                """
                SELECT nome_sancionado, fonte, orgao, total_pago, origem_dado
                FROM estado_ac_fornecedor_sancoes
                ORDER BY total_pago DESC
                LIMIT 10
                """
            ).fetchall()
            log.info("=== Top matches ===")
            for nome, fonte, orgao, valor, origem_dado in top:
                log.info(
                    "  [%s] %-40s | %-15s | %-10s | R$ %s",
                    fonte,
                    (nome or "")[:40],
                    orgao or "",
                    origem_dado,
                    f"{float(valor or 0):,.0f}",
                )
        else:
            log.warning(
                "Zero matches. Com contratos/fornecedores/pagamentos unificados, o bloqueio parece ser ausencia real no CEIS/CNEP para esses docs."
            )
    finally:
        con.close()


if __name__ == "__main__":
    main()
