from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path

import duckdb
import httpx

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

import sync_estado_ac
import sync_sesacre_qsa
from src.ingest.cnpj_enricher import fetch_cnpj
from src.ingest.transparencia_ac_connector import FornecedorDetalheRow, FornecedorResumoRow, TransparenciaAcConnector

log = logging.getLogger("fill_sesacre_prioritarios")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
ORGAO = "SESACRE"


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def select_top_cases(con: duckdb.DuckDBPyConnection, top_n: int) -> list[dict]:
    return con.execute(
        """
        SELECT
            cnpj_cpf,
            nome_sancionado,
            valor_contratado_ac
        FROM v_sancoes_ativas
        WHERE orgao_ac = ?
        ORDER BY valor_contratado_ac DESC
        LIMIT ?
        """,
        [ORGAO, top_n],
    ).fetchdf().to_dict("records")


def select_supplier_rows(con: duckdb.DuckDBPyConnection, cnpjs: list[str]) -> list[dict]:
    if not cnpjs:
        return []
    placeholders = ",".join("?" for _ in cnpjs)
    return con.execute(
        f"""
        WITH base AS (
            SELECT
                ano,
                orgao,
                cnpjcpf AS cnpj,
                razao_social AS fornecedor_nome,
                total_pago,
                total_liquidado,
                total_empenhado,
                entidades_json,
                1 AS pref
            FROM estado_ac_fornecedores
            WHERE orgao = ? AND cnpjcpf IN ({placeholders})

            UNION ALL

            SELECT
                ano,
                orgao,
                fornecedor_cnpj AS cnpj,
                fornecedor_nome,
                MAX(total_pago) AS total_pago,
                MAX(total_pago) AS total_liquidado,
                MAX(total_pago) AS total_empenhado,
                '[]' AS entidades_json,
                2 AS pref
            FROM estado_ac_fornecedor_sancoes
            WHERE orgao = ? AND fornecedor_cnpj IN ({placeholders})
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            ano,
            orgao,
            cnpj,
            fornecedor_nome,
            total_pago,
            total_liquidado,
            total_empenhado,
            entidades_json
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY ano, orgao, cnpj
                    ORDER BY pref, total_pago DESC, fornecedor_nome
                ) AS rn
            FROM base
        )
        WHERE rn = 1
        ORDER BY ano DESC, total_pago DESC, fornecedor_nome
        """,
        [ORGAO, *cnpjs, ORGAO, *cnpjs],
    ).fetchdf().to_dict("records")


def entidades_from_json(raw: object) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except Exception:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    return []


def upsert_targeted_qsa_rows(
    con: duckdb.DuckDBPyConnection,
    supplier_rows: list[dict],
    empresas: dict[str, tuple[dict, list[str]]],
) -> int:
    payload = []
    for row in supplier_rows:
        cnpj = str(row["cnpj"] or "")
        empresa_entry = empresas.get(cnpj)
        if not empresa_entry:
            continue
        empresa, flags = empresa_entry
        row_id = row_hash(row["ano"], row["orgao"], cnpj)
        payload.append(
            (
                row_id,
                int(row["ano"]),
                str(row["orgao"] or ""),
                cnpj,
                str(row["fornecedor_nome"] or ""),
                float(row["total_pago"] or 0),
                float(row["total_liquidado"] or 0),
                float(row["total_empenhado"] or 0),
                empresa["razao_social"],
                empresa["nome_fantasia"],
                empresa["situacao_cadastral"],
                float(empresa["capital_social"] or 0),
                empresa["data_abertura"],
                empresa["municipio"],
                empresa["uf"],
                empresa["cnae_principal"],
                empresa["cnae_descricao"],
                len(empresa.get("socios") or []),
                json.dumps(empresa.get("socios") or [], ensure_ascii=False),
                json.dumps(flags, ensure_ascii=False),
            )
        )
    if not payload:
        return 0

    con.executemany(
        "DELETE FROM estado_ac_fornecedor_qsa WHERE row_id = ?",
        [(row[0],) for row in payload],
    )
    con.executemany(
        """
        INSERT INTO estado_ac_fornecedor_qsa (
            row_id, ano, orgao, cnpj, fornecedor_nome,
            total_pago, total_liquidado, total_empenhado,
            razao_social_receita, nome_fantasia, situacao_cadastral,
            capital_social, data_abertura, municipio, uf,
            cnae_principal, cnae_descricao, qtd_socios, socios_json, flags_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def build_detail_targets(
    supplier_rows: list[dict],
    existing_pairs: set[tuple[int, str]],
) -> list[FornecedorResumoRow]:
    targets: list[FornecedorResumoRow] = []
    for row in supplier_rows:
        ano = int(row["ano"])
        cnpj = str(row["cnpj"] or "")
        if (ano, cnpj) in existing_pairs:
            continue
        entidades = entidades_from_json(row.get("entidades_json"))
        entidade = entidades[0] if entidades else ""
        targets.append(
            FornecedorResumoRow(
                razao_social=str(row["fornecedor_nome"] or ""),
                cnpjcpf=cnpj,
                empenhado=float(row["total_empenhado"] or 0),
                liquidado=float(row["total_liquidado"] or 0),
                pago=float(row["total_pago"] or 0),
                ano=ano,
                orgao=str(row["orgao"] or ORGAO),
                entidade=entidade,
            )
        )
    return targets


def upsert_targeted_detalhes(con: duckdb.DuckDBPyConnection, rows: list[FornecedorDetalheRow]) -> int:
    if not rows:
        return 0
    payload = [
        (
            row_hash(
                row.ano,
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
            row.ano,
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
    con.executemany(
        "DELETE FROM estado_ac_fornecedor_detalhes WHERE row_id = ?",
        [(row[0],) for row in payload],
    )
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


def run(top_n: int, dry_run: bool) -> None:
    con = duckdb.connect(str(DB_PATH))
    sync_sesacre_qsa.ensure_tables(con)
    sync_estado_ac.ensure_tables(con)

    top_cases = select_top_cases(con, top_n)
    cnpjs = [str(row["cnpj_cpf"]) for row in top_cases]
    supplier_rows = select_supplier_rows(con, cnpjs)
    anos = sorted({int(row["ano"]) for row in supplier_rows}, reverse=True)

    log.info("Top sancionados SESACRE: %d casos | %d linhas fornecedor/ano | anos=%s", len(top_cases), len(supplier_rows), anos)
    if dry_run:
        con.close()
        return

    empresas: dict[str, tuple[dict, list[str]]] = {}
    with httpx.Client(
        headers={"User-Agent": "Sentinela/1.0 (Controle Social)"},
        follow_redirects=True,
    ) as client:
        for idx, cnpj in enumerate(cnpjs, start=1):
            log.info("QSA targeted %d/%d: %s", idx, len(cnpjs), cnpj)
            data = fetch_cnpj(client, cnpj)
            if not data:
                log.warning("Sem resposta CNPJ para %s", cnpj)
                time.sleep(0.5)
                continue
            empresa = sync_sesacre_qsa.normalize_company_data(cnpj, data)
            max_pago = max(
                float(row["total_pago"] or 0.0)
                for row in supplier_rows
                if str(row["cnpj"] or "") == cnpj
            )
            flags = sync_sesacre_qsa.qsa_flags(empresa, max_pago)
            sync_sesacre_qsa.upsert_empresa(con, empresa, flags)
            sync_sesacre_qsa.upsert_socios(con, empresa)
            empresas[cnpj] = (empresa, flags)
            time.sleep(0.5)

    qsa_rows = upsert_targeted_qsa_rows(con, supplier_rows, empresas)
    refreshed_rows, qsa_insights = sync_sesacre_qsa.refresh_local(con, anos)

    existing_pairs = {
        (int(row[0]), str(row[1] or ""))
        for row in con.execute(
            f"""
            SELECT DISTINCT ano, cnpjcpf
            FROM estado_ac_fornecedor_detalhes
            WHERE orgao = ? AND cnpjcpf IN ({",".join("?" for _ in cnpjs)})
            """,
            [ORGAO, *cnpjs],
        ).fetchall()
    }
    detail_targets = build_detail_targets(supplier_rows, existing_pairs)
    log.info("Detalhes financeiros faltantes: %d alvo(s)", len(detail_targets))

    connector = TransparenciaAcConnector(force=False, delay_entre_requests=0.4)
    fetched_detail_rows: list[FornecedorDetalheRow] = []
    for idx, target in enumerate(detail_targets, start=1):
        log.info("Detalhe targeted %d/%d: ano=%s fornecedor=%s", idx, len(detail_targets), target.ano, target.razao_social[:120])
        try:
            fetched = connector.get_fornecedor_detalhes(
                target.ano,
                fornecedores=[target],
                max_fornecedores=1,
            )
        except Exception as exc:
            log.warning("Falha detalhe %s/%s: %s", target.ano, target.razao_social, exc)
            continue
        if fetched:
            fetched_detail_rows.extend(fetched)
        time.sleep(0.4)

    detail_rows = upsert_targeted_detalhes(con, fetched_detail_rows)
    con.close()

    log.info(
        "Concluido: %d empresas QSA resolvidas | %d linhas QSA upsert | %d linhas QSA recalculadas | %d insights QSA | %d detalhes upsert",
        len(empresas),
        qsa_rows,
        refreshed_rows,
        qsa_insights,
        detail_rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Preenche QSA/socios e detalhes financeiros para top sancionados SESACRE")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(top_n=args.top_n, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
