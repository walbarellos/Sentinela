from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb
import httpx

from src.ingest.cnpj_enricher import build_socios_df, fetch_cnpj

log = logging.getLogger("sync_sesacre_qsa")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
ORGAO_ALVO = "SESACRE"
TOP_N = 25
MIN_PAGO = 10_000_000

DDL_EMPRESAS = """
CREATE TABLE IF NOT EXISTS empresas_cnpj (
    cnpj VARCHAR,
    razao_social VARCHAR,
    situacao VARCHAR,
    capital_social DOUBLE,
    data_abertura VARCHAR,
    porte VARCHAR,
    cnae_principal VARCHAR,
    municipio VARCHAR,
    uf VARCHAR,
    flags VARCHAR,
    capturado_em TIMESTAMP,
    row_hash VARCHAR
)
"""

DDL_SOCIOS = """
CREATE TABLE IF NOT EXISTS empresa_socios (
    cnpj VARCHAR,
    socio_nome VARCHAR,
    socio_cpf_cnpj VARCHAR,
    qualificacao VARCHAR,
    data_entrada VARCHAR,
    capturado_em TIMESTAMP
)
"""

DDL_FORNECEDOR_QSA = """
CREATE TABLE IF NOT EXISTS estado_ac_fornecedor_qsa (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    orgao VARCHAR,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    total_pago DOUBLE,
    total_liquidado DOUBLE,
    total_empenhado DOUBLE,
    razao_social_receita VARCHAR,
    nome_fantasia VARCHAR,
    situacao_cadastral VARCHAR,
    capital_social DOUBLE,
    data_abertura VARCHAR,
    municipio VARCHAR,
    uf VARCHAR,
    cnae_principal VARCHAR,
    cnae_descricao VARCHAR,
    qtd_socios INTEGER,
    socios_json JSON,
    flags_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_EMPRESAS)
    con.execute(DDL_SOCIOS)
    con.execute(DDL_FORNECEDOR_QSA)


def select_prioritized_suppliers(
    con: duckdb.DuckDBPyConnection,
    *,
    anos: list[int],
    top_n: int,
    min_pago: float,
) -> list[dict]:
    placeholders = ",".join("?" for _ in anos)
    return con.execute(
        f"""
        WITH ranked AS (
            SELECT
                ano,
                orgao,
                cnpjcpf AS cnpj,
                razao_social AS fornecedor_nome,
                total_pago,
                total_liquidado,
                total_empenhado,
                ROW_NUMBER() OVER (
                    PARTITION BY ano
                    ORDER BY total_pago DESC, razao_social
                ) AS rn
            FROM estado_ac_fornecedores
            WHERE orgao = ? AND ano IN ({placeholders}) AND cnpjcpf IS NOT NULL AND cnpjcpf <> ''
        )
        SELECT
            ano,
            orgao,
            cnpj,
            fornecedor_nome,
            total_pago,
            total_liquidado,
            total_empenhado
        FROM ranked
        WHERE rn <= ? OR total_pago >= ?
        ORDER BY ano DESC, total_pago DESC, fornecedor_nome
        """,
        [ORGAO_ALVO, *anos, top_n, min_pago],
    ).fetchdf().to_dict("records")


def normalize_company_data(cnpj: str, data: dict) -> dict:
    atividade = data.get("atividade_principal") or []
    if atividade:
        atividade_principal = atividade[0]
        cnae_principal = str(atividade_principal.get("code") or atividade_principal.get("codigo") or "")
        cnae_descricao = (
            atividade_principal.get("text")
            or atividade_principal.get("descricao")
            or ""
        )
    else:
        cnae_principal = str(data.get("cnae_fiscal") or "")
        cnae_descricao = data.get("cnae_fiscal_descricao") or ""

    socios_df = build_socios_df(cnpj, data)
    socios = socios_df.to_dict("records") if not socios_df.empty else []

    return {
        "cnpj": cnpj,
        "razao_social": data.get("razao_social") or data.get("nome") or "",
        "nome_fantasia": data.get("nome_fantasia") or data.get("fantasia") or "",
        "situacao_cadastral": data.get("descricao_situacao_cadastral") or data.get("situacao") or "",
        "capital_social": float(data.get("capital_social") or 0),
        "data_abertura": data.get("data_inicio_atividade") or data.get("abertura") or "",
        "municipio": data.get("municipio") or data.get("municipio_nome") or "",
        "uf": data.get("uf") or "",
        "cnae_principal": cnae_principal,
        "cnae_descricao": cnae_descricao,
        "socios": socios,
    }


def qsa_flags(empresa: dict, total_pago: float) -> list[str]:
    flags: list[str] = []
    situacao = (empresa.get("situacao_cadastral") or "").upper().strip()
    if situacao and situacao != "ATIVA":
        flags.append(f"SITUACAO_{situacao}")
    capital_social = float(empresa.get("capital_social") or 0)
    if capital_social < 100_000 and total_pago >= 1_000_000:
        flags.append(f"CAPITAL_BAIXO_VS_PAGO:{capital_social:.2f}")
    data_abertura = str(empresa.get("data_abertura") or "")
    if data_abertura and data_abertura[:4].isdigit():
        try:
            ano_abertura = int(data_abertura[:4])
            if ano_abertura >= 2020 and total_pago >= 10_000_000:
                flags.append(f"EMPRESA_RECENTE:{ano_abertura}")
        except Exception:
            pass
    return flags


def upsert_empresa(con: duckdb.DuckDBPyConnection, empresa: dict, flags: list[str]) -> None:
    con.execute("DELETE FROM empresas_cnpj WHERE cnpj = ?", [empresa["cnpj"]])
    con.execute(
        """
        INSERT INTO empresas_cnpj (
            cnpj, razao_social, situacao, capital_social, data_abertura,
            porte, cnae_principal, municipio, uf, flags, capturado_em, row_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """,
        [
            empresa["cnpj"],
            empresa["razao_social"],
            empresa["situacao_cadastral"],
            empresa["capital_social"],
            empresa["data_abertura"],
            "",
            empresa["cnae_principal"],
            empresa["municipio"],
            empresa["uf"],
            json.dumps(flags, ensure_ascii=False),
            empresa["cnpj"],
        ],
    )


def upsert_socios(con: duckdb.DuckDBPyConnection, empresa: dict) -> None:
    con.execute("DELETE FROM empresa_socios WHERE cnpj = ?", [empresa["cnpj"]])
    socios = empresa.get("socios") or []
    if not socios:
        return
    payload = [
        (
            empresa["cnpj"],
            socio.get("socio_nome", ""),
            socio.get("socio_cpf_cnpj", ""),
            socio.get("qualificacao", ""),
            socio.get("data_entrada", ""),
        )
        for socio in socios
    ]
    con.executemany(
        """
        INSERT INTO empresa_socios (
            cnpj, socio_nome, socio_cpf_cnpj, qualificacao, data_entrada, capturado_em
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        payload,
    )


def upsert_supplier_qsa_rows(
    con: duckdb.DuckDBPyConnection,
    supplier_rows: list[dict],
    empresas: dict[str, tuple[dict, list[str]]],
) -> int:
    if not supplier_rows:
        return 0

    anos = sorted({int(row["ano"]) for row in supplier_rows})
    for ano in anos:
        con.execute(
            "DELETE FROM estado_ac_fornecedor_qsa WHERE ano = ? AND orgao = ?",
            [ano, ORGAO_ALVO],
        )

    payload = []
    for row in supplier_rows:
        cnpj = row["cnpj"]
        empresa_entry = empresas.get(cnpj)
        if not empresa_entry:
            continue
        empresa, flags = empresa_entry
        socios_json = json.dumps(
            empresa.get("socios") or [],
            ensure_ascii=False,
        )
        payload.append(
            (
                row_hash(row["ano"], row["orgao"], cnpj),
                row["ano"],
                row["orgao"],
                cnpj,
                row["fornecedor_nome"],
                row["total_pago"],
                row["total_liquidado"],
                row["total_empenhado"],
                empresa["razao_social"],
                empresa["nome_fantasia"],
                empresa["situacao_cadastral"],
                empresa["capital_social"],
                empresa["data_abertura"],
                empresa["municipio"],
                empresa["uf"],
                empresa["cnae_principal"],
                empresa["cnae_descricao"],
                len(empresa.get("socios") or []),
                socios_json,
                json.dumps(flags, ensure_ascii=False),
            )
        )

    if not payload:
        return 0

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


def run_sync(
    *,
    anos: list[int],
    top_n: int,
    min_pago: float,
    dry_run: bool,
    force: bool,
) -> None:
    con = duckdb.connect(str(DB_PATH))
    ensure_tables(con)

    supplier_rows = select_prioritized_suppliers(
        con,
        anos=anos,
        top_n=top_n,
        min_pago=min_pago,
    )
    log.info(
        "Fornecedores priorizados do %s: %d linhas (%s)",
        ORGAO_ALVO,
        len(supplier_rows),
        ", ".join(str(ano) for ano in anos),
    )

    cnpjs = sorted({row["cnpj"] for row in supplier_rows})
    log.info("CNPJs únicos para QSA: %d", len(cnpjs))
    if dry_run:
        con.close()
        return

    empresas: dict[str, tuple[dict, list[str]]] = {}
    try:
        with httpx.Client(
            headers={"User-Agent": "Sentinela/1.0 (Controle Social)"},
            follow_redirects=True,
        ) as client:
            for idx, cnpj in enumerate(cnpjs, start=1):
                log.info("QSA %d/%d: %s", idx, len(cnpjs), cnpj)
                data = fetch_cnpj(client, cnpj)
                if not data:
                    time.sleep(0.5)
                    continue
                empresa = normalize_company_data(cnpj, data)
                max_pago = max(float(row["total_pago"] or 0.0) for row in supplier_rows if row["cnpj"] == cnpj)
                flags = qsa_flags(empresa, max_pago)
                upsert_empresa(con, empresa, flags)
                upsert_socios(con, empresa)
                empresas[cnpj] = (empresa, flags)
                time.sleep(0.5)

        inserted = upsert_supplier_qsa_rows(con, supplier_rows, empresas)
        log.info(
            "Concluído: %d fornecedores priorizados | %d empresas QSA resolvidas | %d linhas materializadas",
            len(supplier_rows),
            len(empresas),
            inserted,
        )
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync QSA para fornecedores priorizados do SESACRE")
    parser.add_argument("--anos", nargs="+", type=int, default=[2024, 2023])
    parser.add_argument("--top-n", type=int, default=TOP_N)
    parser.add_argument("--min-pago", type=float, default=MIN_PAGO)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Ignora cache local do cnpj.ws")
    args = parser.parse_args()
    run_sync(
        anos=args.anos,
        top_n=args.top_n,
        min_pago=args.min_pago,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
