from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.ingest.cnpj_enricher import fetch_cnpj

log = logging.getLogger("sync_trace_norte_rede")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
FOCAL_CNPJ = "37306014000148"

DDL_REDE_EMPRESAS = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_empresas (
    row_id VARCHAR PRIMARY KEY,
    origem VARCHAR,
    cnpj VARCHAR,
    nome_referencia VARCHAR,
    razao_social VARCHAR,
    nome_fantasia VARCHAR,
    situacao VARCHAR,
    capital_social DOUBLE,
    data_abertura VARCHAR,
    municipio VARCHAR,
    uf VARCHAR,
    cnae_principal VARCHAR,
    cnae_descricao VARCHAR,
    qtd_socios INTEGER DEFAULT 0,
    total_contratos INTEGER DEFAULT 0,
    total_valor_brl DOUBLE DEFAULT 0,
    contratos_terceirizacao INTEGER DEFAULT 0,
    valor_terceirizacao_brl DOUBLE DEFAULT 0,
    shared_socio_with_focal BOOLEAN DEFAULT FALSE,
    socio_match_local BOOLEAN DEFAULT FALSE,
    flags_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_REDE_SOCIOS = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_socios (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    socio_nome VARCHAR,
    socio_cpf_cnpj VARCHAR,
    qualificacao VARCHAR,
    data_entrada VARCHAR,
    shared_with_focal BOOLEAN DEFAULT FALSE,
    match_servidores INTEGER DEFAULT 0,
    match_rb_lotacao INTEGER DEFAULT 0,
    match_cross_candidato_servidor INTEGER DEFAULT 0,
    evidencias_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_REDE_CONTRATOS = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_contratos (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    ano INTEGER,
    numero_contrato VARCHAR,
    valor_brl DOUBLE,
    terceirizacao_pessoal BOOLEAN DEFAULT FALSE,
    objeto VARCHAR,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def fix_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(marker in text for marker in ("Ã", "â", "�")):
        for source_encoding in ("latin1", "cp1252"):
            try:
                repaired = text.encode(source_encoding).decode("utf-8")
            except Exception:
                continue
            if repaired and repaired != text:
                return repaired.strip()
    return text


def normalize_text(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", fix_text(value).upper())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip()


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def parse_money(value: object) -> float:
    text = fix_text(value)
    if not text:
        return 0.0
    text = text.replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(text)
    except Exception:
        return 0.0


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_REDE_EMPRESAS)
    con.execute(DDL_REDE_SOCIOS)
    con.execute(DDL_REDE_CONTRATOS)


def select_leads(con: duckdb.DuckDBPyConnection) -> list[dict]:
    return con.execute(
        """
        SELECT
            cnpj,
            fornecedor_nome,
            COUNT(*) AS total_contratos,
            SUM(valor_brl) AS total_valor_brl
        FROM trace_norte_leads
        GROUP BY 1, 2
        ORDER BY total_valor_brl DESC, fornecedor_nome
        """
    ).fetchdf().to_dict("records")


def fetch_company_data(cnpj: str) -> dict | None:
    with httpx.Client(
        headers={"User-Agent": "Sentinela/1.0 (Controle Social)"},
        follow_redirects=True,
    ) as client:
        return fetch_cnpj(client, cnpj)


def parse_company(cnpj: str, data: dict, fallback_name: str) -> tuple[dict, list[dict]]:
    socios_raw = data.get("qsa") or data.get("socios") or []
    socios = []
    for socio in socios_raw:
        socios.append(
            {
                "nome": fix_text(socio.get("nome_socio") or socio.get("nome") or ""),
                "cpf_cnpj": clean_doc(socio.get("cpf_representante_legal") or socio.get("cnpj_cpf") or ""),
                "qualificacao": fix_text(socio.get("qualificacao_socio") or socio.get("qual") or ""),
                "data_entrada": fix_text(socio.get("data_entrada_sociedade") or ""),
            }
        )
    company = {
        "cnpj": cnpj,
        "razao_social": fix_text(data.get("razao_social") or data.get("nome") or fallback_name),
        "nome_fantasia": fix_text(data.get("nome_fantasia") or data.get("fantasia") or ""),
        "situacao": fix_text(data.get("descricao_situacao_cadastral") or data.get("situacao") or ""),
        "capital_social": parse_money(data.get("capital_social") or 0),
        "data_abertura": fix_text(data.get("data_inicio_atividade") or data.get("abertura") or ""),
        "municipio": fix_text(data.get("municipio") or data.get("municipio_nome") or ""),
        "uf": fix_text(data.get("uf") or ""),
        "cnae_principal": str(data.get("cnae_fiscal") or ""),
        "cnae_descricao": fix_text(data.get("cnae_fiscal_descricao") or ""),
        "socios": socios,
    }
    return company, socios


def upsert_global_company(con: duckdb.DuckDBPyConnection, company: dict, socios: list[dict]) -> None:
    con.execute(
        "DELETE FROM empresas_cnpj WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?",
        [company["cnpj"]],
    )
    con.execute(
        """
        INSERT INTO empresas_cnpj (
            cnpj, razao_social, situacao, capital_social, data_abertura, porte,
            cnae_principal, municipio, uf, flags, capturado_em, row_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            company["cnpj"],
            company["razao_social"],
            company["situacao"],
            company["capital_social"],
            company["data_abertura"],
            "",
            company["cnae_principal"],
            company["municipio"],
            company["uf"],
            json.dumps([], ensure_ascii=False),
            datetime.now(UTC),
            company["cnpj"],
        ],
    )
    con.execute(
        "DELETE FROM empresa_socios WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?",
        [company["cnpj"]],
    )
    for socio in socios:
        con.execute(
            """
            INSERT INTO empresa_socios (
                cnpj, socio_nome, socio_cpf_cnpj, qualificacao, data_entrada, capturado_em
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                company["cnpj"],
                socio["nome"],
                socio["cpf_cnpj"],
                socio["qualificacao"],
                socio["data_entrada"],
                datetime.now(UTC),
            ],
        )


def focal_socio_names(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT socio_nome
        FROM empresa_socios
        WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?
        """,
        [FOCAL_CNPJ],
    ).fetchall()
    return {normalize_text(row[0]) for row in rows if fix_text(row[0])}


def base_name_indexes(con: duckdb.DuckDBPyConnection) -> tuple[dict[str, list[dict]], dict[str, list[dict]], dict[str, list[dict]]]:
    servidores = {}
    for nome, cargo, secretaria, vinculo in con.execute(
        "SELECT servidor_nome, cargo, secretaria, vinculo FROM servidores"
    ).fetchall():
        servidores.setdefault(normalize_text(nome), []).append(
            {
                "nome": fix_text(nome),
                "cargo": fix_text(cargo),
                "orgao": fix_text(secretaria),
                "vinculo": fix_text(vinculo),
            }
        )
    lotacao = {}
    for nome, cargo, lotacao_nome, secretaria, vinculo in con.execute(
        "SELECT nome, cargo, lotacao, secretaria, vinculo FROM rb_servidores_lotacao"
    ).fetchall():
        lotacao.setdefault(normalize_text(nome), []).append(
            {
                "nome": fix_text(nome),
                "cargo": fix_text(cargo),
                "orgao": fix_text(secretaria or lotacao_nome),
                "vinculo": fix_text(vinculo),
            }
        )
    cross = {}
    for nm_candidato, servidor, cargo, salario_liquido in con.execute(
        "SELECT nm_candidato, servidor, cargo, salario_liquido FROM cross_candidato_servidor"
    ).fetchall():
        cross.setdefault(normalize_text(nm_candidato), []).append(
            {
                "nome": fix_text(nm_candidato),
                "servidor": fix_text(servidor),
                "cargo": fix_text(cargo),
                "salario_liquido": float(salario_liquido or 0),
            }
        )
    return servidores, lotacao, cross


def contract_stats(con: duckdb.DuckDBPyConnection, cnpj: str) -> tuple[int, float, int, float]:
    row = con.execute(
        """
        SELECT
            COUNT(*) AS total_contratos,
            COALESCE(SUM(valor), 0) AS total_valor_brl,
            SUM(
                CASE WHEN regexp_matches(
                    upper(coalesce(objeto,'')),
                    '(TERCEIR|MAO DE OBRA|M[ÃA]O DE OBRA|POSTOS DE TRABALHO|FORNECIMENTO DE PESSOAL|APOIO ADMINISTRATIVO|SERVI(C|Ç)OS CONTINUADOS|VIGILAN|PORTEIR|RECEPCIONISTA|COPEIR|MOTORISTA)'
                ) THEN 1 ELSE 0 END
            ) AS contratos_terceirizacao,
            COALESCE(SUM(
                CASE WHEN regexp_matches(
                    upper(coalesce(objeto,'')),
                    '(TERCEIR|MAO DE OBRA|M[ÃA]O DE OBRA|POSTOS DE TRABALHO|FORNECIMENTO DE PESSOAL|APOIO ADMINISTRATIVO|SERVI(C|Ç)OS CONTINUADOS|VIGILAN|PORTEIR|RECEPCIONISTA|COPEIR|MOTORISTA)'
                ) THEN valor ELSE 0 END
            ), 0) AS valor_terceirizacao_brl
        FROM estado_ac_contratos
        WHERE regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') = ?
        """,
        [cnpj],
    ).fetchone()
    return int(row[0] or 0), float(row[1] or 0), int(row[2] or 0), float(row[3] or 0)


def refresh_rede_contratos(con: duckdb.DuckDBPyConnection, lead_cnpjs: list[str]) -> int:
    con.execute("DELETE FROM trace_norte_rede_contratos")
    if not lead_cnpjs:
        return 0
    placeholders = ",".join("?" for _ in lead_cnpjs)
    rows = con.execute(
        f"""
        SELECT
            regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') AS cnpj,
            credor AS fornecedor_nome,
            orgao,
            unidade_gestora,
            ano,
            numero AS numero_contrato,
            valor AS valor_brl,
            regexp_matches(
                upper(coalesce(objeto,'')),
                '(TERCEIR|MAO DE OBRA|M[ÃA]O DE OBRA|POSTOS DE TRABALHO|FORNECIMENTO DE PESSOAL|APOIO ADMINISTRATIVO|SERVI(C|Ç)OS CONTINUADOS|VIGILAN|PORTEIR|RECEPCIONISTA|COPEIR|MOTORISTA)'
            ) AS terceirizacao_pessoal,
            objeto
        FROM estado_ac_contratos
        WHERE regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') IN ({placeholders})
        ORDER BY valor DESC, orgao, numero
        """,
        lead_cnpjs,
    ).fetchall()
    payload = [
        (
            row_hash("rede_contrato", *row[:7]),
            clean_doc(row[0]),
            fix_text(row[1]),
            fix_text(row[2]),
            fix_text(row[3]),
            int(row[4] or 0),
            fix_text(row[5]),
            float(row[6] or 0),
            bool(row[7]),
            fix_text(row[8]),
        )
        for row in rows
    ]
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_rede_contratos (
                row_id, cnpj, fornecedor_nome, orgao, unidade_gestora, ano,
                numero_contrato, valor_brl, terceirizacao_pessoal, objeto
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def refresh_network(con: duckdb.DuckDBPyConnection, refresh: bool) -> tuple[int, int]:
    ensure_tables(con)
    leads = select_leads(con)
    focal_names = focal_socio_names(con)
    servidores_idx, lotacao_idx, cross_idx = base_name_indexes(con)

    con.execute("DELETE FROM trace_norte_rede_empresas")
    con.execute("DELETE FROM trace_norte_rede_socios")

    company_payload = []
    socio_payload = []
    lead_cnpjs = []

    for lead in leads:
        cnpj = clean_doc(lead["cnpj"])
        lead_cnpjs.append(cnpj)
        fallback_name = fix_text(lead["fornecedor_nome"])
        existing = con.execute(
            "SELECT razao_social, situacao, capital_social, data_abertura, cnae_principal, municipio, uf FROM empresas_cnpj WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ? LIMIT 1",
            [cnpj],
        ).fetchone()
        existing_socios = con.execute(
            "SELECT socio_nome, socio_cpf_cnpj, qualificacao, data_entrada FROM empresa_socios WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?",
            [cnpj],
        ).fetchall()

        if existing and existing_socios and not refresh:
            company = {
                "cnpj": cnpj,
                "razao_social": fix_text(existing[0]),
                "nome_fantasia": "",
                "situacao": fix_text(existing[1]),
                "capital_social": float(existing[2] or 0),
                "data_abertura": fix_text(existing[3]),
                "municipio": fix_text(existing[5]),
                "uf": fix_text(existing[6]),
                "cnae_principal": fix_text(existing[4]),
                "cnae_descricao": "",
            }
            socios = [
                {
                    "nome": fix_text(row[0]),
                    "cpf_cnpj": clean_doc(row[1]),
                    "qualificacao": fix_text(row[2]),
                    "data_entrada": fix_text(row[3]),
                }
                for row in existing_socios
            ]
        else:
            data = fetch_company_data(cnpj)
            if not data:
                log.warning("Sem resposta de CNPJ para lead %s (%s)", cnpj, fallback_name)
                continue
            company, socios = parse_company(cnpj, data, fallback_name)
            upsert_global_company(con, company, socios)

        total_contratos, total_valor_brl, contratos_terceirizacao, valor_terceirizacao_brl = contract_stats(con, cnpj)
        shared = False
        socio_match_local = False

        for socio in socios:
            normalized = normalize_text(socio["nome"])
            hits_servidores = servidores_idx.get(normalized, [])
            hits_lotacao = lotacao_idx.get(normalized, [])
            hits_cross = cross_idx.get(normalized, [])
            shared_with_focal = normalized in focal_names and bool(normalized)
            shared = shared or shared_with_focal
            local_match = bool(hits_servidores or hits_lotacao or hits_cross)
            socio_match_local = socio_match_local or local_match
            socio_payload.append(
                (
                    row_hash("rede_socio", cnpj, socio["nome"]),
                    cnpj,
                    socio["nome"],
                    socio["cpf_cnpj"],
                    socio["qualificacao"],
                    socio["data_entrada"],
                    shared_with_focal,
                    len(hits_servidores),
                    len(hits_lotacao),
                    len(hits_cross),
                    json.dumps(
                        {
                            "servidores": hits_servidores,
                            "rb_servidores_lotacao": hits_lotacao,
                            "cross_candidato_servidor": hits_cross,
                        },
                        ensure_ascii=False,
                    ),
                )
            )

        flags = []
        if shared:
            flags.append("shared_socio_with_focal")
        if socio_match_local:
            flags.append("socio_match_local")
        if contratos_terceirizacao > 0:
            flags.append("terceirizacao_detectada")

        company_payload.append(
            (
                row_hash("rede_empresa", cnpj),
                "lead_nome_semelhante",
                cnpj,
                fallback_name,
                company["razao_social"],
                company.get("nome_fantasia", ""),
                company.get("situacao", ""),
                float(company.get("capital_social") or 0),
                company.get("data_abertura", ""),
                company.get("municipio", ""),
                company.get("uf", ""),
                company.get("cnae_principal", ""),
                company.get("cnae_descricao", ""),
                len(socios),
                total_contratos,
                total_valor_brl,
                contratos_terceirizacao,
                valor_terceirizacao_brl,
                shared,
                socio_match_local,
                json.dumps(flags, ensure_ascii=False),
            )
        )

    if company_payload:
        con.executemany(
            """
            INSERT INTO trace_norte_rede_empresas (
                row_id, origem, cnpj, nome_referencia, razao_social, nome_fantasia, situacao,
                capital_social, data_abertura, municipio, uf, cnae_principal, cnae_descricao,
                qtd_socios, total_contratos, total_valor_brl, contratos_terceirizacao,
                valor_terceirizacao_brl, shared_socio_with_focal, socio_match_local, flags_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            company_payload,
        )
    if socio_payload:
        con.executemany(
            """
            INSERT INTO trace_norte_rede_socios (
                row_id, cnpj, socio_nome, socio_cpf_cnpj, qualificacao, data_entrada,
                shared_with_focal, match_servidores, match_rb_lotacao,
                match_cross_candidato_servidor, evidencias_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            socio_payload,
        )
    n_rede_contratos = refresh_rede_contratos(con, lead_cnpjs)
    return len(company_payload), len(socio_payload), n_rede_contratos


def build_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP VIEW IF EXISTS v_trace_norte_rede_resumo")
    con.execute(
        """
        CREATE VIEW v_trace_norte_rede_resumo AS
        SELECT
            cnpj,
            nome_referencia,
            razao_social,
            municipio,
            uf,
            qtd_socios,
            total_contratos,
            total_valor_brl,
            contratos_terceirizacao,
            valor_terceirizacao_brl,
            shared_socio_with_focal,
            socio_match_local,
            flags_json
        FROM trace_norte_rede_empresas
        ORDER BY valor_terceirizacao_brl DESC, total_valor_brl DESC, nome_referencia
        """
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Expande a rede de empresas-lead da trilha NORTE.")
    parser.add_argument("--db", default=str(DB_PATH), help="Caminho do DuckDB alvo.")
    parser.add_argument("--refresh-cnpj", action="store_true", help="Forca refresh dos CNPJs-lead.")
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    n_empresas, n_socios, n_contratos = refresh_network(con, refresh=args.refresh_cnpj)
    build_views(con)
    resumo = con.execute("SELECT * FROM v_trace_norte_rede_resumo").fetchdf()
    con.close()

    log.info(
        "trace_norte_rede concluido | empresas=%s | socios=%s | contratos=%s",
        n_empresas,
        n_socios,
        n_contratos,
    )
    if not resumo.empty:
        top = resumo.iloc[0]
        log.info(
            "top lead | %s | terceirizacao=R$ %.2f | shared_with_focal=%s | socio_match_local=%s",
            top["nome_referencia"],
            float(top["valor_terceirizacao_brl"] or 0),
            bool(top["shared_socio_with_focal"]),
            bool(top["socio_match_local"]),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
