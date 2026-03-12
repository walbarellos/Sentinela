from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import unicodedata
import zipfile
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb
import httpx
import pandas as pd

from src.core.insight_classification import ensure_insight_classification_columns

log = logging.getLogger("sync_sesacre_sancoes")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
DATA_DIR = ROOT / "data" / "federal"
CGU_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
ORGAO_ALVO = "SESACRE"
SANCAO_KIND_PREFIX = "SESACRE_SANCAO_"

DDL_FEDERAL_CEIS = """
CREATE TABLE IF NOT EXISTS federal_ceis (
    cnpj VARCHAR,
    nome VARCHAR,
    tipo_sancao VARCHAR,
    data_inicio_sancao VARCHAR,
    data_fim_sancao VARCHAR,
    orgao_sancionador VARCHAR,
    fundamentacao_legal VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
)
"""

DDL_FEDERAL_CNEP = """
CREATE TABLE IF NOT EXISTS federal_cnep (
    cnpj VARCHAR,
    nome VARCHAR,
    tipo_sancao VARCHAR,
    data_inicio_sancao VARCHAR,
    data_fim_sancao VARCHAR,
    multa DOUBLE,
    fundamentacao_legal VARCHAR,
    orgao_sancionador VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
)
"""

DDL_FORNECEDOR_SANCOES = """
CREATE TABLE IF NOT EXISTS estado_ac_fornecedor_sancoes (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    orgao VARCHAR,
    fornecedor_nome VARCHAR,
    fornecedor_cnpj VARCHAR,
    total_pago DOUBLE,
    fonte VARCHAR,
    tipo_sancao VARCHAR,
    data_inicio_sancao VARCHAR,
    data_fim_sancao VARCHAR,
    status_sancao VARCHAR,
    orgao_sancionador VARCHAR,
    fundamentacao_legal VARCHAR,
    multa DOUBLE,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

FIELD_ALIASES = {
    "cnpj": [
        "cnpj",
        "cpf_cnpj_do_sancionado",
        "cpf_ou_cnpj_do_sancionado",
        "cpf_cnpj_sancionado",
        "cpfcnpjsancionado",
        "cnpj_cpf_sancionado",
    ],
    "nome": [
        "nome",
        "nome_sancionado",
        "nomesancionado",
        "nome_ou_razao_social_do_sancionado",
        "nome_ou_razao_social_sancionado",
        "razao_social",
    ],
    "tipo_sancao": [
        "tipo_sancao",
        "tipo_de_sancao",
        "tiposancao",
        "motivo_sancao",
        "descricao_tipo_sancao",
    ],
    "data_inicio_sancao": [
        "data_inicio_sancao",
        "datainiciosancao",
        "data_inicial_sancao",
        "data_de_inicio_da_sancao",
    ],
    "data_fim_sancao": [
        "data_fim_sancao",
        "datafimsancao",
        "data_final_sancao",
        "data_de_fim_da_sancao",
    ],
    "orgao_sancionador": [
        "orgao_sancionador",
        "orgaosancionador",
        "orgao_entidade_sancionadora",
        "orgao_de_sancao",
    ],
    "fundamentacao_legal": [
        "fundamentacao_legal",
        "fundamentacaolegal",
        "fundamentacao",
        "dispositivo_legal",
    ],
    "multa": [
        "multa",
        "valor_multa",
        "valormulta",
        "valor_da_multa",
    ],
}


def normalize_header(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    return text


def only_digits(value: object) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidates = [
        text,
        text[:10],
        text.split("T", 1)[0],
        text.split(" ", 1)[0],
    ]
    for candidate in dict.fromkeys(candidates):
        if not candidate:
            continue
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue
    return None


def sanitize_money(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def infer_status_sancao(data_fim_sancao: object) -> str:
    fim = parse_date(data_fim_sancao)
    if fim is None:
        return "INDEFINIDA"
    if fim >= date.today():
        return "VIGENTE"
    return "EXPIRADA"


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_FEDERAL_CEIS)
    con.execute(DDL_FEDERAL_CNEP)
    con.execute(DDL_FORNECEDOR_SANCOES)
    ensure_table_columns(
        con,
        "federal_ceis",
        {
            "cnpj": "VARCHAR",
            "nome": "VARCHAR",
            "tipo_sancao": "VARCHAR",
            "data_inicio_sancao": "VARCHAR",
            "data_fim_sancao": "VARCHAR",
            "orgao_sancionador": "VARCHAR",
            "fundamentacao_legal": "VARCHAR",
            "ingested_at": "TIMESTAMP",
        },
    )
    ensure_table_columns(
        con,
        "federal_cnep",
        {
            "cnpj": "VARCHAR",
            "nome": "VARCHAR",
            "tipo_sancao": "VARCHAR",
            "data_inicio_sancao": "VARCHAR",
            "data_fim_sancao": "VARCHAR",
            "multa": "DOUBLE",
            "fundamentacao_legal": "VARCHAR",
            "orgao_sancionador": "VARCHAR",
            "ingested_at": "TIMESTAMP",
        },
    )
    ensure_insight_columns(con)
    create_compat_views(con)


def ensure_insight_columns(con: duckdb.DuckDBPyConnection) -> None:
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


def ensure_table_columns(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
    for col, dtype in columns.items():
        if col not in existing:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}")


def create_compat_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE VIEW cgu_ceis AS
        SELECT
            nome AS nome_sancionado,
            cnpj AS cnpj_cpf_sancionado,
            tipo_sancao AS motivo_sancao,
            data_inicio_sancao,
            data_fim_sancao,
            orgao_sancionador
        FROM federal_ceis
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW cgu_cnep AS
        SELECT
            nome AS nome_sancionado,
            cnpj AS cnpj_cpf_sancionado,
            tipo_sancao AS motivo_sancao,
            data_inicio_sancao,
            data_fim_sancao,
            orgao_sancionador,
            multa,
            fundamentacao_legal
        FROM federal_cnep
        """
    )


def canonicalize_df(df: pd.DataFrame, *, include_multa: bool) -> pd.DataFrame:
    renamed = {col: normalize_header(col) for col in df.columns}
    df = df.rename(columns=renamed)

    out = pd.DataFrame()
    for target, aliases in FIELD_ALIASES.items():
        source_col = next((alias for alias in aliases if alias in df.columns), None)
        if source_col is None:
            out[target] = None
        else:
            out[target] = df[source_col]

    out["cnpj"] = out["cnpj"].map(only_digits)
    out["nome"] = out["nome"].fillna("").astype(str).str.strip()
    out["tipo_sancao"] = out["tipo_sancao"].fillna("").astype(str).str.strip()
    out["data_inicio_sancao"] = out["data_inicio_sancao"].fillna("").astype(str).str.strip()
    out["data_fim_sancao"] = out["data_fim_sancao"].fillna("").astype(str).str.strip()
    out["orgao_sancionador"] = out["orgao_sancionador"].fillna("").astype(str).str.strip()
    out["fundamentacao_legal"] = out["fundamentacao_legal"].fillna("").astype(str).str.strip()
    if include_multa:
        out["multa"] = out["multa"].map(sanitize_money)
    else:
        out["multa"] = None

    out = out[(out["cnpj"] != "") & (out["nome"] != "")]
    out = out.drop_duplicates(
        subset=["cnpj", "nome", "tipo_sancao", "data_inicio_sancao", "data_fim_sancao", "orgao_sancionador"]
    ).reset_index(drop=True)
    return out


def fetch_api_rows(token: str, dataset: str) -> pd.DataFrame:
    rows: list[dict] = []
    page = 1
    with httpx.Client(
        base_url=CGU_BASE,
        headers={"chave-api-dados": token, "User-Agent": "Sentinela/1.0"},
        timeout=60,
        follow_redirects=True,
    ) as client:
        while True:
            response = client.get(f"/{dataset}", params={"pagina": page, "quantidade": 500})
            if response.status_code in (401, 403):
                raise RuntimeError("CGU API rejected the request. Configure CGU_API_TOKEN.")
            if response.status_code == 429:
                time.sleep(30)
                continue
            response.raise_for_status()
            payload = response.json()
            if not payload:
                break
            if not isinstance(payload, list):
                payload = [payload]
            rows.extend(payload)
            log.info("%s API page %d: %d rows", dataset.upper(), page, len(payload))
            page += 1
            time.sleep(0.7)
    return pd.DataFrame(rows)


def read_local_dataset(prefix: str, *, include_multa: bool) -> pd.DataFrame:
    candidates = sorted(DATA_DIR.glob(f"{prefix}_*.zip")) + sorted(DATA_DIR.glob(f"{prefix}_*.csv"))
    for path in candidates:
        try:
            if path.suffix.lower() == ".zip":
                if not zipfile.is_zipfile(path):
                    continue
                with zipfile.ZipFile(path, "r") as archive:
                    csv_name = next((name for name in archive.namelist() if name.lower().endswith(".csv")), None)
                    if not csv_name:
                        continue
                    with archive.open(csv_name) as handle:
                        df = pd.read_csv(handle, sep=";", encoding="latin-1", dtype=str)
            else:
                df = pd.read_csv(path, sep=";", encoding="latin-1", dtype=str)
            canonical = canonicalize_df(df, include_multa=include_multa)
            if not canonical.empty:
                log.info("Loaded %s rows from local file %s", len(canonical), path.name)
                return canonical
        except Exception as exc:
            log.warning("Skipping local dataset %s: %s", path, exc)
    return pd.DataFrame()


def replace_dataset(con: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    con.execute(f"DELETE FROM {table_name}")
    if table_name == "federal_ceis":
        payload = [
            (
                row["cnpj"],
                row["nome"],
                row["tipo_sancao"],
                row["data_inicio_sancao"],
                row["data_fim_sancao"],
                row["orgao_sancionador"],
                row["fundamentacao_legal"],
            )
            for _, row in df.iterrows()
        ]
        con.executemany(
            """
            INSERT INTO federal_ceis (
                cnpj, nome, tipo_sancao, data_inicio_sancao, data_fim_sancao,
                orgao_sancionador, fundamentacao_legal, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            payload,
        )
    else:
        payload = [
            (
                row["cnpj"],
                row["nome"],
                row["tipo_sancao"],
                row["data_inicio_sancao"],
                row["data_fim_sancao"],
                row["multa"],
                row["fundamentacao_legal"],
                row["orgao_sancionador"],
            )
            for _, row in df.iterrows()
        ]
        con.executemany(
            """
            INSERT INTO federal_cnep (
                cnpj, nome, tipo_sancao, data_inicio_sancao, data_fim_sancao,
                multa, fundamentacao_legal, orgao_sancionador, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            payload,
        )
    create_compat_views(con)
    return len(payload)


def load_sancoes(
    con: duckdb.DuckDBPyConnection,
    *,
    force_refresh: bool,
) -> tuple[int, int, str]:
    existing_ceis = con.execute("SELECT COUNT(*) FROM federal_ceis").fetchone()[0]
    existing_cnep = con.execute("SELECT COUNT(*) FROM federal_cnep").fetchone()[0]
    if not force_refresh and (existing_ceis or existing_cnep):
        return existing_ceis, existing_cnep, "local_db"

    token = os.environ.get("CGU_API_TOKEN", "").strip()
    if token:
        ceis_df = canonicalize_df(fetch_api_rows(token, "ceis"), include_multa=False)
        cnep_df = canonicalize_df(fetch_api_rows(token, "cnep"), include_multa=True)
        ceis_rows = replace_dataset(con, "federal_ceis", ceis_df)
        cnep_rows = replace_dataset(con, "federal_cnep", cnep_df)
        return ceis_rows, cnep_rows, "cgu_api"

    ceis_df = read_local_dataset("ceis", include_multa=False)
    cnep_df = read_local_dataset("cnep", include_multa=True)
    ceis_rows = replace_dataset(con, "federal_ceis", ceis_df) if not ceis_df.empty else existing_ceis
    cnep_rows = replace_dataset(con, "federal_cnep", cnep_df) if not cnep_df.empty else existing_cnep
    source = "local_file" if (not ceis_df.empty or not cnep_df.empty) else "missing"
    return ceis_rows, cnep_rows, source


def cross_supplier_sancoes(con: duckdb.DuckDBPyConnection, anos: list[int]) -> list[dict]:
    placeholders = ",".join("?" for _ in anos)
    query = f"""
        WITH fornecedores AS (
            SELECT
                ano,
                orgao,
                razao_social AS fornecedor_nome,
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS fornecedor_cnpj,
                total_pago
            FROM estado_ac_fornecedores
            WHERE orgao = ? AND ano IN ({placeholders}) AND cnpjcpf IS NOT NULL AND TRIM(cnpjcpf) <> ''
        ),
        sancoes AS (
            SELECT
                'CEIS' AS fonte,
                REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') AS cnpj,
                nome,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                orgao_sancionador,
                fundamentacao_legal,
                NULL::DOUBLE AS multa
            FROM federal_ceis
            UNION ALL
            SELECT
                'CNEP' AS fonte,
                REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') AS cnpj,
                nome,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                orgao_sancionador,
                fundamentacao_legal,
                multa
            FROM federal_cnep
        )
        SELECT
            f.ano,
            f.orgao,
            f.fornecedor_nome,
            f.fornecedor_cnpj,
            f.total_pago,
            s.fonte,
            s.tipo_sancao,
            s.data_inicio_sancao,
            s.data_fim_sancao,
            s.orgao_sancionador,
            s.fundamentacao_legal,
            s.multa
        FROM fornecedores f
        JOIN sancoes s ON f.fornecedor_cnpj = s.cnpj
        ORDER BY f.ano DESC, f.total_pago DESC, f.fornecedor_nome, s.fonte
    """
    return con.execute(query, [ORGAO_ALVO, *anos]).fetchdf().to_dict("records")


def upsert_cross_rows(con: duckdb.DuckDBPyConnection, rows: list[dict], anos: list[int]) -> int:
    if not rows:
        placeholders = ",".join("?" for _ in anos)
        con.execute(
            f"DELETE FROM estado_ac_fornecedor_sancoes WHERE orgao = ? AND ano IN ({placeholders})",
            [ORGAO_ALVO, *anos],
        )
        return 0

    placeholders = ",".join("?" for _ in anos)
    con.execute(
        f"DELETE FROM estado_ac_fornecedor_sancoes WHERE orgao = ? AND ano IN ({placeholders})",
        [ORGAO_ALVO, *anos],
    )
    payload = [
        (
            row_hash(
                row["ano"],
                row["orgao"],
                row["fornecedor_cnpj"],
                row["fonte"],
                row["tipo_sancao"],
                row["data_inicio_sancao"],
                row["data_fim_sancao"],
            ),
            row["ano"],
            row["orgao"],
            row["fornecedor_nome"],
            row["fornecedor_cnpj"],
            float(row["total_pago"] or 0.0),
            row["fonte"],
            row["tipo_sancao"],
            row["data_inicio_sancao"],
            row["data_fim_sancao"],
            infer_status_sancao(row["data_fim_sancao"]),
            row["orgao_sancionador"],
            row["fundamentacao_legal"],
            sanitize_money(row["multa"]),
        )
        for row in rows
    ]
    con.executemany(
        """
        INSERT INTO estado_ac_fornecedor_sancoes (
            row_id, ano, orgao, fornecedor_nome, fornecedor_cnpj, total_pago,
            fonte, tipo_sancao, data_inicio_sancao, data_fim_sancao, status_sancao,
            orgao_sancionador, fundamentacao_legal, multa, capturado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        payload,
    )
    return len(payload)


def build_insights(rows: list[dict]) -> list[dict]:
    insights: list[dict] = []
    for row in rows:
        status = infer_status_sancao(row["data_fim_sancao"])
        if status not in {"VIGENTE", "INDEFINIDA"}:
            continue

        total_pago = float(row["total_pago"] or 0.0)
        severity = "CRITICO" if row["fonte"] == "CEIS" else "ALTO"
        data_fim = row["data_fim_sancao"] or "sem data final"
        data_inicio = row["data_inicio_sancao"] or "N/I"
        orgao_sancionador = row["orgao_sancionador"] or "N/I"
        fundamentacao = row["fundamentacao_legal"] or "N/I"
        multa = sanitize_money(row["multa"])
        extra_multa = f" Multa informada: R$ {multa:,.2f}." if multa is not None else ""

        insights.append(
            {
                "id": f"SESACRE_SANCAO:{row['ano']}:{row['fonte']}:{row['fornecedor_cnpj']}:{row_hash(row['tipo_sancao'], row['data_inicio_sancao'], row['data_fim_sancao'])}",
                "kind": f"{SANCAO_KIND_PREFIX}FORNECEDOR_ATIVO_ANO",
                "severity": severity,
                "confidence": 92,
                "exposure_brl": total_pago,
                "title": f"SESACRE - fornecedor sancionado ativo: {row['fornecedor_nome']} ({row['fornecedor_cnpj']})",
                "description_md": (
                    f"O fornecedor **{row['fornecedor_nome']}** (`{row['fornecedor_cnpj']}`) recebeu "
                    f"**R$ {total_pago:,.2f}** do **SESACRE** em **{row['ano']}** e consta no cadastro "
                    f"federal **{row['fonte']}** com sancao **{row['tipo_sancao'] or 'N/I'}**. "
                    f"Inicio: **{data_inicio}**. Fim: **{data_fim}**. "
                    f"Status calculado: **{status}**. Orgao sancionador: **{orgao_sancionador}**. "
                    f"Fundamentacao: **{fundamentacao}**.{extra_multa}"
                ),
                "pattern": "SESACRE -> FORNECEDOR_CNPJ -> CEIS/CNEP",
                "sources": ["Portal da Transparencia do Acre", "Portal da Transparencia da CGU"],
                "tags": [
                    "estado_ac",
                    "sesacre",
                    "sancao",
                    row["fonte"].lower(),
                    f"ano:{row['ano']}",
                    f"cnpj:{row['fornecedor_cnpj']}",
                    f"status:{status.lower()}",
                ],
                "sample_n": 1,
                "unit_total": total_pago,
                "esfera": "estadual",
                "ente": "Governo do Estado do Acre",
                "orgao": ORGAO_ALVO,
                "municipio": "",
                "uf": "AC",
                "area_tematica": "saude",
                "sus": True,
                "valor_referencia": total_pago,
                "ano_referencia": int(row["ano"]),
                "fonte": row["fonte"],
            }
        )
    return insights


def upsert_insights(con: duckdb.DuckDBPyConnection, insights: list[dict], anos: list[int]) -> int:
    placeholders = ",".join("?" for _ in anos)
    con.execute(
        f"DELETE FROM insight WHERE kind LIKE ? AND ano_referencia IN ({placeholders})",
        [f"{SANCAO_KIND_PREFIX}%", *anos],
    )
    if not insights:
        return 0

    payload = [
        (
            ins["id"],
            ins["kind"],
            ins["severity"],
            ins["confidence"],
            ins["exposure_brl"],
            ins["title"],
            ins["description_md"],
            ins["pattern"],
            json.dumps(ins["sources"], ensure_ascii=False),
            json.dumps(ins["tags"], ensure_ascii=False),
            ins["sample_n"],
            ins["unit_total"],
            ins["esfera"],
            ins["ente"],
            ins["orgao"],
            ins["municipio"],
            ins["uf"],
            ins["area_tematica"],
            ins["sus"],
            ins["valor_referencia"],
            ins["ano_referencia"],
            ins["fonte"],
        )
        for ins in insights
    ]
    con.executemany(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl,
            title, description_md, pattern, sources, tags,
            sample_n, unit_total,
            esfera, ente, orgao, municipio, uf, area_tematica, sus,
            valor_referencia, ano_referencia, fonte, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        payload,
    )
    return len(payload)


def run_sync(*, anos: list[int], force_refresh: bool, refresh_local: bool) -> None:
    con = duckdb.connect(str(DB_PATH))
    try:
        ensure_tables(con)
        if refresh_local:
            source = "local_db"
            ceis_rows = con.execute("SELECT COUNT(*) FROM federal_ceis").fetchone()[0]
            cnep_rows = con.execute("SELECT COUNT(*) FROM federal_cnep").fetchone()[0]
        else:
            ceis_rows, cnep_rows, source = load_sancoes(con, force_refresh=force_refresh)

        matched_rows = cross_supplier_sancoes(con, anos)
        cross_count = upsert_cross_rows(con, matched_rows, anos)
        insights = build_insights(matched_rows)
        insight_count = upsert_insights(con, insights, anos)

        log.info(
            "Concluido: fonte=%s | CEIS=%d | CNEP=%d | cruzamentos=%d | insights=%d",
            source,
            ceis_rows,
            cnep_rows,
            cross_count,
            insight_count,
        )
        if source == "missing":
            log.warning(
                "Nenhuma fonte de sancoes disponivel. Configure CGU_API_TOKEN ou forneca CSV/ZIP valido em %s.",
                DATA_DIR,
            )
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Cruza fornecedores do SESACRE com CEIS/CNEP")
    parser.add_argument("--anos", nargs="+", type=int, default=[2024, 2023])
    parser.add_argument("--force-refresh", action="store_true", help="Recarrega CEIS/CNEP a partir da fonte externa disponivel")
    parser.add_argument("--refresh-local", action="store_true", help="Nao consulta rede nem arquivo; cruza apenas o que ja esta no DuckDB")
    args = parser.parse_args()
    run_sync(anos=args.anos, force_refresh=args.force_refresh, refresh_local=args.refresh_local)


if __name__ == "__main__":
    main()
