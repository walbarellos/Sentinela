from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import sys
import unicodedata
import zipfile
from datetime import date, datetime
from pathlib import Path

import duckdb
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.insight_classification import ensure_insight_classification_columns

log = logging.getLogger("sync_ceis_cnep")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
FEDERAL_DIR = ROOT / "data" / "federal"
OPEN_DOWNLOAD_BASE = "https://portaldatransparencia.gov.br/download-de-dados"
ORGAO_ALVO = "SESACRE"
KIND_PREFIX = "SESACRE_SANCAO_"


def today_str() -> str:
    return date.today().strftime("%Y%m%d")


def normalize_header(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()


def clean_doc(raw: object) -> str:
    return re.sub(r"\D", "", str(raw or ""))


def parse_float(raw: object) -> float | None:
    text = str(raw or "").strip()
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


def parse_date(raw: object) -> date | None:
    text = str(raw or "").strip()
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
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue
    return None


def infer_status(data_fim_sancao: object) -> str:
    fim = parse_date(data_fim_sancao)
    if fim is None:
        return "INDEFINIDA"
    if fim >= date.today():
        return "VIGENTE"
    return "EXPIRADA"


def short_id(prefix: str, *parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{hashlib.md5(raw.encode('utf-8')).hexdigest()[:16]}"


def ensure_table_columns(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
    for col, dtype in columns.items():
        if col not in existing:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
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
    )
    con.execute(
        """
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
    )
    con.execute(
        """
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
    )
    ensure_table_columns(
        con,
        "federal_ceis",
        {
            "id": "VARCHAR",
            "cadastro": "VARCHAR",
            "codigo_sancao": "VARCHAR",
            "tipo_pessoa": "VARCHAR",
            "nome_informante": "VARCHAR",
            "razao_social_receita": "VARCHAR",
            "nome_fantasia_receita": "VARCHAR",
            "numero_processo": "VARCHAR",
            "categoria_sancao": "VARCHAR",
            "data_publicacao": "VARCHAR",
            "publicacao": "VARCHAR",
            "detalhamento_publicacao": "VARCHAR",
            "data_transito_julgado": "VARCHAR",
            "abrangencia_sancao": "VARCHAR",
            "uf_orgao_sancionador": "VARCHAR",
            "esfera_orgao_sancionador": "VARCHAR",
            "data_origem_informacao": "VARCHAR",
            "origem_informacoes": "VARCHAR",
            "observacoes": "VARCHAR",
            "competencia": "VARCHAR",
        },
    )
    ensure_table_columns(
        con,
        "federal_cnep",
        {
            "id": "VARCHAR",
            "cadastro": "VARCHAR",
            "codigo_sancao": "VARCHAR",
            "tipo_pessoa": "VARCHAR",
            "nome_informante": "VARCHAR",
            "razao_social_receita": "VARCHAR",
            "nome_fantasia_receita": "VARCHAR",
            "numero_processo": "VARCHAR",
            "categoria_sancao": "VARCHAR",
            "data_publicacao": "VARCHAR",
            "publicacao": "VARCHAR",
            "detalhamento_publicacao": "VARCHAR",
            "data_transito_julgado": "VARCHAR",
            "abrangencia_sancao": "VARCHAR",
            "uf_orgao_sancionador": "VARCHAR",
            "esfera_orgao_sancionador": "VARCHAR",
            "data_origem_informacao": "VARCHAR",
            "origem_informacoes": "VARCHAR",
            "observacoes": "VARCHAR",
            "competencia": "VARCHAR",
        },
    )
    ensure_table_columns(
        con,
        "estado_ac_fornecedor_sancoes",
        {
            "nome_sancionado": "VARCHAR",
            "n_pagamentos": "INTEGER",
        },
    )
    ensure_insight_classification_columns(con)
    ensure_insight_columns(con)
    create_compat_views(con)


def ensure_insight_columns(con: duckdb.DuckDBPyConnection) -> None:
    existing = {row[1] for row in con.execute("PRAGMA table_info('insight')").fetchall()}
    for col, dtype in {
        "valor_referencia": "DOUBLE",
        "ano_referencia": "INTEGER",
        "fonte": "VARCHAR",
    }.items():
        if col not in existing:
            con.execute(f"ALTER TABLE insight ADD COLUMN {col} {dtype}")


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
            orgao_sancionador,
            fundamentacao_legal
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
            fundamentacao_legal,
            multa
        FROM federal_cnep
        """
    )


def find_cached_zip(kind: str, data_str: str) -> Path | None:
    candidates = [
        FEDERAL_DIR / f"{data_str}_{kind.upper()}.zip",
        FEDERAL_DIR / f"{kind.lower()}_{data_str}.zip",
        FEDERAL_DIR / f"{kind.upper()}_{data_str}.zip",
    ]
    for path in candidates:
        if path.exists():
            return path
    patterns = [
        f"*{data_str}*{kind.upper()}*.zip",
        f"*{data_str}*{kind.lower()}*.zip",
    ]
    for pattern in patterns:
        matches = sorted(FEDERAL_DIR.glob(pattern))
        if matches:
            return matches[0]
    return None


def resolve_zip_bytes(kind: str, data_str: str, local: Path | None, download: bool) -> tuple[bytes, Path | None]:
    if local and local.exists():
        log.info("Usando arquivo local %s: %s", kind, local)
        return local.read_bytes(), local

    cached = find_cached_zip(kind, data_str)
    if cached is not None:
        log.info("Usando cache %s: %s", kind, cached)
        return cached.read_bytes(), cached

    if not download:
        raise FileNotFoundError(
            f"ZIP {kind} nao encontrado em {FEDERAL_DIR}. Use --download ou --local-{kind.lower()}."
        )

    FEDERAL_DIR.mkdir(parents=True, exist_ok=True)
    url = f"{OPEN_DOWNLOAD_BASE}/{kind.lower()}/{data_str}"
    log.info("Baixando %s de %s", kind, url)
    with httpx.stream(
        "GET",
        url,
        headers={"User-Agent": "Sentinela/1.0"},
        timeout=240,
        follow_redirects=True,
    ) as response:
        response.raise_for_status()
        chunks = []
        for chunk in response.iter_bytes():
            chunks.append(chunk)
        payload = b"".join(chunks)
        final_name = Path(str(response.url).split("?", 1)[0]).name or f"{data_str}_{kind.upper()}.zip"
    out = FEDERAL_DIR / final_name
    out.write_bytes(payload)
    log.info("Salvo em %s", out)
    return payload, out


def read_csv_rows(zip_bytes: bytes) -> list[dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("Nenhum CSV encontrado dentro do ZIP")
        with zf.open(csv_names[0]) as handle:
            text = handle.read().decode("latin-1", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    return [row for row in reader]


def normalize_row(row: dict[str, str]) -> dict[str, str]:
    return {normalize_header(key): str(value or "").strip() for key, value in row.items()}


def pick(row: dict[str, str], *aliases: str) -> str:
    for alias in aliases:
        value = row.get(alias, "")
        if value:
            return value
    return ""


def load_ceis(con: duckdb.DuckDBPyConnection, rows: list[dict[str, str]], competencia: str) -> int:
    con.execute("DELETE FROM federal_ceis WHERE competencia = ?", [competencia])
    payload = []
    for raw in rows:
        row = normalize_row(raw)
        cnpj = clean_doc(pick(row, "cpf_ou_cnpj_do_sancionado", "cnpj_cpf", "cnpj"))
        nome = pick(row, "nome_do_sancionado", "nome_sancionado", "nome")
        codigo_sancao = pick(row, "codigo_da_sancao", "codigo_sancao")
        rid = short_id("CEIS", competencia, codigo_sancao or cnpj or nome, pick(row, "data_inicio_sancao"))
        payload.append(
            (
                rid,
                cnpj,
                nome,
                pick(row, "categoria_da_sancao", "tipo_de_sancao", "tipo_sancao"),
                pick(row, "data_inicio_sancao"),
                pick(row, "data_final_sancao", "data_fim_sancao"),
                pick(row, "orgao_sancionador"),
                pick(row, "fundamentacao_legal"),
                pick(row, "cadastro"),
                codigo_sancao,
                pick(row, "tipo_de_pessoa", "tipo_pessoa"),
                pick(row, "nome_informado_pelo_orgao_sancionador", "nome_informante"),
                pick(row, "razao_social_cadastro_receita", "razao_social_receita"),
                pick(row, "nome_fantasia_cadastro_receita", "nome_fantasia_receita"),
                pick(row, "numero_do_processo", "numero_processo"),
                pick(row, "categoria_da_sancao"),
                pick(row, "data_publicacao"),
                pick(row, "publicacao"),
                pick(row, "detalhamento_do_meio_de_publicacao", "detalhamento_publicacao"),
                pick(row, "data_do_transito_em_julgado", "data_transito_julgado"),
                pick(row, "abragencia_da_sancao", "abrangencia_da_sancao", "abrangencia_sancao"),
                pick(row, "uf_orgao_sancionador"),
                pick(row, "esfera_orgao_sancionador"),
                pick(row, "data_origem_informacao"),
                pick(row, "origem_informacoes"),
                pick(row, "observacoes"),
                competencia,
            )
        )
    con.executemany(
        """
        INSERT INTO federal_ceis (
            id, cnpj, nome, tipo_sancao, data_inicio_sancao, data_fim_sancao,
            orgao_sancionador, fundamentacao_legal, cadastro, codigo_sancao,
            tipo_pessoa, nome_informante, razao_social_receita, nome_fantasia_receita,
            numero_processo, categoria_sancao, data_publicacao, publicacao,
            detalhamento_publicacao, data_transito_julgado, abrangencia_sancao,
            uf_orgao_sancionador, esfera_orgao_sancionador, data_origem_informacao,
            origem_informacoes, observacoes, competencia, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        payload,
    )
    return len(payload)


def load_cnep(con: duckdb.DuckDBPyConnection, rows: list[dict[str, str]], competencia: str) -> int:
    con.execute("DELETE FROM federal_cnep WHERE competencia = ?", [competencia])
    payload = []
    for raw in rows:
        row = normalize_row(raw)
        cnpj = clean_doc(pick(row, "cpf_ou_cnpj_do_sancionado", "cnpj_cpf", "cnpj"))
        nome = pick(row, "nome_do_sancionado", "nome_sancionado", "nome")
        codigo_sancao = pick(row, "codigo_da_sancao", "codigo_sancao")
        rid = short_id("CNEP", competencia, codigo_sancao or cnpj or nome, pick(row, "data_inicio_sancao"))
        payload.append(
            (
                rid,
                cnpj,
                nome,
                pick(row, "categoria_da_sancao", "tipo_de_sancao", "tipo_sancao"),
                pick(row, "data_inicio_sancao"),
                pick(row, "data_final_sancao", "data_fim_sancao"),
                parse_float(pick(row, "valor_da_multa", "valor_multa")),
                pick(row, "orgao_sancionador"),
                pick(row, "fundamentacao_legal"),
                pick(row, "cadastro"),
                codigo_sancao,
                pick(row, "tipo_de_pessoa", "tipo_pessoa"),
                pick(row, "nome_informado_pelo_orgao_sancionador", "nome_informante"),
                pick(row, "razao_social_cadastro_receita", "razao_social_receita"),
                pick(row, "nome_fantasia_cadastro_receita", "nome_fantasia_receita"),
                pick(row, "numero_do_processo", "numero_processo"),
                pick(row, "categoria_da_sancao"),
                pick(row, "data_publicacao"),
                pick(row, "publicacao"),
                pick(row, "detalhamento_do_meio_de_publicacao", "detalhamento_publicacao"),
                pick(row, "data_do_transito_em_julgado", "data_transito_julgado"),
                pick(row, "abragencia_da_sancao", "abrangencia_da_sancao", "abrangencia_sancao"),
                pick(row, "uf_orgao_sancionador"),
                pick(row, "esfera_orgao_sancionador"),
                pick(row, "data_origem_informacao"),
                pick(row, "origem_informacoes"),
                pick(row, "observacoes"),
                competencia,
            )
        )
    con.executemany(
        """
        INSERT INTO federal_cnep (
            id, cnpj, nome, tipo_sancao, data_inicio_sancao, data_fim_sancao,
            multa, orgao_sancionador, fundamentacao_legal, cadastro, codigo_sancao,
            tipo_pessoa, nome_informante, razao_social_receita, nome_fantasia_receita,
            numero_processo, categoria_sancao, data_publicacao, publicacao,
            detalhamento_publicacao, data_transito_julgado, abrangencia_sancao,
            uf_orgao_sancionador, esfera_orgao_sancionador, data_origem_informacao,
            origem_informacoes, observacoes, competencia, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        payload,
    )
    return len(payload)


def supplier_base_sql(con: duckdb.DuckDBPyConnection) -> str:
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "estado_ac_fornecedores" in tables:
        return """
            SELECT
                ano,
                orgao,
                razao_social AS fornecedor_nome,
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS fornecedor_cnpj,
                total_pago,
                n_pagamentos
            FROM estado_ac_fornecedores
            WHERE orgao = ? AND cnpjcpf IS NOT NULL AND TRIM(cnpjcpf) <> ''
        """
    if "estado_ac_pagamentos" in tables:
        return """
            SELECT
                ano,
                orgao,
                credor AS fornecedor_nome,
                REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g') AS fornecedor_cnpj,
                SUM(valor) AS total_pago,
                COUNT(*) AS n_pagamentos
            FROM estado_ac_pagamentos
            WHERE orgao = ? AND cnpjcpf IS NOT NULL AND TRIM(cnpjcpf) <> ''
            GROUP BY ano, orgao, credor, REGEXP_REPLACE(cnpjcpf, '[^0-9]', '', 'g')
        """
    raise RuntimeError("Nenhuma tabela estadual disponivel para cruzamento (estado_ac_fornecedores/estado_ac_pagamentos).")


def cross_with_estado(con: duckdb.DuckDBPyConnection, orgao_alvo: str) -> int:
    base_sql = supplier_base_sql(con)
    con.execute("DELETE FROM estado_ac_fornecedor_sancoes WHERE orgao = ?", [orgao_alvo])
    rows = con.execute(
        f"""
        WITH fornecedores AS (
            {base_sql}
        ),
        sancoes AS (
            SELECT
                'CEIS' AS fonte,
                REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') AS cnpj,
                nome AS nome_sancionado,
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
                nome AS nome_sancionado,
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
            s.nome_sancionado,
            f.total_pago,
            f.n_pagamentos,
            s.fonte,
            s.tipo_sancao,
            s.data_inicio_sancao,
            s.data_fim_sancao,
            s.orgao_sancionador,
            s.fundamentacao_legal,
            s.multa
        FROM fornecedores f
        JOIN sancoes s ON f.fornecedor_cnpj = s.cnpj
        ORDER BY f.total_pago DESC, f.fornecedor_nome, s.fonte
        """,
        [orgao_alvo],
    ).fetchall()

    if not rows:
        return 0

    payload = []
    for (
        ano,
        orgao,
        fornecedor_nome,
        fornecedor_cnpj,
        nome_sancionado,
        total_pago,
        n_pagamentos,
        fonte,
        tipo_sancao,
        data_inicio_sancao,
        data_fim_sancao,
        orgao_sancionador,
        fundamentacao_legal,
        multa,
    ) in rows:
        payload.append(
            (
                short_id(
                    "SANCAO",
                    ano,
                    orgao,
                    fornecedor_cnpj,
                    fonte,
                    tipo_sancao,
                    data_inicio_sancao,
                    data_fim_sancao,
                ),
                ano,
                orgao,
                fornecedor_nome,
                fornecedor_cnpj,
                nome_sancionado,
                float(total_pago or 0.0),
                int(n_pagamentos or 0),
                fonte,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                infer_status(data_fim_sancao),
                orgao_sancionador,
                fundamentacao_legal,
                parse_float(multa),
            )
        )
    con.executemany(
        """
        INSERT INTO estado_ac_fornecedor_sancoes (
            row_id, ano, orgao, fornecedor_nome, fornecedor_cnpj, nome_sancionado,
            total_pago, n_pagamentos, fonte, tipo_sancao, data_inicio_sancao,
            data_fim_sancao, status_sancao, orgao_sancionador, fundamentacao_legal,
            multa, capturado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        payload,
    )
    return len(payload)


def build_insights(con: duckdb.DuckDBPyConnection, orgao_alvo: str) -> int:
    con.execute("DELETE FROM insight WHERE kind LIKE ?", [f"{KIND_PREFIX}%"])
    rows = con.execute(
        """
        SELECT
            ano,
            orgao,
            fornecedor_nome,
            fornecedor_cnpj,
            nome_sancionado,
            total_pago,
            n_pagamentos,
            fonte,
            tipo_sancao,
            data_inicio_sancao,
            data_fim_sancao,
            status_sancao,
            orgao_sancionador,
            fundamentacao_legal,
            multa
        FROM estado_ac_fornecedor_sancoes
        WHERE orgao = ?
        ORDER BY total_pago DESC, fornecedor_nome, fonte
        LIMIT 500
        """,
        [orgao_alvo],
    ).fetchall()
    if not rows:
        return 0

    payload = []
    now = datetime.now()
    for (
        ano,
        orgao,
        fornecedor_nome,
        fornecedor_cnpj,
        nome_sancionado,
        total_pago,
        n_pagamentos,
        fonte,
        tipo_sancao,
        data_inicio_sancao,
        data_fim_sancao,
        status_sancao,
        orgao_sancionador,
        fundamentacao_legal,
        multa,
    ) in rows:
        kind = f"{KIND_PREFIX}{fonte}"
        iid = short_id(kind, ano, orgao, fornecedor_cnpj, tipo_sancao, data_inicio_sancao, data_fim_sancao)
        severity = "CRITICO" if status_sancao in {"VIGENTE", "INDEFINIDA"} else "ALTO"
        vigencia = f"{data_inicio_sancao or 'N/I'} a {data_fim_sancao or 'indefinida'}"
        sancionado = nome_sancionado or fornecedor_nome
        multa_md = ""
        multa_value = parse_float(multa)
        if multa_value is not None:
            multa_md = f"\n\nMulta registrada: **R$ {multa_value:,.2f}**."
        description_md = (
            f"**{sancionado}** (CNPJ/CPF `{fornecedor_cnpj}`) consta no cadastro **{fonte}** "
            f"com sancao do tipo _{tipo_sancao or 'N/I'}_ ({vigencia}), informada por "
            f"**{orgao_sancionador or 'N/I'}**.\n\n"
            f"Apesar da sancao, o orgao **{orgao or 'Governo do Acre'}** realizou "
            f"**{int(n_pagamentos or 0)} pagamento(s)** totalizando **R$ {float(total_pago or 0.0):,.2f}**."
        )
        if fundamentacao_legal:
            description_md += f"\n\nFundamentacao legal: **{fundamentacao_legal}**."
        description_md += multa_md
        payload.append(
            (
                iid,
                kind,
                severity,
                95,
                float(total_pago or 0.0),
                f"[{fonte}] Fornecedor sancionado recebeu R$ {float(total_pago or 0.0):,.0f} do {orgao or 'Governo do Acre'}",
                description_md,
                "SESACRE -> FORNECEDOR_CNPJ -> CEIS/CNEP",
                json.dumps(
                    [
                        f"{OPEN_DOWNLOAD_BASE}/{fonte.lower()}",
                        "Portal da Transparencia do Acre",
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(
                    [
                        fonte,
                        "SESACRE",
                        orgao or "GOVERNO_ACRE",
                        "sancao",
                        f"ano:{ano}",
                        f"cnpj:{fornecedor_cnpj}",
                        f"status:{status_sancao.lower()}",
                    ],
                    ensure_ascii=False,
                ),
                int(n_pagamentos or 0),
                float(total_pago or 0.0),
                now,
                "estadual",
                "Governo do Estado do Acre",
                orgao or "",
                "",
                "AC",
                "saude" if orgao == ORGAO_ALVO else "gestao_estadual",
                orgao == ORGAO_ALVO,
                float(total_pago or 0.0),
                int(ano) if ano is not None else None,
                f"portaldatransparencia.gov.br/{fonte.lower()}",
            )
        )
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
        payload,
    )
    return len(payload)


def run(data_str: str, local_ceis: Path | None, local_cnep: Path | None, download: bool) -> None:
    log.info("=== sync_ceis_cnep.py — data=%s ===", data_str)
    FEDERAL_DIR.mkdir(parents=True, exist_ok=True)

    ceis_bytes, ceis_path = resolve_zip_bytes("CEIS", data_str, local_ceis, download)
    cnep_bytes, cnep_path = resolve_zip_bytes("CNEP", data_str, local_cnep, download)
    if ceis_path:
        log.info("CEIS fonte: %s", ceis_path)
    if cnep_path:
        log.info("CNEP fonte: %s", cnep_path)

    con = duckdb.connect(str(DB_PATH))
    try:
        ensure_tables(con)
        n_ceis = load_ceis(con, read_csv_rows(ceis_bytes), data_str)
        log.info("CEIS carregado: %d registros", n_ceis)
        n_cnep = load_cnep(con, read_csv_rows(cnep_bytes), data_str)
        log.info("CNEP carregado: %d registros", n_cnep)
        n_cruz = cross_with_estado(con, ORGAO_ALVO)
        log.info("Cruzamentos %s x sancoes: %d", ORGAO_ALVO, n_cruz)
        n_ins = build_insights(con, ORGAO_ALVO)
        log.info("Insights gerados: %d", n_ins)
        create_compat_views(con)
        log.info(
            "=== Concluido: CEIS=%d | CNEP=%d | cruzamentos=%d | insights=%d ===",
            n_ceis,
            n_cnep,
            n_cruz,
            n_ins,
        )
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carrega CEIS/CNEP (dados abertos) e cruza com fornecedores do SESACRE"
    )
    parser.add_argument("--data", default=today_str(), help="Data no formato YYYYMMDD")
    parser.add_argument("--download", action="store_true", help="Baixa os ZIPs abertos do portal se nao houver cache local")
    parser.add_argument("--local-ceis", type=Path, default=None)
    parser.add_argument("--local-cnep", type=Path, default=None)
    args = parser.parse_args()
    run(
        data_str=args.data,
        local_ceis=args.local_ceis,
        local_cnep=args.local_cnep,
        download=args.download,
    )


if __name__ == "__main__":
    main()
