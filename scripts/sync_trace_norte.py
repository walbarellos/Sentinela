from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import unicodedata
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.insight_classification import ensure_insight_classification_columns
from src.ingest.cnpj_enricher import fetch_cnpj

log = logging.getLogger("sync_trace_norte")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
TRACE_CNPJ = "37306014000148"
TRACE_NAME = "NORTE DISTRIBUIDORA DE PRODUTOS LTDA"
TRACE_NAME_LIKE = "%NORTE DISTRIBUIDORA%"
KIND_TRACE = "TRACE_NORTE_EXPOSICAO"
KIND_TRACE_MATCH = "TRACE_NORTE_NOME_MATCH"

DDL_TRACE_CONTRATOS = """
CREATE TABLE IF NOT EXISTS trace_norte_contratos (
    row_id VARCHAR PRIMARY KEY,
    esfera VARCHAR,
    ente VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    ano INTEGER,
    numero_contrato VARCHAR,
    numero_processo VARCHAR,
    objeto VARCHAR,
    valor_brl DOUBLE,
    fornecedor_nome VARCHAR,
    cnpj VARCHAR,
    fonte_base VARCHAR,
    detail_url VARCHAR,
    tipo_objeto VARCHAR,
    terceirizacao_pessoal BOOLEAN DEFAULT FALSE,
    sinal_sancao_ativa BOOLEAN DEFAULT FALSE,
    n_sancoes_ativas INTEGER DEFAULT 0,
    sancao_fontes VARCHAR,
    evidencias_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_TRACE_SANCOES = """
CREATE TABLE IF NOT EXISTS trace_norte_sancoes (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    nome_sancionado VARCHAR,
    fonte VARCHAR,
    tipo_sancao VARCHAR,
    data_inicio_sancao VARCHAR,
    data_fim_sancao VARCHAR,
    orgao_sancionador VARCHAR,
    processo_sancao VARCHAR,
    ativa BOOLEAN DEFAULT FALSE,
    abrangencia_sancao VARCHAR,
    fundamento_legal VARCHAR,
    observacoes VARCHAR,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_TRACE_SOCIOS = """
CREATE TABLE IF NOT EXISTS trace_norte_socios (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    socio_nome VARCHAR,
    socio_cpf_cnpj VARCHAR,
    qualificacao VARCHAR,
    data_entrada VARCHAR,
    match_servidores INTEGER DEFAULT 0,
    match_rb_lotacao INTEGER DEFAULT 0,
    match_cross_candidato_servidor INTEGER DEFAULT 0,
    evidencias_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_TRACE_LEADS = """
CREATE TABLE IF NOT EXISTS trace_norte_leads (
    row_id VARCHAR PRIMARY KEY,
    lead_kind VARCHAR,
    esfera VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    ano INTEGER,
    numero_contrato VARCHAR,
    valor_brl DOUBLE,
    fornecedor_nome VARCHAR,
    cnpj VARCHAR,
    objeto VARCHAR,
    evidencias_json JSON,
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


def parse_brl(value: object) -> float:
    text = fix_text(value)
    if not text:
        return 0.0
    text = text.replace("R$", "").replace("%", "").strip()
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_date(value: object) -> date | None:
    text = fix_text(value)
    if not text or text.lower() in {"sem informacao", "sem informação"}:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except Exception:
            continue
    return None


def is_active_sanction(end_date: object) -> bool:
    parsed = parse_date(end_date)
    return parsed is None or parsed >= date.today()


def classify_objeto(objeto: object) -> tuple[str, bool]:
    text = normalize_text(objeto)
    terceirizacao_patterns = (
        r"\bTERCEIRIZ",
        r"\bMAO DE OBRA\b",
        r"\bMÃO DE OBRA\b",
        r"\bFORNECIMENTO DE PESSOAL\b",
        r"\bPOSTOS DE TRABALHO\b",
        r"\bAPOIO ADMINISTRATIVO\b",
        r"\bRECEPCIONISTA\b",
        r"\bCOPEIR",
        r"\bPORTEIR",
        r"\bVIGILAN",
        r"\bAUXILIAR DE SERVICOS GERAIS\b",
        r"\bMOTORISTA\b",
        r"\bDIGITADOR\b",
    )
    for pattern in terceirizacao_patterns:
        if re.search(pattern, text):
            return "terceirizacao_pessoal", True

    bens_patterns = (
        r"\bAQUISIC",
        r"\bFORNECIMENTO\b",
        r"\bGENEROS\b",
        r"\bALIMENTIC",
        r"\bCAFE\b",
        r"\bACUCAR\b",
        r"\bMATERIAL DE CONSUMO\b",
        r"\bHIGIENE\b",
        r"\bLIMPEZA\b",
        r"\bAGUA MINERAL\b",
        r"\bGLP\b",
        r"\bCOPOS? DESCART",
    )
    for pattern in bens_patterns:
        if re.search(pattern, text):
            return "fornecimento_bens", False

    servicos_patterns = (
        r"\bSERVICOS?\b",
        r"\bMANUTEN",
        r"\bLOCAC",
        r"\bTRANSPORTE\b",
        r"\bCONSULTORIA\b",
    )
    for pattern in servicos_patterns:
        if re.search(pattern, text):
            return "servicos_gerais", False

    return "outro", False


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_TRACE_CONTRATOS)
    con.execute(DDL_TRACE_SANCOES)
    con.execute(DDL_TRACE_SOCIOS)
    con.execute(DDL_TRACE_LEADS)


def ensure_insight_columns(con: duckdb.DuckDBPyConnection) -> bool:
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "insight" not in tables:
        log.warning("Tabela insight não encontrada; pulando geração de insights.")
        return False
    ensure_insight_classification_columns(con)
    existing = {row[1] for row in con.execute("PRAGMA table_info('insight')").fetchall()}
    extra_columns = {
        "valor_referencia": "DOUBLE",
        "ano_referencia": "INTEGER",
        "fonte": "VARCHAR",
    }
    for column, dtype in extra_columns.items():
        if column not in existing:
            con.execute(f"ALTER TABLE insight ADD COLUMN {column} {dtype}")
    return True


def upsert_empresa_trace(con: duckdb.DuckDBPyConnection, refresh: bool) -> tuple[dict | None, list[dict]]:
    existing_empresa = con.execute(
        "SELECT * FROM empresas_cnpj WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ? LIMIT 1",
        [TRACE_CNPJ],
    ).fetchone()
    existing_socios = con.execute(
        "SELECT socio_nome, socio_cpf_cnpj, qualificacao, data_entrada FROM empresa_socios WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?",
        [TRACE_CNPJ],
    ).fetchall()
    if existing_empresa and existing_socios and not refresh:
        empresa = {
            "cnpj": TRACE_CNPJ,
            "razao_social": existing_empresa[1],
            "situacao": existing_empresa[2],
            "capital_social": existing_empresa[3],
            "data_abertura": existing_empresa[4],
            "porte": existing_empresa[5],
            "cnae_principal": existing_empresa[6],
            "municipio": existing_empresa[7],
            "uf": existing_empresa[8],
            "flags": existing_empresa[9],
        }
        socios = [
            {
                "nome": row[0],
                "cpf_cnpj": row[1],
                "qualificacao": row[2],
                "data_entrada": row[3],
            }
            for row in existing_socios
        ]
        return empresa, socios

    with httpx.Client(
        headers={"User-Agent": "Sentinela/1.0 (Controle Social)"},
        follow_redirects=True,
    ) as client:
        data = fetch_cnpj(client, TRACE_CNPJ)
    if not data:
        log.warning("Nao foi possivel enriquecer CNPJ %s via BrasilAPI/ReceitaWS.", TRACE_CNPJ)
        return None, []

    empresa = {
        "cnpj": TRACE_CNPJ,
        "razao_social": fix_text(data.get("razao_social") or data.get("nome") or TRACE_NAME),
        "situacao": fix_text(data.get("descricao_situacao_cadastral") or data.get("situacao") or ""),
        "capital_social": parse_brl(data.get("capital_social") or 0),
        "data_abertura": fix_text(data.get("data_inicio_atividade") or data.get("abertura") or ""),
        "porte": fix_text(data.get("nome_porte") or data.get("porte") or ""),
        "cnae_principal": str(data.get("cnae_fiscal") or ""),
        "municipio": fix_text(data.get("municipio") or data.get("municipio_nome") or ""),
        "uf": fix_text(data.get("uf") or ""),
        "flags": json.dumps([], ensure_ascii=False),
    }
    con.execute(
        "DELETE FROM empresas_cnpj WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?",
        [TRACE_CNPJ],
    )
    con.execute(
        """
        INSERT INTO empresas_cnpj (
            cnpj, razao_social, situacao, capital_social, data_abertura, porte,
            cnae_principal, municipio, uf, flags, capturado_em, row_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            TRACE_CNPJ,
            empresa["razao_social"],
            empresa["situacao"],
            empresa["capital_social"],
            empresa["data_abertura"],
            empresa["porte"],
            empresa["cnae_principal"],
            empresa["municipio"],
            empresa["uf"],
            empresa["flags"],
            datetime.now(UTC),
            TRACE_CNPJ,
        ],
    )

    socios = data.get("qsa") or data.get("socios") or []
    parsed_socios: list[dict] = []
    con.execute(
        "DELETE FROM empresa_socios WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?",
        [TRACE_CNPJ],
    )
    for socio in socios:
        nome = fix_text(socio.get("nome_socio") or socio.get("nome") or "")
        cpf_cnpj = clean_doc(socio.get("cpf_representante_legal") or socio.get("cnpj_cpf") or "")
        qualificacao = fix_text(socio.get("qualificacao_socio") or socio.get("qual") or "")
        data_entrada = fix_text(socio.get("data_entrada_sociedade") or "")
        parsed_socios.append(
            {
                "nome": nome,
                "cpf_cnpj": cpf_cnpj,
                "qualificacao": qualificacao,
                "data_entrada": data_entrada,
            }
        )
        con.execute(
            """
            INSERT INTO empresa_socios (
                cnpj, socio_nome, socio_cpf_cnpj, qualificacao, data_entrada, capturado_em
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [TRACE_CNPJ, nome, cpf_cnpj, qualificacao, data_entrada, datetime.now(UTC)],
        )
    return empresa, parsed_socios


def collect_contract_rows(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    municipal_rows = con.execute(
        """
        SELECT
            'municipal' AS esfera,
            'Prefeitura de Rio Branco' AS ente,
            secretaria AS orgao,
            secretaria AS unidade_gestora,
            ano,
            numero_contrato,
            numero_processo,
            objeto,
            valor_referencia_brl AS valor_brl,
            fornecedor,
            cnpj,
            'rb_contratos' AS fonte_base,
            detail_url,
            (
                SELECT COUNT(DISTINCT COALESCE(v.sancao_tipo, v.orgao_sancao, v.sancao_fonte))
                FROM v_rb_contrato_ceis v
                WHERE regexp_replace(coalesce(v.cnpj,''), '\\D', '', 'g') = ?
                  AND v.numero_contrato = rb_contratos.numero_contrato
                  AND v.secretaria = rb_contratos.secretaria
                  AND v.ano = rb_contratos.ano
                  AND v.ativa = TRUE
            ) AS n_sancoes_ativas,
            (
                SELECT string_agg(DISTINCT COALESCE(v.sancao_fonte, v.orgao_sancao), ', ')
                FROM v_rb_contrato_ceis v
                WHERE regexp_replace(coalesce(v.cnpj,''), '\\D', '', 'g') = ?
                  AND v.numero_contrato = rb_contratos.numero_contrato
                  AND v.secretaria = rb_contratos.secretaria
                  AND v.ano = rb_contratos.ano
                  AND v.ativa = TRUE
            ) AS sancao_fontes
        FROM rb_contratos
        WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?
           OR upper(coalesce(fornecedor,'')) LIKE ?
        """,
        [TRACE_CNPJ, TRACE_CNPJ, TRACE_CNPJ, TRACE_NAME_LIKE],
    ).fetchall()

    estaduais_rows = con.execute(
        """
        SELECT
            'estadual' AS esfera,
            ente,
            orgao,
            unidade_gestora,
            ano,
            numero AS numero_contrato,
            NULL AS numero_processo,
            objeto,
            valor AS valor_brl,
            credor AS fornecedor,
            cnpjcpf AS cnpj,
            'estado_ac_contratos' AS fonte_base,
            NULL AS detail_url,
            COALESCE(s.n_sancoes_ativas, 0) AS n_sancoes_ativas,
            COALESCE(s.fonte, '') AS sancao_fontes
        FROM estado_ac_contratos e
        LEFT JOIN sancoes_collapsed s
          ON regexp_replace(coalesce(s.cnpj_cpf,''), '\\D', '', 'g') = regexp_replace(coalesce(e.cnpjcpf,''), '\\D', '', 'g')
         AND s.orgao_ac = e.orgao
         AND s.ativa = TRUE
        WHERE regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') = ?
           OR upper(coalesce(credor,'')) LIKE ?
        """,
        [TRACE_CNPJ, TRACE_NAME_LIKE],
    ).fetchall()
    return municipal_rows + estaduais_rows


def refresh_contracts(con: duckdb.DuckDBPyConnection) -> int:
    rows = collect_contract_rows(con)
    con.execute("DELETE FROM trace_norte_contratos")
    payload = []
    for row in rows:
        (
            esfera,
            ente,
            orgao,
            unidade_gestora,
            ano,
            numero_contrato,
            numero_processo,
            objeto,
            valor_brl,
            fornecedor_nome,
            cnpj,
            fonte_base,
            detail_url,
            n_sancoes_ativas,
            sancao_fontes,
        ) = row
        tipo_objeto, terceirizacao_pessoal = classify_objeto(objeto)
        evidencias = [
            fonte_base,
            "CEIS" if int(n_sancoes_ativas or 0) > 0 else "",
            detail_url or "",
        ]
        payload.append(
            (
                row_hash(esfera, orgao, ano, numero_contrato, clean_doc(cnpj), fornecedor_nome),
                esfera,
                fix_text(ente),
                fix_text(orgao),
                fix_text(unidade_gestora),
                int(ano or 0),
                fix_text(numero_contrato),
                fix_text(numero_processo),
                fix_text(objeto),
                float(valor_brl or 0),
                fix_text(fornecedor_nome),
                clean_doc(cnpj),
                fonte_base,
                fix_text(detail_url),
                tipo_objeto,
                terceirizacao_pessoal,
                bool(int(n_sancoes_ativas or 0) > 0),
                int(n_sancoes_ativas or 0),
                fix_text(sancao_fontes),
                json.dumps([item for item in evidencias if item], ensure_ascii=False),
            )
        )
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_contratos (
                row_id, esfera, ente, orgao, unidade_gestora, ano, numero_contrato, numero_processo,
                objeto, valor_brl, fornecedor_nome, cnpj, fonte_base, detail_url, tipo_objeto,
                terceirizacao_pessoal, sinal_sancao_ativa, n_sancoes_ativas, sancao_fontes, evidencias_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def refresh_sancoes(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DELETE FROM trace_norte_sancoes")
    rows = con.execute(
        """
        SELECT
            cnpj,
            COALESCE(razao_social_receita, nome, ?) AS nome_sancionado,
            cadastro AS fonte,
            tipo_sancao,
            data_inicio_sancao,
            data_fim_sancao,
            orgao_sancionador,
            numero_processo,
            abrangencia_sancao,
            fundamentacao_legal,
            observacoes
        FROM federal_ceis
        WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?
        ORDER BY data_inicio_sancao DESC, orgao_sancionador
        """,
        [TRACE_NAME, TRACE_CNPJ],
    ).fetchall()
    payload = []
    for row in rows:
        payload.append(
            (
                row_hash("sancao", *row[:8]),
                clean_doc(row[0]),
                fix_text(row[1]),
                fix_text(row[2]),
                fix_text(row[3]),
                fix_text(row[4]),
                fix_text(row[5]),
                fix_text(row[6]),
                fix_text(row[7]),
                is_active_sanction(row[5]),
                fix_text(row[8]),
                fix_text(row[9]),
                fix_text(row[10]),
            )
        )
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_sancoes (
                row_id, cnpj, nome_sancionado, fonte, tipo_sancao, data_inicio_sancao,
                data_fim_sancao, orgao_sancionador, processo_sancao, ativa,
                abrangencia_sancao, fundamento_legal, observacoes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def name_match_rows(con: duckdb.DuckDBPyConnection, socios: list[dict]) -> list[tuple]:
    servidores_rows = con.execute(
        "SELECT servidor_nome, cargo, secretaria, vinculo FROM servidores"
    ).fetchall()
    rb_lotacao_rows = con.execute(
        "SELECT nome, cargo, lotacao, secretaria, vinculo FROM rb_servidores_lotacao"
    ).fetchall()
    cross_rows = con.execute(
        "SELECT nm_candidato, servidor, cargo, salario_liquido FROM cross_candidato_servidor"
    ).fetchall()

    servidores_norm = {}
    for nome, cargo, secretaria, vinculo in servidores_rows:
        servidores_norm.setdefault(normalize_text(nome), []).append(
            {
                "nome": fix_text(nome),
                "cargo": fix_text(cargo),
                "orgao": fix_text(secretaria),
                "vinculo": fix_text(vinculo),
            }
        )
    rb_lotacao_norm = {}
    for nome, cargo, lotacao, secretaria, vinculo in rb_lotacao_rows:
        rb_lotacao_norm.setdefault(normalize_text(nome), []).append(
            {
                "nome": fix_text(nome),
                "cargo": fix_text(cargo),
                "orgao": fix_text(secretaria or lotacao),
                "vinculo": fix_text(vinculo),
            }
        )
    cross_norm = {}
    for candidato, servidor, cargo, salario in cross_rows:
        key = normalize_text(candidato)
        cross_norm.setdefault(key, []).append(
            {
                "nome": fix_text(candidato),
                "servidor": fix_text(servidor),
                "cargo": fix_text(cargo),
                "salario": float(salario or 0),
            }
        )

    result = []
    for socio in socios:
        socio_nome = fix_text(socio.get("nome") or "")
        normalized = normalize_text(socio_nome)
        hits_servidores = servidores_norm.get(normalized, [])
        hits_rb = rb_lotacao_norm.get(normalized, [])
        hits_cross = cross_norm.get(normalized, [])
        evidencias = {
            "servidores": hits_servidores,
            "rb_servidores_lotacao": hits_rb,
            "cross_candidato_servidor": hits_cross,
        }
        result.append(
            (
                row_hash("socio", TRACE_CNPJ, socio_nome),
                TRACE_CNPJ,
                socio_nome,
                clean_doc(socio.get("cpf_cnpj")),
                fix_text(socio.get("qualificacao")),
                fix_text(socio.get("data_entrada")),
                len(hits_servidores),
                len(hits_rb),
                len(hits_cross),
                json.dumps(evidencias, ensure_ascii=False),
            )
        )
    return result


def refresh_socios(con: duckdb.DuckDBPyConnection, socios: list[dict]) -> int:
    con.execute("DELETE FROM trace_norte_socios")
    payload = name_match_rows(con, socios)
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_socios (
                row_id, cnpj, socio_nome, socio_cpf_cnpj, qualificacao, data_entrada,
                match_servidores, match_rb_lotacao, match_cross_candidato_servidor, evidencias_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def refresh_leads(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DELETE FROM trace_norte_leads")
    rows = con.execute(
        """
        SELECT
            'nome_semelhante_terceirizacao' AS lead_kind,
            'estadual' AS esfera,
            orgao,
            unidade_gestora,
            ano,
            numero AS numero_contrato,
            valor AS valor_brl,
            credor AS fornecedor_nome,
            cnpjcpf AS cnpj,
            objeto
        FROM estado_ac_contratos
        WHERE upper(coalesce(credor,'')) LIKE '%NORTE%'
          AND regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') <> ?
          AND regexp_matches(
                upper(coalesce(objeto,'')),
                '(TERCEIR|MAO DE OBRA|M[ÃA]O DE OBRA|POSTOS DE TRABALHO|FORNECIMENTO DE PESSOAL|APOIO ADMINISTRATIVO|SERVI(C|Ç)OS CONTINUADOS|VIGILAN|PORTEIR|RECEPCIONISTA|COPEIR|MOTORISTA)'
          )
        ORDER BY valor DESC, orgao, numero
        """,
        [TRACE_CNPJ],
    ).fetchall()
    payload = []
    for row in rows:
        payload.append(
            (
                row_hash("lead", *row),
                row[0],
                row[1],
                fix_text(row[2]),
                fix_text(row[3]),
                int(row[4] or 0),
                fix_text(row[5]),
                float(row[6] or 0),
                fix_text(row[7]),
                clean_doc(row[8]),
                fix_text(row[9]),
                json.dumps(
                    ["estado_ac_contratos", "nome_semelhante", "terceirizacao_textual"],
                    ensure_ascii=False,
                ),
            )
        )
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_leads (
                row_id, lead_kind, esfera, orgao, unidade_gestora, ano, numero_contrato,
                valor_brl, fornecedor_nome, cnpj, objeto, evidencias_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def build_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP VIEW IF EXISTS v_trace_norte_resumo")
    con.execute(
        f"""
        CREATE VIEW v_trace_norte_resumo AS
        WITH contratos AS (
            SELECT
                COUNT(*) AS total_contratos,
                COALESCE(SUM(valor_brl), 0) AS valor_total,
                SUM(CASE WHEN esfera = 'municipal' THEN 1 ELSE 0 END) AS contratos_municipais,
                COALESCE(SUM(CASE WHEN esfera = 'municipal' THEN valor_brl ELSE 0 END), 0) AS valor_municipal,
                SUM(CASE WHEN esfera = 'estadual' THEN 1 ELSE 0 END) AS contratos_estaduais,
                COALESCE(SUM(CASE WHEN esfera = 'estadual' THEN valor_brl ELSE 0 END), 0) AS valor_estadual,
                SUM(CASE WHEN tipo_objeto = 'fornecimento_bens' THEN 1 ELSE 0 END) AS bens_hits,
                SUM(CASE WHEN tipo_objeto = 'servicos_gerais' THEN 1 ELSE 0 END) AS servicos_hits,
                SUM(CASE WHEN tipo_objeto = 'terceirizacao_pessoal' THEN 1 ELSE 0 END) AS terceirizacao_hits,
                COUNT(DISTINCT CASE WHEN esfera = 'estadual' THEN orgao END) AS orgaos_estaduais,
                SUM(CASE WHEN sinal_sancao_ativa THEN 1 ELSE 0 END) AS contratos_com_sancao
            FROM trace_norte_contratos
        ),
        sancoes AS (
            SELECT
                COUNT(*) AS sancoes_total,
                SUM(CASE WHEN ativa THEN 1 ELSE 0 END) AS sancoes_ativas,
                string_agg(DISTINCT orgao_sancionador, ' | ') AS orgaos_sancionadores
            FROM trace_norte_sancoes
        ),
        socios AS (
            SELECT
                COUNT(*) AS socios_total,
                SUM(CASE WHEN (match_servidores + match_rb_lotacao + match_cross_candidato_servidor) > 0 THEN 1 ELSE 0 END) AS socios_com_match
            FROM trace_norte_socios
        ),
        leads AS (
            SELECT
                COUNT(*) AS leads_total,
                COALESCE(SUM(valor_brl), 0) AS leads_valor_total
            FROM trace_norte_leads
        )
        SELECT
            '{TRACE_CNPJ}' AS cnpj,
            '{TRACE_NAME}' AS razao_social_ref,
            contratos.total_contratos,
            contratos.valor_total,
            contratos.contratos_municipais,
            contratos.valor_municipal,
            contratos.contratos_estaduais,
            contratos.valor_estadual,
            contratos.bens_hits,
            contratos.servicos_hits,
            contratos.terceirizacao_hits,
            contratos.orgaos_estaduais,
            contratos.contratos_com_sancao,
            sancoes.sancoes_total,
            sancoes.sancoes_ativas,
            sancoes.orgaos_sancionadores,
            socios.socios_total,
            socios.socios_com_match,
            leads.leads_total,
            leads.leads_valor_total
        FROM contratos, sancoes, socios, leads
        """,
    )


def refresh_insights(con: duckdb.DuckDBPyConnection, enabled: bool) -> int:
    if not enabled:
        return 0
    con.execute(
        "DELETE FROM insight WHERE kind IN (?, ?)",
        [KIND_TRACE, KIND_TRACE_MATCH],
    )
    summary = con.execute("SELECT * FROM v_trace_norte_resumo").fetchone()
    if not summary:
        return 0
    (
        cnpj,
        razao_social_ref,
        total_contratos,
        valor_total,
        contratos_municipais,
        valor_municipal,
        contratos_estaduais,
        valor_estadual,
        bens_hits,
        servicos_hits,
        terceirizacao_hits,
        orgaos_estaduais,
        contratos_com_sancao,
        sancoes_total,
        sancoes_ativas,
        orgaos_sancionadores,
        socios_total,
        socios_com_match,
        leads_total,
        leads_valor_total,
    ) = summary
    title = f"TRACE NORTE: {razao_social_ref} aparece em contratos do Acre sob sancoes ativas"
    description = (
        f"**{razao_social_ref}** (`{cnpj}`) aparece em **{int(total_contratos or 0)} contrato(s)** "
        f"mapeado(s) no Acre, totalizando **R$ {float(valor_total or 0):,.2f}**. "
        f"No recorte atual, ha **{int(contratos_municipais or 0)} contrato(s) municipal(is)** "
        f"(`R$ {float(valor_municipal or 0):,.2f}`) e **{int(contratos_estaduais or 0)} contrato(s) estadual(is)** "
        f"(`R$ {float(valor_estadual or 0):,.2f}`). "
        f"O sancionatorio bruto materializa **{int(sancoes_ativas or 0)} sancao(oes) ativa(s)** "
        f"em **{int(sancoes_total or 0)} registro(s)** CEIS. "
        f"A classificacao conservadora dos objetos indica **{int(bens_hits or 0)} contrato(s)** de fornecimento de bens, "
        f"**{int(servicos_hits or 0)}** de servicos gerais e **{int(terceirizacao_hits or 0)}** com sinal textual direto de terceirizacao de pessoal. "
        f"A checagem nominal exata dos socios encontrou **{int(socios_com_match or 0)}** coincidencia(s) nas bases locais carregadas. "
        f"Em camada separada de triagem, ha **{int(leads_total or 0)} lead(s)** por similaridade nominal com `NORTE` em contratos de terceirizacao, "
        f"somando **R$ {float(leads_valor_total or 0):,.2f}**; isso nao prova vinculacao societaria, mas orienta a expansao da rede investigada."
    )
    payload = [
        (
            row_hash(KIND_TRACE, TRACE_CNPJ),
            KIND_TRACE,
            "CRITICAL",
            96,
            float(valor_total or 0),
            title,
            description,
            "trace_norte_exposicao_sancionada",
            json.dumps(
                ["rb_contratos", "estado_ac_contratos", "federal_ceis", "empresa_socios"],
                ensure_ascii=False,
            ),
            json.dumps(
                ["NORTE", "CEIS", "RIO_BRANCO", "SESACRE", "GOVERNO_ACRE", "empresa_focal"],
                ensure_ascii=False,
            ),
            int(total_contratos or 0),
            float(valor_total or 0),
            datetime.now(),
            "mista",
            "Acre / Rio Branco",
            "NORTE",
            "Rio Branco",
            "AC",
            "controle_externo",
            None,
            float(valor_total or 0),
            None,
            "trace_norte",
        )
    ]
    if int(socios_com_match or 0) > 0:
        payload.append(
            (
                row_hash(KIND_TRACE_MATCH, TRACE_CNPJ),
                KIND_TRACE_MATCH,
                "HIGH",
                90,
                float(valor_total or 0),
                f"TRACE NORTE: socio com coincidencia nominal em base local para {razao_social_ref}",
                (
                    f"A trilha focal da **{razao_social_ref}** encontrou **{int(socios_com_match or 0)} coincidencia(s) nominais exatas** "
                    f"entre socios e bases locais de servidor/candidato. Isso e apenas pista de triagem e precisa de confirmacao documental."
                ),
                "trace_norte_nome_match",
                json.dumps(["empresa_socios", "servidores", "rb_servidores_lotacao", "cross_candidato_servidor"], ensure_ascii=False),
                json.dumps(["NORTE", "nome_match", "triagem"], ensure_ascii=False),
                int(socios_com_match or 0),
                float(valor_total or 0),
                datetime.now(),
                "mista",
                "Acre / Rio Branco",
                "NORTE",
                "Rio Branco",
                "AC",
                "controle_externo",
                None,
                float(valor_total or 0),
                None,
                "trace_norte",
            )
        )

    con.executemany(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl, title, description_md, pattern,
            sources, tags, sample_n, unit_total, created_at, esfera, ente, orgao,
            municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materializa a trilha dedicada da NORTE no banco.")
    parser.add_argument("--db", default=str(DB_PATH), help="Caminho do DuckDB alvo.")
    parser.add_argument("--refresh-cnpj", action="store_true", help="Forca refresh do CNPJ e QSA.")
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    ensure_tables(con)
    insight_enabled = ensure_insight_columns(con)

    empresa, socios = upsert_empresa_trace(con, refresh=args.refresh_cnpj)
    n_contracts = refresh_contracts(con)
    n_sancoes = refresh_sancoes(con)
    n_socios = refresh_socios(con, socios)
    n_leads = refresh_leads(con)
    build_views(con)
    n_insights = refresh_insights(con, enabled=insight_enabled)

    resumo = con.execute("SELECT * FROM v_trace_norte_resumo").fetchone()
    con.close()

    log.info(
        "trace_norte concluido | contratos=%s | sancoes=%s | socios=%s | leads=%s | insights=%s",
        n_contracts,
        n_sancoes,
        n_socios,
        n_leads,
        n_insights,
    )
    if resumo:
        log.info(
            "resumo | total_contratos=%s | valor_total=%.2f | terceirizacao_hits=%s | socios_com_match=%s",
            resumo[2],
            float(resumo[3] or 0),
            resumo[10],
            resumo[17],
        )
    if empresa:
        log.info(
            "empresa | %s | %s/%s | situacao=%s",
            empresa["razao_social"],
            empresa["municipio"],
            empresa["uf"],
            empresa["situacao"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
