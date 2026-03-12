from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.insight_classification import ensure_insight_classification_columns
from src.ingest.riobranco_servidor_detail import RioBrancoServidorDetail
from src.ingest.riobranco_servidor_list import RioBrancoServidorList

log = logging.getLogger("sync_rb_lotacao")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
KIND_PREFIX = "RB_SUS_LOTACAO"
BASE_URL = "https://transparencia.riobranco.ac.gov.br"
DEFAULT_DELAY = 0.4
CHECKPOINT_EVERY = 50
SUS_KEYWORDS = (
    "UBS",
    "UPA",
    "CAPS",
    "SEMSA",
    "FMS",
    "FUNDO MUNICIPAL DE SAUDE",
    "HOSPITAL",
    "MATERNIDADE",
    "POLICLINICA",
    "PRONTO ATENDIMENTO",
    "PSF",
    "ESF",
    "CTA",
    "SAE",
    "CENTRO DE SAUDE",
    "VIGILANCIA SANITARIA",
    "VIGILANCIA EPIDEMIOLOGICA",
)

DDL_LOTACAO = """
CREATE TABLE IF NOT EXISTS rb_servidores_lotacao (
    servidor_id VARCHAR PRIMARY KEY,
    matricula_contrato VARCHAR,
    nome VARCHAR,
    cargo VARCHAR,
    lotacao VARCHAR,
    secretaria VARCHAR,
    unidade VARCHAR,
    vinculo VARCHAR,
    sus BOOLEAN DEFAULT FALSE,
    sus_unidade VARCHAR,
    status VARCHAR DEFAULT 'ok',
    error_msg VARCHAR,
    http_status INTEGER,
    url VARCHAR,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def ensure_rb_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_LOTACAO)
    existing = {row[1] for row in con.execute("PRAGMA table_info('rb_servidores_lotacao')").fetchall()}
    extra_columns = {
        "matricula_contrato": "VARCHAR",
        "status": "VARCHAR DEFAULT 'ok'",
        "error_msg": "VARCHAR",
        "http_status": "INTEGER",
    }
    for column, dtype in extra_columns.items():
        if column not in existing:
            con.execute(f"ALTER TABLE rb_servidores_lotacao ADD COLUMN {column} {dtype}")


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


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Sentinela/3.0",
            "Accept-Encoding": "identity",
        }
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def extract_matricula_contrato(servidor_field: str) -> str:
    if "-" not in (servidor_field or ""):
        return (servidor_field or "").strip()
    return servidor_field.split("-", 1)[0].strip()


def extract_nome_from_servidor(servidor_field: str) -> str:
    if "-" not in (servidor_field or ""):
        return ""
    return servidor_field.split("-", 1)[1].strip()


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", (value or "").upper())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip()


def classify_sus(lotacao: str, secretaria: str, unidade: str) -> tuple[bool, str]:
    combined = normalize_text(" ".join(filter(None, [lotacao, secretaria, unidade])))
    for keyword in SUS_KEYWORDS:
        normalized_keyword = normalize_text(keyword)
        if re.search(rf"(?<![A-Z0-9]){re.escape(normalized_keyword)}(?![A-Z0-9])", combined):
            return True, keyword
    return False, ""


def load_mass_profiles(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, str]]:
    profiles: dict[str, dict[str, str]] = {}
    rows = con.execute(
        """
        SELECT servidor, cargo, vinculo
        FROM rb_servidores_mass
        WHERE servidor IS NOT NULL AND servidor <> ''
        """
    ).fetchall()
    for servidor, cargo, vinculo in rows:
        matricula_contrato = extract_matricula_contrato(servidor or "")
        if not matricula_contrato:
            continue
        profile = profiles.setdefault(
            matricula_contrato,
            {"nome": "", "cargo": "", "vinculo": ""},
        )
        nome = extract_nome_from_servidor(servidor or "")
        if nome and not profile["nome"]:
            profile["nome"] = nome
        if cargo and not profile["cargo"]:
            profile["cargo"] = cargo
        if vinculo and not profile["vinculo"]:
            profile["vinculo"] = vinculo
    return profiles


def upsert_lotacao_row(
    con: duckdb.DuckDBPyConnection,
    payload: dict[str, object],
    base_profile: dict[str, str] | None,
) -> None:
    lotacao = str(payload.get("lotacao") or "")
    secretaria = str(payload.get("secretaria") or "")
    unidade = str(payload.get("unidade") or "")
    sus, sus_keyword = classify_sus(lotacao, secretaria, unidade)
    profile = base_profile or {}

    con.execute(
        """
        INSERT INTO rb_servidores_lotacao (
            servidor_id, matricula_contrato, nome, cargo, lotacao, secretaria, unidade, vinculo,
            sus, sus_unidade, status, error_msg, http_status, url, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (servidor_id) DO UPDATE SET
            matricula_contrato = excluded.matricula_contrato,
            nome = excluded.nome,
            cargo = excluded.cargo,
            lotacao = excluded.lotacao,
            secretaria = excluded.secretaria,
            unidade = excluded.unidade,
            vinculo = excluded.vinculo,
            sus = excluded.sus,
            sus_unidade = excluded.sus_unidade,
            status = excluded.status,
            error_msg = excluded.error_msg,
            http_status = excluded.http_status,
            url = excluded.url,
            scraped_at = excluded.scraped_at
        """,
        [
            str(payload["servidor_id"]),
            str(payload.get("matricula_contrato") or ""),
            str(payload.get("nome") or profile.get("nome") or ""),
            str(payload.get("cargo") or profile.get("cargo") or ""),
            lotacao,
            secretaria,
            unidade,
            str(payload.get("vinculo") or profile.get("vinculo") or ""),
            sus,
            sus_keyword,
            str(payload.get("status") or "ok"),
            str(payload.get("error_msg") or ""),
            payload.get("http_status"),
            str(payload.get("url") or f"{BASE_URL}/servidor/ver/{payload['servidor_id']}/"),
            datetime.now(),
        ],
    )


def fetch_servidor_payload(scraper: RioBrancoServidorDetail, servidor_id: str) -> dict[str, object]:
    try:
        response = scraper.fetch(servidor_id)
        meta = dict(response.get("meta") or {})
        meta["status"] = "ok"
        meta["error_msg"] = ""
        meta["http_status"] = 200
        return meta
    except requests.HTTPError as exc:
        response = exc.response
        status_code = response.status_code if response is not None else None
        status = "not_found" if status_code == 404 else "error"
        return {
            "servidor_id": servidor_id,
            "status": status,
            "error_msg": str(exc),
            "http_status": status_code,
            "url": f"{BASE_URL}/servidor/ver/{servidor_id}/",
        }
    except requests.RequestException as exc:
        return {
            "servidor_id": servidor_id,
            "status": "error",
            "error_msg": str(exc),
            "http_status": None,
            "url": f"{BASE_URL}/servidor/ver/{servidor_id}/",
        }


def collect_lotacao(
    con: duckdb.DuckDBPyConnection,
    *,
    limit: int | None,
    delay: float,
) -> None:
    profiles = load_mass_profiles(con)
    session = build_session()
    list_scraper = RioBrancoServidorList(session=session)
    all_ids = sorted(list_scraper.fetch_all_ids(), key=int)
    done_rows = con.execute(
        """
        SELECT servidor_id
        FROM rb_servidores_lotacao
        WHERE COALESCE(status, 'ok') IN ('ok', 'not_found')
        """
    ).fetchall()
    done_ids = {row[0] for row in done_rows}
    pending_ids = [servidor_id for servidor_id in all_ids if servidor_id not in done_ids]

    log.info("IDs válidos na listagem do portal: %d", len(all_ids))
    log.info("Pendentes para scrape: %d", len(pending_ids))
    if limit is not None:
        pending_ids = pending_ids[:limit]
        log.info("Aplicando --limit=%d", limit)

    scraper = RioBrancoServidorDetail(session=session)

    for index, servidor_id in enumerate(pending_ids, start=1):
        payload = fetch_servidor_payload(scraper, servidor_id)
        matricula_contrato = str(payload.get("matricula_contrato") or "")
        upsert_lotacao_row(con, payload, profiles.get(matricula_contrato))

        if index % CHECKPOINT_EVERY == 0:
            log.info("Processados %d/%d servidores", index, len(pending_ids))
            con.execute("CHECKPOINT")

        if delay > 0:
            time.sleep(delay)

    log.info(
        "Coleta concluída. rb_servidores_lotacao=%d",
        con.execute("SELECT COUNT(*) FROM rb_servidores_lotacao").fetchone()[0],
    )


def reclassify_existing_rows(con: duckdb.DuckDBPyConnection) -> int:
    rows = con.execute(
        """
        SELECT servidor_id, lotacao, secretaria, unidade
        FROM rb_servidores_lotacao
        WHERE COALESCE(status, 'ok') = 'ok'
        """
    ).fetchall()
    updates = []
    for servidor_id, lotacao, secretaria, unidade in rows:
        sus, sus_keyword = classify_sus(lotacao or "", secretaria or "", unidade or "")
        updates.append((sus, sus_keyword, servidor_id))
    if updates:
        con.executemany(
            "UPDATE rb_servidores_lotacao SET sus = ?, sus_unidade = ? WHERE servidor_id = ?",
            updates,
        )
    return con.execute(
        "SELECT COUNT(*) FROM rb_servidores_lotacao WHERE sus = TRUE AND COALESCE(status, 'ok') = 'ok'"
    ).fetchone()[0]


def build_sus_view(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_sus AS
        WITH mass_dedup AS (
            SELECT
                REGEXP_REPLACE(servidor, '-.*', '') AS matricula_contrato,
                MAX(vencimento_base) AS vencimento_base_referencia,
                MAX(salario_bruto) AS salario_bruto_referencia,
                MAX(salario_liquido) AS salario_liquido_referencia,
                COUNT(*) AS folhas_registradas
            FROM rb_servidores_mass
            WHERE servidor IS NOT NULL AND servidor <> ''
            GROUP BY 1
        )
        SELECT
            l.servidor_id,
            l.matricula_contrato,
            l.nome,
            l.cargo,
            l.lotacao,
            l.secretaria,
            l.unidade,
            l.sus_unidade,
            m.vencimento_base_referencia AS vencimento_base,
            m.salario_bruto_referencia AS salario_bruto,
            m.salario_liquido_referencia AS salario_liquido,
            m.folhas_registradas
        FROM rb_servidores_lotacao l
        LEFT JOIN mass_dedup m
          ON m.matricula_contrato = l.matricula_contrato
        WHERE l.sus = TRUE AND COALESCE(l.status, 'ok') = 'ok'
        """
    )
    return con.execute("SELECT COUNT(DISTINCT servidor_id) FROM v_rb_sus").fetchone()[0]


def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    if not ensure_insight_columns(con):
        return 0

    con.execute("DELETE FROM insight WHERE kind LIKE ?", [f"{KIND_PREFIX}%"])
    rows = con.execute(
        """
        SELECT
            COALESCE(NULLIF(unidade, ''), NULLIF(lotacao, ''), NULLIF(secretaria, ''), 'N/I') AS unidade_real,
            COUNT(DISTINCT servidor_id) AS n_servidores,
            AVG(salario_bruto) AS media_bruta_referencia,
            SUM(salario_bruto) AS total_bruto_referencia
        FROM v_rb_sus
        GROUP BY 1
        HAVING COUNT(DISTINCT servidor_id) > 0
        ORDER BY total_bruto_referencia DESC NULLS LAST, n_servidores DESC, unidade_real
        LIMIT 200
        """
    ).fetchall()

    payload = []
    for unidade_real, n_servidores, media_bruta, total_bruto in rows:
        exposure = float(total_bruto or 0)
        unidade_label = unidade_real or "N/I"
        insight_id = "INS_" + hashlib.sha1(
            f"{KIND_PREFIX}|{unidade_label}".encode("utf-8")
        ).hexdigest()[:16]
        payload.append(
            (
                insight_id,
                f"{KIND_PREFIX}_UNIDADE",
                "INFO",
                82,
                exposure,
                f"SUS Rio Branco: {n_servidores} servidores vinculados a {unidade_label}",
                (
                    f"A unidade **{unidade_label}** concentrou **{n_servidores} servidor(es)** "
                    f"classificados como SUS por lotação real, com folha bruta de referência de "
                    f"**R$ {exposure:,.2f}** e média individual de **R$ {float(media_bruta or 0):,.2f}**."
                ),
                "lotacao_sus_por_unidade",
                json.dumps(
                    [
                        "rb_servidores_lotacao",
                        "transparencia.riobranco.ac.gov.br/servidor",
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(
                    ["SUS", "SEMSA", "RIO_BRANCO", "lotacao", "servidores"],
                    ensure_ascii=False,
                ),
                int(n_servidores),
                exposure,
                "municipal",
                "Prefeitura de Rio Branco",
                "SEMSA",
                "Rio Branco",
                "AC",
                "saude",
                True,
                exposure,
                None,
                "transparencia.riobranco.ac.gov.br",
            )
        )

    if payload:
        con.executemany(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md, pattern,
                sources, tags, sample_n, unit_total, created_at, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza lotação real dos servidores de Rio Branco e classifica SUS."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita o número de servidores processados nesta execução.",
    )
    parser.add_argument(
        "--classify-only",
        action="store_true",
        help="Pula a coleta HTTP e apenas reclassifica/view/insights.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Delay entre requests em segundos (padrão: {DEFAULT_DELAY}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(DB_PATH))
    ensure_rb_schema(con)

    if not args.classify_only:
        collect_lotacao(con, limit=args.limit, delay=args.delay)
    else:
        log.info("Pulando coleta HTTP por --classify-only.")

    n_sus = reclassify_existing_rows(con)
    n_view = build_sus_view(con)
    n_insights = build_insights(con)

    log.info("Servidores SUS classificados: %d", n_sus)
    log.info("v_rb_sus: %d servidores", n_view)
    log.info("Insights gerados: %d", n_insights)
    log.info(
        "Resumo final: lotacao=%d | sus=%d | insights=%d",
        con.execute("SELECT COUNT(*) FROM rb_servidores_lotacao").fetchone()[0],
        n_sus,
        n_insights,
    )
    con.close()


if __name__ == "__main__":
    main()
