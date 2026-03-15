"""
sync_rb_lotacao.py
------------------
Enriquece rb_servidores_mass com lotacao/secretaria/unidade reais,
raspados do portal JSF de Rio Branco por servidor_id.

Cria:
  - TABLE rb_servidores_lotacao   — detalhe por servidor (lotacao, secretaria, unidade)
  - VIEW  v_rb_sus                — servidores classificados como SUS por unidade real
  - INSERTs em insight            — kind=RB_SUS_LOTACAO_*

Fluxo:
  1. Extrai IDs únicos de rb_servidores_mass (campo `servidor` = "ID/seq-NOME")
  2. Busca /servidor/ver/<id>/ para cada ID ainda não processado
  3. Persiste lotacao + secretaria + unidade no DuckDB
  4. Classifica SUS por unidade (UBS, UPA, CAPS, SEMSA, FMS, Hospital)
  5. Gera insights canonicos

Uso:
    # Primeira carga (~20k requests, pode demorar horas — use --limit para testar):
    .venv/bin/python scripts/sync_rb_lotacao.py --limit 200

    # Carga completa (resumível: só processa IDs ainda não no banco):
    .venv/bin/python scripts/sync_rb_lotacao.py

    # Só reclassifica e recria insights (sem novas requests):
    .venv/bin/python scripts/sync_rb_lotacao.py --classify-only
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger("sync_rb_lotacao")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DUCKDB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
BASE_URL    = "https://transparencia.riobranco.ac.gov.br"
DELAY       = 0.4   # segundos entre requests
KIND_PREFIX = "RB_SUS_LOTACAO"

# Unidades que caracterizam SUS municipal (Rio Branco)
SUS_KEYWORDS = (
    "UBS", "UPA", "CAPS", "SEMSA", "FMS", "FUNDO MUNICIPAL DE SAUDE",
    "HOSPITAL", "MATERNIDADE", "POLICLINICA", "POLICLÍNICA",
    "PRONTO ATENDIMENTO", "PSF", "ESF", "CTA", "SAE",
    "CENTRO DE SAUDE", "CENTRO DE SAÚDE",
    "VIGILANCIA SANITARIA", "VIGILÂNCIA SANITÁRIA",
    "VIGILANCIA EPIDEMIOLOGICA", "VIGILÂNCIA EPIDEMIOLÓGICA",
)

DDL_LOTACAO = """
CREATE TABLE IF NOT EXISTS rb_servidores_lotacao (
    servidor_id   VARCHAR PRIMARY KEY,
    nome          VARCHAR,
    cargo         VARCHAR,
    lotacao       VARCHAR,
    secretaria    VARCHAR,
    unidade       VARCHAR,
    vinculo       VARCHAR,
    sus           BOOLEAN DEFAULT FALSE,
    sus_unidade   VARCHAR,    -- keyword que disparou a classificação SUS
    url           VARCHAR,
    scraped_at    TIMESTAMP DEFAULT current_timestamp
)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_id(servidor_field: str) -> str | None:
    """
    '600141/1-ABDEL BARBOSA DERZE'  →  '600141'
    '716447/1-ABEL FELIPE...'       →  '716447'
    """
    m = re.match(r"^(\d+)/", servidor_field.strip())
    return m.group(1) if m else None


def _norm(s: str) -> str:
    import unicodedata
    s = s.upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()


def _classify_sus(lotacao: str, secretaria: str, unidade: str) -> tuple[bool, str]:
    """Retorna (is_sus, keyword_disparadora)."""
    combined = _norm(f"{lotacao} {secretaria} {unidade}")
    for kw in SUS_KEYWORDS:
        if kw in combined:
            return True, kw
    return False, ""


# ── Scraper ───────────────────────────────────────────────────────────────────

def _scrape_detail(session: requests.Session, servidor_id: str) -> dict:
    url = f"{BASE_URL}/servidor/ver/{servidor_id}/"
    try:
        r = session.get(url, timeout=20)
        if r.status_code == 404:
            return {"servidor_id": servidor_id, "url": url, "not_found": True}
        r.raise_for_status()
    except Exception as e:
        log.warning("Erro ao buscar %s: %s", servidor_id, e)
        return {"servidor_id": servidor_id, "url": url, "error": str(e)}

    soup = BeautifulSoup(r.text, "html.parser")
    data: dict = {"servidor_id": servidor_id, "url": url}
    text = soup.get_text("\n", strip=True)

    for key in ("Nome", "Cargo", "Lotação", "Lotacao", "Secretaria",
                "Vínculo", "Vinculo", "Unidade"):
        canon = key.lower().replace("ã", "a").replace("í", "i")
        m = re.search(rf"(?i){key}\s*[:\-–]\s*(.+)", text)
        if m:
            data[canon] = m.group(1).strip()

    # Aliases
    data.setdefault("lotacao", data.pop("lota\u00e7\u00e3o", ""))
    data.setdefault("vinculo", data.pop("v\u00ednculo", ""))

    return data


# ── Persistência ──────────────────────────────────────────────────────────────

def _upsert_row(con: duckdb.DuckDBPyConnection, d: dict):
    if d.get("not_found") or d.get("error"):
        # Insere placeholder para não reprocessar
        con.execute(
            "INSERT OR IGNORE INTO rb_servidores_lotacao (servidor_id, url) VALUES (?, ?)",
            [d["servidor_id"], d.get("url", "")],
        )
        return

    lotacao    = d.get("lotacao", "") or ""
    secretaria = d.get("secretaria", "") or ""
    unidade    = d.get("unidade", "") or ""
    sus, kw    = _classify_sus(lotacao, secretaria, unidade)

    con.execute(
        """
        INSERT INTO rb_servidores_lotacao
            (servidor_id, nome, cargo, lotacao, secretaria, unidade,
             vinculo, sus, sus_unidade, url)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT (servidor_id) DO UPDATE SET
            nome       = excluded.nome,
            cargo      = excluded.cargo,
            lotacao    = excluded.lotacao,
            secretaria = excluded.secretaria,
            unidade    = excluded.unidade,
            vinculo    = excluded.vinculo,
            sus        = excluded.sus,
            sus_unidade= excluded.sus_unidade,
            scraped_at = current_timestamp
        """,
        [
            d["servidor_id"],
            d.get("nome", ""),
            d.get("cargo", ""),
            lotacao, secretaria, unidade,
            d.get("vinculo", ""),
            sus, kw,
            d.get("url", ""),
        ],
    )


# ── Coleta ────────────────────────────────────────────────────────────────────

def collect(con: duckdb.DuckDBPyConnection, limit: int | None):
    # IDs únicos da tabela mass
    all_ids_raw = con.execute(
        "SELECT DISTINCT servidor FROM rb_servidores_mass"
    ).fetchall()

    all_ids = []
    for (s,) in all_ids_raw:
        sid = _extract_id(s or "")
        if sid:
            all_ids.append(sid)
    all_ids = list(set(all_ids))
    log.info("IDs únicos em rb_servidores_mass: %d", len(all_ids))

    # Já processados
    done = {r[0] for r in con.execute(
        "SELECT servidor_id FROM rb_servidores_lotacao"
    ).fetchall()}
    pending = [sid for sid in all_ids if sid not in done]
    log.info("Pendentes (não scrapeados ainda): %d", len(pending))

    if limit:
        pending = pending[:limit]
        log.info("Limitando a %d por --limit", limit)

    session = requests.Session()
    session.headers.update({"User-Agent": "Sentinela/3.0"})

    for i, sid in enumerate(pending, 1):
        d = _scrape_detail(session, sid)
        _upsert_row(con, d)

        if i % 50 == 0:
            log.info("  %d/%d scrapeados...", i, len(pending))
            con.execute("CHECKPOINT")  # persiste ao longo do processo

        time.sleep(DELAY)

    log.info("Coleta concluída. Total em rb_servidores_lotacao: %d",
             con.execute("SELECT COUNT(*) FROM rb_servidores_lotacao").fetchone()[0])


# ── View SUS ──────────────────────────────────────────────────────────────────

def build_view(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_sus AS
        SELECT
            l.servidor_id,
            l.nome,
            l.cargo,
            l.lotacao,
            l.secretaria,
            l.unidade,
            l.sus_unidade,
            m.vencimento_base,
            m.salario_bruto,
            m.ano_id,
            m.mes_id
        FROM rb_servidores_lotacao l
        JOIN rb_servidores_mass m
          ON l.servidor_id = REGEXP_REPLACE(m.servidor, '/.*', '')
        WHERE l.sus = TRUE
    """)
    n = con.execute("SELECT COUNT(DISTINCT servidor_id) FROM v_rb_sus").fetchone()[0]
    log.info("v_rb_sus: %d servidores SUS por lotação real", n)
    return n


# ── Insights ──────────────────────────────────────────────────────────────────

def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "insight" not in tables:
        log.warning("Tabela insight não existe.")
        return 0

    con.execute(f"DELETE FROM insight WHERE kind LIKE '{KIND_PREFIX}%'")

    # Insight 1: concentração de servidores SUS por unidade
    rows = con.execute("""
        SELECT
            COALESCE(unidade, lotacao, secretaria) AS unidade_real,
            COUNT(DISTINCT servidor_id)            AS n_servidores,
            AVG(salario_bruto)                     AS media_bruto,
            SUM(salario_bruto)                     AS total_bruto
        FROM v_rb_sus
        GROUP BY 1
        HAVING COUNT(DISTINCT servidor_id) > 0
        ORDER BY n_servidores DESC
        LIMIT 200
    """).fetchall()

    agora = datetime.now()
    records = []
    for unidade_real, n_srv, media, total in rows:
        iid = "INS_" + hashlib.md5(
            f"{KIND_PREFIX}{unidade_real}".encode()
        ).hexdigest()[:12]
        records.append((
            iid,
            f"{KIND_PREFIX}_UNIDADE",
            "INFO",
            80,
            float(total or 0),
            f"SUS Rio Branco — {n_srv} servidores em {unidade_real or 'N/I'}",
            (
                f"A unidade **{unidade_real or 'N/I'}** (SEMSA/SUS) possui "
                f"**{n_srv} servidor(es)** com folha total de "
                f"**R$ {float(total or 0):,.2f}** "
                f"(média R$ {float(media or 0):,.2f}/servidor)."
            ),
            "servidores_sus_por_unidade",
            json.dumps(["transparencia.riobranco.ac.gov.br/servidor"]),
            json.dumps(["SUS", "SEMSA", "RIO_BRANCO", "servidores"]),
            int(n_srv),
            float(total or 0),
            agora,
            "municipal",
            "Prefeitura de Rio Branco",
            "SEMSA",
            "Rio Branco",
            "AC",
            "saude",
            True,
            float(total or 0),
            None,
            "transparencia.riobranco.ac.gov.br",
        ))

    if records:
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


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Raspa lotacao/unidade de servidores de RB e classifica SUS"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limita o número de servidores a raspar (para teste)",
    )
    parser.add_argument(
        "--classify-only", action="store_true",
        help="Só reclassifica SUS e recria view/insights sem novas requests",
    )
    parser.add_argument(
        "--delay", type=float, default=DELAY,
        help=f"Delay entre requests em segundos (padrão: {DELAY})",
    )
    args = parser.parse_args()

    global DELAY
    DELAY = args.delay

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(DDL_LOTACAO)

    if not args.classify_only:
        collect(con, args.limit)
    else:
        log.info("--classify-only: pulando coleta.")

    # Reclassifica todas as linhas existentes
    log.info("Reclassificando SUS em rb_servidores_lotacao...")
    rows = con.execute(
        "SELECT servidor_id, lotacao, secretaria, unidade FROM rb_servidores_lotacao"
    ).fetchall()
    updates = []
    for sid, lotacao, secretaria, unidade in rows:
        sus, kw = _classify_sus(lotacao or "", secretaria or "", unidade or "")
        updates.append((sus, kw, sid))
    con.executemany(
        "UPDATE rb_servidores_lotacao SET sus=?, sus_unidade=? WHERE servidor_id=?",
        updates,
    )
    n_sus = con.execute(
        "SELECT COUNT(*) FROM rb_servidores_lotacao WHERE sus=TRUE"
    ).fetchone()[0]
    log.info("Servidores SUS classificados: %d", n_sus)

    n_view = build_view(con)
    n_ins  = build_insights(con)
    log.info("Insights gerados: %d", n_ins)

    log.info("=== Concluído: lotacao=%d | sus=%d | insights=%d ===",
             con.execute("SELECT COUNT(*) FROM rb_servidores_lotacao").fetchone()[0],
             n_sus, n_ins)
    con.close()


if __name__ == "__main__":
    main()
