"""
sync_ceis_cnep.py
-----------------
Carrega CEIS e CNEP do Portal da Transparência (dados abertos, sem token)
no DuckDB e materializa o cruzamento com fornecedores do Governo do Acre.

URL real do portal (formato diário, confirmado 12/03/2026):
  https://portaldatransparencia.gov.br/download-de-dados/ceis/YYYYMMDD
  https://portaldatransparencia.gov.br/download-de-dados/cnep/YYYYMMDD
  → redireciona para YYYYMMDD_CEIS.zip / YYYYMMDD_CNEP.zip

Uso:
    # ZIPs já baixados em data/federal/:
    python scripts/sync_ceis_cnep.py --data 20260312

    # Baixar agora (passa data de hoje):
    python scripts/sync_ceis_cnep.py --data 20260312 --download

    # Apontar ZIPs manualmente:
    python scripts/sync_ceis_cnep.py \
        --local-ceis data/federal/20260312_CEIS.zip \
        --local-cnep data/federal/20260312_CNEP.zip
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import sys
import uuid
import zipfile
from datetime import datetime, date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger("sync_ceis_cnep")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DUCKDB_PATH   = ROOT / "data" / "sentinela_analytics.duckdb"
FEDERAL_DIR   = ROOT / "data" / "federal"
BASE_URL      = "https://portaldatransparencia.gov.br/download-de-dados"

SANCAO_KIND_PREFIX = "SESACRE_SANCAO"

# ── DDL ───────────────────────────────────────────────────────────────────────

DDL_CEIS = """
CREATE TABLE IF NOT EXISTS federal_ceis (
    id                      VARCHAR PRIMARY KEY,
    cnpj_cpf                VARCHAR,
    nome_sancionado         VARCHAR,
    nome_informante         VARCHAR,
    tipo_pessoa             VARCHAR,
    uf_sancionado           VARCHAR,
    tipo_sancao             VARCHAR,
    data_inicio_sancao      VARCHAR,
    data_fim_sancao         VARCHAR,
    data_publicacao         VARCHAR,
    numero_processo         VARCHAR,
    fundamentacao_legal     VARCHAR,
    descricao_fundamentacao VARCHAR,
    quantidade_meses_pena   INTEGER,
    competencia             VARCHAR,
    inserted_at             TIMESTAMP DEFAULT current_timestamp
)
"""

DDL_CNEP = """
CREATE TABLE IF NOT EXISTS federal_cnep (
    id                      VARCHAR PRIMARY KEY,
    cnpj_cpf                VARCHAR,
    nome_sancionado         VARCHAR,
    nome_informante         VARCHAR,
    tipo_pessoa             VARCHAR,
    uf_sancionado           VARCHAR,
    tipo_sancao             VARCHAR,
    data_inicio_sancao      VARCHAR,
    data_fim_sancao         VARCHAR,
    data_publicacao         VARCHAR,
    numero_processo         VARCHAR,
    fundamentacao_legal     VARCHAR,
    descricao_fundamentacao VARCHAR,
    valor_multa             DOUBLE,
    competencia             VARCHAR,
    inserted_at             TIMESTAMP DEFAULT current_timestamp
)
"""

DDL_CRUZAMENTOS = """
CREATE TABLE IF NOT EXISTS estado_ac_fornecedor_sancoes (
    id                 VARCHAR PRIMARY KEY,
    cnpj_cpf           VARCHAR,
    nome_sancionado    VARCHAR,
    fonte              VARCHAR,
    tipo_sancao        VARCHAR,
    data_inicio_sancao VARCHAR,
    data_fim_sancao    VARCHAR,
    nome_informante    VARCHAR,
    orgao_ac           VARCHAR,
    valor_pago_ac      DOUBLE,
    n_pagamentos_ac    INTEGER,
    inserted_at        TIMESTAMP DEFAULT current_timestamp
)
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_str() -> str:
    return date.today().strftime("%Y%m%d")


def _col(row: dict, *candidates: str) -> str:
    for c in candidates:
        v = row.get(c, "")
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _clean_doc(raw: str) -> str:
    return re.sub(r"\D", "", raw or "")


def _parse_int(raw: str) -> int:
    try:
        return int(str(raw).strip())
    except Exception:
        return 0


def _parse_float(raw: str) -> float:
    try:
        return float(str(raw).replace(".", "").replace(",", "."))
    except Exception:
        return 0.0


def _short_id(prefix: str, *parts: str) -> str:
    h = hashlib.md5("|".join(parts).encode()).hexdigest()[:12]
    return f"{prefix}_{h}"


def _read_csv_from_zip(zip_bytes: bytes) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_files:
            raise ValueError("Nenhum CSV no ZIP")
        with zf.open(csv_files[0]) as f:
            text = f.read().decode("latin-1", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    rows = list(reader)
    log.info("CSV lido: %d registros", len(rows))
    return rows


def _download(url: str) -> bytes:
    import requests
    log.info("Baixando %s", url)
    r = requests.get(
        url,
        headers={"User-Agent": "Sentinela/2.0 (dados abertos CGU)"},
        timeout=120,
        allow_redirects=True,
    )
    r.raise_for_status()
    log.info("  %.1f MB recebidos", len(r.content) / 1e6)
    return r.content


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_ceis(con: duckdb.DuckDBPyConnection, rows: list[dict], competencia: str) -> int:
    con.execute("DELETE FROM federal_ceis WHERE competencia = ?", [competencia])
    records = []
    for row in rows:
        cnpj = _clean_doc(_col(row,
            "CNPJ", "CPF", "CNPJ/CPF", "cnpj_cpf",
            "CPF/CNPJ do Sancionado",
        ))
        nome = _col(row, "NOME SANCIONADO", "Nome Sancionado", "nome_sancionado",
                    "Nome do Sancionado")
        rid = _short_id("CEIS", cnpj, nome,
                        _col(row, "DATA INÍCIO SANÇÃO", "Data Início Sanção"))
        records.append((
            rid, cnpj, nome,
            _col(row, "NOME ÓRGÃO SANCIONADOR", "Órgão Sancionador", "nome_informante",
                 "Nome do Órgão Sancionador"),
            _col(row, "TIPO PESSOA", "Tipo de Pessoa", "tipo_pessoa"),
            _col(row, "UF ÓRGÃO SANCIONADOR", "UF do Órgão Sancionador", "uf_sancionado"),
            _col(row, "TIPO SANÇÃO", "Tipo de Sanção", "tipo_sancao"),
            _col(row, "DATA INÍCIO SANÇÃO", "Data Início Sanção", "data_inicio_sancao"),
            _col(row, "DATA FIM SANÇÃO", "Data Fim Sanção", "data_fim_sancao"),
            _col(row, "DATA PUBLICAÇÃO", "Data de Publicação", "data_publicacao"),
            _col(row, "NÚMERO PROCESSO", "Número do Processo", "numero_processo"),
            _col(row, "FUNDAMENTAÇÃO LEGAL", "Fundamentação Legal", "fundamentacao_legal"),
            _col(row, "DESCRIÇÃO FUNDAMENTAÇÃO LEGAL", "Descrição da Fundamentação",
                 "descricao_fundamentacao"),
            _parse_int(_col(row, "QUANTIDADE MESES PENA", "Quantidade de Meses de Pena")),
            competencia,
        ))
    con.executemany(
        """INSERT OR IGNORE INTO federal_ceis
           (id, cnpj_cpf, nome_sancionado, nome_informante, tipo_pessoa,
            uf_sancionado, tipo_sancao, data_inicio_sancao, data_fim_sancao,
            data_publicacao, numero_processo, fundamentacao_legal,
            descricao_fundamentacao, quantidade_meses_pena, competencia)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        records,
    )
    return len(records)


def load_cnep(con: duckdb.DuckDBPyConnection, rows: list[dict], competencia: str) -> int:
    con.execute("DELETE FROM federal_cnep WHERE competencia = ?", [competencia])
    records = []
    for row in rows:
        cnpj = _clean_doc(_col(row,
            "CNPJ", "CPF", "CNPJ/CPF", "cnpj_cpf",
            "CPF/CNPJ do Sancionado",
        ))
        nome = _col(row, "NOME SANCIONADO", "Nome Sancionado", "nome_sancionado",
                    "Nome do Sancionado")
        rid = _short_id("CNEP", cnpj, nome,
                        _col(row, "DATA INÍCIO SANÇÃO", "Data Início Sanção"))
        records.append((
            rid, cnpj, nome,
            _col(row, "NOME ÓRGÃO SANCIONADOR", "Órgão Sancionador", "nome_informante",
                 "Nome do Órgão Sancionador"),
            _col(row, "TIPO PESSOA", "Tipo de Pessoa", "tipo_pessoa"),
            _col(row, "UF ÓRGÃO SANCIONADOR", "UF do Órgão Sancionador", "uf_sancionado"),
            _col(row, "TIPO SANÇÃO", "Tipo de Sanção", "tipo_sancao"),
            _col(row, "DATA INÍCIO SANÇÃO", "Data Início Sanção", "data_inicio_sancao"),
            _col(row, "DATA FIM SANÇÃO", "Data Fim Sanção", "data_fim_sancao"),
            _col(row, "DATA PUBLICAÇÃO", "Data de Publicação", "data_publicacao"),
            _col(row, "NÚMERO PROCESSO", "Número do Processo", "numero_processo"),
            _col(row, "FUNDAMENTAÇÃO LEGAL", "Fundamentação Legal", "fundamentacao_legal"),
            _col(row, "DESCRIÇÃO FUNDAMENTAÇÃO LEGAL", "Descrição da Fundamentação",
                 "descricao_fundamentacao"),
            _parse_float(_col(row, "VALOR MULTA", "Valor da Multa", "valor_multa")),
            competencia,
        ))
    con.executemany(
        """INSERT OR IGNORE INTO federal_cnep
           (id, cnpj_cpf, nome_sancionado, nome_informante, tipo_pessoa,
            uf_sancionado, tipo_sancao, data_inicio_sancao, data_fim_sancao,
            data_publicacao, numero_processo, fundamentacao_legal,
            descricao_fundamentacao, valor_multa, competencia)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        records,
    )
    return len(records)


# ── Cruzamento ────────────────────────────────────────────────────────────────

def cross_with_estado(con: duckdb.DuckDBPyConnection) -> int:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "estado_ac_pagamentos" not in tables:
        log.warning("estado_ac_pagamentos não existe — rode sync_estado_ac.py primeiro.")
        return 0

    con.execute("DELETE FROM estado_ac_fornecedor_sancoes")

    for fonte, tabela in (("CEIS", "federal_ceis"), ("CNEP", "federal_cnep")):
        valor_col = "0.0" if fonte == "CEIS" else "0.0"
        con.execute(f"""
            INSERT INTO estado_ac_fornecedor_sancoes
                (id, cnpj_cpf, nome_sancionado, fonte, tipo_sancao,
                 data_inicio_sancao, data_fim_sancao, nome_informante,
                 orgao_ac, valor_pago_ac, n_pagamentos_ac)
            SELECT
                md5(s.cnpj_cpf || '{fonte}' || s.tipo_sancao || p.orgao_canonico)
                    AS id,
                s.cnpj_cpf,
                s.nome_sancionado,
                '{fonte}'               AS fonte,
                s.tipo_sancao,
                s.data_inicio_sancao,
                s.data_fim_sancao,
                s.nome_informante,
                p.orgao_canonico        AS orgao_ac,
                SUM(p.valor)            AS valor_pago_ac,
                COUNT(*)                AS n_pagamentos_ac
            FROM {tabela} s
            JOIN estado_ac_pagamentos p
              ON s.cnpj_cpf = p.cnpjcpf
             AND LENGTH(s.cnpj_cpf) >= 11
            GROUP BY s.cnpj_cpf, s.nome_sancionado, s.tipo_sancao,
                     s.data_inicio_sancao, s.data_fim_sancao,
                     s.nome_informante, p.orgao_canonico
        """)

    total = con.execute(
        "SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes"
    ).fetchone()[0]
    return total


# ── Insights (schema canônico real) ──────────────────────────────────────────

def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "insight" not in tables:
        log.warning("Tabela insight não existe.")
        return 0

    con.execute(f"DELETE FROM insight WHERE kind LIKE '{SANCAO_KIND_PREFIX}%'")

    rows = con.execute("""
        SELECT
            cnpj_cpf,
            nome_sancionado,
            fonte,
            tipo_sancao,
            orgao_ac,
            valor_pago_ac,
            n_pagamentos_ac,
            data_inicio_sancao,
            data_fim_sancao,
            nome_informante
        FROM estado_ac_fornecedor_sancoes
        ORDER BY valor_pago_ac DESC
        LIMIT 500
    """).fetchall()

    if not rows:
        return 0

    AREA_MAP = {
        "SESACRE": "saude", "SEE": "educacao", "SEFAZ": "financas",
        "SEJUSP": "seguranca", "SEINFRA": "infraestrutura", "SEOP": "obras",
        "SEMA": "meio_ambiente", "SEAGRO": "agropecuaria", "SEAS": "assistencia_social",
    }

    records = []
    agora = datetime.now()
    for (cnpj, nome, fonte, tipo, orgao, valor_pago, n_pag,
         dt_ini, dt_fim, informante) in rows:

        kind    = f"{SANCAO_KIND_PREFIX}_{fonte}"
        iid     = _short_id(kind, cnpj, orgao or "")

        # severidade: CRITICAL se sanção ainda ativa, HIGH caso contrário
        ativa = not dt_fim or dt_fim.strip() == ""
        severity   = "CRITICAL" if ativa else "HIGH"
        confidence = 95

        vigencia = f"{dt_ini} a {dt_fim or 'indefinida'}"

        title = (
            f"[{fonte}] Fornecedor sancionado recebeu R$ {valor_pago:,.0f} "
            f"do {orgao or 'Governo do Acre'}"
        )
        description_md = (
            f"**{nome}** (CNPJ/CPF `{cnpj}`) consta no cadastro **{fonte}** "
            f"com sanção do tipo _{tipo}_ "
            f"({vigencia}), informada por {informante or 'N/I'}.\n\n"
            f"Apesar da sanção, o órgão **{orgao}** realizou **{n_pag} pagamento(s)** "
            f"totalizando **R$ {valor_pago:,.2f}**."
        )

        records.append((
            iid,
            kind,
            severity,
            confidence,
            valor_pago,           # exposure_brl
            title,
            description_md,
            "fornecedor_sancionado_ativo",
            json.dumps([f"portaldatransparencia.gov.br/{fonte.lower()}"]),
            json.dumps([fonte, "SESACRE", orgao or "GOVERNO_ACRE", "sancao"]),
            n_pag,                # sample_n
            valor_pago,           # unit_total
            agora,                # created_at
            "estadual",           # esfera
            "Governo do Estado do Acre",  # ente
            orgao or "",          # orgao
            "",                   # municipio
            "AC",                 # uf
            AREA_MAP.get(orgao, "gestao_estadual"),  # area_tematica
            orgao == "SESACRE",   # sus
            valor_pago,           # valor_referencia
            None,                 # ano_referencia
            f"portaldatransparencia.gov.br/{fonte.lower()}",  # fonte
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


# ── Views de compatibilidade ──────────────────────────────────────────────────

def create_compat_views(con: duckdb.DuckDBPyConnection):
    con.execute("CREATE OR REPLACE VIEW cgu_ceis AS SELECT * FROM federal_ceis")
    con.execute("CREATE OR REPLACE VIEW cgu_cnep AS SELECT * FROM federal_cnep")
    log.info("Views cgu_ceis / cgu_cnep (re)criadas.")


# ── Entry point ───────────────────────────────────────────────────────────────

def run(
    data_str: str,
    local_ceis: Path | None,
    local_cnep: Path | None,
    download: bool,
):
    log.info("=== sync_ceis_cnep.py — data=%s ===", data_str)
    FEDERAL_DIR.mkdir(parents=True, exist_ok=True)

    # ── Resolve ZIPs ──────────────────────────────────────────────────────────
    def resolve_zip(kind: str, local: Path | None) -> bytes:
        fname = FEDERAL_DIR / f"{data_str}_{kind}.zip"
        if local and local.exists():
            log.info("Usando local: %s", local)
            return local.read_bytes()
        if fname.exists():
            log.info("Usando cache: %s", fname)
            return fname.read_bytes()
        if download:
            url = f"{BASE_URL}/{kind.lower()}/{data_str}"
            data = _download(url)
            fname.write_bytes(data)
            log.info("Salvo em %s", fname)
            return data
        raise FileNotFoundError(
            f"ZIP {kind} não encontrado em {fname}. "
            f"Passe --download ou --local-{kind.lower()} <caminho>."
        )

    ceis_bytes = resolve_zip("CEIS", local_ceis)
    cnep_bytes = resolve_zip("CNEP", local_cnep)

    # ── Banco ─────────────────────────────────────────────────────────────────
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(DDL_CEIS)
    con.execute(DDL_CNEP)
    con.execute(DDL_CRUZAMENTOS)

    # ── Load ──────────────────────────────────────────────────────────────────
    n_ceis = load_ceis(con, _read_csv_from_zip(ceis_bytes), data_str)
    log.info("CEIS: %d registros carregados", n_ceis)

    n_cnep = load_cnep(con, _read_csv_from_zip(cnep_bytes), data_str)
    log.info("CNEP: %d registros carregados", n_cnep)

    # ── Cruzamento ────────────────────────────────────────────────────────────
    n_cruz = cross_with_estado(con)
    log.info("Cruzamentos com fornecedores do estado: %d", n_cruz)

    # ── Insights ──────────────────────────────────────────────────────────────
    n_ins = build_insights(con)
    log.info("Insights gerados: %d", n_ins)

    create_compat_views(con)

    log.info(
        "=== Concluído: CEIS=%d | CNEP=%d | cruzamentos=%d | insights=%d ===",
        n_ceis, n_cnep, n_cruz, n_ins,
    )

    if n_cruz > 0:
        top = con.execute("""
            SELECT nome_sancionado, fonte, orgao_ac, valor_pago_ac
            FROM estado_ac_fornecedor_sancoes
            ORDER BY valor_pago_ac DESC LIMIT 10
        """).fetchall()
        log.info("=== Top fornecedores sancionados ===")
        for nome, fonte, orgao, valor in top:
            log.info("  [%s] %-40s | %-15s | R$ %,.0f",
                     fonte, (nome or "")[:40], orgao, valor)

    con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Carrega CEIS/CNEP (dados abertos) no DuckDB e cruza com Governo do Acre"
    )
    parser.add_argument(
        "--data", default=_today_str(),
        help="Data no formato YYYYMMDD (padrão: hoje)",
    )
    parser.add_argument(
        "--download", action="store_true",
        help="Baixar ZIPs do portal se não existirem localmente",
    )
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
