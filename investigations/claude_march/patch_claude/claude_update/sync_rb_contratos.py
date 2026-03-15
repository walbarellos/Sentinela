"""
sync_rb_contratos.py
--------------------
Raspa /contrato/ do portal JSF de Rio Branco.

Fluxo validado em 2026-03-12:
  - URL real: https://transparencia.riobranco.ac.gov.br/contrato/
  - Busca: POST completo (não partial/ajax) com form_id=j_idt35
  - Autocomplete de secretaria: query "SEMSA" → id=4065 (+ ~90 sub-unidades)
  - 2024+SEMSA retorna ~510 contratos em HTML
  - Fornecedor/CNPJ: NÃO aparece na listagem; aparece no detalhe /contrato/ver/{id}/
    mas de forma inconsistente — preferência pelo campo <table> de "Dados do Contrato"

Cria:
  - TABLE rb_contratos          — uma linha por contrato por unidade/ano
  - VIEW  v_rb_contratos_sus    — contratos SUS (objeto/unidade contém keywords)
  - VIEW  v_rb_contrato_ceis    — JOIN com v_sancoes_ativas por CNPJ (se disponível)
  - INSERTs em insight          — kind=RB_SUS_CONTRATO / RB_CONTRATO_SANCIONADO

Uso:
    # Teste rápido (não grava):
    .venv/bin/python scripts/sync_rb_contratos.py --anos 2025 --dry-run

    # Só SEMSA, 2024 e 2025:
    .venv/bin/python scripts/sync_rb_contratos.py --anos 2024 2025 --unit-contains semsa

    # Carga completa (todas as unidades, pode demorar):
    .venv/bin/python scripts/sync_rb_contratos.py --anos 2023 2024 2025

    # Re-scrape forçado:
    .venv/bin/python scripts/sync_rb_contratos.py --anos 2025 --force
"""
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
from typing import Optional

import duckdb
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger("sync_rb_contratos")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DEFAULT_DB  = ROOT / "data" / "sentinela_analytics.duckdb"
BASE        = "https://transparencia.riobranco.ac.gov.br"
URL_CONT    = f"{BASE}/contrato/"
DELAY       = 0.6

# ---------------------------------------------------------------------------
# Classificação SUS
# ---------------------------------------------------------------------------
SUS_KW = (
    "SAUDE", "SEMSA", "FMS", "FUNDO MUNICIPAL DE SAUDE",
    "UBS", "UPA", "CAPS", "HOSPITAL", "MATERNIDADE",
    "VIGILANCIA SANITARIA", "VIGILANCIA EPIDEMIOLOGICA",
    "PSF", "ESF", "POLICLINICA", "PRONTO ATENDIMENTO",
    "CTA", "SAE", "CENTRO DE SAUDE",
)


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()


def _sus(texto: str) -> tuple[bool, str]:
    t = _norm(texto)
    for kw in SUS_KW:
        if kw in t:
            return True, kw
    return False, ""


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS rb_contratos (
    row_id          VARCHAR PRIMARY KEY,
    ano             INTEGER,
    numero_contrato VARCHAR DEFAULT '',
    numero_processo VARCHAR DEFAULT '',
    numero_termo    VARCHAR DEFAULT '',
    objeto          VARCHAR DEFAULT '',
    unidade         VARCHAR DEFAULT '',  -- secretaria/unidade do filtro
    unidade_id      VARCHAR DEFAULT '',  -- id no autocomplete do portal
    situacao        VARCHAR DEFAULT '',
    data_inicio     VARCHAR DEFAULT '',
    data_fim        VARCHAR DEFAULT '',
    valor_contrato  DOUBLE  DEFAULT 0,
    valor_atual     DOUBLE  DEFAULT 0,
    fornecedor      VARCHAR DEFAULT '',  -- pode estar vazio (detalhe opcional)
    cnpj            VARCHAR DEFAULT '',  -- pode estar vazio
    sus             BOOLEAN DEFAULT FALSE,
    sus_keyword     VARCHAR DEFAULT '',
    url_detalhe     VARCHAR DEFAULT '',
    raw_json        VARCHAR DEFAULT '[]',
    capturado_em    TIMESTAMP
)
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Sentinela/3.0"})
    return s


def _get_vs(html: str) -> str:
    m = re.search(r'javax\.faces\.ViewState[^>]*value="([^"]+)"', html)
    return m.group(1) if m else ""


def _clean_cnpj(raw: str) -> str:
    return re.sub(r"\D", "", raw or "")


def _parse_valor(raw: str) -> float:
    try:
        return float(re.sub(r"[^\d,]", "", raw or "0").replace(",", "."))
    except Exception:
        return 0.0


def _row_id(*parts: str) -> str:
    return "CONT_" + hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()[:14]


# ---------------------------------------------------------------------------
# Descoberta de campos JSF
# ---------------------------------------------------------------------------

class JsfFields:
    """Extrai os IDs dos campos JSF necessários do HTML inicial de /contrato/."""

    def __init__(self, html: str):
        self.html = html
        self.vs       = _get_vs(html)
        self.form_id  = self._form()
        self.exerc_id = self._exerc()
        self.exerc_map= self._exerc_map()
        self.ac_id    = self._autocomplete()
        self.hidden_id= self._hidden()
        self.btn_id   = self._btn()
        self.export_id= self._export()

    def _form(self) -> str:
        m = re.search(r'<form[^>]+id="(j_idt\d+)"', self.html)
        return m.group(1) if m else "j_idt35"

    def _exerc(self) -> str:
        m = re.search(
            r'<select[^>]+id="([^"]*[Ee]xercicio[^"]*)"', self.html)
        return m.group(1) if m else ""

    def _exerc_map(self) -> dict[str, str]:
        """ano_str → option_value."""
        m = re.search(
            r'<select[^>]+id="[^"]*[Ee]xercicio[^"]*"[^>]*>(.*?)</select>',
            self.html, re.DOTALL)
        if not m:
            return {}
        out: dict[str, str] = {}
        for val, label in re.findall(
            r'<option[^>]+value="([^"]+)"[^>]*>([^<]+)</option>',
            m.group(1)
        ):
            yr = re.search(r"\d{4}", label)
            if yr:
                out[yr.group(0)] = val
        return out

    def _autocomplete(self) -> str:
        m = re.search(
            r'id="([^"]*(?:[Ss]ecretar|[Uu]nidade|acSecretar|acUnidade)[^"]*)"',
            self.html)
        return m.group(1) if m else ""

    def _hidden(self) -> str:
        if not self.ac_id:
            return ""
        m = re.search(rf'id="({re.escape(self.ac_id)}_hinput)"', self.html)
        return m.group(1) if m else (self.ac_id + "_hinput")

    def _btn(self) -> str:
        m = re.search(
            r'id="([^"]*(?:[Pp]esquisar|btnProcurar|[Bb]uscar)[^"]*)"',
            self.html)
        return m.group(1) if m else ""

    def _export(self) -> str:
        m = re.search(
            r'id="([^"]*(?:[Ee]xport|[Cc]sv|[Dd]ownload|[Ee]xcel)[^"]*)"',
            self.html)
        return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Autocomplete de unidades
# ---------------------------------------------------------------------------

def _list_units(
    session: requests.Session,
    fields: JsfFields,
    query: str = " ",
) -> list[tuple[str, str]]:
    """
    Retorna lista de (unit_id, unit_label) do autocomplete de secretaria.
    Usa query=" " para trazer todas (padrão do portal de despesas que já funciona).
    """
    if not fields.ac_id:
        return []

    payload = {
        fields.form_id: fields.form_id,
        "javax.faces.ViewState": fields.vs,
        f"{fields.ac_id}_query": query,
        fields.ac_id: query,
    }
    # Padrão PrimeFaces autocomplete
    payload["javax.faces.partial.ajax"]   = "true"
    payload["javax.faces.source"]         = fields.ac_id
    payload["javax.faces.partial.execute"] = fields.ac_id
    payload["javax.faces.partial.render"] = fields.ac_id

    headers = {
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": URL_CONT,
    }

    r = session.post(URL_CONT, data=payload, headers=headers, timeout=20)
    if r.status_code != 200:
        return []

    items = re.findall(
        r'data-item-value="(\d+)"[^>]*data-item-label="([^"]+)"',
        r.text
    )
    return items


# ---------------------------------------------------------------------------
# Busca por ano + unidade (POST completo — validado)
# ---------------------------------------------------------------------------

def _search_contracts(
    session: requests.Session,
    ano: int,
    unit_id: str,
    unit_label: str,
    fields: JsfFields,
) -> list[dict]:
    """
    POST completo (não partial/ajax) para buscar contratos de um ano + unidade.
    Padrão confirmado: igual ao que funciona em /licitacao/ com Pesquisar.
    """
    ano_val = fields.exerc_map.get(str(ano), str(ano))

    payload: dict[str, str] = {
        fields.form_id: fields.form_id,
        "javax.faces.ViewState": fields.vs,
    }
    if fields.exerc_id:
        payload[fields.exerc_id] = ano_val
    if fields.hidden_id and unit_id:
        payload[fields.hidden_id] = unit_id
    if fields.ac_id and unit_label:
        payload[fields.ac_id] = unit_label
    if fields.btn_id:
        payload[fields.btn_id] = fields.btn_id

    r = session.post(URL_CONT, data=payload, timeout=30)
    r.raise_for_status()

    rows   = _parse_table(r.text, ano, unit_label, unit_id)
    links  = _parse_links(r.text)

    # Enlaça URL de detalhe na linha
    for i, row in enumerate(rows):
        row["url_detalhe"] = links.get(i, "")

    return rows


def _parse_table(html: str, ano: int, unit_label: str, unit_id: str) -> list[dict]:
    """
    Layout típico da tabela /contrato/ (confirmado nas inspeções):
    [0] Nº Termo  [1] Nº Contrato  [2] Processo  [3] Exercício
    [4] Objeto    [5] Secretaria   [6] Situação   [7] Dt Início
    [8] Dt Fim    [9] Valor        [10] Valor Atual  [11] Saldo
    Pode variar: aceita >= 4 colunas.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for tr in soup.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        t = [td.get_text(" ", strip=True) for td in tds]
        if not any(t):
            continue

        rows.append({
            "ano":             ano,
            "numero_termo":    t[0]  if len(t) > 0  else "",
            "numero_contrato": t[1]  if len(t) > 1  else "",
            "numero_processo": t[2]  if len(t) > 2  else "",
            # t[3] = exercício (já sabemos)
            "objeto":          t[4]  if len(t) > 4  else (t[3] if len(t) > 3 else ""),
            "unidade":         unit_label,
            "unidade_id":      unit_id,
            "situacao":        t[6]  if len(t) > 6  else "",
            "data_inicio":     t[7]  if len(t) > 7  else "",
            "data_fim":        t[8]  if len(t) > 8  else "",
            "valor_contrato":  _parse_valor(t[9])   if len(t) > 9  else 0.0,
            "valor_atual":     _parse_valor(t[10])  if len(t) > 10 else 0.0,
            "fornecedor":      "",
            "cnpj":            "",
            "_raw":            t,
        })
    return rows


def _parse_links(html: str) -> dict[int, str]:
    soup = BeautifulSoup(html, "html.parser")
    out: dict[int, str] = {}
    for i, tr in enumerate(soup.find_all("tr")[1:]):
        a = tr.find("a", href=True)
        if a:
            href = a["href"]
            out[i] = (BASE + href) if href.startswith("/") else href
    return out


# ---------------------------------------------------------------------------
# Enriquecimento de detalhe (fornecedor + CNPJ)
# ATENÇÃO: O detalhe /contrato/ver/ não expõe fornecedor/CNPJ de forma
# consistente na versão atual do portal. Este método tenta, mas pode retornar
# vazio — isso é esperado e não quebra o fluxo.
# ---------------------------------------------------------------------------

_CNPJ_RE = re.compile(r'\d{2}[\.\-]?\d{3}[\.\-]?\d{3}[\/\-]?\d{4}[\.\-]?\d{2}')

def _enrich_detail(session: requests.Session, url: str, delay: float) -> dict:
    if not url:
        return {}
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    data: dict = {}

    # Tenta campo "Contratado" ou "Empresa" ou "Fornecedor"
    for kw in ("Contratado", "Empresa", "Fornecedor", "Credor"):
        m = re.search(rf"(?i){kw}\s*[:\-–]\s*(.{{3,80}})", text)
        if m:
            data["fornecedor"] = m.group(1).strip()
            break

    # CNPJ por regex
    cnpj_m = _CNPJ_RE.search(text)
    if cnpj_m:
        data["cnpj"] = _clean_cnpj(cnpj_m.group(0))

    time.sleep(delay)
    return data


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = datetime.now()
    recs = []
    for d in rows:
        texto = f"{d.get('objeto','')} {d.get('unidade','')}"
        is_sus, kw = _sus(texto)
        rid = _row_id(
            str(d["ano"]),
            d.get("numero_contrato", "") or d.get("numero_termo", ""),
            d.get("objeto", "")[:40],
            d.get("unidade_id", ""),
        )
        recs.append((
            rid,
            int(d["ano"]),
            d.get("numero_contrato", ""),
            d.get("numero_processo", ""),
            d.get("numero_termo", ""),
            d.get("objeto", ""),
            d.get("unidade", ""),
            d.get("unidade_id", ""),
            d.get("situacao", ""),
            d.get("data_inicio", ""),
            d.get("data_fim", ""),
            float(d.get("valor_contrato") or 0),
            float(d.get("valor_atual") or 0),
            d.get("fornecedor", ""),
            d.get("cnpj", ""),
            is_sus, kw,
            d.get("url_detalhe", ""),
            json.dumps(d.get("_raw", [])),
            now,
        ))
    con.executemany(
        """INSERT OR IGNORE INTO rb_contratos
           (row_id, ano, numero_contrato, numero_processo, numero_termo,
            objeto, unidade, unidade_id, situacao, data_inicio, data_fim,
            valor_contrato, valor_atual, fornecedor, cnpj,
            sus, sus_keyword, url_detalhe, raw_json, capturado_em)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        recs,
    )
    return len(recs)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def _build_views(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contratos_sus AS
        SELECT * FROM rb_contratos
        WHERE sus = TRUE
        ORDER BY valor_contrato DESC
    """)

    # JOIN com CEIS/CNEP (só se v_sancoes_ativas existir e cnpj preenchido)
    views  = {r[0] for r in con.execute(
        "SELECT view_name FROM information_schema.views").fetchall()}
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    sancoes_src = (
        "v_sancoes_ativas" if "v_sancoes_ativas" in views
        else ("sancoes_collapsed" if "sancoes_collapsed" in tables else None)
    )
    if sancoes_src:
        con.execute(f"""
            CREATE OR REPLACE VIEW v_rb_contrato_ceis AS
            SELECT
                c.row_id,
                c.ano,
                c.numero_contrato,
                c.objeto,
                c.unidade,
                c.fornecedor,
                c.cnpj,
                c.valor_contrato,
                c.sus,
                s.fonte        AS sancao_fonte,
                s.tipo_sancao  AS sancao_tipo,
                s.data_inicio  AS sancao_inicio,
                s.data_fim     AS sancao_fim,
                s.orgao_sancao,
                s.ativa
            FROM rb_contratos c
            JOIN {sancoes_src} s
              ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
               = REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g')
            WHERE c.cnpj <> ''
        """)
        n_ativas = con.execute(
            "SELECT COUNT(*) FROM v_rb_contrato_ceis WHERE ativa"
        ).fetchone()[0]
        log.info("v_rb_contrato_ceis: %d contratos com sanção ativa", n_ativas)

    n = con.execute("SELECT COUNT(*) FROM v_rb_contratos_sus").fetchone()[0]
    log.info("v_rb_contratos_sus: %d", n)


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------

def _build_insights(con: duckdb.DuckDBPyConnection) -> int:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "insight" not in tables:
        log.warning("Tabela insight não existe; pulando geração.")
        return 0

    con.execute("""
        DELETE FROM insight
        WHERE kind IN ('RB_SUS_CONTRATO','RB_CONTRATO_SANCIONADO')
    """)

    agora = datetime.now()
    recs: list = []

    # Contratos SUS por unidade/ano — sempre disponível (sem CNPJ)
    for (ano, unidade, n, total) in con.execute("""
        SELECT ano, unidade, COUNT(*) AS n, SUM(valor_contrato) AS total
        FROM rb_contratos WHERE sus = TRUE
        GROUP BY ano, unidade ORDER BY total DESC NULLS LAST LIMIT 300
    """).fetchall():
        iid = "INS_" + hashlib.md5(
            f"RB_SUS_CONTRATO{ano}{unidade}".encode()
        ).hexdigest()[:12]
        recs.append((
            iid, "RB_SUS_CONTRATO", "HIGH", 78,
            float(total or 0),
            f"SUS Rio Branco — {n} contratos em {unidade or 'N/I'} ({ano})",
            (f"A unidade **{unidade or 'N/I'}** (SUS/SEMSA) celebrou "
             f"**{n} contrato(s)** em {ano}, valor total "
             f"**R$ {float(total or 0):,.2f}**."),
            "contratos_sus_por_unidade_ano",
            json.dumps(["transparencia.riobranco.ac.gov.br/contrato"]),
            json.dumps(["SUS", "SEMSA", "contrato", "RIO_BRANCO"]),
            int(n), float(total or 0), agora,
            "municipal", "Prefeitura de Rio Branco", "SEMSA",
            "Rio Branco", "AC", "saude", True,
            float(total or 0), int(ano),
            "transparencia.riobranco.ac.gov.br",
        ))

    # Contratos SUS × CEIS — só se houver CNPJ enriquecido
    views = {r[0] for r in con.execute(
        "SELECT view_name FROM information_schema.views").fetchall()}
    if "v_rb_contrato_ceis" in views:
        for (cnpj, fornecedor, unidade, n_c, total, n_s) in con.execute("""
            SELECT cnpj, fornecedor, unidade,
                   COUNT(DISTINCT numero_contrato),
                   SUM(valor_contrato),
                   COUNT(DISTINCT sancao_fonte)
            FROM v_rb_contrato_ceis
            WHERE ativa = TRUE AND sus = TRUE
            GROUP BY cnpj, fornecedor, unidade
            ORDER BY SUM(valor_contrato) DESC LIMIT 200
        """).fetchall():
            iid = "INS_" + hashlib.md5(
                f"RB_CONTRATO_SANCIONADO{cnpj}{unidade}".encode()
            ).hexdigest()[:12]
            recs.append((
                iid, "RB_CONTRATO_SANCIONADO", "CRITICAL", 95,
                float(total or 0),
                f"⚠️ RB SUS — {fornecedor or cnpj} SANCIONADO × {unidade or 'N/I'}",
                (f"**{fornecedor or cnpj}** (`{cnpj}`) possui **{n_s} "
                 f"sanção(ões) ativa(s)** (CEIS/CNEP) e foi contratado "
                 f"**{n_c} vez(es)** pelo SUS de Rio Branco "
                 f"(**{unidade or 'N/I'}**), total "
                 f"**R$ {float(total or 0):,.2f}**."),
                "contrato_sus_fornecedor_sancionado_municipal",
                json.dumps([
                    "transparencia.riobranco.ac.gov.br/contrato",
                    "portaldatransparencia.gov.br/download-de-dados/ceis",
                ]),
                json.dumps(["CEIS", "CNEP", "SUS", "SEMSA", "RIO_BRANCO",
                            "sancionado", "CRITICAL"]),
                int(n_c), float(total or 0), agora,
                "municipal", "Prefeitura de Rio Branco", "SEMSA",
                "Rio Branco", "AC", "saude", True,
                float(total or 0), None,
                "transparencia.riobranco.ac.gov.br + CGU",
            ))

    if recs:
        con.executemany(
            """INSERT OR IGNORE INTO insight
               (id, kind, severity, confidence, exposure_brl,
                title, description_md, pattern, sources, tags,
                sample_n, unit_total, created_at,
                esfera, ente, orgao, municipio, uf,
                area_tematica, sus, valor_referencia, ano_referencia, fonte)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            recs,
        )
    return len(recs)


# ---------------------------------------------------------------------------
# Coleta por ano
# ---------------------------------------------------------------------------

def collect_year(
    session: requests.Session,
    fields: JsfFields,
    ano: int,
    unit_filter: str,
    enrich_cnpj: bool,
    delay: float,
) -> list[dict]:
    """
    Coleta todos os contratos de um ano, iterando pelas unidades do autocomplete.
    unit_filter: substring para filtrar unidades (ex: "semsa"); "" = todas.
    """
    # Descobre unidades — usa query do filtro para restringir via autocomplete
    query = unit_filter or " "
    units = _list_units(session, fields, query=query)

    if not units:
        log.warning("Autocomplete não retornou unidades para query=%r. "
                    "Tentando busca sem filtro de unidade.", query)
        units = [("", "")]

    if unit_filter:
        needle = _norm(unit_filter)
        units = [(uid, ulabel) for uid, ulabel in units
                 if needle in _norm(ulabel)]
        log.info("Unidades filtradas por %r: %d", unit_filter, len(units))
    else:
        log.info("Total de unidades encontradas: %d", len(units))

    all_rows: list[dict] = []
    for i, (uid, ulabel) in enumerate(units, 1):
        log.info("  [%d/%d] %s (%s) — ano %d",
                 i, len(units), ulabel, uid, ano)
        try:
            rows = _search_contracts(session, ano, uid, ulabel, fields)
        except Exception as e:
            log.warning("    Falhou: %s", e)
            rows = []

        # Enriquece CNPJ somente para linhas SUS que têm URL de detalhe
        if enrich_cnpj:
            sus_missing = [
                r for r in rows
                if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]
                and not r.get("cnpj")
                and r.get("url_detalhe")
            ]
            for row in sus_missing:
                detail = _enrich_detail(session, row["url_detalhe"], delay)
                row.update({k: v for k, v in detail.items() if v})

        all_rows.extend(rows)
        log.info("    → %d contratos; acumulado: %d", len(rows), len(all_rows))
        time.sleep(delay)

    return all_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Raspa /contrato/ de Rio Branco (JSF, POST completo validado)"
    )
    parser.add_argument("--anos",         nargs="+", type=int, default=[2024, 2025])
    parser.add_argument("--unit-contains", default="semsa",
                        help="Substring para filtrar unidades (padrão: semsa). "
                             "Use '' para todas as unidades.")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Não grava no banco; exibe amostra no log")
    parser.add_argument("--force",        action="store_true",
                        help="Apaga dados dos anos informados antes de reinserir")
    parser.add_argument("--no-enrich",    action="store_true",
                        help="Não faz scrape de detalhe para CNPJ")
    parser.add_argument("--delay",        type=float, default=DELAY)
    parser.add_argument("--db-path",      default=str(DEFAULT_DB))
    args = parser.parse_args()

    s = _session()

    # Carrega campos JSF (uma vez — ViewState por sessão)
    r0 = s.get(URL_CONT, timeout=20); r0.raise_for_status()
    fields = JsfFields(r0.text)
    log.info("JSF: form=%s exerc=%s ac=%s hidden=%s btn=%s",
             fields.form_id, fields.exerc_id,
             fields.ac_id, fields.hidden_id, fields.btn_id)
    log.info("Exercícios disponíveis: %s", fields.exerc_map)

    con = duckdb.connect(args.db_path, read_only=args.dry_run)
    if not args.dry_run:
        con.execute(DDL)

    if args.force and not args.dry_run:
        for ano in args.anos:
            con.execute("DELETE FROM rb_contratos WHERE ano = ?", [ano])
        log.info("Dados dos anos %s apagados (--force).", args.anos)

    total = 0
    for ano in args.anos:
        log.info("══ Ano %d ══════════════════════════════════", ano)
        rows = collect_year(
            session     = s,
            fields      = fields,
            ano         = ano,
            unit_filter = args.unit_contains,
            enrich_cnpj = not args.no_enrich,
            delay       = args.delay,
        )

        if args.dry_run:
            sus = [r for r in rows
                   if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]]
            log.info("[dry-run] %d contratos | %d SUS (não gravados)",
                     len(rows), len(sus))
            for r in sus[:8]:
                log.info("  [SUS] %s | %s | R$ %.0f | cnpj=%s",
                         r.get("unidade","")[:30],
                         r.get("objeto","")[:60],
                         r.get("valor_contrato", 0),
                         r.get("cnpj","") or "(vazio)")
            continue

        n = _upsert(con, rows)
        log.info("Gravado %d: %d linhas", ano, n)
        total += n

    if not args.dry_run:
        _build_views(con)
        n_ins = _build_insights(con)

        n_total = con.execute("SELECT COUNT(*) FROM rb_contratos").fetchone()[0]
        n_sus   = con.execute("SELECT COUNT(*) FROM v_rb_contratos_sus").fetchone()[0]
        v_sus   = con.execute(
            "SELECT COALESCE(SUM(valor_contrato),0) FROM v_rb_contratos_sus"
        ).fetchone()[0]
        log.info("══ RESUMO ══════════════════════════════════")
        log.info("  rb_contratos total : %d", n_total)
        log.info("  contratos SUS      : %d | R$ %,.0f", n_sus, float(v_sus))
        log.info("  insights gerados   : %d", n_ins)

        # Verifica se CEIS cruzou
        views = {r[0] for r in con.execute(
            "SELECT view_name FROM information_schema.views").fetchall()}
        if "v_rb_contrato_ceis" in views:
            n_ceis = con.execute(
                "SELECT COUNT(DISTINCT cnpj) FROM v_rb_contrato_ceis WHERE ativa"
            ).fetchone()[0]
            log.info("  CEIS × contratos   : %d fornecedores sancionados", n_ceis)

    con.close()


if __name__ == "__main__":
    main()
