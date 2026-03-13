"""
sync_rb_licitacao_contrato.py
------------------------------
Raspa /licitacao/ e /contrato/ do portal JSF de Rio Branco.
URLs confirmadas em 2026-03-12:
  https://transparencia.riobranco.ac.gov.br/licitacao/
  https://transparencia.riobranco.ac.gov.br/contrato/

Cria:
  - TABLE rb_licitacoes        — uma linha por licitação
  - TABLE rb_contratos         — uma linha por contrato
  - VIEW  v_rb_licitacoes_sus  — filtro saúde/SEMSA
  - VIEW  v_rb_contratos_sus   — filtro saúde/SEMSA
  - VIEW  v_rb_contrato_ceis   — join rb_contratos × v_sancoes_ativas (CEIS/CNEP)
  - INSERTs em insight         — kind=RB_SUS_LICITACAO / RB_SUS_CONTRATO / RB_CONTRATO_SANCIONADO

Uso:
    # Teste rápido (só 2025, sem gravar):
    .venv/bin/python scripts/sync_rb_licitacao_contrato.py --anos 2025 --dry-run

    # Carga completa:
    .venv/bin/python scripts/sync_rb_licitacao_contrato.py --anos 2023 2024 2025

    # Força re-scrape ignorando o que já está no banco:
    .venv/bin/python scripts/sync_rb_licitacao_contrato.py --anos 2025 --force
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

log = logging.getLogger("sync_rb_licitacao_contrato")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DEFAULT_DB   = ROOT / "data" / "sentinela_analytics.duckdb"
BASE         = "https://transparencia.riobranco.ac.gov.br"
URL_LIC      = f"{BASE}/licitacao/"
URL_CONT     = f"{BASE}/contrato/"
DELAY        = 0.5   # segundos entre requests
PAGE_SIZE    = 500   # linhas por página (se o portal suportar)

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
DDL_LICITACOES = """
CREATE TABLE IF NOT EXISTS rb_licitacoes (
    row_id           VARCHAR PRIMARY KEY,
    ano              INTEGER,
    numero_processo  VARCHAR,
    modalidade       VARCHAR,
    objeto           VARCHAR,
    unidade          VARCHAR,
    situacao         VARCHAR,
    data_abertura    VARCHAR,
    valor_estimado   DOUBLE  DEFAULT 0,
    valor_homologado DOUBLE  DEFAULT 0,
    fornecedor_venc  VARCHAR DEFAULT '',
    cnpj_venc        VARCHAR DEFAULT '',
    sus              BOOLEAN DEFAULT FALSE,
    sus_keyword      VARCHAR DEFAULT '',
    url_detalhe      VARCHAR DEFAULT '',
    raw_json         VARCHAR DEFAULT '[]',
    capturado_em     TIMESTAMP
)
"""

DDL_CONTRATOS = """
CREATE TABLE IF NOT EXISTS rb_contratos (
    row_id          VARCHAR PRIMARY KEY,
    ano             INTEGER,
    numero_contrato VARCHAR,
    objeto          VARCHAR,
    unidade         VARCHAR,
    fornecedor      VARCHAR DEFAULT '',
    cnpj            VARCHAR DEFAULT '',
    data_inicio     VARCHAR DEFAULT '',
    data_fim        VARCHAR DEFAULT '',
    valor_contrato  DOUBLE  DEFAULT 0,
    situacao        VARCHAR DEFAULT '',
    sus             BOOLEAN DEFAULT FALSE,
    sus_keyword     VARCHAR DEFAULT '',
    url_detalhe     VARCHAR DEFAULT '',
    raw_json        VARCHAR DEFAULT '[]',
    capturado_em    TIMESTAMP
)
"""


# ---------------------------------------------------------------------------
# Helpers JSF
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Sentinela/3.0",
        "Accept": "text/html,application/xhtml+xml,*/*",
    })
    return s


def _get_vs(html: str) -> str:
    m = re.search(r'javax\.faces\.ViewState[^>]*value="([^"]+)"', html)
    return m.group(1) if m else ""


def _parse_partial(xml: str) -> dict[str, str]:
    """Extrai blocos CDATA de uma resposta partial/ajax JSF."""
    out: dict[str, str] = {}
    for m in re.finditer(
        r'<update id="([^"]+)"[^>]*><!\[CDATA\[(.*?)\]\]></update>',
        xml, re.DOTALL
    ):
        out[m.group(1)] = m.group(2)
    return out


def _clean_cnpj(raw: str) -> str:
    return re.sub(r"\D", "", raw or "")


def _parse_valor(raw: str) -> float:
    try:
        return float(re.sub(r"[^\d,]", "", raw or "0").replace(",", "."))
    except Exception:
        return 0.0


def _row_id(prefix: str, *parts: str) -> str:
    return prefix + "_" + hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()[:14]


# ---------------------------------------------------------------------------
# Descoberta de campos no formulário
# ---------------------------------------------------------------------------

def _find_form_id(html: str) -> str:
    # /licitacao/ usa j_idt35 confirmado; /contrato/ deve ser igual
    m = re.search(r'<form[^>]+id="(j_idt\d+)"', html)
    return m.group(1) if m else "j_idt35"


def _find_exercicio_select(html: str) -> tuple[str, dict[str, str]]:
    """
    Retorna (component_id, {ano_str: option_value}).
    Funciona para qualquer nome de select que contenha 'xercicio' ou 'ano'.
    """
    m = re.search(
        r'<select[^>]+id="([^"]*(?:[Ee]xercicio|[Aa]no)[^"]*)"[^>]*>(.*?)</select>',
        html, re.DOTALL
    )
    if not m:
        return "", {}
    sel_id = m.group(1)
    options: dict[str, str] = {}
    for opt in re.finditer(
        r'<option[^>]+value="([^"]*)"[^>]*>([^<]+)</option>',
        m.group(2)
    ):
        val, label = opt.group(1), opt.group(2).strip()
        year_m = re.search(r"\d{4}", label)
        if year_m:
            options[year_m.group(0)] = val
    return sel_id, options


def _find_btn_pesquisar(html: str) -> str:
    m = re.search(
        r'id="([^"]*(?:[Pp]esquisar|btnProcurar|[Bb]uscar)[^"]*)"',
        html
    )
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Paginação: detecta se existe "próxima página"
# ---------------------------------------------------------------------------

def _has_next_page(html: str) -> bool:
    """True se o HTML da tabela contém link/botão de próxima página."""
    soup = BeautifulSoup(html, "html.parser")
    # Padrão Mojarra: âncora com ícone de seta ou texto "Próxima"
    for a in soup.find_all("a"):
        txt = a.get_text(strip=True).lower()
        if any(k in txt for k in ("próxima", "proxima", "next", "›", "»")):
            return True
    return False


def _click_next(
    session: requests.Session,
    url: str,
    form_id: str,
    vs: str,
    nav_btn_id: str,
) -> tuple[str, str]:
    """Clica no botão de próxima página, retorna (html_update, novo_vs)."""
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": nav_btn_id,
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "Formulario",
        form_id: form_id,
        nav_btn_id: nav_btn_id,
        "javax.faces.ViewState": vs,
    }
    headers = {
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": url,
    }
    r = session.post(url, data=payload, headers=headers, timeout=30)
    r.raise_for_status()
    updates = _parse_partial(r.text)
    html_body = updates.get("Formulario") or "".join(updates.values())
    new_vs = _get_vs(r.text) or vs
    return html_body, new_vs


# ---------------------------------------------------------------------------
# Parser de tabela genérico
# ---------------------------------------------------------------------------

ColMap = list[tuple[str, int]]  # [(campo_destino, índice_td)]


def _parse_table_rows(html: str) -> list[list[str]]:
    """Retorna lista de linhas (cada linha = lista de textos por coluna)."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[list[str]] = []
    for tr in soup.find_all("tr")[1:]:  # pula header
        tds = tr.find_all("td")
        if not tds:
            continue
        texts = [td.get_text(" ", strip=True) for td in tds]
        if any(t for t in texts):
            out.append(texts)
    return out


def _find_detail_links(html: str, base: str) -> dict[int, str]:
    """Mapeia índice de tr → URL de detalhe (href relativo → absoluto)."""
    soup = BeautifulSoup(html, "html.parser")
    links: dict[int, str] = {}
    for i, tr in enumerate(soup.find_all("tr")[1:]):
        a = tr.find("a", href=True)
        if a:
            href = a["href"]
            links[i] = (base + href) if href.startswith("/") else href
    return links


# ---------------------------------------------------------------------------
# Busca por ano (licitações)
# ---------------------------------------------------------------------------

def _search_licitacoes_year(
    session: requests.Session,
    ano: int,
) -> list[dict]:
    r = session.get(URL_LIC, timeout=30)
    r.raise_for_status()
    html_ini = r.text
    vs       = _get_vs(html_ini)
    form_id  = _find_form_id(html_ini)
    exc_id, exc_map = _find_exercicio_select(html_ini)
    btn_id   = _find_btn_pesquisar(html_ini)

    ano_val  = exc_map.get(str(ano), str(ano))

    # Payload de busca
    payload: dict[str, str] = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source":        btn_id or form_id,
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "Formulario",
        form_id: form_id,
        "javax.faces.ViewState": vs,
    }
    if exc_id:
        payload[exc_id] = ano_val
    if btn_id:
        payload[btn_id] = btn_id

    headers = {
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": URL_LIC,
    }

    resp = session.post(URL_LIC, data=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    updates  = _parse_partial(resp.text)
    html_tab = updates.get("Formulario") or "".join(updates.values())
    vs       = _get_vs(resp.text) or vs

    all_rows: list[list[str]] = []
    all_links: dict[int, str] = {}
    page = 0

    while True:
        rows  = _parse_table_rows(html_tab)
        links = _find_detail_links(html_tab, BASE)
        offset = len(all_rows)
        all_rows.extend(rows)
        all_links.update({offset + k: v for k, v in links.items()})
        page += 1
        log.info("  Licitações %d pág %d: %d linhas acumuladas", ano, page, len(all_rows))

        if not _has_next_page(html_tab):
            break

        # Procura botão de próxima página
        soup_nav = BeautifulSoup(html_tab, "html.parser")
        nav_btn = None
        for a in soup_nav.find_all("a"):
            txt = a.get_text(strip=True).lower()
            if any(k in txt for k in ("próxima", "proxima", "next", "›", "»")):
                nav_btn = a.get("id") or a.get("onclick", "")
                nav_btn_id_m = re.search(r"'([^']+)'", nav_btn)
                nav_btn = nav_btn_id_m.group(1) if nav_btn_id_m else nav_btn
                break

        if not nav_btn:
            break

        html_tab, vs = _click_next(session, URL_LIC, form_id, vs, nav_btn)
        time.sleep(DELAY)

    return _build_licitacao_dicts(all_rows, all_links, ano)


# ---------------------------------------------------------------------------
# Busca por ano (contratos)
# ---------------------------------------------------------------------------

def _search_contratos_year(
    session: requests.Session,
    ano: int,
) -> list[dict]:
    r = session.get(URL_CONT, timeout=30)
    r.raise_for_status()
    html_ini = r.text
    vs       = _get_vs(html_ini)
    form_id  = _find_form_id(html_ini)
    exc_id, exc_map = _find_exercicio_select(html_ini)
    btn_id   = _find_btn_pesquisar(html_ini)
    ano_val  = exc_map.get(str(ano), str(ano))

    payload: dict[str, str] = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source":        btn_id or form_id,
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "Formulario",
        form_id: form_id,
        "javax.faces.ViewState": vs,
    }
    if exc_id:
        payload[exc_id] = ano_val
    if btn_id:
        payload[btn_id] = btn_id

    headers = {
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": URL_CONT,
    }

    resp = session.post(URL_CONT, data=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    updates  = _parse_partial(resp.text)
    html_tab = updates.get("Formulario") or "".join(updates.values())
    vs       = _get_vs(resp.text) or vs

    all_rows: list[list[str]] = []
    all_links: dict[int, str] = {}
    page = 0

    while True:
        rows  = _parse_table_rows(html_tab)
        links = _find_detail_links(html_tab, BASE)
        offset = len(all_rows)
        all_rows.extend(rows)
        all_links.update({offset + k: v for k, v in links.items()})
        page += 1
        log.info("  Contratos %d pág %d: %d linhas acumuladas", ano, page, len(all_rows))

        if not _has_next_page(html_tab):
            break

        soup_nav = BeautifulSoup(html_tab, "html.parser")
        nav_btn = None
        for a in soup_nav.find_all("a"):
            txt = a.get_text(strip=True).lower()
            if any(k in txt for k in ("próxima", "proxima", "next", "›", "»")):
                nav_btn = a.get("id") or a.get("onclick", "")
                nav_btn_id_m = re.search(r"'([^']+)'", nav_btn)
                nav_btn = nav_btn_id_m.group(1) if nav_btn_id_m else nav_btn
                break

        if not nav_btn:
            break

        html_tab, vs = _click_next(session, URL_CONT, form_id, vs, nav_btn)
        time.sleep(DELAY)

    return _build_contrato_dicts(all_rows, all_links, ano)


# ---------------------------------------------------------------------------
# Scrape de detalhe: enriquece CNPJ e fornecedor quando ausentes na listagem
# ---------------------------------------------------------------------------

def _enrich_detail(
    session: requests.Session,
    url: str,
) -> dict[str, str]:
    if not url:
        return {}
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception:
        return {}
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    data: dict[str, str] = {}
    for key in ("Fornecedor", "CNPJ", "Objeto", "Modalidade",
                "Situação", "Unidade", "Valor Contrato",
                "Valor Homologado", "Valor Estimado"):
        m = re.search(rf"(?i){re.escape(key)}\s*[:\-–]\s*(.+)", text)
        if m:
            data[key.lower().replace(" ", "_").replace("ã", "a").replace("ç", "c")] = m.group(1).strip()
    return data


# ---------------------------------------------------------------------------
# Montagem dos dicts de licitação / contrato
# ---------------------------------------------------------------------------

def _build_licitacao_dicts(
    rows: list[list[str]],
    links: dict[int, str],
    ano: int,
) -> list[dict]:
    """
    Layout esperado da tabela /licitacao/:
    [0] Número processo  [1] Modalidade  [2] Objeto  [3] Unidade
    [4] Situação         [5] Data        [6] Val.Estimado  [7] Val.Homologado
    [8] Fornecedor venc  [9] CNPJ venc
    Colunas opcionais: podem faltar ou ter ordem diferente.
    O parser tenta ser robusto aceitando >= 3 colunas.
    """
    out = []
    for i, t in enumerate(rows):
        if len(t) < 2:
            continue
        d = {
            "ano":             ano,
            "numero_processo": t[0] if len(t) > 0 else "",
            "modalidade":      t[1] if len(t) > 1 else "",
            "objeto":          t[2] if len(t) > 2 else "",
            "unidade":         t[3] if len(t) > 3 else "",
            "situacao":        t[4] if len(t) > 4 else "",
            "data_abertura":   t[5] if len(t) > 5 else "",
            "valor_estimado":  _parse_valor(t[6]) if len(t) > 6 else 0.0,
            "valor_homologado":_parse_valor(t[7]) if len(t) > 7 else 0.0,
            "fornecedor_venc": t[8] if len(t) > 8 else "",
            "cnpj_venc":       _clean_cnpj(t[9]) if len(t) > 9 else "",
            "url_detalhe":     links.get(i, ""),
            "_raw":            t,
        }
        out.append(d)
    return out


def _build_contrato_dicts(
    rows: list[list[str]],
    links: dict[int, str],
    ano: int,
) -> list[dict]:
    """
    Layout esperado da tabela /contrato/:
    [0] Número  [1] Objeto  [2] Unidade  [3] Fornecedor  [4] CNPJ
    [5] Início  [6] Fim     [7] Valor    [8] Situação
    """
    out = []
    for i, t in enumerate(rows):
        if len(t) < 2:
            continue
        d = {
            "ano":             ano,
            "numero_contrato": t[0] if len(t) > 0 else "",
            "objeto":          t[1] if len(t) > 1 else "",
            "unidade":         t[2] if len(t) > 2 else "",
            "fornecedor":      t[3] if len(t) > 3 else "",
            "cnpj":            _clean_cnpj(t[4]) if len(t) > 4 else "",
            "data_inicio":     t[5] if len(t) > 5 else "",
            "data_fim":        t[6] if len(t) > 6 else "",
            "valor_contrato":  _parse_valor(t[7]) if len(t) > 7 else 0.0,
            "situacao":        t[8] if len(t) > 8 else "",
            "url_detalhe":     links.get(i, ""),
            "_raw":            t,
        }
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Enriquecimento de CNPJ via detalhe (batch, só para linhas SUS sem CNPJ)
# ---------------------------------------------------------------------------

def _enrich_missing_cnpjs(
    session: requests.Session,
    rows: list[dict],
    cnpj_field: str,
    delay: float,
) -> list[dict]:
    missing = [r for r in rows if not r.get(cnpj_field) and r.get("url_detalhe")]
    if not missing:
        return rows
    log.info("Enriquecendo CNPJ de %d linhas SUS sem CNPJ na listagem...", len(missing))
    for r in missing:
        detail = _enrich_detail(session, r["url_detalhe"])
        if detail.get("cnpj"):
            r[cnpj_field] = _clean_cnpj(detail["cnpj"])
        if detail.get("fornecedor") and not r.get("fornecedor_venc"):
            r["fornecedor_venc"] = detail.get("fornecedor", "")
        time.sleep(delay)
    return rows


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

def _upsert_licitacoes(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict],
) -> int:
    if not rows:
        return 0
    now = datetime.now()
    recs = []
    for d in rows:
        texto = f"{d.get('objeto','')} {d.get('unidade','')} {d.get('fornecedor_venc','')}"
        is_sus, kw = _sus(texto)
        rid = _row_id("LIC", str(d["ano"]),
                      d.get("numero_processo", ""), d.get("objeto", "")[:40])
        recs.append((
            rid,
            int(d["ano"]),
            d.get("numero_processo", ""),
            d.get("modalidade", ""),
            d.get("objeto", ""),
            d.get("unidade", ""),
            d.get("situacao", ""),
            d.get("data_abertura", ""),
            float(d.get("valor_estimado") or 0),
            float(d.get("valor_homologado") or 0),
            d.get("fornecedor_venc", ""),
            d.get("cnpj_venc", ""),
            is_sus, kw,
            d.get("url_detalhe", ""),
            json.dumps(d.get("_raw", [])),
            now,
        ))
    con.executemany(
        """INSERT OR IGNORE INTO rb_licitacoes
           (row_id, ano, numero_processo, modalidade, objeto, unidade,
            situacao, data_abertura, valor_estimado, valor_homologado,
            fornecedor_venc, cnpj_venc, sus, sus_keyword,
            url_detalhe, raw_json, capturado_em)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        recs,
    )
    return len(recs)


def _upsert_contratos(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict],
) -> int:
    if not rows:
        return 0
    now = datetime.now()
    recs = []
    for d in rows:
        texto = f"{d.get('objeto','')} {d.get('unidade','')} {d.get('fornecedor','')}"
        is_sus, kw = _sus(texto)
        rid = _row_id("CONT", str(d["ano"]),
                      d.get("numero_contrato", ""), d.get("objeto", "")[:40])
        recs.append((
            rid,
            int(d["ano"]),
            d.get("numero_contrato", ""),
            d.get("objeto", ""),
            d.get("unidade", ""),
            d.get("fornecedor", ""),
            d.get("cnpj", ""),
            d.get("data_inicio", ""),
            d.get("data_fim", ""),
            float(d.get("valor_contrato") or 0),
            d.get("situacao", ""),
            is_sus, kw,
            d.get("url_detalhe", ""),
            json.dumps(d.get("_raw", [])),
            now,
        ))
    con.executemany(
        """INSERT OR IGNORE INTO rb_contratos
           (row_id, ano, numero_contrato, objeto, unidade, fornecedor, cnpj,
            data_inicio, data_fim, valor_contrato, situacao,
            sus, sus_keyword, url_detalhe, raw_json, capturado_em)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        recs,
    )
    return len(recs)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def _build_views(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_licitacoes_sus AS
        SELECT * FROM rb_licitacoes
        WHERE sus = TRUE
        ORDER BY valor_homologado DESC
    """)
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contratos_sus AS
        SELECT * FROM rb_contratos
        WHERE sus = TRUE
        ORDER BY valor_contrato DESC
    """)

    # Cruzamento com CEIS/CNEP — só se a view de sanções existir
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    views  = {r[0] for r in con.execute(
        "SELECT view_name FROM information_schema.views"
    ).fetchall()}
    has_sancoes = "v_sancoes_ativas" in views or "sancoes_collapsed" in tables

    if has_sancoes:
        sancoes_src = "v_sancoes_ativas" if "v_sancoes_ativas" in views else "sancoes_collapsed"
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
                s.fonte         AS sancao_fonte,
                s.tipo_sancao   AS sancao_tipo,
                s.data_inicio   AS sancao_inicio,
                s.data_fim      AS sancao_fim,
                s.orgao_sancao,
                s.ativa
            FROM rb_contratos c
            JOIN {sancoes_src} s
              ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
               = REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g')
            WHERE c.cnpj != ''
        """)
        n = con.execute(
            "SELECT COUNT(*) FROM v_rb_contrato_ceis WHERE ativa"
        ).fetchone()[0]
        log.info("v_rb_contrato_ceis: %d contratos com sanção ativa", n)

    n_l = con.execute("SELECT COUNT(*) FROM v_rb_licitacoes_sus").fetchone()[0]
    n_c = con.execute("SELECT COUNT(*) FROM v_rb_contratos_sus").fetchone()[0]
    log.info("v_rb_licitacoes_sus=%d | v_rb_contratos_sus=%d", n_l, n_c)


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
        WHERE kind IN ('RB_SUS_LICITACAO','RB_SUS_CONTRATO','RB_CONTRATO_SANCIONADO')
    """)

    agora = datetime.now()
    recs  = []

    # ── Licitações SUS por unidade/ano ──────────────────────────────────────
    for (ano, unidade, n, total) in con.execute("""
        SELECT ano, unidade, COUNT(*), SUM(valor_homologado)
        FROM rb_licitacoes WHERE sus=TRUE
        GROUP BY ano, unidade ORDER BY SUM(valor_homologado) DESC LIMIT 300
    """).fetchall():
        iid = "INS_" + hashlib.md5(
            f"RB_SUS_LICITACAO{ano}{unidade}".encode()
        ).hexdigest()[:12]
        recs.append((
            iid, "RB_SUS_LICITACAO", "HIGH", 80,
            float(total or 0),
            f"SUS Rio Branco — {n} licitações em {unidade or 'N/I'} ({ano})",
            (f"A unidade **{unidade or 'N/I'}** abriu **{n} licitação(ões)** "
             f"SUS em {ano}, valor homologado total "
             f"**R$ {float(total or 0):,.2f}**."),
            "licitacoes_sus_por_unidade_ano",
            json.dumps(["transparencia.riobranco.ac.gov.br/licitacao"]),
            json.dumps(["SUS", "SEMSA", "licitacao", "RIO_BRANCO"]),
            int(n), float(total or 0), agora,
            "municipal", "Prefeitura de Rio Branco", "SEMSA",
            "Rio Branco", "AC", "saude", True,
            float(total or 0), int(ano),
            "transparencia.riobranco.ac.gov.br",
        ))

    # ── Contratos SUS por fornecedor ─────────────────────────────────────────
    for (fornecedor, cnpj, unidade, n, total) in con.execute("""
        SELECT fornecedor, cnpj, unidade, COUNT(*), SUM(valor_contrato)
        FROM rb_contratos WHERE sus=TRUE AND cnpj <> ''
        GROUP BY fornecedor, cnpj, unidade
        ORDER BY SUM(valor_contrato) DESC LIMIT 300
    """).fetchall():
        iid = "INS_" + hashlib.md5(
            f"RB_SUS_CONTRATO{cnpj}{unidade}".encode()
        ).hexdigest()[:12]
        recs.append((
            iid, "RB_SUS_CONTRATO", "HIGH", 82,
            float(total or 0),
            f"SUS RB — {fornecedor or cnpj} × {unidade or 'N/I'}",
            (f"**{fornecedor}** (`{cnpj}`) possui **{n} contrato(s)** SUS "
             f"com **{unidade or 'N/I'}**, total "
             f"**R$ {float(total or 0):,.2f}**."),
            "contratos_sus_por_fornecedor",
            json.dumps(["transparencia.riobranco.ac.gov.br/contrato"]),
            json.dumps(["SUS", "SEMSA", "contrato", "RIO_BRANCO"]),
            int(n), float(total or 0), agora,
            "municipal", "Prefeitura de Rio Branco", "SEMSA",
            "Rio Branco", "AC", "saude", True,
            float(total or 0), None,
            "transparencia.riobranco.ac.gov.br",
        ))

    # ── Contratos SUS × CEIS/CNEP (se view existir) ──────────────────────────
    views = {r[0] for r in con.execute(
        "SELECT view_name FROM information_schema.views"
    ).fetchall()}
    if "v_rb_contrato_ceis" in views:
        for (cnpj, fornecedor, unidade, n_cont, total, n_sanc) in con.execute("""
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
                f"⚠️ RB SUS — {fornecedor or cnpj} SANCIONADO contratado por {unidade or 'N/I'}",
                (f"**{fornecedor}** (`{cnpj}`) possui **{n_sanc} sanção(ões) "
                 f"ativa(s)** (CEIS/CNEP) e ainda assim foi contratado "
                 f"**{n_cont} vez(es)** pelo SUS de Rio Branco "
                 f"(**{unidade or 'N/I'}**), total "
                 f"**R$ {float(total or 0):,.2f}**."),
                "contrato_sus_fornecedor_sancionado",
                json.dumps([
                    "transparencia.riobranco.ac.gov.br/contrato",
                    "portaldatransparencia.gov.br/download-de-dados/ceis",
                ]),
                json.dumps(["CEIS", "CNEP", "SUS", "SEMSA", "RIO_BRANCO",
                            "sancionado", "CRITICAL"]),
                int(n_cont), float(total or 0), agora,
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
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Raspa /licitacao/ e /contrato/ de Rio Branco (JSF)"
    )
    parser.add_argument("--anos", nargs="+", type=int, default=[2024, 2025])
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Não grava no banco; mostra amostra SUS no log",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Apaga dados existentes dos anos informados antes de reinserir",
    )
    parser.add_argument(
        "--no-enrich", action="store_true",
        help="Não faz scrape de detalhe para enriquecer CNPJ faltando",
    )
    parser.add_argument("--delay", type=float, default=DELAY)
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    args = parser.parse_args()

    global DELAY
    DELAY = args.delay

    con = duckdb.connect(args.db_path, read_only=args.dry_run)
    if not args.dry_run:
        con.execute(DDL_LICITACOES)
        con.execute(DDL_CONTRATOS)

    if args.force and not args.dry_run:
        for ano in args.anos:
            con.execute("DELETE FROM rb_licitacoes WHERE ano = ?", [ano])
            con.execute("DELETE FROM rb_contratos WHERE ano = ?", [ano])
        log.info("Dados dos anos %s apagados (--force).", args.anos)

    session = _make_session()
    total_lic = total_cont = 0

    for ano in args.anos:
        log.info("══ Ano %d ══════════════════════════════════════", ano)

        # Licitações
        try:
            lic_rows = _search_licitacoes_year(session, ano)
            time.sleep(DELAY)
        except Exception as e:
            log.error("Licitações %d falhou: %s", ano, e)
            lic_rows = []

        # Contratos
        try:
            cont_rows = _search_contratos_year(session, ano)
            time.sleep(DELAY)
        except Exception as e:
            log.error("Contratos %d falhou: %s", ano, e)
            cont_rows = []

        if args.dry_run:
            lic_sus  = [r for r in lic_rows  if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]]
            cont_sus = [r for r in cont_rows if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]]
            log.info("[dry-run] %d/%d licitações SUS | %d/%d contratos SUS",
                     len(lic_sus), len(lic_rows), len(cont_sus), len(cont_rows))
            for r in lic_sus[:5]:
                log.info("  [LIC ] %s | %s | R$ %.0f",
                         r.get("unidade",""), r.get("objeto","")[:60],
                         r.get("valor_homologado", 0))
            for r in cont_sus[:5]:
                log.info("  [CONT] %s | %s | R$ %.0f",
                         r.get("unidade",""), r.get("objeto","")[:60],
                         r.get("valor_contrato", 0))
            continue

        # Enriquece CNPJ de linhas SUS sem CNPJ
        if not args.no_enrich:
            sus_lic  = [r for r in lic_rows  if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]]
            sus_cont = [r for r in cont_rows if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]]
            _enrich_missing_cnpjs(session, sus_lic,  "cnpj_venc",  DELAY)
            _enrich_missing_cnpjs(session, sus_cont, "cnpj",       DELAY)

        n_l = _upsert_licitacoes(con, lic_rows)
        n_c = _upsert_contratos(con, cont_rows)
        log.info("Gravado %d: %d licitações | %d contratos", ano, n_l, n_c)
        total_lic  += n_l
        total_cont += n_c

    if not args.dry_run:
        _build_views(con)
        n_ins = _build_insights(con)

        # Resumo final
        log.info("══ RESUMO ══════════════════════════════════════")
        log.info("  rb_licitacoes   : %d total",
                 con.execute("SELECT COUNT(*) FROM rb_licitacoes").fetchone()[0])
        log.info("  rb_contratos    : %d total",
                 con.execute("SELECT COUNT(*) FROM rb_contratos").fetchone()[0])
        log.info("  licitações SUS  : %d",
                 con.execute("SELECT COUNT(*) FROM v_rb_licitacoes_sus").fetchone()[0])
        log.info("  contratos SUS   : %d | R$ %,.0f",
                 *con.execute(
                     "SELECT COUNT(*), COALESCE(SUM(valor_contrato),0) FROM v_rb_contratos_sus"
                 ).fetchone())

        views = {r[0] for r in con.execute(
            "SELECT view_name FROM information_schema.views"
        ).fetchall()}
        if "v_rb_contrato_ceis" in views:
            n_ceis = con.execute(
                "SELECT COUNT(DISTINCT cnpj) FROM v_rb_contrato_ceis WHERE ativa"
            ).fetchone()[0]
            log.info("  contratos SUS × CEIS ativas: %d fornecedores", n_ceis)

        log.info("  insights gerados: %d", n_ins)

    con.close()


if __name__ == "__main__":
    main()
