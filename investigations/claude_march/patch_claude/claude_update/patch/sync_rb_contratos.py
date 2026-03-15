"""
sync_rb_contratos.py
--------------------
Raspa /contrato/ do portal de transparência de Rio Branco.

IDs JSF confirmados em inspeção real (2026-03-12):
  form_id    = "j_idt35"
  exercício  = "Formulario:j_idt73:j_idt75"
  secretaria = "Formulario:j_idt110:j_idt110"        (autocomplete input)
  sec_hidden = "Formulario:j_idt110:j_idt110_hinput" (value selecionado)
  submit     = name="Formulario:j_idt117"             (sem id= no HTML)
  SEMSA      = value "4065"

Busca: POST completo (não partial/ajax) — padrão que retornou 510 contratos
SEMSA/2024 na inspeção real.

Cria:
  TABLE rb_contratos        — uma linha por contrato
  VIEW  v_rb_contratos_sus  — filtro SUS/SEMSA
  VIEW  v_rb_contrato_ceis  — JOIN com sancoes_collapsed por CNPJ
  INSERTs em insight        — RB_SUS_CONTRATO / RB_CONTRATO_SANCIONADO

Uso:
  .venv/bin/python scripts/sync_rb_contratos.py --anos 2025 --dry-run
  .venv/bin/python scripts/sync_rb_contratos.py --anos 2024 2025
  .venv/bin/python scripts/sync_rb_contratos.py --anos 2024 2025 --unit-contains ""
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

DEFAULT_DB = ROOT / "data" / "sentinela_analytics.duckdb"
BASE       = "https://transparencia.riobranco.ac.gov.br"
URL        = f"{BASE}/contrato/"
DELAY      = 0.6

# ─── IDs JSF hardcoded (confirmados em inspeção real) ────────────────────────
_FORM      = "j_idt35"
_EXERC     = "Formulario:j_idt73:j_idt75"
_AC        = "Formulario:j_idt110:j_idt110"
_HINPUT    = "Formulario:j_idt110:j_idt110_hinput"
_SUBMIT    = "Formulario:j_idt117"
_SEMSA_ID  = "4065"
_SEMSA_LBL = "01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA"

# ─── Classificação SUS ────────────────────────────────────────────────────────
SUS_KW = (
    "SAUDE", "SEMSA", "FMS", "FUNDO MUNICIPAL DE SAUDE",
    "UBS", "UPA", "CAPS", "HOSPITAL", "MATERNIDADE",
    "VIGILANCIA SANITARIA", "VIGILANCIA EPIDEMIOLOGICA",
    "PSF", "ESF", "POLICLINICA", "PRONTO ATENDIMENTO",
    "CTA", "SAE", "CENTRO DE SAUDE",
)

DDL = """
CREATE TABLE IF NOT EXISTS rb_contratos (
    row_id          VARCHAR PRIMARY KEY,
    ano             INTEGER,
    numero_contrato VARCHAR DEFAULT '',
    numero_processo VARCHAR DEFAULT '',
    numero_termo    VARCHAR DEFAULT '',
    objeto          VARCHAR DEFAULT '',
    unidade         VARCHAR DEFAULT '',
    unidade_id      VARCHAR DEFAULT '',
    situacao        VARCHAR DEFAULT '',
    data_inicio     VARCHAR DEFAULT '',
    data_fim        VARCHAR DEFAULT '',
    valor_contrato  DOUBLE  DEFAULT 0,
    valor_atual     DOUBLE  DEFAULT 0,
    fornecedor      VARCHAR DEFAULT '',
    cnpj            VARCHAR DEFAULT '',
    sus             BOOLEAN DEFAULT FALSE,
    sus_keyword     VARCHAR DEFAULT '',
    url_detalhe     VARCHAR DEFAULT '',
    raw_json        VARCHAR DEFAULT '[]',
    capturado_em    TIMESTAMP
)
"""

# ─── Helpers ──────────────────────────────────────────────────────────────────

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

def _val(raw: str) -> float:
    try:
        return float(re.sub(r"[^\d,]", "", raw or "0").replace(",", "."))
    except Exception:
        return 0.0

def _cnpj(raw: str) -> str:
    return re.sub(r"\D", "", raw or "")

def _rid(*parts: str) -> str:
    return "CONT_" + hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()[:14]

def _vs(html: str) -> str:
    m = re.search(r'javax\.faces\.ViewState[^>]*value="([^"]+)"', html)
    return m.group(1) if m else ""

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Sentinela/3.0"})
    return s


# ─── Descoberta de exercícios disponíveis ─────────────────────────────────────

def _exerc_opts(html: str) -> dict[str, str]:
    """Retorna {ano_str: option_value} do select de exercício."""
    m = re.search(
        r'<select[^>]+id="[^"]*"[^>]*>(.*?)</select>',
        html, re.DOTALL
    )
    if not m:
        return {}
    opts: dict[str, str] = {}
    for val, label in re.findall(
        r'<option[^>]+value="([^"]+)"[^>]*>([^<]+)</option>',
        m.group(1)
    ):
        yr = re.search(r"\d{4}", label)
        if yr:
            opts[yr.group(0)] = val
    return opts


# ─── Autocomplete de secretarias/unidades ─────────────────────────────────────

def _list_units(
    session: requests.Session,
    vs: str,
    query: str = " ",
) -> list[tuple[str, str]]:
    """Retorna [(unit_id, unit_label), ...] via autocomplete PrimeFaces."""
    payload = {
        "javax.faces.partial.ajax":    "true",
        "javax.faces.source":          _AC,
        "javax.faces.partial.execute": _AC,
        "javax.faces.partial.render":  _AC,
        f"{_AC}_query":                query,
        _AC:                           query,
        _FORM:                         _FORM,
        "javax.faces.ViewState":       vs,
    }
    headers = {
        "Faces-Request":    "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          URL,
    }
    try:
        r = session.post(URL, data=payload, headers=headers, timeout=20)
        if r.status_code != 200:
            return []
        return re.findall(
            r'data-item-value="(\d+)"[^>]*data-item-label="([^"]+)"',
            r.text,
        )
    except Exception as e:
        log.warning("Autocomplete falhou: %s", e)
        return []


# ─── POST de busca por ano + unidade ─────────────────────────────────────────

def _search(
    session:   requests.Session,
    ano:       int,
    unit_id:   str,
    unit_lbl:  str,
    vs:        str,
    opts:      dict[str, str],
) -> tuple[list[dict], str]:
    """
    POST completo (validado).  Retorna (linhas, novo_vs).
    """
    ano_val = opts.get(str(ano), str(ano))
    payload = {
        _FORM:                   _FORM,
        "javax.faces.ViewState": vs,
        _EXERC:                  ano_val,
        _HINPUT:                 unit_id,
        _AC:                     unit_lbl,
        _SUBMIT:                 _SUBMIT,
    }
    r = session.post(URL, data=payload, timeout=30)
    r.raise_for_status()
    rows  = _parse_table(r.text, ano, unit_lbl, unit_id)
    new_vs = _vs(r.text) or vs
    return rows, new_vs


# ─── Parser de tabela ─────────────────────────────────────────────────────────
# Layout confirmado em inspeção real de /contrato/:
# [0] Nº Termo  [1] Nº Contrato  [2] Processo  [3] Exercício
# [4] Objeto    [5] Secretaria   [6] Situação   [7] Dt Início
# [8] Dt Fim    [9] Valor       [10] Valor Atual [11] Saldo  [12] Variação

def _parse_table(
    html: str, ano: int, unit_lbl: str, unit_id: str
) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    for tr in soup.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        t = [td.get_text(" ", strip=True) for td in tds]
        if not any(t):
            continue
        a = tr.find("a", href=True)
        det = (BASE + a["href"]) if a and a["href"].startswith("/") else ""
        out.append({
            "ano":             ano,
            "numero_termo":    t[0]  if len(t) > 0 else "",
            "numero_contrato": t[1]  if len(t) > 1 else "",
            "numero_processo": t[2]  if len(t) > 2 else "",
            "objeto":          t[4]  if len(t) > 4 else (t[3] if len(t) > 3 else ""),
            "unidade":         unit_lbl,
            "unidade_id":      unit_id,
            "situacao":        t[6]  if len(t) > 6 else "",
            "data_inicio":     t[7]  if len(t) > 7 else "",
            "data_fim":        t[8]  if len(t) > 8 else "",
            "valor_contrato":  _val(t[9])  if len(t) > 9  else 0.0,
            "valor_atual":     _val(t[10]) if len(t) > 10 else 0.0,
            "fornecedor":      "",
            "cnpj":            "",
            "url_detalhe":     det,
            "_raw":            t,
        })
    return out


# ─── Enriquecimento CNPJ via detalhe ─────────────────────────────────────────
_RE_CNPJ = re.compile(r'\d{2}[\.\-]?\d{3}[\.\-]?\d{3}[\/\-]?\d{4}[\.\-]?\d{2}')

def _enrich(session: requests.Session, url: str) -> tuple[str, str]:
    """Retorna (fornecedor, cnpj). Vazio se não encontrar — esperado."""
    if not url:
        return "", ""
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception:
        return "", ""
    text = BeautifulSoup(r.text, "html.parser").get_text("\n", strip=True)
    forn = ""
    for kw in ("Contratado", "Empresa", "Fornecedor", "Credor"):
        m = re.search(rf"(?i){kw}\s*[:\-–]\s*(.{{3,100}})", text)
        if m:
            forn = m.group(1).strip()
            break
    m = _RE_CNPJ.search(text)
    return forn, (_cnpj(m.group(0)) if m else "")


# ─── Persistência ─────────────────────────────────────────────────────────────

def _upsert(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    now  = datetime.now()
    recs = []
    for d in rows:
        is_sus, kw = _sus(f"{d.get('objeto','')} {d.get('unidade','')}")
        rid = _rid(
            str(d["ano"]),
            d.get("numero_contrato") or d.get("numero_termo", ""),
            d.get("objeto", "")[:40],
            d.get("unidade_id", ""),
        )
        recs.append((
            rid, int(d["ano"]),
            d.get("numero_contrato",""), d.get("numero_processo",""),
            d.get("numero_termo",""),    d.get("objeto",""),
            d.get("unidade",""),         d.get("unidade_id",""),
            d.get("situacao",""),        d.get("data_inicio",""),
            d.get("data_fim",""),
            float(d.get("valor_contrato") or 0),
            float(d.get("valor_atual") or 0),
            d.get("fornecedor",""),      d.get("cnpj",""),
            is_sus, kw,
            d.get("url_detalhe",""),
            json.dumps(d.get("_raw",[])),
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


# ─── Views ────────────────────────────────────────────────────────────────────

def _build_views(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contratos_sus AS
        SELECT * FROM rb_contratos WHERE sus = TRUE
        ORDER BY valor_contrato DESC
    """)

    tables  = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "sancoes_collapsed" not in tables:
        log.info("sancoes_collapsed ausente; pulando v_rb_contrato_ceis.")
        return

    # Detecta colunas reais de sancoes_collapsed para JOIN portável
    sc = {r[1]: r[2] for r in con.execute(
        "PRAGMA table_info('sancoes_collapsed')"
    ).fetchall()}

    def _col(preferred: list[str], fallback: str = "''") -> str:
        for c in preferred:
            if c in sc:
                return f"s.{c}"
        return fallback

    cnpj_col  = _col(["cnpj", "cnpj_cpf", "nr_cnpj"])
    tipo_col  = _col(["tipo_sancao", "tipo", "descricao_tipo"])
    org_col   = _col(["orgao_sancao", "orgao", "nome_orgao"])
    ativa_col = _col(["ativa"])
    di_col    = _col(["data_inicio", "data_inicio_vigencia"])
    df_col    = _col(["data_fim", "data_fim_vigencia"])
    fonte_col = _col(["fonte"])

    if cnpj_col == "''":
        log.warning("sancoes_collapsed sem coluna cnpj; pulando join.")
        return

    con.execute(f"""
        CREATE OR REPLACE VIEW v_rb_contrato_ceis AS
        SELECT
            c.row_id, c.ano, c.numero_contrato, c.objeto,
            c.unidade, c.fornecedor, c.cnpj, c.valor_contrato, c.sus,
            {fonte_col}  AS sancao_fonte,
            {tipo_col}   AS sancao_tipo,
            {di_col}     AS sancao_inicio,
            {df_col}     AS sancao_fim,
            {org_col}    AS orgao_sancao,
            {ativa_col}  AS ativa
        FROM rb_contratos c
        JOIN sancoes_collapsed s
          ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
           = REGEXP_REPLACE({cnpj_col}, '[^0-9]', '', 'g')
        WHERE c.cnpj <> ''
    """)
    n = con.execute(
        f"SELECT COUNT(*) FROM v_rb_contrato_ceis WHERE {ativa_col} = TRUE"
    ).fetchone()[0]
    log.info("v_rb_contrato_ceis: %d contratos com sanção ativa", n)


# ─── Insights ─────────────────────────────────────────────────────────────────

def _build_insights(con: duckdb.DuckDBPyConnection) -> int:
    if "insight" not in {r[0] for r in con.execute("SHOW TABLES").fetchall()}:
        return 0
    con.execute(
        "DELETE FROM insight WHERE kind IN ('RB_SUS_CONTRATO','RB_CONTRATO_SANCIONADO')"
    )
    agora = datetime.now()
    recs: list = []

    for (ano, unidade, n, total) in con.execute("""
        SELECT ano, unidade, COUNT(*) AS n, SUM(valor_contrato) AS total
        FROM rb_contratos WHERE sus = TRUE
        GROUP BY ano, unidade ORDER BY total DESC NULLS LAST LIMIT 300
    """).fetchall():
        iid = "INS_" + hashlib.md5(
            f"RB_SUS_CONTRATO{ano}{unidade}".encode()
        ).hexdigest()[:12]
        recs.append((
            iid, "RB_SUS_CONTRATO", "HIGH", 78, float(total or 0),
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
            float(total or 0), int(ano), "transparencia.riobranco.ac.gov.br",
        ))

    views = {r[0] for r in con.execute(
        "SELECT view_name FROM information_schema.views"
    ).fetchall()}
    if "v_rb_contrato_ceis" in views:
        try:
            for (cnpj, forn, unid, n_c, total, n_s) in con.execute("""
                SELECT cnpj, fornecedor, unidade,
                       COUNT(DISTINCT numero_contrato),
                       SUM(valor_contrato),
                       COUNT(DISTINCT sancao_tipo)
                FROM v_rb_contrato_ceis
                WHERE ativa = TRUE AND sus = TRUE
                GROUP BY cnpj, fornecedor, unidade
                ORDER BY SUM(valor_contrato) DESC LIMIT 200
            """).fetchall():
                iid = "INS_" + hashlib.md5(
                    f"RB_CONTRATO_SANCIONADO{cnpj}{unid}".encode()
                ).hexdigest()[:12]
                recs.append((
                    iid, "RB_CONTRATO_SANCIONADO", "CRITICAL", 95,
                    float(total or 0),
                    f"⚠️ RB SUS — {forn or cnpj} SANCIONADO × {unid or 'N/I'}",
                    (f"**{forn or cnpj}** (`{cnpj}`) possui "
                     f"**{n_s} tipo(s) de sanção ativa** (CEIS/CNEP) e foi "
                     f"contratado **{n_c} vez(es)** pelo SUS de Rio Branco "
                     f"(**{unid or 'N/I'}**), total "
                     f"**R$ {float(total or 0):,.2f}**."),
                    "contrato_sus_fornecedor_sancionado_municipal",
                    json.dumps([
                        "transparencia.riobranco.ac.gov.br/contrato",
                        "portaldatransparencia.gov.br/download-de-dados/ceis",
                    ]),
                    json.dumps(["CEIS","CNEP","SUS","SEMSA","RIO_BRANCO",
                                "sancionado","CRITICAL"]),
                    int(n_c), float(total or 0), agora,
                    "municipal", "Prefeitura de Rio Branco", "SEMSA",
                    "Rio Branco", "AC", "saude", True,
                    float(total or 0), None,
                    "transparencia.riobranco.ac.gov.br + CGU",
                ))
        except Exception as e:
            log.warning("v_rb_contrato_ceis query falhou: %s", e)

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


# ─── Coleta por ano ───────────────────────────────────────────────────────────

def collect_year(
    session:    requests.Session,
    ano:        int,
    unit_filter: str,
    enrich:     bool,
    delay:      float,
) -> list[dict]:

    r0 = session.get(URL, timeout=20); r0.raise_for_status()
    vs   = _vs(r0.text)
    opts = _exerc_opts(r0.text)
    log.info("Exercícios disponíveis: %s", list(opts.keys()))

    # Resolve unidades
    query  = unit_filter.strip() or " "
    units  = _list_units(session, vs, query)

    if not units and unit_filter:
        # Tenta " " e filtra local
        all_u = _list_units(session, vs, " ")
        needle = _norm(unit_filter)
        units  = [(uid, ul) for uid, ul in all_u if needle in _norm(ul)]

    # Fallback hardcoded para SEMSA
    if not units and "semsa" in (unit_filter or "").lower():
        units = [(_SEMSA_ID, _SEMSA_LBL)]
        log.info("Autocomplete silencioso; usando SEMSA hardcoded (%s).", _SEMSA_ID)

    if not units:
        log.warning("Nenhuma unidade encontrada; buscando sem filtro.")
        units = [("", "")]

    log.info("Unidades para %d: %d", ano, len(units))

    all_rows: list[dict] = []
    for i, (uid, ulbl) in enumerate(units, 1):
        log.info("  [%d/%d] %s — ano %d", i, len(units), ulbl[:70], ano)
        try:
            rows, vs = _search(session, ano, uid, ulbl, vs, opts)
        except Exception as e:
            log.warning("    Falhou: %s", e)
            rows = []

        if enrich:
            for row in rows:
                if (not row.get("cnpj") and row.get("url_detalhe")
                        and _sus(f"{row.get('objeto','')} {row.get('unidade','')}}")[0]):
                    forn, cnpj = _enrich(session, row["url_detalhe"])
                    if forn:
                        row["fornecedor"] = forn
                    if cnpj:
                        row["cnpj"] = cnpj
                    time.sleep(delay * 0.4)

        all_rows.extend(rows)
        log.info("    → %d contratos | acumulado %d", len(rows), len(all_rows))
        time.sleep(delay)

    return all_rows


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Raspa /contrato/ de Rio Branco (IDs JSF reais hardcoded)"
    )
    ap.add_argument("--anos",          nargs="+", type=int, default=[2024, 2025])
    ap.add_argument("--unit-contains", default="semsa",
                    help="Filtro de unidade (padrão: semsa). Use '' para todas.")
    ap.add_argument("--dry-run",       action="store_true")
    ap.add_argument("--force",         action="store_true",
                    help="Apaga anos antes de reinserir")
    ap.add_argument("--no-enrich",     action="store_true",
                    help="Não scrape /contrato/ver/ para CNPJ")
    ap.add_argument("--delay",         type=float, default=DELAY)
    ap.add_argument("--db-path",       default=str(DEFAULT_DB))
    args = ap.parse_args()

    con = duckdb.connect(args.db_path, read_only=args.dry_run)
    if not args.dry_run:
        con.execute(DDL)
    if args.force and not args.dry_run:
        for ano in args.anos:
            con.execute("DELETE FROM rb_contratos WHERE ano = ?", [ano])
        log.info("Anos %s apagados.", args.anos)

    s = _session()
    total = 0

    for ano in args.anos:
        log.info("══ Ano %d ══════════════════════════════════", ano)
        rows = collect_year(
            session     = s,
            ano         = ano,
            unit_filter = args.unit_contains,
            enrich      = not args.no_enrich,
            delay       = args.delay,
        )

        if args.dry_run:
            sus = [r for r in rows
                   if _sus(f"{r.get('objeto','')} {r.get('unidade','')}}")[0]]
            log.info("[dry-run] %d total | %d SUS (não gravados)", len(rows), len(sus))
            for r in sus[:10]:
                log.info("  [SUS] %-35s | %-55s | R$ %10.0f | cnpj=%s",
                         r.get("unidade","")[:35], r.get("objeto","")[:55],
                         r.get("valor_contrato",0),
                         r.get("cnpj","") or "(vazio)")
            continue

        n = _upsert(con, rows)
        log.info("Gravado %d: %d linhas", ano, n)
        total += n

    if not args.dry_run:
        _build_views(con)
        n_ins = _build_insights(con)
        n_tot = con.execute("SELECT COUNT(*) FROM rb_contratos").fetchone()[0]
        n_sus, v_sus = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(valor_contrato),0) FROM v_rb_contratos_sus"
        ).fetchone()
        log.info("══ RESUMO ══════════════════════════════════")
        log.info("  rb_contratos total : %d", n_tot)
        log.info("  contratos SUS      : %d | R$ %,.0f", n_sus, float(v_sus))
        log.info("  insights gerados   : %d", n_ins)
        views = {r[0] for r in con.execute(
            "SELECT view_name FROM information_schema.views").fetchall()}
        if "v_rb_contrato_ceis" in views:
            try:
                n_c = con.execute(
                    "SELECT COUNT(DISTINCT cnpj) FROM v_rb_contrato_ceis WHERE ativa = TRUE"
                ).fetchone()[0]
                log.info("  CEIS × contratos   : %d fornecedores sancionados", n_c)
            except Exception:
                pass
    con.close()


if __name__ == "__main__":
    main()
