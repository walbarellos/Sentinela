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

from src.core.insight_classification import ensure_insight_classification_columns
from src.ingest.riobranco_http import fetch_html

log = logging.getLogger("sync_rb_contratos")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DEFAULT_DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
BASE_URL = "https://transparencia.riobranco.ac.gov.br/contrato/"
KIND_PREFIX = "RB_SUS_CONTRATO"
KIND_SANCAO = "RB_CONTRATO_SANCIONADO"
DEFAULT_DELAY = 0.8
PRIMARY_FORM_ID = "Formulario"
PRIMARY_YEAR_SELECT = "Formulario:j_idt73:j_idt75"
PRIMARY_YEAR_OVERLAY = "Formulario:j_idt73:j_idt80_input"
PRIMARY_AUTOCOMPLETE = "Formulario:j_idt110:j_idt110"
PRIMARY_AUTOCOMPLETE_INPUT = "Formulario:j_idt110:j_idt110_input"
PRIMARY_AUTOCOMPLETE_HIDDEN = "Formulario:j_idt110:j_idt110_hinput"
PRIMARY_OBJECT_INPUT = "Formulario:j_idt115"
PRIMARY_SEARCH_SUBMIT = "Formulario:j_idt117"
SEMSA_FALLBACK_ID = "4065"
SEMSA_FALLBACK_LABEL = "01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA"
SUS_KEYWORDS = (
    "SEMSA",
    "SAUDE",
    "FUNDO MUNICIPAL DE SAUDE",
    "UBS",
    "UPA",
    "CAPS",
    "HOSPITAL",
    "MATERNIDADE",
    "PRONTO ATENDIMENTO",
    "POLICLINICA",
    "CTA",
    "SAE",
)

DDL_RB_CONTRATOS = """
CREATE TABLE IF NOT EXISTS rb_contratos (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    exercicio_id VARCHAR,
    origem_coleta VARCHAR,
    secretaria_filtro_id VARCHAR,
    secretaria_filtro_nome VARCHAR,
    numero_termo VARCHAR,
    numero_contrato VARCHAR,
    numero_processo VARCHAR,
    exercicio VARCHAR,
    objeto VARCHAR,
    secretaria VARCHAR,
    valor_brl DOUBLE,
    data_lancamento VARCHAR,
    situacao_contrato VARCHAR,
    valor_atual_brl DOUBLE,
    saldo_atual_brl DOUBLE,
    variacao_atual_brl DOUBLE,
    valor_referencia_brl DOUBLE,
    fornecedor VARCHAR,
    cnpj VARCHAR,
    detail_id VARCHAR,
    detail_url VARCHAR,
    sus BOOLEAN DEFAULT FALSE,
    sus_keyword VARCHAR,
    raw_json JSON,
    fonte VARCHAR,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def normalize_text(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", fix_text(value).upper())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip()


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


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def classify_sus(secretaria: str, objeto: str, secretaria_filtro: str) -> tuple[bool, str]:
    combined = normalize_text(" ".join(filter(None, [secretaria, objeto, secretaria_filtro])))
    for keyword in SUS_KEYWORDS:
        normalized_keyword = normalize_text(keyword)
        if re.search(rf"(?<![A-Z0-9]){re.escape(normalized_keyword)}(?![A-Z0-9])", combined):
            return True, keyword
    return False, ""


def short_id(prefix: str, *parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{hashlib.md5(raw.encode('utf-8')).hexdigest()[:16]}"


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_RB_CONTRATOS)
    existing = {row[1] for row in con.execute("PRAGMA table_info('rb_contratos')").fetchall()}
    extra_columns = {
        "exercicio_id": "VARCHAR",
        "origem_coleta": "VARCHAR",
        "secretaria_filtro_id": "VARCHAR",
        "secretaria_filtro_nome": "VARCHAR",
        "fornecedor": "VARCHAR",
        "cnpj": "VARCHAR",
        "detail_id": "VARCHAR",
        "detail_url": "VARCHAR",
        "fonte": "VARCHAR",
    }
    for column, dtype in extra_columns.items():
        if column not in existing:
            con.execute(f"ALTER TABLE rb_contratos ADD COLUMN {column} {dtype}")


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


class RioBrancoContratoSession:
    def __init__(self, delay: float = DEFAULT_DELAY):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Sentinela/3.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept-Encoding": "identity",
            }
        )
        self.viewstate = ""
        self.page_html = ""
        self.form_id = PRIMARY_FORM_ID
        self.year_options: dict[int, str] = {}
        self.controls: dict[str, str] = {}

    def load(self) -> None:
        self.page_html = fetch_html(self.session, BASE_URL, timeout=30)
        self.viewstate = self._extract_viewstate(self.page_html)
        if not self.viewstate:
            raise RuntimeError("ViewState não encontrado no portal de contratos.")
        self._extract_controls()
        self._extract_year_options()

    @staticmethod
    def _extract_viewstate(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        form = soup.find("form", {"id": PRIMARY_FORM_ID}) or soup.find("form")
        if form is None:
            return ""
        field = form.find("input", {"name": "javax.faces.ViewState"})
        return field.get("value", "").strip() if field else ""

    @staticmethod
    def _search(pattern: str, text: str) -> str:
        match = re.search(pattern, text)
        return match.group(1) if match else ""

    def _extract_controls(self) -> None:
        html = self.page_html
        self.form_id = PRIMARY_FORM_ID if 'id="Formulario"' in html else self._search(r'<form[^>]+id="([^"]+)"', html) or PRIMARY_FORM_ID
        controls = {
            "year_select": PRIMARY_YEAR_SELECT if f'id="{PRIMARY_YEAR_SELECT}"' in html else self._search(r'<select id="([^"]+:j_idt75)" name="[^"]+" class="form-control"', html),
            "year_overlay_input": PRIMARY_YEAR_OVERLAY if f'id="{PRIMARY_YEAR_OVERLAY}"' in html else self._search(r'<select id="([^"]+:j_idt80_input)" name="[^"]+" size="2"', html),
            "autocomplete": PRIMARY_AUTOCOMPLETE if f'id="{PRIMARY_AUTOCOMPLETE}"' in html else self._search(r'<span id="([^"]+:j_idt110:[^"]+)" class="ui-autocomplete">', html),
            "autocomplete_input": PRIMARY_AUTOCOMPLETE_INPUT if f'id="{PRIMARY_AUTOCOMPLETE_INPUT}"' in html else self._search(r'<input id="([^"]+_input)" name="[^"]+" type="text" class="ui-autocomplete-input', html),
            "autocomplete_hidden": PRIMARY_AUTOCOMPLETE_HIDDEN if f'id="{PRIMARY_AUTOCOMPLETE_HIDDEN}"' in html else self._search(r'<input id="([^"]+_hinput)" name="[^"]+" type="hidden"', html),
            "object_input": PRIMARY_OBJECT_INPUT if f'name="{PRIMARY_OBJECT_INPUT}"' in html else self._search(r'<input type="text" name="([^"]+:j_idt115)" class="form-control"', html),
            "search_submit": PRIMARY_SEARCH_SUBMIT if f'name="{PRIMARY_SEARCH_SUBMIT}"' in html else self._search(r'<input type="submit" name="([^"]+:j_idt117)" value="Pesquisar"', html),
        }
        missing = [key for key, value in controls.items() if not value]
        if missing:
            raise RuntimeError(f"Controles JSF não localizados em /contrato/: {missing}")
        self.controls = controls

    def _extract_year_options(self) -> None:
        soup = BeautifulSoup(self.page_html, "html.parser")
        year_select = soup.find("select", {"id": self.controls["year_select"]})
        if year_select is None:
            raise RuntimeError("Select de exercício não encontrado.")
        year_options: dict[int, str] = {}
        for option in year_select.find_all("option"):
            label = fix_text(option.get_text(" ", strip=True))
            value = option.get("value", "").strip()
            if not value:
                continue
            match = re.search(r"\b(\d{4})\b", label)
            if match:
                year_options.setdefault(int(match.group(1)), value)
        if not year_options:
            raise RuntimeError("Nenhum exercício válido encontrado em /contrato/.")
        self.year_options = year_options

    def _post_partial(self, payload: dict[str, str]) -> str:
        response = self.session.post(
            BASE_URL,
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Faces-Request": "partial/ajax",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": BASE_URL,
                "Origin": "https://transparencia.riobranco.ac.gov.br",
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.text

    def list_secretarias(self, query: str = " ") -> list[dict[str, str]]:
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": self.controls["autocomplete"],
            "javax.faces.partial.execute": self.controls["autocomplete"],
            "javax.faces.partial.render": self.controls["autocomplete"],
            self.controls["autocomplete"]: query,
            self.controls["autocomplete_input"]: query,
            f"{self.controls['autocomplete']}_query": query,
            self.form_id: self.form_id,
            "javax.faces.ViewState": self.viewstate,
        }
        xml_text = self._post_partial(payload)
        root = BeautifulSoup(xml_text, "xml")
        secretarias: dict[str, str] = {}
        for update in root.find_all("update"):
            update_id = update.get("id", "")
            if self.controls["autocomplete"] not in update_id:
                continue
            html_fragment = update.text or ""
            fragment = BeautifulSoup(html_fragment, "html.parser")
            for item in fragment.find_all("li", class_=re.compile("ui-autocomplete-item")):
                secretaria_id = (item.get("data-item-value") or "").strip()
                secretaria_nome = fix_text(item.get("data-item-label") or item.get_text(" ", strip=True))
                if secretaria_id and secretaria_nome:
                    secretarias[secretaria_id] = secretaria_nome
        return [
            {"secretaria_id": secretaria_id, "secretaria_nome": secretaria_nome}
            for secretaria_id, secretaria_nome in sorted(secretarias.items(), key=lambda item: item[1])
        ]

    def search_html(
        self,
        *,
        ano: int,
        secretaria_id: str = "",
        secretaria_nome: str = "",
        objeto: str = "",
    ) -> str:
        if ano not in self.year_options:
            raise ValueError(f"Ano {ano} não está disponível no portal.")
        exercicio_id = self.year_options[ano]
        payload = {
            self.form_id: self.form_id,
            self.controls["year_select"]: exercicio_id,
            self.controls["year_overlay_input"]: exercicio_id,
            self.controls["autocomplete_input"]: secretaria_nome,
            self.controls["autocomplete_hidden"]: secretaria_id,
            self.controls["object_input"]: objeto,
            self.controls["search_submit"]: "Pesquisar",
            "javax.faces.ViewState": self.viewstate,
        }
        response = self.session.post(
            BASE_URL,
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": BASE_URL,
                "Origin": "https://transparencia.riobranco.ac.gov.br",
            },
            timeout=120,
        )
        response.raise_for_status()
        self.page_html = response.text
        self.viewstate = self._extract_viewstate(self.page_html) or self.viewstate
        return self.page_html


def parse_result_rows(
    page_html: str,
    *,
    ano: int,
    exercicio_id: str,
    origem_coleta: str,
    secretaria_filtro_id: str,
    secretaria_filtro_nome: str,
) -> list[dict[str, object]]:
    soup = BeautifulSoup(page_html, "html.parser")
    pattern = re.compile(r"/contrato/ver/(\d+)/")
    rows: list[dict[str, object]] = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 14:
            continue

        values = [fix_text(td.get_text(" ", strip=True)) for td in cells]
        if not any(values):
            continue

        detail_link = tr.find("a", href=pattern)
        detail_href = detail_link.get("href", "").strip() if detail_link else ""
        detail_match = pattern.search(detail_href) if detail_href else None
        detail_id = detail_match.group(1) if detail_match else ""
        detail_url = detail_href if detail_href.startswith("http") else (f"https://transparencia.riobranco.ac.gov.br{detail_href}" if detail_href else "")

        numero_termo = values[2]
        numero_contrato = values[3]
        numero_processo = values[4]
        exercicio = values[5]
        objeto = values[6]
        secretaria = values[7]
        valor_brl = parse_brl(values[8])
        data_lancamento = values[9]
        situacao_contrato = values[10]
        valor_atual_brl = parse_brl(values[11])
        saldo_atual_brl = parse_brl(values[12])
        variacao_atual_brl = parse_brl(values[13])
        valor_referencia = max(valor_brl, valor_atual_brl)
        sus, sus_keyword = classify_sus(secretaria, objeto, secretaria_filtro_nome)

        if not any([numero_termo, numero_contrato, numero_processo, objeto]):
            continue

        rows.append(
            {
                "row_id": short_id("RBC", ano, numero_termo, numero_contrato, numero_processo, objeto),
                "ano": ano,
                "exercicio_id": exercicio_id,
                "origem_coleta": origem_coleta,
                "secretaria_filtro_id": secretaria_filtro_id,
                "secretaria_filtro_nome": secretaria_filtro_nome,
                "numero_termo": numero_termo,
                "numero_contrato": numero_contrato,
                "numero_processo": numero_processo,
                "exercicio": exercicio,
                "objeto": objeto,
                "secretaria": secretaria,
                "valor_brl": valor_brl,
                "data_lancamento": data_lancamento,
                "situacao_contrato": situacao_contrato,
                "valor_atual_brl": valor_atual_brl,
                "saldo_atual_brl": saldo_atual_brl,
                "variacao_atual_brl": variacao_atual_brl,
                "valor_referencia_brl": valor_referencia,
                "fornecedor": "",
                "cnpj": "",
                "detail_id": detail_id,
                "detail_url": detail_url,
                "sus": sus,
                "sus_keyword": sus_keyword,
                "raw_json": json.dumps(values, ensure_ascii=False),
                "fonte": "transparencia.riobranco.ac.gov.br/contrato",
            }
        )
    return rows


def extract_detail_fields(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    data: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) != 2:
            continue
        label = normalize_text(cells[0].get_text(" ", strip=True)).rstrip(":")
        value = fix_text(cells[1].get_text(" ", strip=True))
        if label and value:
            data[label] = value
    return data


def enrich_contract_detail(session: requests.Session, detail_url: str) -> tuple[str, str]:
    if not detail_url:
        return "", ""
    try:
        html = fetch_html(session, detail_url, timeout=30)
    except Exception:
        return "", ""

    fields = extract_detail_fields(html)
    fornecedor = ""
    for label in ("CONTRATADO", "FORNECEDOR", "CREDOR", "EMPRESA", "RAZAO SOCIAL"):
        if label in fields:
            fornecedor = fields[label]
            break
    cnpj = clean_doc(fields.get("CNPJ", ""))
    if cnpj:
        return fornecedor, cnpj

    text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
    if not fornecedor:
        for keyword in ("Contratado", "Fornecedor", "Credor", "Empresa"):
            match = re.search(rf"(?i){keyword}\s*[:\-–]\s*(.{{3,120}})", text)
            if match:
                fornecedor = fix_text(match.group(1))
                break
    match = re.search(r"\d{2}[\.\-]?\d{3}[\.\-]?\d{3}[\/\-]?\d{4}[\.\-]?\d{2}", text)
    if match:
        cnpj = clean_doc(match.group(0))
    return fornecedor, cnpj


def upsert_rows(con: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0
    payload = [
        (
            row["row_id"],
            row["ano"],
            row["exercicio_id"],
            row["origem_coleta"],
            row["secretaria_filtro_id"],
            row["secretaria_filtro_nome"],
            row["numero_termo"],
            row["numero_contrato"],
            row["numero_processo"],
            row["exercicio"],
            row["objeto"],
            row["secretaria"],
            row["valor_brl"],
            row["data_lancamento"],
            row["situacao_contrato"],
            row["valor_atual_brl"],
            row["saldo_atual_brl"],
            row["variacao_atual_brl"],
            row["valor_referencia_brl"],
            row["fornecedor"],
            row["cnpj"],
            row["detail_id"],
            row["detail_url"],
            row["sus"],
            row["sus_keyword"],
            row["raw_json"],
            row["fonte"],
        )
        for row in rows
    ]
    con.executemany(
        """
        INSERT INTO rb_contratos (
            row_id, ano, exercicio_id, origem_coleta, secretaria_filtro_id, secretaria_filtro_nome,
            numero_termo, numero_contrato, numero_processo, exercicio, objeto, secretaria, valor_brl,
            data_lancamento, situacao_contrato, valor_atual_brl, saldo_atual_brl, variacao_atual_brl,
            valor_referencia_brl, fornecedor, cnpj, detail_id, detail_url, sus, sus_keyword, raw_json,
            fonte, capturado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (row_id) DO UPDATE SET
            exercicio_id = excluded.exercicio_id,
            origem_coleta = excluded.origem_coleta,
            secretaria_filtro_id = excluded.secretaria_filtro_id,
            secretaria_filtro_nome = excluded.secretaria_filtro_nome,
            numero_termo = excluded.numero_termo,
            numero_contrato = excluded.numero_contrato,
            numero_processo = excluded.numero_processo,
            exercicio = excluded.exercicio,
            objeto = excluded.objeto,
            secretaria = excluded.secretaria,
            valor_brl = excluded.valor_brl,
            data_lancamento = excluded.data_lancamento,
            situacao_contrato = excluded.situacao_contrato,
            valor_atual_brl = excluded.valor_atual_brl,
            saldo_atual_brl = excluded.saldo_atual_brl,
            variacao_atual_brl = excluded.variacao_atual_brl,
            valor_referencia_brl = excluded.valor_referencia_brl,
            fornecedor = excluded.fornecedor,
            cnpj = excluded.cnpj,
            detail_id = excluded.detail_id,
            detail_url = excluded.detail_url,
            sus = excluded.sus,
            sus_keyword = excluded.sus_keyword,
            raw_json = excluded.raw_json,
            fonte = excluded.fonte,
            capturado_em = excluded.capturado_em
        """,
        [row + (datetime.now(),) for row in payload],
    )
    return len(rows)


def build_sancoes_view(con: duckdb.DuckDBPyConnection) -> bool:
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "sancoes_collapsed" not in tables:
        return False

    columns = {row[1] for row in con.execute("PRAGMA table_info('sancoes_collapsed')").fetchall()}

    def column_expr(candidates: tuple[str, ...], default: str = "NULL") -> str:
        for candidate in candidates:
            if candidate in columns:
                return f"s.{candidate}"
        return default

    cnpj_expr = column_expr(("cnpj", "cnpj_cpf", "nr_cnpj"), "NULL")
    if cnpj_expr == "NULL":
        log.warning("sancoes_collapsed sem coluna de CNPJ; pulando v_rb_contrato_ceis.")
        return False

    fonte_expr = column_expr(("fonte",), "NULL")
    tipo_expr = column_expr(("tipo_sancao", "tipos_sancao", "tipo", "descricao_tipo"), "NULL")
    inicio_expr = column_expr(("data_inicio", "data_inicio_vigencia", "data_inicio_mais_antiga"), "NULL")
    fim_expr = column_expr(("data_fim", "data_fim_vigencia", "data_fim_mais_recente"), "NULL")
    orgao_expr = column_expr(("orgao_sancao", "orgao", "orgao_ac", "nome_orgao"), "NULL")
    ativa_expr = column_expr(("ativa",), "FALSE")

    con.execute(
        f"""
        CREATE OR REPLACE VIEW v_rb_contrato_ceis AS
        SELECT
            c.row_id,
            c.ano,
            c.numero_termo,
            c.numero_contrato,
            c.numero_processo,
            c.objeto,
            c.secretaria,
            c.fornecedor,
            c.cnpj,
            c.valor_referencia_brl,
            c.sus,
            {fonte_expr} AS sancao_fonte,
            {tipo_expr} AS sancao_tipo,
            {inicio_expr} AS sancao_inicio,
            {fim_expr} AS sancao_fim,
            {orgao_expr} AS orgao_sancao,
            CAST(COALESCE({ativa_expr}, FALSE) AS BOOLEAN) AS ativa
        FROM rb_contratos c
        JOIN sancoes_collapsed s
          ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
           = REGEXP_REPLACE(CAST({cnpj_expr} AS VARCHAR), '[^0-9]', '', 'g')
        WHERE c.cnpj <> ''
        """
    )
    total = con.execute("SELECT COUNT(*) FROM v_rb_contrato_ceis WHERE ativa = TRUE").fetchone()[0]
    log.info("v_rb_contrato_ceis: %d contratos com sanção ativa", total)
    return True


def build_views(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_contratos_sus AS
        SELECT *
        FROM rb_contratos
        WHERE sus = TRUE
        """
    )
    build_sancoes_view(con)
    views = {row[0] for row in con.execute("SELECT table_name FROM information_schema.views").fetchall()}
    sancao_cte = ""
    sancao_join = ""
    sancao_count_expr = "0"
    sancao_source_expr = "''"
    sancao_cols = """
            FALSE AS sancao_ativa,
            0 AS n_sancoes_ativas,
            '' AS sancao_fontes
    """
    if "v_rb_contrato_ceis" in views:
        sancao_cte = """
        , sancoes AS (
            SELECT
                row_id,
                COUNT(*) FILTER (WHERE ativa = TRUE) AS n_sancoes_ativas,
                STRING_AGG(DISTINCT COALESCE(sancao_fonte, ''), ', ' ORDER BY COALESCE(sancao_fonte, '')) AS sancao_fontes
            FROM v_rb_contrato_ceis
            GROUP BY 1
        )
        """
        sancao_join = "LEFT JOIN sancoes s ON s.row_id = c.row_id"
        sancao_count_expr = "COALESCE(s.n_sancoes_ativas, 0)"
        sancao_source_expr = "COALESCE(s.sancao_fontes, '')"
        sancao_cols = """
            CAST(COALESCE(s.n_sancoes_ativas, 0) > 0 AS BOOLEAN) AS sancao_ativa,
            COALESCE(s.n_sancoes_ativas, 0) AS n_sancoes_ativas,
            COALESCE(s.sancao_fontes, '') AS sancao_fontes
        """

    con.execute(
        f"""
        CREATE OR REPLACE VIEW v_rb_contratos_triagem AS
        WITH latest_ocr AS (
            SELECT o.*
            FROM rb_contratos_pdf_ocr o
            JOIN (
                SELECT contrato_row_id, MAX(updated_at) AS updated_at
                FROM rb_contratos_pdf_ocr
                GROUP BY 1
            ) x
              ON x.contrato_row_id = o.contrato_row_id
             AND x.updated_at = o.updated_at
        )
        {sancao_cte}
        SELECT
            c.row_id,
            c.ano,
            c.numero_contrato,
            c.numero_processo,
            c.secretaria,
            c.objeto,
            c.valor_referencia_brl,
            c.situacao_contrato,
            c.fornecedor,
            c.cnpj,
            CAST(COALESCE(c.fornecedor, '') <> '' AS BOOLEAN) AS tem_fornecedor,
            CAST(COALESCE(c.cnpj, '') <> '' AS BOOLEAN) AS tem_cnpj,
            COALESCE(o.status, '') AS ocr_status,
            COALESCE(o.source_kind, '') AS ocr_source_kind,
            {sancao_cols},
            CASE
                WHEN {sancao_count_expr} > 0 THEN 'denuncia_imediata'
                WHEN COALESCE(c.cnpj, '') = '' AND COALESCE(c.valor_referencia_brl, 0) >= 50000 THEN 'extrair_cnpj_prioritario'
                WHEN COALESCE(c.cnpj, '') = '' THEN 'buscar_fonte_externa'
                WHEN COALESCE(c.fornecedor, '') = '' THEN 'normalizar_fornecedor'
                ELSE 'revisar'
            END AS fila_investigacao,
            CASE
                WHEN {sancao_count_expr} > 0 THEN 100
                WHEN COALESCE(c.cnpj, '') = '' AND COALESCE(c.valor_referencia_brl, 0) >= 50000 THEN 80
                WHEN COALESCE(c.cnpj, '') = '' THEN 60
                WHEN COALESCE(c.fornecedor, '') = '' THEN 40
                ELSE 20
            END AS prioridade
        FROM rb_contratos c
        LEFT JOIN latest_ocr o ON o.contrato_row_id = c.row_id
        {sancao_join}
        WHERE c.sus = TRUE
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_contratos_pendencias AS
        SELECT
            ano,
            numero_processo,
            objeto,
            COUNT(*) AS n_contratos,
            SUM(valor_referencia_brl) AS total_brl,
            MIN(prioridade) AS prioridade_min,
            MAX(prioridade) AS prioridade_max,
            STRING_AGG(numero_contrato, ', ' ORDER BY numero_contrato) AS contratos,
            STRING_AGG(DISTINCT fila_investigacao, ', ' ORDER BY fila_investigacao) AS filas
        FROM v_rb_contratos_triagem
        WHERE tem_cnpj = FALSE OR tem_fornecedor = FALSE OR sancao_ativa = TRUE
        GROUP BY 1, 2, 3
        ORDER BY prioridade_max DESC, total_brl DESC NULLS LAST
        """
    )
    return con.execute("SELECT COUNT(*) FROM v_rb_contratos_sus").fetchone()[0]


def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    if not ensure_insight_columns(con):
        return 0

    con.execute("DELETE FROM insight WHERE kind IN (?, ?)", [f"{KIND_PREFIX}_ITEM", KIND_SANCAO])
    rows = con.execute(
        """
        SELECT
            ano,
            secretaria,
            numero_termo,
            numero_contrato,
            numero_processo,
            objeto,
            valor_referencia_brl,
            situacao_contrato,
            detail_url
        FROM rb_contratos
        WHERE sus = TRUE
        ORDER BY valor_referencia_brl DESC NULLS LAST, numero_contrato, numero_termo
        LIMIT 300
        """
    ).fetchall()

    payload = []
    for (
        ano,
        secretaria,
        numero_termo,
        numero_contrato,
        numero_processo,
        objeto,
        valor_referencia_brl,
        situacao_contrato,
        detail_url,
    ) in rows:
        exposure = float(valor_referencia_brl or 0.0)
        contrato_ref = numero_contrato or numero_termo or "sem_numero"
        insight_id = "INS_" + hashlib.sha1(
            f"{KIND_PREFIX}|{ano}|{contrato_ref}|{numero_processo}|{objeto}".encode("utf-8")
        ).hexdigest()[:16]
        description = (
            f"O portal de contratos de Rio Branco registrou contrato SUS em **{ano}** para **{secretaria}**, "
            f"com **processo {numero_processo or 'não informado'}**, "
            f"**valor de referência R$ {exposure:,.2f}** e "
            f"objeto: **{objeto or 'não informado'}**."
        )
        if situacao_contrato:
            description += f" Situação reportada: **{situacao_contrato}**."
        if detail_url:
            description += f" Detalhe público: {detail_url}"
        payload.append(
            (
                insight_id,
                f"{KIND_PREFIX}_ITEM",
                "INFO",
                86,
                exposure,
                f"SUS Rio Branco: contrato {contrato_ref} na {secretaria} por R$ {exposure:,.2f}",
                description,
                "contrato_sus_municipal",
                json.dumps(
                    ["rb_contratos", "transparencia.riobranco.ac.gov.br/contrato"],
                    ensure_ascii=False,
                ),
                json.dumps(
                    ["SUS", "SEMSA", "RIO_BRANCO", "contratos"],
                    ensure_ascii=False,
                ),
                1,
                exposure,
                "municipal",
                "Prefeitura de Rio Branco",
                "SEMSA",
                "Rio Branco",
                "AC",
                "saude",
                True,
                exposure,
                int(ano),
                "transparencia.riobranco.ac.gov.br/contrato",
            )
        )

    views = {row[0] for row in con.execute("SELECT table_name FROM information_schema.views").fetchall()}
    if "v_rb_contrato_ceis" in views:
        sancao_rows = con.execute(
            """
            WITH contratos_sancionados AS (
                SELECT DISTINCT
                    cnpj,
                    COALESCE(fornecedor, cnpj) AS fornecedor,
                    secretaria,
                    numero_contrato,
                    valor_referencia_brl,
                    sancao_tipo
                FROM v_rb_contrato_ceis
                WHERE ativa = TRUE AND sus = TRUE
            )
            SELECT
                cnpj,
                fornecedor,
                secretaria,
                COUNT(DISTINCT numero_contrato) AS n_contratos,
                SUM(valor_referencia_brl) AS total_brl,
                COUNT(DISTINCT sancao_tipo) AS n_tipos
            FROM contratos_sancionados
            GROUP BY 1, 2, 3
            ORDER BY total_brl DESC NULLS LAST
            LIMIT 200
            """
        ).fetchall()
        for cnpj, fornecedor, secretaria, n_contratos, total_brl, n_tipos in sancao_rows:
            exposure = float(total_brl or 0.0)
            insight_id = "INS_" + hashlib.sha1(
                f"{KIND_SANCAO}|{cnpj}|{secretaria}".encode("utf-8")
            ).hexdigest()[:16]
            payload.append(
                (
                    insight_id,
                    KIND_SANCAO,
                    "CRITICAL",
                    95,
                    exposure,
                    f"RB SUS: fornecedor sancionado {fornecedor or cnpj} contratado por {secretaria}",
                    (
                        f"**{fornecedor or cnpj}** (`{cnpj}`) aparece com **{n_tipos} tipo(s) de sanção ativa** "
                        f"e, ainda assim, consta em **{n_contratos} contrato(s)** do SUS municipal de Rio Branco "
                        f"em **{secretaria}**, totalizando **R$ {exposure:,.2f}**."
                    ),
                    "contrato_sus_fornecedor_sancionado_municipal",
                    json.dumps(
                        [
                            "rb_contratos",
                            "sancoes_collapsed",
                            "transparencia.riobranco.ac.gov.br/contrato",
                        ],
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        ["CEIS", "CNEP", "SUS", "SEMSA", "RIO_BRANCO", "sancionado"],
                        ensure_ascii=False,
                    ),
                    int(n_contratos or 0),
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
                    "transparencia.riobranco.ac.gov.br/contrato + CGU",
                )
            )

    if payload:
        con.executemany(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md, pattern,
                sources, tags, sample_n, unit_total, created_at, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[:12] + (datetime.now(),) + row[12:] for row in payload],
        )
    return len(payload)


def collect_year(
    session: RioBrancoContratoSession,
    *,
    ano: int,
    limit_secretarias: int | None,
    secretaria_contains: str,
    objeto: str,
    enrich_details: bool,
) -> list[dict[str, object]]:
    log.info("Coletando contratos para %d", ano)
    exercicio_id = session.year_options[ano]
    query = secretaria_contains or " "
    secretarias = session.list_secretarias(query=query)
    if not secretarias and secretaria_contains:
        secretarias = session.list_secretarias(query=" ")
        needle = normalize_text(secretaria_contains)
        secretarias = [
            secretaria
            for secretaria in secretarias
            if needle in normalize_text(secretaria["secretaria_nome"])
        ]
    if not secretarias and "semsa" in normalize_text(secretaria_contains):
        secretarias = [
            {
                "secretaria_id": SEMSA_FALLBACK_ID,
                "secretaria_nome": SEMSA_FALLBACK_LABEL,
            }
        ]
        log.info("Autocomplete silencioso; usando fallback hardcoded da SEMSA.")
    if not secretarias and not secretaria_contains:
        secretarias = [{"secretaria_id": "", "secretaria_nome": ""}]
    if limit_secretarias is not None:
        secretarias = secretarias[:limit_secretarias]

    rows: list[dict[str, object]] = []
    for index, secretaria in enumerate(secretarias, start=1):
        log.info(
            "  [%d/%d] %s (%s) — ano %d",
            index,
            len(secretarias),
            secretaria["secretaria_nome"] or "sem_filtro",
            secretaria["secretaria_id"] or "",
            ano,
        )
        page_html = session.search_html(
            ano=ano,
            secretaria_id=secretaria["secretaria_id"],
            secretaria_nome=secretaria["secretaria_nome"],
            objeto=objeto,
        )
        batch = parse_result_rows(
            page_html,
            ano=ano,
            exercicio_id=exercicio_id,
            origem_coleta="html_full_post",
            secretaria_filtro_id=secretaria["secretaria_id"],
            secretaria_filtro_nome=secretaria["secretaria_nome"],
        )
        if enrich_details:
            for row in batch:
                if row["sus"] and row["detail_url"] and not row["cnpj"]:
                    fornecedor, cnpj = enrich_contract_detail(session.session, str(row["detail_url"]))
                    if fornecedor:
                        row["fornecedor"] = fornecedor
                    if cnpj:
                        row["cnpj"] = cnpj
                    time.sleep(session.delay * 0.3)
        rows.extend(batch)
        log.info("    -> %d contratos | acumulado %d", len(batch), len(rows))
        time.sleep(session.delay)
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza o relatório de contratos do portal de Rio Branco."
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Caminho do DuckDB de destino.",
    )
    parser.add_argument("--anos", nargs="+", type=int, required=True, help="Anos a coletar no portal.")
    parser.add_argument(
        "--limit-secretarias",
        type=int,
        default=None,
        help="Limita secretarias no fallback por autocomplete.",
    )
    parser.add_argument(
        "--secretaria-contains",
        default="",
        help="Filtra secretarias por trecho no nome, útil para testar saúde/SEMSA.",
    )
    parser.add_argument(
        "--objeto",
        default="",
        help="Filtro livre de objeto do contrato.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Não grava no banco; apenas mostra uma amostra do que foi coletado.",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Não tenta enriquecer fornecedor/CNPJ via página de detalhe.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Delay entre ações JSF. Padrão: {DEFAULT_DELAY}s.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = None if args.dry_run else duckdb.connect(str(args.db_path))
    if con is not None:
        ensure_tables(con)

    session = RioBrancoContratoSession(delay=args.delay)
    session.load()

    total_rows = 0
    for ano in args.anos:
        rows = collect_year(
            session,
            ano=ano,
            limit_secretarias=args.limit_secretarias,
            secretaria_contains=args.secretaria_contains,
            objeto=args.objeto,
            enrich_details=not args.no_enrich,
        )
        if args.dry_run:
            sus_rows = [row for row in rows if row["sus"]]
            log.info("[dry-run] ano=%d | total=%d | sus=%d", ano, len(rows), len(sus_rows))
            for row in sus_rows[:10]:
                log.info(
                    "  [SUS] %s | %s | R$ %.2f | cnpj=%s",
                    str(row["secretaria"])[:40],
                    str(row["objeto"])[:70],
                    float(row["valor_referencia_brl"] or 0.0),
                    row["cnpj"] or "(vazio)",
                )
            continue

        inserted = upsert_rows(con, rows)
        total_rows += inserted
        log.info("Ano %d: %d linhas persistidas em rb_contratos", ano, inserted)
        con.execute("CHECKPOINT")

    if con is not None:
        n_sus = build_views(con)
        n_insights = build_insights(con)
        log.info(
            "Resumo final: rb_contratos=%d | v_rb_contratos_sus=%d | insights=%d",
            con.execute("SELECT COUNT(*) FROM rb_contratos").fetchone()[0],
            n_sus,
            n_insights,
        )
        con.close()


if __name__ == "__main__":
    main()
