from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sys
import time
import unicodedata
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.insight_classification import ensure_insight_classification_columns
from src.ingest.riobranco_http import fetch_html
from src.ingest.riobranco_jsf import extract_viewstate, parse_partial_xml_updates

log = logging.getLogger("sync_rb_despesas")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DEFAULT_DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
BASE_URL = "https://transparencia.riobranco.ac.gov.br/despesa/"
KIND_PREFIX = "RB_SUS_DESPESA"
DEFAULT_DELAY = 0.8
DEFAULT_TIPO_DESPESA = "POR_UNIDADE"
DEFAULT_FORM_ID = "Formulario"
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

DDL_RB_DESPESAS = """
CREATE TABLE IF NOT EXISTS rb_despesas_unidade (
    row_id VARCHAR PRIMARY KEY,
    ano INTEGER,
    exercicio_id VARCHAR,
    origem_coleta VARCHAR,
    tipo_despesa VARCHAR,
    unidade_filtro_id VARCHAR,
    unidade_filtro_nome VARCHAR,
    unidade_relatorio VARCHAR,
    unidade_link VARCHAR,
    orcado_brl DOUBLE,
    atualizado_brl DOUBLE,
    empenhado_brl DOUBLE,
    liquidado_brl DOUBLE,
    pago_brl DOUBLE,
    sus BOOLEAN DEFAULT FALSE,
    sus_keyword VARCHAR,
    raw_json JSON,
    fonte VARCHAR,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", (value or "").upper())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip()


def parse_brl(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    text = text.replace("R$", "").replace(".", "").replace("%", "").strip()
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def classify_sus(unidade: str) -> tuple[bool, str]:
    normalized = normalize_text(unidade)
    for keyword in SUS_KEYWORDS:
        normalized_keyword = normalize_text(keyword)
        if re.search(rf"(?<![A-Z0-9]){re.escape(normalized_keyword)}(?![A-Z0-9])", normalized):
            return True, keyword
    return False, ""


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_RB_DESPESAS)


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


class RioBrancoDespesaSession:
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
        self.form_id = DEFAULT_FORM_ID
        self.year_options: dict[int, str] = {}
        self.controls: dict[str, str] = {}

    def load(self) -> None:
        self.page_html = fetch_html(self.session, BASE_URL, timeout=30)
        self.viewstate = extract_viewstate(self.page_html) or ""
        if not self.viewstate:
            raise RuntimeError("ViewState não encontrado no portal de despesas.")
        self._extract_controls()
        self._extract_year_options()

    def _extract_controls(self) -> None:
        html = self.page_html
        controls = {
            "tipo_despesa": self._search(r'<select id="([^"]+)" name="[^"]+" class="form-control" size="1" onchange="PrimeFaces\.ab\(\{s:this,e:\'change\',p:\'Formulario:j_idt73\'', html),
            "btn_procurar": self._search(r'id="([^"]+:btnProcurar)"', html),
            "year_select": self._search(r'<select id="([^"]+:j_idt79)" name="[^"]+" class="form-control"', html),
            "year_overlay_input": self._search(r'<select id="([^"]+:j_idt84_input)" name="[^"]+" size="2"', html),
            "csv_export": self._search(r"\{'([^']+:j_idt101)':'[^']+:j_idt101'\}", html),
            "autocomplete": "Formulario:acunidade:acunidade",
            "autocomplete_input": "Formulario:acunidade:acunidade_input",
            "autocomplete_hidden": "Formulario:acunidade:acunidade_hinput",
        }
        missing = [key for key, value in controls.items() if not value]
        if missing:
            raise RuntimeError(f"Controles JSF não localizados em /despesa/: {missing}")
        self.controls = controls

    def _extract_year_options(self) -> None:
        soup = BeautifulSoup(self.page_html, "html.parser")
        year_select = soup.find("select", {"id": self.controls["year_select"]})
        if year_select is None:
            raise RuntimeError("Select de exercício não encontrado.")
        year_options: dict[int, str] = {}
        for option in year_select.find_all("option"):
            label = option.get_text(" ", strip=True)
            value = option.get("value", "").strip()
            if not value or not label.isdigit():
                continue
            year_options[int(label)] = value
        if not year_options:
            raise RuntimeError("Nenhum exercício válido encontrado em /despesa/.")
        self.year_options = year_options

    @staticmethod
    def _search(pattern: str, text: str) -> str:
        match = re.search(pattern, text)
        return match.group(1) if match else ""

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
        updates = parse_partial_xml_updates(response.text)
        next_viewstate = updates.get("javax.faces.ViewState")
        if next_viewstate:
            self.viewstate = next_viewstate
        return response.text

    def list_units(self) -> list[dict[str, str]]:
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": self.controls["autocomplete"],
            "javax.faces.partial.execute": self.controls["autocomplete"],
            "javax.faces.partial.render": self.controls["autocomplete"],
            self.controls["autocomplete"]: "",
            f"{self.controls['autocomplete']}_query": " ",
            self.form_id: self.form_id,
            "javax.faces.ViewState": self.viewstate,
        }
        xml_text = self._post_partial(payload)
        root = BeautifulSoup(xml_text, "xml")
        units: dict[str, str] = {}
        for update in root.find_all("update"):
            update_id = update.get("id", "")
            if "acunidade" not in update_id:
                continue
            html_fragment = update.text or ""
            fragment = BeautifulSoup(html_fragment, "html.parser")
            for item in fragment.find_all("li", class_=re.compile("ui-autocomplete-item")):
                unit_id = (item.get("data-item-value") or "").strip()
                unit_name = (item.get("data-item-label") or item.get_text(" ", strip=True)).strip()
                if unit_id and unit_name:
                    units[unit_id] = unit_name
        return [{"unit_id": unit_id, "unit_name": unit_name} for unit_id, unit_name in sorted(units.items(), key=lambda item: item[1])]

    def apply_filters(
        self,
        *,
        ano: int,
        unidade_id: str = "",
        unidade_nome: str = "",
        tipo_despesa: str = DEFAULT_TIPO_DESPESA,
    ) -> None:
        if ano not in self.year_options:
            raise ValueError(f"Ano {ano} não está disponível no portal.")
        exercicio_id = self.year_options[ano]

        if unidade_id and unidade_nome:
            payload_select = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": self.controls["autocomplete"],
                "javax.faces.partial.execute": self.controls["autocomplete"],
                "javax.faces.behavior.event": "itemSelect",
                self.form_id: self.form_id,
                self.controls["autocomplete_input"]: unidade_nome,
                self.controls["autocomplete_hidden"]: unidade_id,
                "javax.faces.ViewState": self.viewstate,
            }
            self._post_partial(payload_select)
            time.sleep(self.delay)

        payload_search = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": self.controls["btn_procurar"],
            "javax.faces.partial.execute": self.form_id,
            "javax.faces.partial.render": self.form_id,
            self.controls["btn_procurar"]: self.controls["btn_procurar"],
            self.form_id: self.form_id,
            self.controls["tipo_despesa"]: tipo_despesa,
            self.controls["year_select"]: exercicio_id,
            self.controls["year_overlay_input"]: exercicio_id,
            self.controls["autocomplete_input"]: unidade_nome,
            self.controls["autocomplete_hidden"]: unidade_id,
            "javax.faces.ViewState": self.viewstate,
        }
        self._post_partial(payload_search)
        time.sleep(self.delay)

    def export_csv(
        self,
        *,
        ano: int,
        unidade_id: str = "",
        unidade_nome: str = "",
        tipo_despesa: str = DEFAULT_TIPO_DESPESA,
    ) -> pd.DataFrame:
        exercicio_id = self.year_options[ano]
        payload = {
            self.form_id: self.form_id,
            self.controls["tipo_despesa"]: tipo_despesa,
            self.controls["year_select"]: exercicio_id,
            self.controls["year_overlay_input"]: exercicio_id,
            self.controls["autocomplete_input"]: unidade_nome,
            self.controls["autocomplete_hidden"]: unidade_id,
            "javax.faces.ViewState": self.viewstate,
            self.controls["csv_export"]: self.controls["csv_export"],
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
        return parse_csv_response(response.content)


def parse_csv_response(content: bytes) -> pd.DataFrame:
    if not content:
        return pd.DataFrame()
    for encoding in ("utf-8", "utf-8-sig", "latin1"):
        try:
            text = content.decode(encoding)
        except Exception:
            continue

        sample = "\n".join(text.splitlines()[:10])
        delimiter = None
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t|")
            delimiter = dialect.delimiter
        except Exception:
            delimiter = None

        candidate_delimiters = [delimiter] if delimiter else []
        candidate_delimiters.extend([";", "\t", ",", "|"])
        seen: set[str] = set()
        for candidate in candidate_delimiters:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            try:
                df = pd.read_csv(StringIO(text), sep=candidate, engine="python")
                if not df.empty or len(df.columns) > 1:
                    return df
            except Exception:
                continue
    raise RuntimeError("Não foi possível interpretar o CSV exportado pelo portal de despesas.")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed: dict[str, str] = {}
    for column in df.columns:
        normalized = unicodedata.normalize("NFKD", str(column))
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
        renamed[column] = normalized.strip("_")
    return df.rename(columns=renamed)


def coerce_report_rows(
    df: pd.DataFrame,
    *,
    ano: int,
    exercicio_id: str,
    origem_coleta: str,
    unidade_filtro_id: str,
    unidade_filtro_nome: str,
    tipo_despesa: str,
) -> list[dict[str, object]]:
    if df.empty:
        return []

    df = normalize_columns(df)
    column_aliases = {
        "unidade": ("unidade",),
        "orcado_brl": ("orcado_r", "orcado"),
        "atualizado_brl": ("atualizado_r", "atualizado"),
        "empenhado_brl": ("empenhado_r", "empenhado"),
        "liquidado_brl": ("liquidado_r", "liquidado"),
        "pago_brl": ("pago_r", "pago"),
    }
    resolved_columns: dict[str, str] = {}
    for target, aliases in column_aliases.items():
        for alias in aliases:
            if alias in df.columns:
                resolved_columns[target] = alias
                break

    if "unidade" not in resolved_columns:
        raise RuntimeError(f"CSV de despesas não trouxe coluna de unidade. Colunas: {list(df.columns)}")

    records: list[dict[str, object]] = []
    for row in df.to_dict("records"):
        unidade = str(row.get(resolved_columns["unidade"], "") or "").strip()
        if not unidade or unidade.upper().startswith("TOTAL "):
            continue
        if unidade == unidade_filtro_nome and "TOTAL" in unidade.upper():
            continue
        sus, sus_keyword = classify_sus(unidade)
        row_id = hashlib.sha1(
            f"{ano}|{tipo_despesa}|{unidade}|{origem_coleta}|{unidade_filtro_id}".encode("utf-8")
        ).hexdigest()
        records.append(
            {
                "row_id": row_id,
                "ano": ano,
                "exercicio_id": exercicio_id,
                "origem_coleta": origem_coleta,
                "tipo_despesa": tipo_despesa,
                "unidade_filtro_id": unidade_filtro_id,
                "unidade_filtro_nome": unidade_filtro_nome,
                "unidade_relatorio": unidade,
                "unidade_link": "",
                "orcado_brl": parse_brl(row.get(resolved_columns.get("orcado_brl", ""), "")),
                "atualizado_brl": parse_brl(row.get(resolved_columns.get("atualizado_brl", ""), "")),
                "empenhado_brl": parse_brl(row.get(resolved_columns.get("empenhado_brl", ""), "")),
                "liquidado_brl": parse_brl(row.get(resolved_columns.get("liquidado_brl", ""), "")),
                "pago_brl": parse_brl(row.get(resolved_columns.get("pago_brl", ""), "")),
                "sus": sus,
                "sus_keyword": sus_keyword,
                "raw_json": json.dumps(row, ensure_ascii=False),
                "fonte": "transparencia.riobranco.ac.gov.br/despesa",
            }
        )
    return records


def upsert_rows(con: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0
    payload = [
        (
            row["row_id"],
            row["ano"],
            row["exercicio_id"],
            row["origem_coleta"],
            row["tipo_despesa"],
            row["unidade_filtro_id"],
            row["unidade_filtro_nome"],
            row["unidade_relatorio"],
            row["unidade_link"],
            row["orcado_brl"],
            row["atualizado_brl"],
            row["empenhado_brl"],
            row["liquidado_brl"],
            row["pago_brl"],
            row["sus"],
            row["sus_keyword"],
            row["raw_json"],
            row["fonte"],
        )
        for row in rows
    ]
    con.executemany(
        """
        INSERT INTO rb_despesas_unidade (
            row_id, ano, exercicio_id, origem_coleta, tipo_despesa, unidade_filtro_id,
            unidade_filtro_nome, unidade_relatorio, unidade_link, orcado_brl, atualizado_brl,
            empenhado_brl, liquidado_brl, pago_brl, sus, sus_keyword, raw_json, fonte, capturado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (row_id) DO UPDATE SET
            exercicio_id = excluded.exercicio_id,
            origem_coleta = excluded.origem_coleta,
            tipo_despesa = excluded.tipo_despesa,
            unidade_filtro_id = excluded.unidade_filtro_id,
            unidade_filtro_nome = excluded.unidade_filtro_nome,
            unidade_relatorio = excluded.unidade_relatorio,
            unidade_link = excluded.unidade_link,
            orcado_brl = excluded.orcado_brl,
            atualizado_brl = excluded.atualizado_brl,
            empenhado_brl = excluded.empenhado_brl,
            liquidado_brl = excluded.liquidado_brl,
            pago_brl = excluded.pago_brl,
            sus = excluded.sus,
            sus_keyword = excluded.sus_keyword,
            raw_json = excluded.raw_json,
            fonte = excluded.fonte,
            capturado_em = excluded.capturado_em
        """,
        [row + (datetime.now(),) for row in payload],
    )
    return len(rows)


def build_views(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rb_despesas_sus AS
        SELECT *
        FROM rb_despesas_unidade
        WHERE sus = TRUE
        """
    )
    return con.execute("SELECT COUNT(*) FROM v_rb_despesas_sus").fetchone()[0]


def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    if not ensure_insight_columns(con):
        return 0

    con.execute("DELETE FROM insight WHERE kind LIKE ?", [f"{KIND_PREFIX}%"])
    rows = con.execute(
        """
        SELECT
            ano,
            unidade_relatorio,
            MAX(pago_brl) AS pago_brl,
            MAX(empenhado_brl) AS empenhado_brl,
            MAX(liquidado_brl) AS liquidado_brl
        FROM rb_despesas_unidade
        WHERE sus = TRUE
        GROUP BY 1, 2
        ORDER BY pago_brl DESC NULLS LAST, unidade_relatorio
        LIMIT 200
        """
    ).fetchall()

    payload = []
    for ano, unidade_relatorio, pago_brl, empenhado_brl, liquidado_brl in rows:
        exposure = float(pago_brl or 0)
        insight_id = "INS_" + hashlib.sha1(
            f"{KIND_PREFIX}|{ano}|{unidade_relatorio}".encode("utf-8")
        ).hexdigest()[:16]
        payload.append(
            (
                insight_id,
                f"{KIND_PREFIX}_UNIDADE_ANO",
                "INFO",
                84,
                exposure,
                f"SUS Rio Branco: {unidade_relatorio} executou R$ {exposure:,.2f} em {ano}",
                (
                    f"A unidade **{unidade_relatorio}** apareceu no portal de despesas como recorte SUS em **{ano}**, "
                    f"com **R$ {float(empenhado_brl or 0):,.2f} empenhados**, "
                    f"**R$ {float(liquidado_brl or 0):,.2f} liquidados** e "
                    f"**R$ {exposure:,.2f} pagos**."
                ),
                "despesa_sus_por_unidade",
                json.dumps(
                    ["rb_despesas_unidade", "transparencia.riobranco.ac.gov.br/despesa"],
                    ensure_ascii=False,
                ),
                json.dumps(
                    ["SUS", "SEMSA", "RIO_BRANCO", "despesas", "unidade"],
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
                "transparencia.riobranco.ac.gov.br/despesa",
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
    session: RioBrancoDespesaSession,
    *,
    ano: int,
    limit_units: int | None,
    unit_contains: str,
) -> list[dict[str, object]]:
    log.info("Coletando despesas por unidade para %d", ano)
    session.apply_filters(ano=ano, tipo_despesa=DEFAULT_TIPO_DESPESA)
    df = session.export_csv(ano=ano, tipo_despesa=DEFAULT_TIPO_DESPESA)
    exercicio_id = session.year_options[ano]
    rows = coerce_report_rows(
        df,
        ano=ano,
        exercicio_id=exercicio_id,
        origem_coleta="export_all_units",
        unidade_filtro_id="",
        unidade_filtro_nome="",
        tipo_despesa=DEFAULT_TIPO_DESPESA,
    )
    if len(rows) > 1:
        log.info("Exportação anual de %d retornou %d linhas.", ano, len(rows))
        return rows

    log.warning("Exportação anual de %d retornou %d linha(s); ativando fallback por unidade.", ano, len(rows))
    units = session.list_units()
    if unit_contains:
        needle = normalize_text(unit_contains)
        units = [unit for unit in units if needle in normalize_text(unit["unit_name"])]
    if limit_units is not None:
        units = units[:limit_units]
    fallback_rows: list[dict[str, object]] = []
    for index, unit in enumerate(units, start=1):
        session.apply_filters(
            ano=ano,
            unidade_id=unit["unit_id"],
            unidade_nome=unit["unit_name"],
            tipo_despesa=DEFAULT_TIPO_DESPESA,
        )
        unit_df = session.export_csv(
            ano=ano,
            unidade_id=unit["unit_id"],
            unidade_nome=unit["unit_name"],
            tipo_despesa=DEFAULT_TIPO_DESPESA,
        )
        fallback_rows.extend(
            coerce_report_rows(
                unit_df,
                ano=ano,
                exercicio_id=exercicio_id,
                origem_coleta="export_per_unit",
                unidade_filtro_id=unit["unit_id"],
                unidade_filtro_nome=unit["unit_name"],
                tipo_despesa=DEFAULT_TIPO_DESPESA,
            )
        )
        if index % 20 == 0:
            log.info("  %d/%d unidades processadas no fallback de %d", index, len(units), ano)
        time.sleep(session.delay)
    return fallback_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza o relatório de despesas por unidade do portal de Rio Branco."
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Caminho do DuckDB de destino. Útil para validar sem competir com outra carga em andamento.",
    )
    parser.add_argument("--anos", nargs="+", type=int, required=True, help="Anos a coletar no portal.")
    parser.add_argument("--limit-units", type=int, default=None, help="Limita unidades no fallback por autocomplete.")
    parser.add_argument(
        "--unit-contains",
        default="",
        help="Filtra unidades do fallback por trecho no nome, útil para testar saúde/SEMSA.",
    )
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help=f"Delay entre ações JSF. Padrão: {DEFAULT_DELAY}s.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db_path))
    ensure_tables(con)

    session = RioBrancoDespesaSession(delay=args.delay)
    session.load()

    total_rows = 0
    for ano in args.anos:
        rows = collect_year(
            session,
            ano=ano,
            limit_units=args.limit_units,
            unit_contains=args.unit_contains,
        )
        inserted = upsert_rows(con, rows)
        total_rows += inserted
        log.info("Ano %d: %d linhas persistidas em rb_despesas_unidade", ano, inserted)
        con.execute("CHECKPOINT")

    n_sus = build_views(con)
    n_insights = build_insights(con)
    log.info(
        "Resumo final: rb_despesas_unidade=%d | v_rb_despesas_sus=%d | insights=%d",
        con.execute("SELECT COUNT(*) FROM rb_despesas_unidade").fetchone()[0],
        n_sus,
        n_insights,
    )
    con.close()


if __name__ == "__main__":
    main()
