"""
SENTINELA // SCRAPER JSF — transparencia.riobranco.ac.gov.br
Engenharia reversa do portal PrimeFaces de Rio Branco.

COMO FUNCIONA O JSF:
1. GET na página → recebe HTML com ViewState (token de sessão)
2. POST de volta com ViewState + parâmetros do componente PrimeFaces
3. Resposta em XML partial-response ou HTML atualizado
4. Exportação CSV via botão p:commandButton que faz POST

ESTRATÉGIA:
- Prioritizar botão de exportação CSV (quando existe)
- Fallback: paginar via AJAX (PrimeFaces p:dataTable lazy loading)
- Última opção: parsear HTML renderizado página a página

USO:
    python -m src.ingest.riobranco_jsf_v2 --endpoint diaria
    python -m src.ingest.riobranco_jsf_v2 --endpoint obra
    python -m src.ingest.riobranco_jsf_v2 --endpoint servidor
    python -m src.ingest.riobranco_jsf_v2 --endpoint despesa
    python -m src.ingest.riobranco_jsf_v2 --endpoint contratacao
"""

import argparse
import re
import time
import logging
from io import StringIO
from pathlib import Path

import duckdb
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from rich.console import Console

console = Console()
log = logging.getLogger("sentinela.jsf")

DB_PATH = "data/sentinela_analytics.duckdb"
BASE_URL = "https://transparencia.riobranco.ac.gov.br"

ENDPOINTS = {
    "diaria":      f"{BASE_URL}/diaria/",
    "obra":        f"{BASE_URL}/obra/",
    "servidor":    f"{BASE_URL}/servidor/",
    "despesa":     f"{BASE_URL}/despesa/",
    "contratacao": f"{BASE_URL}/contratacao/",
    "receita":     f"{BASE_URL}/receita/",
}

TABLE_MAP = {
    "diaria":      "diarias",
    "obra":        "obras",
    "servidor":    "servidores",
    "despesa":     "despesas",
    "contratacao": "licitacoes",
    "receita":     "receitas",
}


class JsfSession:
    """
    Sessão persistente com um portal JSF/PrimeFaces.
    Gerencia cookies, ViewState e partial responses.
    """

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.Client(
            timeout=60,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
            },
            follow_redirects=True,
        )
        self.view_state: str | None = None
        self.form_id: str | None = None

    def get_page(self, url: str) -> BeautifulSoup:
        """Carrega a página e extrai ViewState."""
        resp = self.client.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        self._extract_view_state(soup)
        return soup

    def _extract_view_state(self, soup: BeautifulSoup):
        """Extrai javax.faces.ViewState do HTML."""
        vs = soup.find("input", {"name": "javax.faces.ViewState"})
        if vs:
            self.view_state = vs.get("value", "")

        # Detecta form principal
        forms = soup.find_all("form")
        if forms:
            self.form_id = forms[0].get("id", "form")

    def find_export_button(self, soup: BeautifulSoup) -> str | None:
        """
        Procura botão de exportação CSV/Excel no PrimeFaces.
        Exemplos de IDs comuns: 'btnExportar', 'btnCsv', 'exportBtn', 'j_idt123'
        """
        # Botões com texto ou título relacionado a export
        export_keywords = ["exportar", "csv", "excel", "baixar", "download", "xls"]

        for btn in soup.find_all(["button", "a", "input"]):
            text = (btn.get_text() or btn.get("value") or btn.get("title") or "").lower()
            if any(kw in text for kw in export_keywords):
                return btn.get("id") or btn.get("name")

        # PrimeFaces p:commandButton para export tem classe específica
        for btn in soup.select("[class*='export'], [id*='export'], [id*='Export'], [id*='csv'], [id*='Csv']"):
            return btn.get("id") or btn.get("name")

        return None

    def try_csv_export(self, page_url: str, button_id: str) -> pd.DataFrame | None:
        """
        Tenta exportar CSV via botão PrimeFaces.
        PrimeFaces DataExporter faz um POST e retorna o arquivo diretamente.
        """
        if not self.view_state:
            return None

        # POST simulando clique no botão de export
        form_data = {
            "javax.faces.ViewState": self.view_state,
            "javax.faces.source": button_id,
            "javax.faces.partial.ajax": "true",
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "@all",
            button_id: button_id,
        }
        if self.form_id:
            form_data[self.form_id] = self.form_id
            form_data[f"{self.form_id}_SUBMIT"] = "1"

        try:
            resp = self.client.post(
                page_url,
                data=form_data,
                headers={
                    "Faces-Request": "partial/ajax",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": page_url,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )

            content_type = resp.headers.get("content-type", "")

            # Resposta direta de CSV
            if "text/csv" in content_type or "application/vnd" in content_type:
                return pd.read_csv(StringIO(resp.text), sep=";", encoding="latin1")

            # Resposta XML com redirect para arquivo
            if "xml" in content_type or resp.text.strip().startswith("<?xml"):
                # PrimeFaces partial response pode ter <redirect url="..."/>
                match = re.search(r'<redirect url="([^"]+)"', resp.text)
                if match:
                    file_url = match.group(1)
                    if not file_url.startswith("http"):
                        file_url = self.base_url + file_url
                    file_resp = self.client.get(file_url)
                    return pd.read_csv(StringIO(file_resp.text), sep=";", encoding="latin1")

        except Exception as e:
            log.debug(f"Export via botão falhou: {e}")

        return None

    def paginate_table(
        self,
        page_url: str,
        soup: BeautifulSoup,
        max_pages: int = 500,
    ) -> pd.DataFrame:
        """
        Paginação AJAX do p:dataTable PrimeFaces.
        Cada página é requisitada via POST com parâmetro de página.
        """
        frames = []
        page = 0

        # Detecta o ID do dataTable
        datatable_id = self._find_datatable_id(soup)
        if not datatable_id:
            console.print("[yellow]⚠ Nenhum dataTable detectado — parseando HTML estático[/yellow]")
            return self._parse_html_table(soup)

        console.print(f"[cyan]DataTable detectado: {datatable_id}[/cyan]")

        while page < max_pages:
            if page == 0:
                # Primeira página já está no HTML
                df = self._parse_html_table(soup)
                if not df.empty:
                    frames.append(df)
                    console.print(f"[dim]Pág. 0: {len(df)} registros[/dim]")
            else:
                # Requisição AJAX para próxima página
                df = self._ajax_paginate(page_url, datatable_id, page)
                if df is None or df.empty:
                    break
                frames.append(df)
                console.print(f"[dim]Pág. {page}: {len(df)} registros[/dim]")

            page += 1
            time.sleep(0.8)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _find_datatable_id(self, soup: BeautifulSoup) -> str | None:
        """Encontra o ID do componente p:dataTable no HTML."""
        # PrimeFaces renderiza com role="grid" ou class contendo "ui-datatable"
        table = soup.find(attrs={"role": "grid"})
        if table:
            return table.get("id", "").replace("_data", "").replace(":data", "")

        table = soup.select_one(".ui-datatable")
        if table:
            return table.get("id", "")

        # Fallback: qualquer tabela com thead
        table = soup.find("table")
        if table:
            return table.get("id", "form:tabela")

        return None

    def _ajax_paginate(
        self, page_url: str, datatable_id: str, page: int
    ) -> pd.DataFrame | None:
        """
        Simula clique no botão de próxima página do p:dataTable.
        O PrimeFaces envia: datatable_id + '_pagination' como source.
        """
        if not self.view_state:
            return None

        # rows por página (tenta 50)
        first_row = page * 50

        form_data = {
            "javax.faces.ViewState": self.view_state,
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": datatable_id,
            "javax.faces.partial.execute": datatable_id,
            "javax.faces.partial.render": datatable_id,
            f"{datatable_id}_pagination": "true",
            f"{datatable_id}_first": str(first_row),
            f"{datatable_id}_rows": "50",
            f"{datatable_id}_page": str(page + 1),
        }

        if self.form_id:
            form_data[self.form_id] = self.form_id
            form_data[f"{self.form_id}_SUBMIT"] = "1"

        try:
            resp = self.client.post(
                page_url,
                data=form_data,
                headers={
                    "Faces-Request": "partial/ajax",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": page_url,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                timeout=30,
            )

            # Resposta é XML com CDATA contendo HTML
            xml_content = resp.text

            # Extrai conteúdo do CDATA
            cdata_match = re.search(r"<!\[CDATA\[(.*?)\]\]>", xml_content, re.DOTALL)
            if cdata_match:
                html_fragment = cdata_match.group(1)
                soup = BeautifulSoup(html_fragment, "html.parser")
                df = self._parse_html_table(soup)
                if not df.empty:
                    # Atualiza ViewState se veio na resposta
                    vs_match = re.search(r"javax\.faces\.ViewState[^>]*value=\"([^\"]+)\"", xml_content)
                    if vs_match:
                        self.view_state = vs_match.group(1)
                    return df

            # Se HTML sem CDATA
            if "<tr" in xml_content:
                soup = BeautifulSoup(xml_content, "html.parser")
                return self._parse_html_table(soup)

        except Exception as e:
            log.debug(f"AJAX paginate erro pág {page}: {e}")

        return None

    def _parse_html_table(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Extrai dados de tabela HTML renderizada pelo PrimeFaces."""
        tables = soup.find_all("table")
        if not tables:
            return pd.DataFrame()

        frames = []
        for table in tables:
            # Extrai headers
            headers = []
            thead = table.find("thead")
            if thead:
                headers = [th.get_text(strip=True) for th in thead.find_all(["th", "td"])]

            # Extrai linhas
            tbody = table.find("tbody")
            if tbody:
                rows = []
                for tr in tbody.find_all("tr"):
                    cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                    if cells and any(c for c in cells):
                        rows.append(cells)

                if rows:
                    if headers and len(headers) == len(rows[0]):
                        df = pd.DataFrame(rows, columns=headers)
                    else:
                        df = pd.DataFrame(rows)
                    frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def close(self):
        self.client.close()


# ─── PIPELINE POR ENDPOINT ─────────────────────────────────────────────────────

def scrape_endpoint(endpoint: str) -> pd.DataFrame:
    url = ENDPOINTS[endpoint]
    console.print(f"\n[bold cyan]Scraping: {url}[/bold cyan]")

    session = JsfSession(BASE_URL)

    try:
        # 1. Carrega página inicial
        soup = session.get_page(url)
        console.print(f"[green]✓ Página carregada — ViewState: {bool(session.view_state)}[/green]")

        # 2. Tenta exportação CSV (caminho feliz)
        export_btn = session.find_export_button(soup)
        if export_btn:
            console.print(f"[green]Botão de export detectado: {export_btn}[/green]")
            df = session.try_csv_export(url, export_btn)
            if df is not None and not df.empty:
                console.print(f"[green]✓ CSV exportado: {len(df)} registros[/green]")
                return df
            else:
                console.print("[yellow]Export CSV falhou — tentando paginação AJAX[/yellow]")
        else:
            console.print("[yellow]Nenhum botão de export — tentando paginação AJAX[/yellow]")

        # 3. Paginação AJAX
        df = session.paginate_table(url, soup)
        if not df.empty:
            console.print(f"[green]✓ Paginação: {len(df)} registros[/green]")
            return df

        console.print("[red]✗ Nenhum dado extraído[/red]")
        return pd.DataFrame()

    finally:
        session.close()


def save_to_db(df: pd.DataFrame, table: str):
    if df.empty:
        return

    conn = duckdb.connect(DB_PATH)
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    from datetime import datetime
    import hashlib

    df.columns = [
        c.lower().strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
        for c in df.columns
    ]
    df["capturado_em"] = datetime.utcnow().isoformat()
    df["row_hash"] = df.apply(
        lambda row: hashlib.md5(str(row.values).encode()).hexdigest(), axis=1
    )

    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")

    try:
        existing = set(conn.execute(f"SELECT row_hash FROM {table}").fetchdf()["row_hash"])
    except Exception:
        existing = set()

    new = df[~df["row_hash"].isin(existing)]
    if not new.empty:
        conn.execute(f"INSERT INTO {table} SELECT * FROM new")
        console.print(f"[green]✓ {len(new)} novos registros → {table}[/green]")
    else:
        console.print(f"[dim]Sem novos registros em {table}[/dim]")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper JSF — Portal Transparência Rio Branco")
    parser.add_argument(
        "--endpoint",
        choices=list(ENDPOINTS.keys()),
        required=True,
        help="Endpoint para coletar",
    )
    parser.add_argument("--no-save", action="store_true", help="Não salva no DB (debug)")
    args = parser.parse_args()

    df = scrape_endpoint(args.endpoint)

    if not df.empty:
        console.print(df.head(5).to_string())

        if not args.no_save:
            save_to_db(df, TABLE_MAP[args.endpoint])
    else:
        console.print("[red]Nenhum dado coletado[/red]")
        console.print("\n[yellow]DICA: Use as DevTools do browser para capturar o request real:[/yellow]")
        console.print(f"  1. Abra {ENDPOINTS[args.endpoint]}")
        console.print("  2. F12 → Network → filtre por XHR")
        console.print("  3. Clique no botão de exportar / próxima página")
        console.print("  4. Copie o request como curl")
        console.print("  5. Adapte os parâmetros neste scraper")
