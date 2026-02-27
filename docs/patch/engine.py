"""
SENTINELA // MOTOR DE INGESTÃO UNIVERSAL
Coleta automatizada de todas as fontes públicas com:
- Rate limiting respeitoso
- Retry com backoff exponencial
- Progresso em tempo real
- Armazenamento incremental no DuckDB
- Detecção automática de novos registros (deduplicação por hash)

USO:
    python -m src.ingest.engine --source rb_diarias
    python -m src.ingest.engine --source tse_doacoes
    python -m src.ingest.engine --priority 1          # roda tudo crítico
    python -m src.ingest.engine --all                 # roda tudo
"""

import argparse
import hashlib
import io
import logging
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich import print as rprint

from .sources_registry import SOURCES, SOURCE_BY_ID, SOURCES_BY_PRIORITY, CollectMethod, DataSource

console = Console()

DB_PATH = Path("data/sentinela_analytics.duckdb")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("sentinela.ingest")


# ─── HTTP CLIENT ──────────────────────────────────────────────────────────────

def make_client() -> httpx.Client:
    return httpx.Client(
        timeout=30,
        headers={
            "User-Agent": (
                "Sentinela/1.0 (Controle Social - Dados Públicos; "
                "https://github.com/walbarellos/Sentinela)"
            ),
            "Accept": "application/json, text/csv, */*",
        },
        follow_redirects=True,
    )


def fetch_with_retry(
    client: httpx.Client,
    url: str,
    method: str = "GET",
    max_retries: int = 3,
    **kwargs,
) -> httpx.Response:
    for attempt in range(max_retries):
        try:
            resp = client.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            log.warning(f"Retry {attempt + 1}/{max_retries} para {url} — aguardando {wait}s — {e}")
            time.sleep(wait)


# ─── DUCKDB HELPERS ───────────────────────────────────────────────────────────

def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def upsert_df(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str) -> int:
    """
    Insere DataFrame no DuckDB. Cria tabela se não existir.
    Adiciona coluna 'row_hash' para deduplicação e 'capturado_em'.
    Retorna número de linhas novas inseridas.
    """
    if df.empty:
        return 0

    # Normaliza nomes de colunas
    df.columns = [
        c.lower()
         .strip()
         .replace(" ", "_")
         .replace("-", "_")
         .replace("/", "_")
         .replace("(", "")
         .replace(")", "")
        for c in df.columns
    ]

    df["capturado_em"] = datetime.utcnow().isoformat()

    # Hash por linha para dedup
    df["row_hash"] = df.drop(columns=["capturado_em"], errors="ignore").apply(
        lambda row: hashlib.md5(str(row.values).encode()).hexdigest(),
        axis=1,
    )

    # Cria tabela se não existir
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0
    """)

    # Verifica quais hashes já existem
    try:
        existing = set(
            conn.execute(f"SELECT row_hash FROM {table}").fetchdf()["row_hash"].tolist()
        )
    except Exception:
        existing = set()

    new_rows = df[~df["row_hash"].isin(existing)]

    if not new_rows.empty:
        conn.execute(f"INSERT INTO {table} SELECT * FROM new_rows")

    return len(new_rows)


# ─── COLETORES POR MÉTODO ─────────────────────────────────────────────────────

class CollectorBase:
    def __init__(self, source: DataSource, client: httpx.Client):
        self.source = source
        self.client = client

    def collect(self) -> pd.DataFrame:
        raise NotImplementedError


class RestJsonCollector(CollectorBase):
    """
    API REST que retorna JSON paginado.
    Detecta automaticamente: 'next', 'pagina', 'page', 'offset'.
    """

    def collect(self) -> pd.DataFrame:
        frames = []
        url = self.source.base_url
        params = dict(self.source.params)
        page = 1

        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"[cyan]{self.source.name}", total=None)

            while url:
                params["pagina"] = page
                params["page"] = page

                try:
                    resp = fetch_with_retry(self.client, url, params=params)
                except Exception as e:
                    console.print(f"[red]ERRO {self.source.id}: {e}")
                    break

                data = resp.json()

                # Normaliza resposta — vários formatos de API paginada
                rows = None
                if isinstance(data, list):
                    rows = data
                    next_url = None
                elif isinstance(data, dict):
                    for key in ["data", "results", "registros", "itens", "content"]:
                        if key in data and isinstance(data[key], list):
                            rows = data[key]
                            break
                    if rows is None:
                        rows = [data]
                    next_url = data.get("next") or data.get("proxima_pagina")

                if not rows:
                    break

                frames.append(pd.DataFrame(rows))
                progress.update(task, advance=len(rows), description=f"[cyan]{self.source.name} — {sum(len(f) for f in frames)} registros")

                # Paginação
                if next_url:
                    url = next_url
                    params = {}
                    page = 1
                elif len(rows) == 0:
                    break
                else:
                    page += 1
                    # Stop se página vazia
                    if len(rows) < 10:
                        break

                time.sleep(0.35)  # rate limit gentil

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class CsvBulkCollector(CollectorBase):
    """Download de CSV ou ZIP contendo CSV."""

    def collect(self) -> pd.DataFrame:
        console.print(f"[yellow]↓ Download: {self.source.name}...")

        try:
            resp = fetch_with_retry(self.client, self.source.base_url)
        except Exception as e:
            console.print(f"[red]ERRO download {self.source.id}: {e}")
            return pd.DataFrame()

        content_type = resp.headers.get("content-type", "")
        data = resp.content

        # ZIP → extrair CSV
        if "zip" in content_type or self.source.base_url.endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
                    if not csv_files:
                        console.print(f"[red]ZIP sem CSV: {self.source.id}")
                        return pd.DataFrame()

                    frames = []
                    for csv_file in csv_files:
                        with zf.open(csv_file) as f:
                            try:
                                df = pd.read_csv(
                                    f,
                                    encoding="latin1",
                                    sep=";",
                                    low_memory=False,
                                    on_bad_lines="skip",
                                )
                                frames.append(df)
                            except Exception as e:
                                log.warning(f"Erro lendo {csv_file}: {e}")
                    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            except zipfile.BadZipFile:
                pass

        # CSV direto
        try:
            return pd.read_csv(
                io.BytesIO(data),
                encoding="latin1",
                sep=";",
                low_memory=False,
                on_bad_lines="skip",
            )
        except Exception:
            try:
                return pd.read_csv(io.BytesIO(data), low_memory=False)
            except Exception as e:
                console.print(f"[red]Falha parse CSV {self.source.id}: {e}")
                return pd.DataFrame()


class QueridoDiarioCollector(CollectorBase):
    """
    API do Querido Diário — diários oficiais municipais OCR-izados.
    https://queridodiario.ok.org.br/api
    """

    SEARCH_TERMS = [
        "diária", "portaria", "concessão de diária",
        "dispensa de licitação", "fracionamento",
        "inexigibilidade", "emergência", "urgência",
        "contratação direta",
    ]

    def collect(self) -> pd.DataFrame:
        frames = []
        territory_id = self.source.params.get("territory_id", "1200401")

        for term in self.SEARCH_TERMS:
            params = {
                "territory_ids": territory_id,
                "querystring": term,
                "excerpt_size": 500,
                "number_of_excerpts": 10,
                "size": 100,
                "offset": 0,
            }
            try:
                resp = fetch_with_retry(
                    self.client,
                    "https://queridodiario.ok.org.br/api/gazettes",
                    params=params,
                )
                data = resp.json()
                gazettes = data.get("gazettes", [])
                for g in gazettes:
                    for excerpt in g.get("excerpts", []):
                        frames.append({
                            "data": g.get("date"),
                            "url": g.get("url"),
                            "termo_busca": term,
                            "trecho": excerpt,
                            "territorio": territory_id,
                        })
            except Exception as e:
                log.warning(f"QD erro para '{term}': {e}")
            time.sleep(0.5)

        return pd.DataFrame(frames) if frames else pd.DataFrame()


class SiconfiCollector(CollectorBase):
    """
    API SICONFI — Tesouro Nacional.
    Coleta RREO (RCL) e RGF (despesa pessoal) para Rio Branco.
    """

    def collect(self) -> pd.DataFrame:
        # RREO Anexo 1 — Balanço Orçamentário (tem a RCL)
        anos = [2022, 2023, 2024]
        frames = []

        for ano in anos:
            for periodo in range(1, 7):  # bimestres
                url = (
                    f"https://siconfi.tesouro.gov.br/siconfi/api/public/relatorio/rreo"
                    f"?co_municipio=1200401&an_exercicio={ano}&nr_periodo={periodo}"
                    f"&co_tipo_demonstrativo=RREO&no_anexo=RREO-Anexo%2001"
                )
                try:
                    resp = fetch_with_retry(self.client, url)
                    data = resp.json()
                    items = data.get("items", [])
                    for item in items:
                        item["ano"] = ano
                        item["periodo"] = periodo
                        frames.append(item)
                    time.sleep(0.3)
                except Exception as e:
                    log.warning(f"SICONFI {ano}-{periodo}: {e}")

        return pd.DataFrame(frames) if frames else pd.DataFrame()


class HtmlScrapeCollector(CollectorBase):
    """Scraper HTML genérico com BeautifulSoup."""

    def collect(self) -> pd.DataFrame:
        try:
            resp = fetch_with_retry(self.client, self.source.base_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Tenta extrair tabelas HTML
            tables = soup.find_all("table")
            if tables:
                frames = []
                for table in tables:
                    try:
                        df = pd.read_html(str(table))[0]
                        frames.append(df)
                    except Exception:
                        pass
                if frames:
                    return pd.concat(frames, ignore_index=True)

            # Fallback: extrai links e texto de itens
            items = []
            for row in soup.select("tr, .item, .row, li"):
                text = row.get_text(separator=" | ", strip=True)
                if text:
                    items.append({"texto": text, "url_origem": self.source.base_url})

            return pd.DataFrame(items) if items else pd.DataFrame()

        except Exception as e:
            console.print(f"[red]HTML scrape erro {self.source.id}: {e}")
            return pd.DataFrame()


class JsfScrapeCollector(CollectorBase):
    """
    Portal JSF/PrimeFaces de Rio Branco.
    Reutiliza o jsf_client.py existente no repositório.
    """

    def collect(self) -> pd.DataFrame:
        console.print(
            f"[yellow]⚠ {self.source.id}: Portal JSF — use src/ingest/riobranco_*.py existentes "
            f"(jsf_client.py). Este engine não reimplementa o JSF driver."
        )
        return pd.DataFrame()


# ─── FACTORY ──────────────────────────────────────────────────────────────────

COLLECTORS = {
    CollectMethod.REST_JSON: RestJsonCollector,
    CollectMethod.CSV_DIRECT: RestJsonCollector,  # mesma lógica de paginação
    CollectMethod.CSV_BULK: CsvBulkCollector,
    CollectMethod.QUERIDO_DIARIO: QueridoDiarioCollector,
    CollectMethod.SICONFI: SiconfiCollector,
    CollectMethod.HTML_SCRAPE: HtmlScrapeCollector,
    CollectMethod.JSF_SCRAPE: JsfScrapeCollector,
}


# ─── PIPELINE ─────────────────────────────────────────────────────────────────

def run_source(source: DataSource, client: httpx.Client, conn: duckdb.DuckDBPyConnection) -> dict:
    start = time.time()
    result = {
        "source_id": source.id,
        "source_name": source.name,
        "rows_new": 0,
        "error": None,
        "duration_s": 0,
    }

    try:
        collector_cls = COLLECTORS[source.method]
        collector = collector_cls(source, client)
        df = collector.collect()

        if not df.empty:
            rows_new = upsert_df(conn, df, source.table)
            result["rows_new"] = rows_new
            console.print(
                f"[green]✓[/green] [bold]{source.name}[/bold] — "
                f"[cyan]{len(df)}[/cyan] lidos, "
                f"[green]{rows_new}[/green] novos → tabela [bold]{source.table}[/bold]"
            )
        else:
            console.print(f"[dim]○ {source.name} — sem dados[/dim]")

    except Exception as e:
        result["error"] = str(e)
        console.print(f"[red]✗ {source.name}: {e}[/red]")
        log.exception(f"Erro coletando {source.id}")

    result["duration_s"] = round(time.time() - start, 1)
    return result


def run_pipeline(
    sources: list[DataSource],
    dry_run: bool = False,
) -> list[dict]:
    results = []
    conn = get_conn()

    with make_client() as client:
        for source in sources:
            if not source.active:
                console.print(f"[dim]— {source.name} (inativo)[/dim]")
                continue

            console.rule(f"[bold cyan]{source.id}[/bold cyan]")

            if dry_run:
                console.print(f"[dim]DRY RUN: {source.name} → {source.base_url}[/dim]")
                continue

            result = run_source(source, client, conn)
            results.append(result)

    # Sumário
    if results and not dry_run:
        table = Table(title="SUMÁRIO DA COLETA", border_style="cyan")
        table.add_column("Fonte", style="white")
        table.add_column("Novos", style="green", justify="right")
        table.add_column("Tempo", style="dim", justify="right")
        table.add_column("Status", justify="center")

        for r in results:
            status = "[red]ERRO[/red]" if r["error"] else "[green]OK[/green]"
            table.add_row(
                r["source_name"],
                str(r["rows_new"]),
                f"{r['duration_s']}s",
                status,
            )

        console.print(table)

    conn.close()
    return results


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SENTINELA — Motor de Ingestão de Dados Públicos"
    )
    parser.add_argument("--source", help="ID da fonte (ex: rb_diarias, tse_doacoes)")
    parser.add_argument("--priority", type=int, help="Roda todas as fontes de prioridade N")
    parser.add_argument("--all", action="store_true", help="Roda todas as fontes ativas")
    parser.add_argument("--list", action="store_true", help="Lista fontes disponíveis")
    parser.add_argument("--dry-run", action="store_true", help="Não coleta, apenas lista")
    args = parser.parse_args()

    if args.list or args.dry_run:
        table = Table(title="FONTES DISPONÍVEIS", border_style="cyan")
        table.add_column("ID", style="cyan")
        table.add_column("Nome", style="white")
        table.add_column("Método", style="yellow")
        table.add_column("Prior.", style="bold", justify="center")
        table.add_column("Tabela", style="dim")
        for s in SOURCES_BY_PRIORITY:
            table.add_row(
                s.id, s.name, s.method.value, str(s.priority), s.table
            )
        console.print(table)
        if not args.dry_run:
            return

    if args.source:
        if args.source not in SOURCE_BY_ID:
            console.print(f"[red]Fonte '{args.source}' não encontrada. Use --list.[/red]")
            return
        sources = [SOURCE_BY_ID[args.source]]

    elif args.priority:
        sources = [s for s in SOURCES_BY_PRIORITY if s.priority <= args.priority]
        console.print(f"[cyan]{len(sources)} fontes de prioridade ≤ {args.priority}[/cyan]")

    elif args.all or args.dry_run:
        sources = SOURCES_BY_PRIORITY

    else:
        parser.print_help()
        return

    run_pipeline(sources, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
