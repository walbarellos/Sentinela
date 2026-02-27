"""
SENTINELA // CGU (CEIS/CNEP) + PNCP + CNPJ/QSA
Todos os endpoints verificados ao vivo em 27/02/2026.

══════════════════════════════════════════════════════════════
FONTE 1 ── CGU: CEIS + CNEP
  Swagger: https://api.portaldatransparencia.gov.br/swagger-ui/index.html
  Base:    https://api.portaldatransparencia.gov.br/api-de-dados/
  Auth:    header  chave-api-dados: <token>
  Token:   https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
  Limite:  90 req/min (06h-23h59)  |  300 req/min (00h-05h59)
  Resp:    JSON array (lista vazia = fim da paginação)

FONTE 2 ── PNCP: Contratos & Contratações
  Swagger: https://pncp.gov.br/api/consulta/swagger-ui/index.html
  Base:    https://pncp.gov.br/api/consulta/v1/
  Auth:    nenhuma  (pública, sem token)
  ⚠️ PARÂMETROS CORRETOS (verificados):
    /contratos           → cnpjOrgao (CNPJ da Prefeitura, 14 dígitos SEM pontuação)
    /contratacoes/...    → codigoMunicipioIbge + uf
  CNPJ Prefeitura Rio Branco: 04034583000194

FONTE 3 ── CNPJ / QSA  (BrasilAPI verificada ao vivo)
  Endpoint: https://brasilapi.com.br/api/cnpj/v1/{cnpj}
  Auth:     nenhuma
  Schema real (campos confirmados da resposta ao vivo):
    cnpj, razao_social, nome_fantasia, descricao_situacao_cadastral,
    capital_social, data_inicio_atividade, descricao_porte,
    cnae_fiscal, cnae_fiscal_descricao, municipio, uf, email,
    qsa[].nome_socio, qsa[].cnpj_cpf_do_socio,
    qsa[].qualificacao_socio, qsa[].data_entrada_sociedade,
    qsa[].faixa_etaria

USO:
  # [Obrigatório 1x] Cadastre o token CGU:
  # https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
  export CGU_API_TOKEN="seu-token-aqui"

  python -m src.ingest.cgu_pncp_cnpj --ceis
  python -m src.ingest.cgu_pncp_cnpj --cnep
  python -m src.ingest.cgu_pncp_cnpj --pncp-contratos
  python -m src.ingest.cgu_pncp_cnpj --pncp-dispensas
  python -m src.ingest.cgu_pncp_cnpj --cnpj
  python -m src.ingest.cgu_pncp_cnpj --cross
  python -m src.ingest.cgu_pncp_cnpj --all
══════════════════════════════════════════════════════════════
"""

import argparse
import hashlib
import logging
import os
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import duckdb
import httpx
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, track

console = Console()
log = logging.getLogger("sentinela")
logging.basicConfig(level=logging.WARNING)

DB_PATH = Path("data/sentinela_analytics.duckdb")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── Identificadores de Rio Branco ─────────────────────────────────────────────
RB_IBGE            = "1200401"
RB_CNPJ_PREFEITURA = "04034583000194"   # CNPJ da Prefeitura de Rio Branco/AC
RB_UF              = "AC"


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def upsert(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str) -> int:
    """Insere linhas novas (deduplicação por hash). Cria tabela se não existir."""
    if df.empty:
        return 0
    df = df.copy()
    df.columns = [
        c.lower().strip()
         .replace(" ", "_").replace("/", "_")
         .replace("(", "").replace(")", "").replace("-", "_")
        for c in df.columns
    ]
    df["capturado_em"] = datetime.utcnow().isoformat()
    df["row_hash"] = df.apply(
        lambda r: hashlib.md5(str(r.values).encode()).hexdigest(), axis=1
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")
    try:
        existing = set(conn.execute(f"SELECT row_hash FROM {table}").fetchdf()["row_hash"])
    except Exception:
        existing = set()
    new = df[~df["row_hash"].isin(existing)]
    if not new.empty:
        conn.execute(f"INSERT INTO {table} SELECT * FROM new")
    return len(new)


def clean_cnpj(v) -> str:
    return "".join(filter(str.isdigit, str(v or "")))


def cgu_client(token: str) -> httpx.Client:
    return httpx.Client(
        timeout=60, follow_redirects=True,
        headers={
            "chave-api-dados": token,
            "Accept": "application/json",
            "User-Agent": "Sentinela/1.0 (Controle Social - github.com/walbarellos/Sentinela)",
        },
    )


def pncp_client() -> httpx.Client:
    return httpx.Client(
        timeout=60, follow_redirects=True,
        headers={
            "Accept": "application/json",
            "User-Agent": "Sentinela/1.0 (Controle Social)",
        },
    )


def get_token() -> str:
    token = os.environ.get("CGU_API_TOKEN", "")
    if not token:
        cfg = Path("config/cgu_token.txt")
        if cfg.exists():
            token = cfg.read_text().strip()
    if not token:
        console.print(
            "[red]Token CGU não encontrado.[/red]\n"
            "  1. Cadastre em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email\n"
            "  2. [bold]export CGU_API_TOKEN='seu-token'[/bold]\n"
            "     ou salve em: [bold]config/cgu_token.txt[/bold]"
        )
    return token


# ══════════════════════════════════════════════════════════════════════════════
# FONTE 1 ── CGU CEIS / CNEP
# ══════════════════════════════════════════════════════════════════════════════

CGU_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"


def _paginate_cgu(client: httpx.Client, endpoint: str, extra: dict = {}) -> pd.DataFrame:
    """
    Pagina a API CGU até array vazio.
    pagina (base 1) | quantidade (max 500)
    """
    frames, page = [], 1

    with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}"),
                  BarColumn(), console=console, transient=True) as prog:
        task = prog.add_task("Coletando...", total=None)

        while True:
            try:
                resp = client.get(endpoint, params={"pagina": page, "quantidade": 500, **extra})
            except Exception as e:
                console.print(f"[red]Erro de rede: {e}[/red]")
                break

            if resp.status_code == 401:
                console.print("[red]Token CGU inválido ou expirado. Re-cadastre em portaldatransparencia.gov.br[/red]")
                break
            if resp.status_code == 429:
                console.print("[yellow]Rate limit — aguardando 30s[/yellow]")
                time.sleep(30)
                continue
            if not resp.is_success:
                console.print(f"[red]HTTP {resp.status_code}: {resp.text[:200]}[/red]")
                break

            data = resp.json()
            if not data:  # array vazio = última página
                break

            rows = data if isinstance(data, list) else [data]
            frames.append(pd.DataFrame(rows))
            total = sum(len(f) for f in frames)
            prog.update(task, description=f"CGU — {total:,} registros...")

            page += 1
            time.sleep(0.7)  # 90 req/min → seguro em ~1.4/s

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def ingest_ceis(token: Optional[str] = None) -> int:
    """
    Baixa CEIS completo (Empresas Inidôneas e Suspensas).
    Campos-chave: nomeSancionado, cpfCnpjSancionado, tipoSancao,
                  dataInicioSancao, dataFimSancao (null = indefinida),
                  orgaoSancionador
    """
    console.rule("[bold red]CGU — CEIS[/bold red]")
    token = token or get_token()
    if not token:
        return 0

    with cgu_client(token) as c:
        df = _paginate_cgu(c, f"{CGU_BASE}/ceis")

    if df.empty:
        console.print("[red]CEIS sem dados[/red]")
        return 0

    conn = get_conn()
    n = upsert(conn, df, "cgu_ceis")
    console.print(f"[green]✓ CEIS: {len(df):,} total, {n:,} novos → cgu_ceis[/green]")

    try:
        console.print(conn.execute(
            "SELECT tiposancao, COUNT(*) n FROM cgu_ceis GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
        ).fetchdf().to_string(index=False))
    except Exception:
        pass

    conn.close()
    return n


def ingest_cnep(token: Optional[str] = None) -> int:
    """
    Baixa CNEP (Lei Anticorrupção 12.846/13).
    Campos-chave: nomeSancionado, cpfCnpjSancionado, tipoSancao,
                  valorMulta, acordoLeniencia, fundamentacaoLegal
    """
    console.rule("[bold red]CGU — CNEP[/bold red]")
    token = token or get_token()
    if not token:
        return 0

    with cgu_client(token) as c:
        df = _paginate_cgu(c, f"{CGU_BASE}/cnep")

    conn = get_conn()
    n = upsert(conn, df, "cgu_cnep")
    console.print(f"[green]✓ CNEP: {len(df):,} total, {n:,} novos → cgu_cnep[/green]")
    conn.close()
    return n


def cross_sancoes_contratos(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    CRUZAMENTO CRÍTICO: empresa sancionada (CEIS ou CNEP) × contrato ativo.
    Match por CNPJ limpo (prioritário) e por nome (fallback).
    Base legal: Lei 14.133/21 art.156 + Lei 8.429/92 art.10.
    """
    console.rule("[bold red]CRUZAMENTO: Sancionadas × Contratos Ativos[/bold red]")

    sql = """
        WITH sancoes AS (
            SELECT nomesancionado, cpfcnpjsancionado,
                   tiposancao, datainiciosancao, datafimsancao,
                   orgaosancionador, 'CEIS' AS cadastro
            FROM cgu_ceis
            UNION ALL
            SELECT nomesancionado, cpfcnpjsancionado,
                   tiposancao, datainiciosancao, datafimsancao,
                   orgaosancionador, 'CNEP'
            FROM cgu_cnep
        ),
        vigentes AS (
            SELECT *,
                CASE
                    WHEN datafimsancao IS NULL OR TRIM(datafimsancao) = ''
                        THEN 'INDEFINIDA'
                    WHEN TRY_CAST(datafimsancao AS DATE) >= CURRENT_DATE
                        THEN 'VIGENTE'
                    ELSE 'EXPIRADA'
                END AS status_sancao
            FROM sancoes
        )
        SELECT
            s.nomesancionado      AS empresa_sancionada,
            s.cpfcnpjsancionado   AS cnpj_sancionado,
            s.tiposancao, s.status_sancao,
            s.datainiciosancao, s.datafimsancao,
            s.orgaosancionador, s.cadastro,
            o.empresa_nome AS empresa_contratada,
            o.secretaria, o.data_contrato,
            CAST(o.valor_total AS DOUBLE) AS valor_contrato
        FROM vigentes s
        JOIN obras o ON (
            REGEXP_REPLACE(s.cpfcnpjsancionado, '[^0-9]', '', 'g')
                = REGEXP_REPLACE(COALESCE(o.empresa_id::VARCHAR,''), '[^0-9]', '', 'g')
            OR o.empresa_nome ILIKE
                '%' || SPLIT_PART(UPPER(TRIM(s.nomesancionado)), ' ', 1) || '%'
        )
        WHERE s.status_sancao IN ('VIGENTE', 'INDEFINIDA')
        ORDER BY valor_contrato DESC
    """
    try:
        result = conn.execute(sql).fetchdf()
        if not result.empty:
            console.print(f"[bold red]⚑ {len(result)} CONTRATOS COM EMPRESAS SANCIONADAS![/bold red]")
            for _, r in result.head(10).iterrows():
                console.print(
                    f"  [red]CRÍTICO[/red] {r['empresa_sancionada']} "
                    f"({r['tiposancao']} — {r['status_sancao']}) "
                    f"→ R${r['valor_contrato']:,.0f} em {r['secretaria']}"
                )
            conn.execute("CREATE TABLE IF NOT EXISTS cross_sancoes_contratos AS SELECT * FROM result WHERE 1=0")
            conn.execute("DELETE FROM cross_sancoes_contratos")
            conn.execute("INSERT INTO cross_sancoes_contratos SELECT * FROM result")
        else:
            console.print("[dim]Nenhum match (cgu_ceis/cnep + obras precisam estar carregados)[/dim]")
        return result
    except Exception as e:
        console.print(f"[yellow]{e}[/yellow]")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# FONTE 2 ── PNCP
# ══════════════════════════════════════════════════════════════════════════════
# ⚠️ PARÂMETROS CORRETOS (verificados no catálogo ODA):
#
#  /contratos
#    - dataInicial, dataFinal  (YYYYMMDD)
#    - cnpjOrgao               (CNPJ do órgão, 14 dígitos sem pontuação)
#    - pagina, tamanhoPagina
#
#  /contratacoes/publicacao
#    - dataInicial, dataFinal
#    - codigoMunicipioIbge     (1200401)
#    - uf                      (AC)
#    - codigoModalidadeContratacao  (8=Dispensa, 9=Inexigibilidade)
#    - pagina, tamanhoPagina
#
#  Resp: { "data": [...], "totalPaginas": N, "totalRegistros": N }

PNCP_BASE = "https://pncp.gov.br/api/consulta/v1"

MODALIDADES = {
    6: "Pregão Eletrônico",
    7: "Pregão Presencial",
    8: "Dispensa de Licitação",    # ← foco: fracionamento
    9: "Inexigibilidade",          # ← foco: direcionamento
}


def _paginate_pncp(client: httpx.Client, endpoint: str, params: dict) -> pd.DataFrame:
    """Pagina PNCP (lê totalPaginas da resposta)."""
    frames, page = [], 1

    while True:
        try:
            resp = client.get(endpoint, params={**params, "pagina": page, "tamanhoPagina": 500})
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                break
            console.print(f"[red]PNCP {e.response.status_code}: {e.response.text[:200]}[/red]")
            break
        except Exception as e:
            console.print(f"[red]PNCP rede: {e}[/red]")
            break

        payload = resp.json()

        if isinstance(payload, list):
            rows, total_pages = payload, 1
        else:
            rows = payload.get("data") or payload.get("itens") or []
            total_pages = payload.get("totalPaginas", 1)
            if page == 1:
                console.print(f"  [dim]{payload.get('totalRegistros', '?'):,} registros / {total_pages} páginas[/dim]")

        if not rows:
            break

        frames.append(pd.json_normalize(rows))
        console.print(f"  [dim]Pág {page}/{total_pages}: {len(rows)} registros[/dim]")

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def ingest_pncp_contratos(anos: list[int] = [2022, 2023, 2024]) -> int:
    """
    Contratos da Prefeitura de RB no PNCP.
    Usa cnpjOrgao=04034583000194 — parâmetro correto para /contratos.
    Divide por trimestre para não sobrecarregar.
    """
    console.rule("[bold cyan]PNCP — Contratos (cnpjOrgao = Prefeitura RB)[/bold cyan]")
    conn = get_conn()
    total_new = 0

    with pncp_client() as c:
        for ano in anos:
            for q, (m_ini, m_fim, ult_dia) in enumerate(
                [(1, 3, 31), (4, 6, 30), (7, 9, 30), (10, 12, 31)], start=1
            ):
                di   = f"{ano}{m_ini:02d}01"
                dfim = f"{ano}{m_fim:02d}{ult_dia}"
                console.print(f"\n[cyan]Q{q}/{ano}: {di[:6]} → {dfim[:6]}[/cyan]")

                df = _paginate_pncp(c, f"{PNCP_BASE}/contratos", {
                    "dataInicial": di,
                    "dataFinal":   dfim,
                    "cnpjOrgao":   RB_CNPJ_PREFEITURA,
                })

                if not df.empty:
                    n = upsert(conn, df, "pncp_contratos")
                    total_new += n
                    console.print(f"  [green]{len(df):,} contratos, {n:,} novos[/green]")
                time.sleep(1)

    conn.close()
    console.print(f"\n[green]✓ PNCP contratos: {total_new:,} novos[/green]")
    return total_new


def ingest_pncp_dispensas(anos: list[int] = [2022, 2023, 2024]) -> int:
    """
    Dispensas (8) e Inexigibilidades (9) em Rio Branco via /contratacoes/publicacao.
    Usa codigoMunicipioIbge + uf — parâmetros corretos para este endpoint.
    """
    console.rule("[bold cyan]PNCP — Dispensas & Inexigibilidades (RB)[/bold cyan]")
    conn = get_conn()
    total_new = 0

    with pncp_client() as c:
        for ano in anos:
            for modalidade, nome in [(8, "Dispensa"), (9, "Inexigibilidade")]:
                console.print(f"\n[cyan]{ano} — {nome}[/cyan]")
                df = _paginate_pncp(c, f"{PNCP_BASE}/contratacoes/publicacao", {
                    "dataInicial":                 f"{ano}0101",
                    "dataFinal":                   f"{ano}1231",
                    "uf":                          RB_UF,
                    "codigoMunicipioIbge":         RB_IBGE,
                    "codigoModalidadeContratacao": modalidade,
                })

                if not df.empty:
                    df["modalidade_nome"] = nome
                    n = upsert(conn, df, "pncp_dispensas")
                    total_new += n
                    console.print(f"  [green]{len(df):,} registros, {n:,} novos[/green]")
                time.sleep(1)

    conn.close()
    console.print(f"\n[green]✓ PNCP dispensas: {total_new:,} novos[/green]")
    return total_new


def detect_fracionamento_pncp(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Mesmo fornecedor + mesma secretaria + ≥3 dispensas abaixo do limite
    → total acima de R$57.278,16 em até 180 dias.
    Lei 14.133/21 art. 29, §2°.
    """
    sql = """
        SELECT
            COALESCE(fornecedores_razaosocial, fornecedores_cnpj) AS fornecedor,
            unidadeorgao_nome AS secretaria,
            COUNT(*) AS num,
            SUM(TRY_CAST(valorglobal AS DOUBLE)) AS total,
            MIN(datainclusao) AS primeiro,
            MAX(datainclusao) AS ultimo
        FROM pncp_dispensas
        WHERE modalidade_nome = 'Dispensa'
          AND TRY_CAST(valorglobal AS DOUBLE) < 57278.16
        GROUP BY 1, 2
        HAVING num >= 3 AND total > 57278.16
        ORDER BY total DESC
    """
    try:
        result = conn.execute(sql).fetchdf()
        if not result.empty:
            console.print(f"[red]⚑ {len(result)} casos de possível fracionamento (PNCP)[/red]")
            for _, r in result.head(8).iterrows():
                console.print(
                    f"  {str(r['fornecedor'])[:40]} | {str(r['secretaria'])[:30]} | "
                    f"{int(r['num'])} contratos | R${float(r['total']):,.2f}"
                )
        return result
    except Exception as e:
        console.print(f"[dim]Fracionamento PNCP: {e}[/dim]")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# FONTE 3 ── CNPJ / QSA (BrasilAPI)
# ══════════════════════════════════════════════════════════════════════════════

CNPJ_APIS = [
    "https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
    "https://www.receitaws.com.br/v1/cnpj/{cnpj}",
]

# Sobrenomes muito comuns — excluir do match de nepotismo para reduzir falsos positivos
SOBRENOMES_EXCLUIR = {
    "SILVA", "SANTOS", "OLIVEIRA", "SOUZA", "LIMA", "PEREIRA", "FERREIRA",
    "COSTA", "RODRIGUES", "ALVES", "NASCIMENTO", "CARVALHO", "GOMES",
    "MARTINS", "ARAUJO", "ARAÚJO",
}


def fetch_cnpj(client: httpx.Client, cnpj: str) -> dict | None:
    """BrasilAPI → ReceitaWS em cascata."""
    clean = clean_cnpj(cnpj)
    if len(clean) != 14:
        return None
    for tpl in CNPJ_APIS:
        try:
            resp = client.get(tpl.format(cnpj=clean))
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                console.print(f"[yellow]Rate limit — aguardando 22s[/yellow]")
                time.sleep(22)
                resp = client.get(tpl.format(cnpj=clean))
                if resp.status_code == 200:
                    return resp.json()
        except Exception as e:
            log.debug(f"CNPJ {clean}: {e}")
    return None


def parse_cnpj_response(cnpj: str, data: dict) -> tuple[dict, list[dict]]:
    """
    Normaliza resposta BrasilAPI para schema interno.
    Schema confirmado ao vivo — campos exatos da API.
    """
    clean = clean_cnpj(cnpj)

    cap = data.get("capital_social") or 0
    try:
        capital = float(cap) if not isinstance(cap, str) else float(cap.replace(",", "."))
    except Exception:
        capital = 0.0

    empresa = {
        "cnpj":           clean,
        "razao_social":   data.get("razao_social") or data.get("nome") or "",
        "nome_fantasia":  data.get("nome_fantasia") or data.get("fantasia") or "",
        # BrasilAPI: descricao_situacao_cadastral  |  ReceitaWS: situacao
        "situacao":       data.get("descricao_situacao_cadastral") or data.get("situacao") or "",
        "capital_social": capital,
        # BrasilAPI: data_inicio_atividade  |  ReceitaWS: abertura
        "data_abertura":  data.get("data_inicio_atividade") or data.get("abertura") or "",
        "porte":          data.get("descricao_porte") or data.get("porte") or "",
        "cnae_cod":       str(data.get("cnae_fiscal") or ""),
        "cnae_desc":      data.get("cnae_fiscal_descricao") or "",
        "municipio":      data.get("municipio") or "",
        "uf":             data.get("uf") or "",
        "email":          data.get("email") or "",
    }

    socios = []
    for s in (data.get("qsa") or data.get("socios") or []):
        socios.append({
            "cnpj":          clean,
            # BrasilAPI: nome_socio  |  ReceitaWS: nome
            "nome_socio":    s.get("nome_socio") or s.get("nome") or "",
            # CPF mascarado pela API (***000000**)
            "cpf_mask":      s.get("cnpj_cpf_do_socio") or s.get("cnpj_cpf") or "",
            "qualificacao":  s.get("qualificacao_socio") or s.get("qual") or "",
            "data_entrada":  s.get("data_entrada_sociedade") or "",
            "faixa_etaria":  s.get("faixa_etaria") or "",
        })

    return empresa, socios


def _emite_flags(cnpj: str, empresa: dict):
    flags = []
    ab = empresa.get("data_abertura", "")
    if ab:
        try:
            age = (date.today() - date.fromisoformat(ab[:10])).days
            if age < 365:
                flags.append(f"EMPRESA_JOVEM {age}d")
        except Exception:
            pass
    if empresa["capital_social"] < 5_000:
        flags.append(f"CAPITAL R${empresa['capital_social']:,.0f}")
    sit = empresa["situacao"].upper()
    if sit and "ATIVA" not in sit:
        flags.append(f"SITUAÇÃO={sit}")
    if flags:
        console.print(
            f"  [red]⚑[/red] {cnpj} {empresa['razao_social'][:35]}: "
            f"[yellow]{' | '.join(flags)}[/yellow]"
        )


def ingest_cnpj(limit: int = 0) -> int:
    """
    Enriquece CNPJs do DB via BrasilAPI.
    Coleta de: obras, licitacoes, pncp_contratos, pncp_dispensas, tse_doacoes.
    """
    console.rule("[bold cyan]CNPJ — Enriquecimento (BrasilAPI)[/bold cyan]")
    conn = get_conn()

    cnpjs: set[str] = set()
    for table, col in [
        ("obras",          "empresa_id"),
        ("licitacoes",     "empresa_cnpj"),
        ("pncp_contratos", "fornecedores_cnpj"),
        ("pncp_dispensas", "fornecedores_cnpj"),
        ("tse_doacoes",    "nr_cpf_cnpj_doador"),
    ]:
        try:
            for v in conn.execute(
                f"SELECT DISTINCT {col}::VARCHAR FROM {table} WHERE {col} IS NOT NULL"
            ).fetchdf().iloc[:, 0]:
                c = clean_cnpj(v)
                if len(c) == 14:
                    cnpjs.add(c)
        except Exception:
            pass

    try:
        done = set(conn.execute("SELECT cnpj FROM empresas_cnpj").fetchdf()["cnpj"])
        cnpjs -= done
    except Exception:
        pass

    lista = list(cnpjs)[:limit] if limit else list(cnpjs)
    console.print(f"[cyan]{len(lista)} CNPJs para enriquecer[/cyan]")

    if not lista:
        conn.close()
        return 0

    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresas_cnpj (
            cnpj VARCHAR, razao_social VARCHAR, nome_fantasia VARCHAR,
            situacao VARCHAR, capital_social DOUBLE, data_abertura VARCHAR,
            porte VARCHAR, cnae_cod VARCHAR, cnae_desc VARCHAR,
            municipio VARCHAR, uf VARCHAR, email VARCHAR,
            capturado_em VARCHAR, row_hash VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresa_socios (
            cnpj VARCHAR, nome_socio VARCHAR, cpf_mask VARCHAR,
            qualificacao VARCHAR, data_entrada VARCHAR, faixa_etaria VARCHAR,
            capturado_em VARCHAR
        )
    """)

    inseridos = 0
    with httpx.Client(
        timeout=20, follow_redirects=True,
        headers={"User-Agent": "Sentinela/1.0 (Controle Social)"},
    ) as c:
        for cnpj in track(lista, description="Enriquecendo CNPJs..."):
            data = fetch_cnpj(c, cnpj)
            if not data:
                time.sleep(1)
                continue

            empresa, socios = parse_cnpj_response(cnpj, data)
            _emite_flags(cnpj, empresa)

            n = upsert(conn, pd.DataFrame([empresa]), "empresas_cnpj")
            inseridos += n

            if socios:
                df_s = pd.DataFrame(socios)
                df_s["capturado_em"] = datetime.utcnow().isoformat()
                conn.execute("INSERT INTO empresa_socios SELECT * FROM df_s")

            time.sleep(0.4)   # ~2.5 req/s, dentro do limite

    conn.close()
    console.print(f"[green]✓ {inseridos} empresas novas enriquecidas[/green]")
    return inseridos


def cross_nepotismo(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Sócios de empresas contratadas × servidores municipais.
    Match por último sobrenome excluindo os mais comuns do Brasil.
    Base legal: Súmula Vinculante nº 13 STF.
    """
    console.rule("[bold cyan]CRUZAMENTO: Sócios × Servidores (Nepotismo)[/bold cyan]")
    excluidos = ", ".join(f"'{s}'" for s in SOBRENOMES_EXCLUIR)

    sql = f"""
        WITH serv AS (
            SELECT servidor_nome, secretaria, cargo,
                   SPLIT_PART(TRIM(UPPER(servidor_nome)), ' ', -1) AS sobrenome
            FROM servidores WHERE LENGTH(TRIM(servidor_nome)) > 5
        ),
        socios AS (
            SELECT s.nome_socio, s.qualificacao, s.data_entrada,
                   e.razao_social AS empresa, e.cnpj,
                   e.capital_social, e.data_abertura,
                   SPLIT_PART(TRIM(UPPER(s.nome_socio)), ' ', -1) AS sobrenome
            FROM empresa_socios s
            JOIN empresas_cnpj e ON s.cnpj = e.cnpj
            WHERE LENGTH(TRIM(s.nome_socio)) > 5
        )
        SELECT
            sv.servidor_nome, sv.secretaria, sv.cargo,
            sc.nome_socio, sc.empresa, sc.cnpj,
            sc.capital_social, sc.data_abertura, sc.qualificacao,
            sv.sobrenome
        FROM serv sv
        JOIN socios sc ON sv.sobrenome = sc.sobrenome
        WHERE LENGTH(sv.sobrenome) > 4
          AND sv.sobrenome NOT IN ({excluidos})
        ORDER BY sc.data_abertura DESC NULLS LAST
    """

    try:
        result = conn.execute(sql).fetchdf()
        if not result.empty:
            console.print(f"[red]⚑ {len(result)} matches sobrenome servidor↔sócio[/red]")
            jovens = result[
                pd.to_datetime(result["data_abertura"], errors="coerce") > pd.Timestamp("2021-01-01")
            ]
            if not jovens.empty:
                console.print(f"[bold red]{len(jovens)} com empresa aberta recentemente (risco elevado):[/bold red]")
                for _, r in jovens.head(10).iterrows():
                    console.print(
                        f"  [bold]{r['servidor_nome']}[/bold] ({r['secretaria']}) "
                        f"↔ [red]{r['nome_socio']}[/red] — {r['empresa']} "
                        f"(aberta: {r['data_abertura']}, capital: R${float(r.get('capital_social') or 0):,.0f})"
                    )
            conn.execute("CREATE TABLE IF NOT EXISTS cross_nepotismo AS SELECT * FROM result WHERE 1=0")
            conn.execute("DELETE FROM cross_nepotismo")
            conn.execute("INSERT INTO cross_nepotismo SELECT * FROM result")
        else:
            console.print("[dim]Nenhum match (empresa_socios + servidores precisam estar carregados)[/dim]")
        return result
    except Exception as e:
        console.print(f"[yellow]{e}[/yellow]")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="Sentinela — CGU / PNCP / CNPJ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
FLUXO RECOMENDADO:
  1. Cadastre token CGU (gratuito):
     https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
  2. export CGU_API_TOKEN="seu-token"
  3. python -m src.ingest.cgu_pncp_cnpj --ceis --cnep
  4. python -m src.ingest.cgu_pncp_cnpj --pncp-contratos --pncp-dispensas
  5. python -m src.ingest.cgu_pncp_cnpj --cnpj
  6. python -m src.ingest.cgu_pncp_cnpj --cross
        """
    )
    ap.add_argument("--ceis",           action="store_true", help="Baixa CEIS")
    ap.add_argument("--cnep",           action="store_true", help="Baixa CNEP")
    ap.add_argument("--pncp-contratos", action="store_true", dest="pncp_contratos",
                    help="PNCP: contratos (cnpjOrgao=Prefeitura)")
    ap.add_argument("--pncp-dispensas", action="store_true", dest="pncp_dispensas",
                    help="PNCP: dispensas + inexigibilidades (codigoMunicipioIbge)")
    ap.add_argument("--cnpj",           action="store_true", help="Enriquece CNPJs via BrasilAPI")
    ap.add_argument("--cross",          action="store_true", help="Roda todos os cruzamentos")
    ap.add_argument("--all",            action="store_true", help="Tudo em sequência")
    ap.add_argument("--anos", nargs="+", type=int, default=[2022, 2023, 2024])
    ap.add_argument("--token", default="", help="Token CGU (ou via CGU_API_TOKEN env)")
    args = ap.parse_args()

    token = args.token or get_token()

    if args.all:
        args.ceis = args.cnep = args.pncp_contratos = args.pncp_dispensas = args.cnpj = args.cross = True

    if args.ceis:             ingest_ceis(token)
    if args.cnep:             ingest_cnep(token)
    if args.pncp_contratos:   ingest_pncp_contratos(args.anos)
    if args.pncp_dispensas:   ingest_pncp_dispensas(args.anos)
    if args.cnpj:             ingest_cnpj()

    if args.cross:
        conn = get_conn()
        cross_sancoes_contratos(conn)
        detect_fracionamento_pncp(conn)
        cross_nepotismo(conn)
        conn.close()


if __name__ == "__main__":
    main()
