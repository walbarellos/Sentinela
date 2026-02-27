"""
SENTINELA // ENRIQUECIMENTO CNPJ
Dado os CNPJs já coletados (obras, contratos, licitações),
busca dados completos na Receita Federal via BrasilAPI
e armazena sócios (QSA) para cruzamento com servidores.

USO:
    python -m src.ingest.cnpj_enricher
    python -m src.ingest.cnpj_enricher --min-value 50000
"""

import argparse
import time
import logging
from datetime import datetime, date

import duckdb
import httpx
import pandas as pd
from rich.console import Console
from rich.progress import track

console = Console()
log = logging.getLogger("sentinela.cnpj")

DB_PATH = "data/sentinela_analytics.duckdb"

# Limite de dispensa de licitação — Lei 14.133/21
LIMITE_DISPENSA_SERVICOS = 57_278.16
LIMITE_DISPENSA_OBRAS = 114_556.32


def get_all_cnpjs(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Extrai todos os CNPJs únicos das tabelas de contratos/obras."""
    cnpjs = set()

    queries = [
        "SELECT DISTINCT empresa_id::VARCHAR as cnpj FROM obras WHERE empresa_id IS NOT NULL",
        "SELECT DISTINCT empresa_cnpj::VARCHAR as cnpj FROM licitacoes WHERE empresa_cnpj IS NOT NULL",
        "SELECT DISTINCT empresa_cnpj::VARCHAR as cnpj FROM pncp_contratos WHERE empresa_cnpj IS NOT NULL",
    ]

    for q in queries:
        try:
            rows = conn.execute(q).fetchdf()
            cnpjs.update(rows["cnpj"].dropna().tolist())
        except Exception:
            pass

    # Normaliza: só dígitos, 14 chars
    result = []
    for cnpj in cnpjs:
        clean = "".join(filter(str.isdigit, str(cnpj)))
        if len(clean) == 14:
            result.append(clean)

    return list(set(result))


def fetch_cnpj(client: httpx.Client, cnpj: str) -> dict | None:
    """Consulta BrasilAPI. Fallback: ReceitaWS."""
    urls = [
        f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
        f"https://receitaws.com.br/v1/cnpj/{cnpj}",
    ]

    for url in urls:
        try:
            resp = client.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                console.print("[yellow]Rate limit — aguardando 10s...[/yellow]")
                time.sleep(10)
        except Exception as e:
            log.debug(f"CNPJ {cnpj} erro em {url}: {e}")

    return None


def analyze_cnpj(data: dict, contracts: list[dict]) -> list[str]:
    """
    Detecta flags de risco a partir dos dados do CNPJ.
    Retorna lista de strings descrevendo cada anomalia.
    """
    flags = []

    # Data de abertura vs data do contrato
    abertura_str = data.get("data_inicio_atividade") or data.get("abertura")
    if abertura_str:
        try:
            # BrasilAPI: "YYYY-MM-DD", ReceitaWS: "DD/MM/YYYY"
            if "/" in abertura_str:
                abertura = datetime.strptime(abertura_str, "%d/%m/%Y").date()
            else:
                abertura = datetime.strptime(abertura_str[:10], "%Y-%m-%d").date()

            for contract in contracts:
                contract_date = contract.get("data_contrato")
                if contract_date:
                    try:
                        if isinstance(contract_date, str):
                            cd = datetime.strptime(contract_date[:10], "%Y-%m-%d").date()
                        else:
                            cd = contract_date
                        idade_dias = (cd - abertura).days
                        if idade_dias < 180:
                            flags.append(
                                f"EMPRESA_JOVEM: {idade_dias}d na data do contrato "
                                f"R${contract.get('valor', 0):,.0f}"
                            )
                    except Exception:
                        pass
        except Exception:
            pass

    # Capital social irrisório
    capital = data.get("capital_social") or 0
    if isinstance(capital, str):
        capital = float("".join(c for c in capital if c.isdigit() or c == ".") or 0)
    if capital < 10_000:
        flags.append(f"CAPITAL_SOCIAL_BAIXO: R${capital:,.0f}")

    # Situação cadastral
    situacao = (data.get("situacao") or data.get("descricao_situacao_cadastral") or "").upper()
    if situacao not in ("ATIVA", "ACTIVE", ""):
        flags.append(f"SITUACAO_IRREGULAR: {situacao}")

    # CNAE incompatível? (heurística simples)
    cnae = data.get("cnae_fiscal") or data.get("atividade_principal", [{}])
    if isinstance(cnae, list) and cnae:
        cnae = cnae[0].get("code", "") or cnae[0].get("codigo", "")
    cnae_str = str(cnae)

    # Empresas de limpeza (74-77) ganhando obras de engenharia (41-43)
    has_engenharia = any(
        c.get("tipo", "") in ("obra", "construção", "reforma", "reforma")
        for c in contracts
    )
    if has_engenharia and cnae_str.startswith(("74", "75", "77", "81", "82")):
        flags.append(f"CNAE_INCOMPATIVEL: {cnae_str} vs contratos de engenharia")

    # Porte mínimo com contratos grandes
    porte = (data.get("porte") or data.get("nome_porte") or "").upper()
    total_contracts = sum(c.get("valor", 0) for c in contracts)
    if "MICRO" in porte and total_contracts > 500_000:
        flags.append(f"PORTE_VS_CONTRATOS: ME com R${total_contracts:,.0f} em contratos")

    return flags


def build_socios_df(cnpj: str, data: dict) -> pd.DataFrame:
    """Extrai o QSA (Quadro Societário) como DataFrame."""
    socios = data.get("qsa") or data.get("socios") or []
    rows = []
    for s in socios:
        rows.append({
            "cnpj": cnpj,
            "socio_nome": s.get("nome_socio") or s.get("nome") or "",
            "socio_cpf_cnpj": s.get("cpf_representante_legal") or s.get("cnpj_cpf") or "",
            "qualificacao": s.get("qualificacao_socio") or s.get("qual") or "",
            "data_entrada": s.get("data_entrada_sociedade") or "",
        })
    return pd.DataFrame(rows)


def run_enrichment(min_value: float = 0):
    conn = duckdb.connect(DB_PATH)

    # Cria tabelas se não existirem
    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresas_cnpj (
            cnpj VARCHAR,
            razao_social VARCHAR,
            situacao VARCHAR,
            capital_social DOUBLE,
            data_abertura VARCHAR,
            porte VARCHAR,
            cnae_principal VARCHAR,
            municipio VARCHAR,
            uf VARCHAR,
            flags VARCHAR,          -- JSON array de flags
            capturado_em TIMESTAMP,
            row_hash VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresa_socios (
            cnpj VARCHAR,
            socio_nome VARCHAR,
            socio_cpf_cnpj VARCHAR,
            qualificacao VARCHAR,
            data_entrada VARCHAR,
            capturado_em TIMESTAMP
        )
    """)

    # CNPJs já enriquecidos
    try:
        done = set(conn.execute("SELECT cnpj FROM empresas_cnpj").fetchdf()["cnpj"].tolist())
    except Exception:
        done = set()

    cnpjs = [c for c in get_all_cnpjs(conn) if c not in done]
    console.print(f"[cyan]{len(cnpjs)} CNPJs para enriquecer[/cyan]")

    with httpx.Client(
        headers={"User-Agent": "Sentinela/1.0 (Controle Social)"},
        follow_redirects=True,
    ) as client:
        for cnpj in track(cnpjs, description="Enriquecendo CNPJs..."):
            data = fetch_cnpj(client, cnpj)
            if not data:
                time.sleep(1)
                continue

            # Busca contratos deste CNPJ
            try:
                contracts_df = conn.execute(
                    "SELECT * FROM obras WHERE empresa_id::VARCHAR = ?", [cnpj]
                ).fetchdf()
                contracts = contracts_df.to_dict("records") if not contracts_df.empty else []
            except Exception:
                contracts = []

            flags = analyze_cnpj(data, contracts)

            import json
            row = {
                "cnpj": cnpj,
                "razao_social": data.get("razao_social") or data.get("nome") or "",
                "situacao": data.get("descricao_situacao_cadastral") or data.get("situacao") or "",
                "capital_social": float(data.get("capital_social") or 0),
                "data_abertura": data.get("data_inicio_atividade") or data.get("abertura") or "",
                "porte": data.get("nome_porte") or data.get("porte") or "",
                "cnae_principal": str(data.get("cnae_fiscal") or ""),
                "municipio": data.get("municipio") or data.get("municipio_nome") or "",
                "uf": data.get("uf") or "",
                "flags": json.dumps(flags, ensure_ascii=False),
                "capturado_em": datetime.utcnow(),
                "row_hash": cnpj,
            }

            conn.execute(
                "INSERT INTO empresas_cnpj VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                list(row.values()),
            )

            # Sócios
            socios_df = build_socios_df(cnpj, data)
            if not socios_df.empty:
                socios_df["capturado_em"] = datetime.utcnow()
                conn.execute("INSERT INTO empresa_socios SELECT * FROM socios_df")

            if flags:
                console.print(
                    f"[red]⚑ {cnpj}[/red] — {data.get('razao_social', '')}: {'; '.join(flags)}"
                )

            time.sleep(0.4)  # respeitar rate limit BrasilAPI

    conn.close()
    console.print("[green]✓ Enriquecimento CNPJ concluído[/green]")


def run_nepotismo_check():
    """
    Cruzamento: sócios de empresas contratadas vs servidores municipais.
    Usa similaridade de sobrenome como proxy.
    """
    conn = duckdb.connect(DB_PATH)

    try:
        result = conn.execute("""
            WITH servidor_sobrenomes AS (
                SELECT
                    servidor_nome,
                    secretaria,
                    cargo,
                    -- último token do nome como sobrenome
                    SPLIT_PART(TRIM(UPPER(servidor_nome)), ' ', -1) AS sobrenome
                FROM servidores
                WHERE LENGTH(TRIM(servidor_nome)) > 3
            ),
            socio_sobrenomes AS (
                SELECT
                    s.cnpj,
                    e.razao_social AS empresa_nome,
                    s.socio_nome,
                    SPLIT_PART(TRIM(UPPER(s.socio_nome)), ' ', -1) AS sobrenome
                FROM empresa_socios s
                JOIN empresas_cnpj e ON s.cnpj = e.cnpj
                WHERE LENGTH(TRIM(s.socio_nome)) > 3
            )
            SELECT
                sv.servidor_nome,
                sv.secretaria,
                sv.cargo,
                sc.empresa_nome,
                sc.socio_nome,
                sv.sobrenome AS sobrenome_comum,
                sc.cnpj
            FROM servidor_sobrenomes sv
            JOIN socio_sobrenomes sc ON sv.sobrenome = sc.sobrenome
            WHERE LENGTH(sv.sobrenome) > 3  -- evita sobrenomes curtos (Silva, etc não conta sozinho)
            ORDER BY sv.sobrenome
        """).fetchdf()

        if not result.empty:
            console.print(f"\n[red]⚑ {len(result)} possíveis casos de nepotismo/conflito:[/red]")
            for _, row in result.iterrows():
                console.print(
                    f"  [bold]{row['servidor_nome']}[/bold] ({row['secretaria']}) "
                    f"↔ [red]{row['socio_nome']}[/red] — sócio de [bold]{row['empresa_nome']}[/bold] "
                    f"[dim](sobrenome: {row['sobrenome_comum']})[/dim]"
                )

            # Salva resultado
            conn.execute("""
                CREATE TABLE IF NOT EXISTS flags_nepotismo AS SELECT * FROM result WHERE 1=0
            """)
            conn.execute("INSERT INTO flags_nepotismo SELECT * FROM result")
            console.print(f"[green]Salvo em flags_nepotismo ({len(result)} registros)[/green]")
        else:
            console.print("[dim]Nenhum match de sobrenome encontrado[/dim]")

    except Exception as e:
        console.print(f"[red]Erro no cruzamento: {e}[/red]")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-value", type=float, default=0)
    parser.add_argument("--nepotismo", action="store_true", help="Só roda cruzamento de sobrenomes")
    args = parser.parse_args()

    if args.nepotismo:
        run_nepotismo_check()
    else:
        run_enrichment(min_value=args.min_value)
        run_nepotismo_check()
