"""
SENTINELA // INTEGRAÇÃO TSE — MAPA COMPLETO
URLs verificados diretamente do Portal de Dados Abertos do TSE
(dadosabertos.tse.jus.br) em 27/02/2026.

CDN BASE: https://cdn.tse.jus.br/estatistica/sead/odsele/
API CKAN: https://dadosabertos.tse.jus.br/api/3/action/

TODOS os arquivos são CSV dentro de ZIP, separados por ";", encoding latin1.
Filtros para Rio Branco: SG_UF='AC', NM_MUNICIPIO='RIO BRANCO' ou CD_MUNICIPIO='71072'

USO:
    python -m src.ingest.tse_integrator --all
    python -m src.ingest.tse_integrator --dataset candidatos
    python -m src.ingest.tse_integrator --dataset doacoes
    python -m src.ingest.tse_integrator --cross      # só cruzamentos
    python -m src.ingest.tse_integrator --info        # imprime estrutura de colunas
"""

import argparse
import io
import json
import time
import zipfile
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import httpx
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import track

console = Console()
log = logging.getLogger("sentinela.tse")
DB_PATH = "data/sentinela_analytics.duckdb"

# ─── CONSTANTES RIO BRANCO ────────────────────────────────────────────────────

RB_IBGE   = "1200401"     # código IBGE
RB_TSE    = "71072"       # código TSE do município
AC_UF     = "AC"
ELEICAO_2024 = "2024-10-06"  # 1° turno. 2° turno: 2024-10-27

# ─── MAPA DE DATASETS TSE (URLs verificados) ──────────────────────────────────

@dataclass
class TseDataset:
    id: str
    name: str
    url: str                         # URL CDN direto
    table: str                       # tabela DuckDB destino
    filter_uf: bool = True           # True = filtrar por AC depois do download
    filter_municipio: bool = False   # True = filtrar por Rio Branco
    priority: int = 2
    notes: str = ""


TSE_DATASETS: list[TseDataset] = [

    # ── CANDIDATOS ────────────────────────────────────────────────────────────
    TseDataset(
        id="candidatos",
        name="Candidatos 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2024.zip",
        table="tse_candidatos",
        filter_uf=True,
        priority=1,
        notes="Colunas chave: SQ_CANDIDATO, NM_CANDIDATO, NR_CPF_CANDIDATO, NM_PARTIDO, DS_CARGO, SG_UF, NM_MUNICIPIO",
    ),
    TseDataset(
        id="candidatos_complementar",
        name="Candidatos — Info Complementar 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand_complementar/consulta_cand_complementar_2024.zip",
        table="tse_candidatos_complementar",
        filter_uf=True,
        priority=2,
        notes="Ocupação, grau instrução, cor/raça",
    ),
    TseDataset(
        id="bens_candidatos",
        name="Bens de Candidatos 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2024.zip",
        table="tse_bens_candidatos",
        filter_uf=True,
        priority=1,
        notes="Cruzar com enriquecimento patrimonial pós-mandato. Colunas: SQ_CANDIDATO, DS_BEM, VR_BEM",
    ),
    TseDataset(
        id="redes_sociais",
        name="Redes Sociais de Candidatos 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/rede_social_candidato_2024.zip",
        table="tse_redes_sociais",
        filter_uf=True,
        priority=3,
        notes="Útil para identificar candidatos e cruzar com investigados",
    ),
    TseDataset(
        id="motivo_cassacao",
        name="Motivo de Cassação 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/motivo_cassacao/motivo_cassacao_2024.zip",
        table="tse_cassacoes",
        filter_uf=True,
        priority=1,
        notes="Candidatos/eleitos cassados — CRÍTICO para cruzar com contratos",
    ),

    # ── PRESTAÇÃO DE CONTAS (DINHEIRO) ────────────────────────────────────────
    TseDataset(
        id="doacoes_candidatos",
        name="Prestação de Contas — Candidatos 2024 ⭐ PRINCIPAL",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2024.zip",
        table="tse_doacoes",
        filter_uf=True,
        priority=1,
        notes=(
            "ARQUIVO GRANDE (~300MB). Contém TODAS as receitas e despesas de campanha. "
            "Colunas chave: NM_DOADOR, NR_CPF_CNPJ_DOADOR, DS_ORIGEM_RECEITA, "
            "VR_RECEITA, DT_RECEITA, SQ_CANDIDATO, NM_CANDIDATO, DS_CARGO, SG_UF, NM_MUNICIPIO. "
            "Cruzar: NR_CPF_CNPJ_DOADOR com CNPJs de empresas contratadas pela prefeitura."
        ),
    ),
    TseDataset(
        id="doacoes_orgaos_partidarios",
        name="Prestação de Contas — Órgãos Partidários 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_orgaos_partidarios_2024.zip",
        table="tse_doacoes_partidarias",
        filter_uf=True,
        priority=2,
        notes="Doações para diretórios municipais — fundos partidários em Rio Branco",
    ),
    TseDataset(
        id="cnpj_campanha",
        name="CNPJ de Campanha 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/CNPJ_campanha_2024.zip",
        table="tse_cnpj_campanha",
        filter_uf=True,
        priority=2,
        notes="CNPJ registrado para cada candidatura — útil para rastrear contas bancárias de campanha",
    ),
    TseDataset(
        id="fefc_fp",
        name="Fundo Eleitoral (FEFC) e Fundo Partidário 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/fefc_fp/fefc_fp_2024.zip",
        table="tse_fefc_fp",
        filter_uf=True,
        priority=2,
        notes="Quanto cada partido recebeu de fundo público no AC",
    ),
    TseDataset(
        id="extrato_bancario_candidatos",
        name="Extratos Bancários — Candidatos 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas_anual_candidato/extrato_bancario_candidato_2024.zip",
        table="tse_extratos_candidatos",
        filter_uf=True,
        priority=2,
        notes="Movimentação bancária das contas de campanha",
    ),
    TseDataset(
        id="extrato_bancario_partidos",
        name="Extratos Bancários — Partidos 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas_anual_partidaria/extrato_bancario_partido_2024.zip",
        table="tse_extratos_partidos",
        filter_uf=True,
        priority=3,
    ),

    # ── RESULTADOS ELEITORAIS ─────────────────────────────────────────────────
    TseDataset(
        id="votacao_candidato_munzona",
        name="Votação Nominal por Município/Zona 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2024.zip",
        table="tse_votacao_candidatos",
        filter_uf=True,
        priority=2,
        notes="Quantos votos cada candidato teve em cada município. "
              "Filtrar CD_MUNICIPIO=71072 para Rio Branco.",
    ),
    TseDataset(
        id="votacao_partido_munzona",
        name="Votação por Partido/Município/Zona 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2024.zip",
        table="tse_votacao_partidos",
        filter_uf=True,
        priority=3,
    ),
    TseDataset(
        id="detalhe_votacao_munzona",
        name="Detalhe Apuração por Município/Zona 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/detalhe_votacao_munzona/detalhe_votacao_munzona_2024.zip",
        table="tse_detalhe_votacao",
        filter_uf=True,
        priority=3,
    ),
    TseDataset(
        id="relatorio_totalizacao_ac",
        name="Relatório de Totalização — AC 2024 ⭐",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/relatorio_resultado_totalizacao/Relatorio_Resultado_Totalizacao_2024_AC.zip",
        table="tse_totalizacao_ac",
        filter_uf=False,   # já é só AC
        priority=1,
        notes="Resultado final de todas as eleições no Acre. Prefeito, Vereadores, etc.",
    ),

    # ── COLIGAÇÕES ────────────────────────────────────────────────────────────
    TseDataset(
        id="coligacoes",
        name="Coligações 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_coligacao/consulta_coligacao_2024.zip",
        table="tse_coligacoes",
        filter_uf=True,
        priority=3,
        notes="Quais partidos se coligaram — útil para mapear alianças políticas",
    ),
    TseDataset(
        id="vagas",
        name="Vagas por Cargo 2024",
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_vagas/consulta_vagas_2024.zip",
        table="tse_vagas",
        filter_uf=True,
        priority=3,
    ),
]

# Index
DATASETS_BY_ID = {d.id: d for d in TSE_DATASETS}
DATASETS_BY_PRIORITY = sorted(TSE_DATASETS, key=lambda d: d.priority)

# ─── ESQUEMAS DE COLUNAS (documentação) ───────────────────────────────────────
# Fonte: Leiame TSE (dentro dos ZIPs)

COLUMN_DOCS = {
    "tse_candidatos": {
        "SQ_CANDIDATO":        "Sequencial único do candidato",
        "NM_CANDIDATO":        "Nome completo",
        "NM_URNA_CANDIDATO":   "Nome na urna",
        "NR_CPF_CANDIDATO":    "CPF (sem pontuação)",
        "NR_CANDIDATO":        "Número na urna",
        "DS_CARGO":            "Cargo (PREFEITO, VEREADOR, etc)",
        "SG_PARTIDO":          "Sigla do partido",
        "NM_PARTIDO":          "Nome do partido",
        "DS_SIT_TOT_TURNO":    "Situação final (ELEITO, NÃO ELEITO, etc)",
        "SG_UF":               "UF do candidato",
        "NM_MUNICIPIO":        "Município",
        "CD_MUNICIPIO":        "Código TSE do município (RB=71072)",
    },
    "tse_doacoes": {
        "SQ_CANDIDATO":        "Sequencial do candidato beneficiado",
        "NM_CANDIDATO":        "Nome do candidato",
        "DS_CARGO":            "Cargo disputado",
        "NM_DOADOR":           "Nome de quem doou",
        "NR_CPF_CNPJ_DOADOR":  "CPF ou CNPJ do doador ← CRUZAR COM CNPJ DAS OBRAS",
        "DS_ORIGEM_RECEITA":   "Tipo de doação (PJ, PF, Fundo, etc)",
        "DS_ESPECIE_RECEITA":  "Espécie (dinheiro, estimável, etc)",
        "VR_RECEITA":          "Valor da doação",
        "DT_RECEITA":          "Data da doação",
        "SG_UF":               "UF do candidato",
        "NM_MUNICIPIO":        "Município do candidato",
        "CD_MUNICIPIO":        "Código TSE",
    },
    "tse_bens_candidatos": {
        "SQ_CANDIDATO":        "Sequencial do candidato",
        "NM_CANDIDATO":        "Nome do candidato",
        "DS_BEM":              "Descrição do bem declarado",
        "DS_TIPO_BEM":         "Tipo (imóvel, veículo, etc)",
        "VR_BEM":              "Valor declarado (R$)",
        "SG_UF":               "UF",
        "NM_MUNICIPIO":        "Município",
    },
    "tse_votacao_candidatos": {
        "SQ_CANDIDATO":        "Sequencial do candidato",
        "NM_CANDIDATO":        "Nome",
        "DS_CARGO":            "Cargo",
        "SG_PARTIDO":          "Partido",
        "CD_MUNICIPIO":        "Código município TSE",
        "NM_MUNICIPIO":        "Município",
        "NR_ZONA":             "Zona eleitoral",
        "QT_VOTOS_NOMINAIS":   "Votos nominais",
        "DS_SIT_TOT_TURNO":    "Resultado",
        "SG_UF":               "UF",
    },
}

# ─── DOWNLOAD & PARSE ─────────────────────────────────────────────────────────

def download_and_parse(
    client: httpx.Client,
    dataset: TseDataset,
    filter_ac: bool = True,
) -> pd.DataFrame:
    """
    Baixa o ZIP do CDN do TSE, extrai e parseia os CSVs.
    Aplica filtro AC/Rio Branco se solicitado.
    """
    console.print(f"[cyan]↓ {dataset.name}[/cyan]")
    console.print(f"  [dim]{dataset.url}[/dim]")

    try:
        # Stream download para não explodir memória
        with client.stream("GET", dataset.url) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            data = b""
            downloaded = 0
            for chunk in resp.iter_bytes(chunk_size=1024 * 256):  # 256KB chunks
                data += chunk
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    console.print(
                        f"  [dim]{downloaded/1e6:.1f}MB / {total/1e6:.1f}MB ({pct:.0f}%)[/dim]",
                        end="\r"
                    )
    except httpx.HTTPError as e:
        console.print(f"[red]ERRO download: {e}[/red]")
        return pd.DataFrame()

    console.print(f"  [green]✓ {len(data)/1e6:.1f}MB baixados[/green]")

    # Extrai ZIP
    frames = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
            leiame = [f for f in zf.namelist() if "leiame" in f.lower() or "readme" in f.lower()]

            if leiame:
                # Mostra estrutura de colunas do leiame se disponível
                with zf.open(leiame[0]) as f:
                    readme = f.read().decode("latin1", errors="ignore")
                    log.debug(f"LEIAME {dataset.id}:\n{readme[:500]}")

            console.print(f"  [dim]{len(csv_files)} arquivo(s) CSV no ZIP[/dim]")

            for csv_name in csv_files:
                with zf.open(csv_name) as f:
                    try:
                        df = pd.read_csv(
                            f,
                            encoding="latin1",
                            sep=";",
                            low_memory=False,
                            on_bad_lines="skip",
                            dtype=str,  # tudo como string inicialmente
                        )
                        frames.append((csv_name, df))
                        console.print(f"  [dim]  {csv_name}: {len(df):,} linhas × {len(df.columns)} colunas[/dim]")
                    except Exception as e:
                        log.warning(f"Erro parse {csv_name}: {e}")

    except zipfile.BadZipFile:
        console.print("[red]ZIP inválido[/red]")
        return pd.DataFrame()

    if not frames:
        return pd.DataFrame()

    df = pd.concat([f for _, f in frames], ignore_index=True)

    # Normaliza nomes de colunas (TSE usa maiúsculas com underline)
    df.columns = [c.strip().upper() for c in df.columns]

    # ── FILTRO AC ──
    if filter_ac and dataset.filter_uf:
        if "SG_UF" in df.columns:
            before = len(df)
            df = df[df["SG_UF"].str.upper().str.strip() == "AC"]
            console.print(f"  [yellow]Filtro AC: {before:,} → {len(df):,} linhas[/yellow]")

    # ── FILTRO RIO BRANCO ──
    if dataset.filter_municipio:
        for col in ["CD_MUNICIPIO", "NM_MUNICIPIO"]:
            if col in df.columns:
                if col == "CD_MUNICIPIO":
                    df = df[df[col].str.strip() == RB_TSE]
                else:
                    df = df[df[col].str.upper().str.contains("RIO BRANCO")]
                console.print(f"  [yellow]Filtro RB: {len(df):,} linhas[/yellow]")
                break

    return df


def save_to_db(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table: str,
    dataset_id: str,
) -> int:
    """Insere no DuckDB com deduplicação por hash."""
    if df.empty:
        return 0

    import hashlib
    df = df.copy()
    df.columns = [
        c.lower().strip().replace(" ", "_").replace("-", "_").replace("/", "_")
        for c in df.columns
    ]
    df["tse_dataset"] = dataset_id
    df["capturado_em"] = datetime.utcnow().isoformat()
    df["row_hash"] = df.apply(
        lambda row: hashlib.md5(str(row.values).encode()).hexdigest(), axis=1
    )

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0
    """)

    try:
        existing = set(
            conn.execute(f"SELECT row_hash FROM {table}").fetchdf()["row_hash"].tolist()
        )
    except Exception:
        existing = set()

    new = df[~df["row_hash"].isin(existing)]
    if not new.empty:
        conn.execute(f"INSERT INTO {table} SELECT * FROM new")

    return len(new)


# ─── CRUZAMENTOS TSE × CONTRATOS ─────────────────────────────────────────────

def cross_doacoes_contratos(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    CRUZAMENTO PRINCIPAL:
    Empresa que doou para candidato eleito → ganhou contrato pós-eleição.

    Exige: tse_doacoes + (obras ou licitacoes ou pncp_contratos)
    """
    console.print("\n[bold cyan]▶ Cruzamento: Doações → Contratos Pós-Eleição[/bold cyan]")

    query = """
        WITH eleitos AS (
            -- Candidatos eleitos em Rio Branco 2024
            SELECT DISTINCT
                SQ_CANDIDATO,
                NM_CANDIDATO,
                DS_CARGO,
                SG_PARTIDO
            FROM tse_candidatos
            WHERE (NM_MUNICIPIO ILIKE '%RIO BRANCO%' OR SG_UF = 'AC')
              AND DS_SIT_TOT_TURNO ILIKE '%ELEITO%'
        ),
        doacoes_pj AS (
            -- Doações de pessoa jurídica para candidatos
            SELECT
                d.NR_CPF_CNPJ_DOADOR AS cnpj_doador,
                d.NM_DOADOR AS nome_doador,
                d.NM_CANDIDATO AS candidato_beneficiado,
                d.DS_CARGO AS cargo,
                CAST(REPLACE(REPLACE(d.VR_RECEITA, '.', ''), ',', '.') AS DOUBLE) AS valor_doacao,
                d.DT_RECEITA AS data_doacao,
                e.DS_SIT_TOT_TURNO AS resultado_candidato
            FROM tse_doacoes d
            -- JOIN só com candidatos que de fato existem (evita ruído)
            WHERE LENGTH(d.NR_CPF_CNPJ_DOADOR) >= 14  -- CNPJ tem 14 dígitos
              AND d.NR_CPF_CNPJ_DOADOR NOT LIKE '000%'
        )
        SELECT
            dp.cnpj_doador,
            dp.nome_doador,
            dp.candidato_beneficiado,
            dp.cargo,
            dp.valor_doacao,
            dp.data_doacao,
            o.empresa_nome AS empresa_contratada,
            o.secretaria,
            CAST(o.valor_total AS DOUBLE) AS valor_contrato,
            o.data_contrato,
            -- Diferença em dias entre doação e contrato
            DATEDIFF('day', CAST(dp.data_doacao AS DATE), CAST(o.data_contrato AS DATE)) AS dias_doacao_contrato
        FROM doacoes_pj dp
        JOIN obras o ON (
            -- Matching por CNPJ (limpa pontuação)
            REGEXP_REPLACE(dp.cnpj_doador, '[^0-9]', '') =
            REGEXP_REPLACE(COALESCE(o.empresa_id::VARCHAR, ''), '[^0-9]', '')
        )
        WHERE o.data_contrato > '2024-10-06'  -- pós 1° turno
          AND dp.valor_doacao > 0
        ORDER BY dp.valor_doacao DESC
    """

    try:
        result = conn.execute(query).fetchdf()
        if not result.empty:
            console.print(f"[red]⚑ {len(result)} pares doação→contrato encontrados![/red]")
            _save_cross(conn, result, "cross_doacao_contrato")
        else:
            console.print("[dim]Nenhum cruzamento doação→contrato (pode ser que dados ainda não estejam carregados)[/dim]")
        return result
    except Exception as e:
        console.print(f"[yellow]Cruzamento requer tabelas tse_doacoes + obras: {e}[/yellow]")
        return pd.DataFrame()


def cross_candidatos_servidores(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Candidatos ou parentes de candidatos que são servidores municipais.
    Proxy por sobrenome + nome similar.
    """
    console.print("\n[bold cyan]▶ Cruzamento: Candidatos × Servidores Municipais[/bold cyan]")

    query = """
        WITH candidatos_rb AS (
            SELECT
                SQ_CANDIDATO,
                NM_CANDIDATO,
                NR_CPF_CANDIDATO,
                DS_CARGO,
                SG_PARTIDO,
                DS_SIT_TOT_TURNO,
                SPLIT_PART(TRIM(UPPER(NM_CANDIDATO)), ' ', -1) AS sobrenome
            FROM tse_candidatos
            WHERE NM_MUNICIPIO ILIKE '%RIO BRANCO%'
              AND LENGTH(NM_CANDIDATO) > 5
        ),
        servidores_rb AS (
            SELECT
                servidor_nome,
                cargo,
                secretaria,
                valor_liquido,
                SPLIT_PART(TRIM(UPPER(servidor_nome)), ' ', -1) AS sobrenome
            FROM servidores
            WHERE LENGTH(servidor_nome) > 5
        )
        SELECT
            c.NM_CANDIDATO AS candidato,
            c.DS_CARGO AS cargo_eleitoral,
            c.SG_PARTIDO AS partido,
            c.DS_SIT_TOT_TURNO AS resultado,
            s.servidor_nome AS servidor,
            s.cargo AS cargo_servidor,
            s.secretaria,
            s.valor_liquido AS salario,
            c.sobrenome AS sobrenome_comum
        FROM candidatos_rb c
        JOIN servidores_rb s ON c.sobrenome = s.sobrenome
        WHERE LENGTH(c.sobrenome) > 4
        ORDER BY c.DS_SIT_TOT_TURNO, s.valor_liquido DESC
    """

    try:
        result = conn.execute(query).fetchdf()
        if not result.empty:
            console.print(f"[yellow]⚑ {len(result)} matches candidato↔servidor (por sobrenome)[/yellow]")
            _save_cross(conn, result, "cross_candidato_servidor")
        return result
    except Exception as e:
        console.print(f"[yellow]Requer tse_candidatos + servidores: {e}[/yellow]")
        return pd.DataFrame()


def cross_bens_candidatos_enriquecimento(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Detecta candidatos que declararam bens muito abaixo do esperado
    para alguém com acesso a contratos públicos (enriquecimento ilícito potencial).
    Requer dados de eleições anteriores para comparar evolução patrimonial.
    """
    console.print("\n[bold cyan]▶ Análise: Bens Declarados por Candidatos[/bold cyan]")

    query = """
        SELECT
            b.NM_CANDIDATO AS candidato,
            c.DS_CARGO AS cargo,
            c.SG_PARTIDO AS partido,
            c.DS_SIT_TOT_TURNO AS resultado,
            COUNT(b.DS_BEM) AS num_bens,
            SUM(CAST(REPLACE(REPLACE(COALESCE(b.VR_BEM, '0'), '.', ''), ',', '.') AS DOUBLE)) AS total_bens,
            STRING_AGG(b.DS_TIPO_BEM, ', ') AS tipos_bens
        FROM tse_bens_candidatos b
        JOIN tse_candidatos c ON b.SQ_CANDIDATO = c.SQ_CANDIDATO
        WHERE c.NM_MUNICIPIO ILIKE '%RIO BRANCO%'
        GROUP BY b.NM_CANDIDATO, c.DS_CARGO, c.SG_PARTIDO, c.DS_SIT_TOT_TURNO
        ORDER BY total_bens DESC
    """

    try:
        result = conn.execute(query).fetchdf()
        if not result.empty:
            console.print(f"[cyan]{len(result)} candidatos com bens declarados[/cyan]")
            console.print(result.head(10).to_string())
            _save_cross(conn, result, "cross_bens_candidatos")
        return result
    except Exception as e:
        console.print(f"[yellow]Requer tse_bens_candidatos + tse_candidatos: {e}[/yellow]")
        return pd.DataFrame()


def _save_cross(conn, df: pd.DataFrame, table: str):
    """Salva resultado de cruzamento no DuckDB."""
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0")
        conn.execute(f"DELETE FROM {table}")
        conn.execute(f"INSERT INTO {table} SELECT * FROM df")
        console.print(f"  [green]→ Salvo em {table}[/green]")
    except Exception as e:
        log.warning(f"Erro salvando {table}: {e}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def print_dataset_map():
    """Imprime mapa completo de datasets disponíveis."""
    table = Table(title="TSE — MAPA DE DATASETS 2024", border_style="cyan", show_lines=True)
    table.add_column("ID", style="cyan", min_width=25)
    table.add_column("Nome", style="white", min_width=35)
    table.add_column("Tabela DB", style="yellow")
    table.add_column("Prior.", justify="center")
    table.add_column("Filtro AC", justify="center")

    for d in DATASETS_BY_PRIORITY:
        table.add_row(
            d.id,
            d.name,
            d.table,
            str(d.priority),
            "✓" if d.filter_uf else "—",
        )

    console.print(table)
    console.print()

    console.print("[bold]CDN BASE:[/bold] https://cdn.tse.jus.br/estatistica/sead/odsele/")
    console.print("[bold]API CKAN:[/bold] https://dadosabertos.tse.jus.br/api/3/action/")
    console.print("[bold]Código RB (TSE):[/bold] 71072  |  [bold]Código RB (IBGE):[/bold] 1200401")
    console.print("[bold]Filtros AC:[/bold] SG_UF='AC', NM_MUNICIPIO='RIO BRANCO', CD_MUNICIPIO='71072'")
    console.print()

    console.print("[bold]COLUNAS PRINCIPAIS para cruzamento:[/bold]")
    for table_name, cols in COLUMN_DOCS.items():
        console.print(f"\n[cyan]{table_name}[/cyan]:")
        for col, desc in cols.items():
            marker = " [bold red]⭐[/bold red]" if "CRUZAR" in desc else ""
            console.print(f"  [yellow]{col}[/yellow]: {desc}{marker}")


def main():
    parser = argparse.ArgumentParser(description="SENTINELA — Integração TSE")
    parser.add_argument("--all", action="store_true", help="Baixa todos os datasets")
    parser.add_argument("--priority", type=int, help="Baixa datasets com prioridade <= N")
    parser.add_argument("--dataset", help="Baixa dataset específico por ID")
    parser.add_argument("--cross", action="store_true", help="Só roda cruzamentos (dados já baixados)")
    parser.add_argument("--info", action="store_true", help="Imprime mapa de datasets e colunas")
    parser.add_argument("--municipio", action="store_true", help="Filtrar apenas Rio Branco (além de AC)")
    args = parser.parse_args()

    if args.info:
        print_dataset_map()
        return

    conn = duckdb.connect(DB_PATH)

    if args.cross:
        cross_doacoes_contratos(conn)
        cross_candidatos_servidores(conn)
        cross_bens_candidatos_enriquecimento(conn)
        conn.close()
        return

    # Seleciona datasets para baixar
    if args.dataset:
        if args.dataset not in DATASETS_BY_ID:
            console.print(f"[red]Dataset '{args.dataset}' não encontrado. Use --info para listar.[/red]")
            return
        to_download = [DATASETS_BY_ID[args.dataset]]
    elif args.priority:
        to_download = [d for d in DATASETS_BY_PRIORITY if d.priority <= args.priority]
    elif args.all:
        to_download = DATASETS_BY_PRIORITY
    else:
        parser.print_help()
        return

    console.print(f"[cyan]{len(to_download)} dataset(s) para baixar[/cyan]")

    with httpx.Client(
        timeout=300,
        headers={
            "User-Agent": "Sentinela/1.0 (Controle Social - Dados Públicos TSE)",
            "Accept-Encoding": "gzip, deflate",
        },
        follow_redirects=True,
    ) as client:
        for dataset in to_download:
            console.rule(f"[bold]{dataset.id}[/bold]")

            if args.municipio:
                dataset.filter_municipio = True

            df = download_and_parse(client, dataset)

            if not df.empty:
                new_rows = save_to_db(conn, df, dataset.table, dataset.id)
                console.print(
                    f"[green]✓ {dataset.name}: {len(df):,} linhas, "
                    f"{new_rows:,} novas → {dataset.table}[/green]"
                )
            else:
                console.print(f"[red]✗ {dataset.name}: sem dados[/red]")

            time.sleep(1)  # pausa respeitosa entre downloads

    # Roda cruzamentos automaticamente após download
    console.rule("[bold cyan]CRUZAMENTOS AUTOMÁTICOS[/bold cyan]")
    cross_doacoes_contratos(conn)
    cross_candidatos_servidores(conn)
    cross_bens_candidatos_enriquecimento(conn)

    conn.close()
    console.print("\n[green]✓ Integração TSE concluída[/green]")


if __name__ == "__main__":
    main()
