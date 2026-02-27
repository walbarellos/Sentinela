"""
SENTINELA // ENGINE DE CRUZAMENTOS
Todos os detectores de anomalias, com base legal citada.

USO:
    python -m src.core.cross_reference_engine
    python -m src.core.cross_reference_engine --detector fracionamento
    python -m src.core.cross_reference_engine --export-csv
"""

import argparse
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()
log = logging.getLogger("sentinela.cross")
DB_PATH = "data/sentinela_analytics.duckdb"

# ─── ESTRUTURA DE ALERTA ──────────────────────────────────────────────────────


@dataclass
class Alert:
    detector_id: str
    severity: str                    # CRÍTICO, ALTO, MÉDIO
    entity_type: str                 # servidor, empresa, contrato
    entity_name: str
    description: str
    exposure_brl: float
    base_legal: str
    evidence: list[dict] = field(default_factory=list)
    dossie_id: str = ""

    def __post_init__(self):
        if not self.dossie_id:
            prefix = self.detector_id[:3].upper()
            self.dossie_id = f"{prefix}_{hash(self.entity_name) % 999999}"


# ─── BASE LEGAL ───────────────────────────────────────────────────────────────

LEGAL = {
    "fracionamento": (
        "Lei 14.133/21 art. 29, §2° — É vedada a adoção da contratação direta, "
        "nos casos em que, pela natureza do objeto, seja possível seu parcelamento, "
        "com vistas a reduzir o valor individual para enquadramento como dispensa."
    ),
    "teto_constitucional": (
        "CF art. 37, XI — A remuneração e o subsídio dos ocupantes de cargos, "
        "funções e empregos públicos não poderá exceder o subsídio mensal dos "
        "Ministros do Supremo Tribunal Federal."
    ),
    "nepotismo": (
        "Súmula Vinculante n° 13 STF — A nomeação de cônjuge, companheiro ou "
        "parente em linha reta, colateral ou por afinidade, até o terceiro grau, "
        "inclusive, da autoridade nomeante viola a Constituição Federal."
    ),
    "empresa_jovem": (
        "Lei 14.133/21 art. 67 — Qualificação técnica: comprovação de aptidão "
        "para o desempenho de atividade pertinente e compatível em características, "
        "quantidades e prazos com o objeto da licitação."
    ),
    "doacao_contrato": (
        "Lei 12.846/13 (Lei Anticorrupção) art. 5°, IV — Atos que prejudiquem "
        "o patrimônio público no processo licitatório ou na execução de contratos. "
        "Lei 9.840/99 art. 41-A — Captação ilícita de sufrágio."
    ),
    "concentracao_mercado": (
        "Lei 14.133/21 art. 5°, V — Competitividade: buscam-se a mais ampla "
        "participação dos licitantes. Concentração > 40% pode configurar "
        "direcionamento de licitação (art. 337-F CP — fraude em licitação, pena 4-8 anos)."
    ),
    "viagem_bloco": (
        "Decreto Municipal de Rio Branco sobre diárias — Obrigatoriedade de "
        "portaria de concessão publicada no Diário Oficial. "
        "Ausência configura improbidade administrativa (Lei 8.429/92 art. 10)."
    ),
    "lrf_pessoal": (
        "LC 101/2000 (LRF) art. 19, III — A despesa total com pessoal em cada "
        "período de apuração não poderá exceder 60% da receita corrente líquida "
        "para os Municípios."
    ),
    "duplo_vinculo": (
        "CF art. 37, XVI — É vedada a acumulação remunerada de cargos ou empregos "
        "públicos, exceto nas hipóteses taxativas do inciso XVI."
    ),
    "outlier_salarial": (
        "CF art. 37, X — A remuneração dos servidores públicos somente poderá "
        "ser fixada ou alterada por lei específica. Valores discrepantes sem "
        "base legal configuram improbidade (Lei 8.429/92 art. 9°, I)."
    ),
    "empresa_suspensa": (
        "Lei 14.133/21 art. 156 — São sanções aplicáveis: suspensão do direito "
        "de licitar e contratar. CEIS/CNEP: consulta obrigatória antes de qualquer "
        "contratação pública (art. 7° Lei 10.520/02)."
    ),
    "fim_de_semana": (
        "Jurisprudência TCU — Diárias pagas em sábados/domingos sem programação "
        "oficial comprovada configuram dano ao erário (Acórdão 2.484/2021-TCU-Plenário)."
    ),
}


# ─── DETECTORES ───────────────────────────────────────────────────────────────

def detect_fracionamento(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """
    Detecta bid-splitting: mesmo fornecedor, mesma secretaria,
    múltiplos contratos abaixo do limite de dispensa, dentro de 6 meses.
    Limite serviços: R$ 57.278,16 (Lei 14.133/21, corrigido IPCA).
    """
    try:
        df = conn.execute("""
            SELECT
                empresa_nome,
                secretaria,
                COUNT(*) AS num_contratos,
                SUM(CAST(valor_total AS DOUBLE)) AS valor_agregado,
                MAX(CAST(valor_total AS DOUBLE)) AS maior_contrato,
                MIN(data_contrato) AS primeiro,
                MAX(data_contrato) AS ultimo
            FROM (
                SELECT empresa_nome, secretaria, valor_total, data_contrato FROM obras
                UNION ALL
                SELECT empresa_nome, secretaria, valor_total, data_contrato FROM licitacoes
            )
            WHERE CAST(valor_total AS DOUBLE) < 57278.16
              AND empresa_nome IS NOT NULL
              AND secretaria IS NOT NULL
            GROUP BY empresa_nome, secretaria
            HAVING num_contratos >= 3
              AND valor_agregado > 57278.16
            ORDER BY valor_agregado DESC
            LIMIT 100
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_fracionamento: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            detector_id="FRAC",
            severity="CRÍTICO",
            entity_type="empresa",
            entity_name=row["empresa_nome"],
            description=(
                f"{int(row['num_contratos'])} contratos de {row['empresa_nome']} "
                f"na {row['secretaria']}, todos abaixo do limite de dispensa. "
                f"Valor agregado: R${row['valor_agregado']:,.2f} "
                f"(período: {row['primeiro']} a {row['ultimo']})"
            ),
            exposure_brl=float(row["valor_agregado"]),
            base_legal=LEGAL["fracionamento"],
        ))
    return alerts


def detect_outlier_salarial(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """Z-score > 3 por cargo/grupo."""
    try:
        df = conn.execute("""
            WITH stats AS (
                SELECT
                    cargo,
                    AVG(CAST(valor_liquido AS DOUBLE)) AS media,
                    STDDEV(CAST(valor_liquido AS DOUBLE)) AS desvio,
                    COUNT(*) AS n
                FROM servidores
                WHERE valor_liquido IS NOT NULL
                GROUP BY cargo
                HAVING n >= 5
            )
            SELECT
                s.servidor_nome,
                s.cargo,
                s.secretaria,
                CAST(s.valor_liquido AS DOUBLE) AS salario,
                st.media,
                st.desvio,
                (CAST(s.valor_liquido AS DOUBLE) - st.media) / NULLIF(st.desvio, 0) AS zscore
            FROM servidores s
            JOIN stats st ON s.cargo = st.cargo
            WHERE (CAST(s.valor_liquido AS DOUBLE) - st.media) / NULLIF(st.desvio, 0) > 3
            ORDER BY zscore DESC
            LIMIT 200
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_outlier_salarial: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        severity = "CRÍTICO" if row["zscore"] > 6 else "ALTO" if row["zscore"] > 3 else "MÉDIO"
        alerts.append(Alert(
            detector_id="SAL",
            severity=severity,
            entity_type="servidor",
            entity_name=row.get("servidor_nome", ""),
            description=(
                f"Salário R${row['salario']:,.2f} representa {row['zscore']:.1f}σ "
                f"acima da média do cargo '{row['cargo']}' "
                f"(média: R${row['media']:,.2f}, DP: R${row['desvio']:,.2f})"
            ),
            exposure_brl=float(row["salario"]) - float(row["media"]),
            base_legal=LEGAL["outlier_salarial"],
        ))
    return alerts


def detect_viagem_bloco(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """≥ 3 servidores com mesma origem, destino e data = viagem em bloco."""
    try:
        df = conn.execute("""
            SELECT
                data_saida,
                destino,
                COUNT(*) AS num_servidores,
                SUM(CAST(valor AS DOUBLE)) AS valor_total,
                STRING_AGG(servidor_nome, ' | ') AS servidores,
                secretaria
            FROM diarias
            WHERE data_saida IS NOT NULL AND destino IS NOT NULL
            GROUP BY data_saida, destino, secretaria
            HAVING num_servidores >= 3
            ORDER BY valor_total DESC
            LIMIT 100
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_viagem_bloco: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            detector_id="DIA",
            severity="CRÍTICO" if row["num_servidores"] >= 5 else "ALTO",
            entity_type="servidor",
            entity_name=f"Grupo {row['secretaria']} → {row['destino']}",
            description=(
                f"{int(row['num_servidores'])} servidores viajaram juntos para {row['destino']} "
                f"em {row['data_saida']}. Valor total: R${row['valor_total']:,.2f}. "
                f"Verifique portaria no D.O. e programação do evento."
            ),
            exposure_brl=float(row["valor_total"]),
            base_legal=LEGAL["viagem_bloco"],
        ))
    return alerts


def detect_concentracao_mercado(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """Empresa com > 30% do volume de uma secretaria."""
    try:
        df = conn.execute("""
            WITH totais_secretaria AS (
                SELECT
                    secretaria,
                    SUM(CAST(valor_total AS DOUBLE)) AS total_secretaria
                FROM obras
                GROUP BY secretaria
            ),
            por_empresa AS (
                SELECT
                    secretaria,
                    empresa_nome,
                    SUM(CAST(valor_total AS DOUBLE)) AS total_empresa,
                    COUNT(*) AS num_contratos
                FROM obras
                GROUP BY secretaria, empresa_nome
            )
            SELECT
                pe.secretaria,
                pe.empresa_nome,
                pe.total_empresa,
                pe.num_contratos,
                ts.total_secretaria,
                ROUND(pe.total_empresa / ts.total_secretaria * 100, 1) AS pct_mercado
            FROM por_empresa pe
            JOIN totais_secretaria ts ON pe.secretaria = ts.secretaria
            WHERE pct_mercado > 30
              AND ts.total_secretaria > 100000
            ORDER BY pct_mercado DESC
            LIMIT 50
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_concentracao_mercado: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        severity = "CRÍTICO" if row["pct_mercado"] > 50 else "ALTO"
        alerts.append(Alert(
            detector_id="OB",
            severity=severity,
            entity_type="empresa",
            entity_name=row["empresa_nome"],
            description=(
                f"{row['empresa_nome']} detém {row['pct_mercado']}% do volume da "
                f"{row['secretaria']} ({int(row['num_contratos'])} contratos, "
                f"R${row['total_empresa']:,.2f} de R${row['total_secretaria']:,.2f} total)"
            ),
            exposure_brl=float(row["total_empresa"]),
            base_legal=LEGAL["concentracao_mercado"],
        ))
    return alerts


def detect_empresa_suspensa(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """Empresa com contrato ativo que consta no CEIS/CNEP."""
    try:
        df = conn.execute("""
            SELECT
                o.empresa_nome,
                o.secretaria,
                SUM(CAST(o.valor_total AS DOUBLE)) AS total_contratos,
                c.motivo_sancao,
                c.data_inicio_sancao,
                c.data_fim_sancao
            FROM obras o
            JOIN cgu_ceis c ON o.empresa_nome ILIKE '%' || c.nome_sancionado || '%'
                            OR c.cnpj_cpf_sancionado = o.empresa_id::VARCHAR
            GROUP BY o.empresa_nome, o.secretaria, c.motivo_sancao,
                     c.data_inicio_sancao, c.data_fim_sancao
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_empresa_suspensa (esperado se CEIS não coletado ainda): {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            detector_id="CEIS",
            severity="CRÍTICO",
            entity_type="empresa",
            entity_name=row["empresa_nome"],
            description=(
                f"{row['empresa_nome']} consta no CEIS/CNEP: {row['motivo_sancao']} "
                f"(sanção: {row['data_inicio_sancao']} a {row['data_fim_sancao']}). "
                f"Possui R${row['total_contratos']:,.2f} em contratos ativos."
            ),
            exposure_brl=float(row["total_contratos"]),
            base_legal=LEGAL["empresa_suspensa"],
        ))
    return alerts


def detect_doacao_to_contrato(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """
    Empresa doou para candidato eleito → ganhou contratos pós-eleição.
    Requer tabelas: tse_doacoes + obras/licitacoes.
    """
    try:
        df = conn.execute("""
            SELECT
                d.nome_doador,
                d.cpf_cnpj_doador,
                d.nome_candidato,
                d.valor_receita AS valor_doacao,
                d.data_receita AS data_doacao,
                o.empresa_nome,
                SUM(CAST(o.valor_total AS DOUBLE)) AS contratos_pos_eleicao
            FROM tse_doacoes d
            JOIN obras o ON (
                d.cpf_cnpj_doador = o.empresa_id::VARCHAR
                OR o.empresa_nome ILIKE '%' || SPLIT_PART(d.nome_doador, ' ', 1) || '%'
            )
            WHERE o.data_contrato > '2024-10-06'  -- dia da eleição 2024
              AND d.data_receita < '2024-10-07'
            GROUP BY
                d.nome_doador, d.cpf_cnpj_doador,
                d.nome_candidato, d.valor_receita, d.data_receita,
                o.empresa_nome
            ORDER BY contratos_pos_eleicao DESC
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_doacao_to_contrato: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            detector_id="TSE",
            severity="CRÍTICO",
            entity_type="empresa",
            entity_name=row["empresa_nome"],
            description=(
                f"{row['nome_doador']} doou R${float(row['valor_doacao']):,.2f} "
                f"para {row['nome_candidato']} em {row['data_doacao']}. "
                f"Após a eleição, recebeu R${float(row['contratos_pos_eleicao']):,.2f} "
                f"em contratos públicos."
            ),
            exposure_brl=float(row["contratos_pos_eleicao"]),
            base_legal=LEGAL["doacao_contrato"],
        ))
    return alerts


def detect_fim_de_semana(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """Diárias pagas em sábado/domingo (dia da semana 6 ou 0 no DuckDB)."""
    try:
        df = conn.execute("""
            SELECT
                servidor_nome,
                data_saida,
                CAST(valor AS DOUBLE) AS valor,
                destino,
                secretaria,
                DAYOFWEEK(CAST(data_saida AS DATE)) AS dia_semana
            FROM diarias
            WHERE DAYOFWEEK(CAST(data_saida AS DATE)) IN (1, 7)  -- dom=1, sab=7
              AND CAST(valor AS DOUBLE) > 0
            ORDER BY valor DESC
            LIMIT 200
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_fim_de_semana: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        dia = "Domingo" if row["dia_semana"] == 1 else "Sábado"
        alerts.append(Alert(
            detector_id="FDS",
            severity="MÉDIO",
            entity_type="servidor",
            entity_name=row["servidor_nome"],
            description=(
                f"Diária de R${row['valor']:,.2f} paga em {dia} ({row['data_saida']}) "
                f"para viagem a {row['destino']}. Verificar se houve evento oficial."
            ),
            exposure_brl=float(row["valor"]),
            base_legal=LEGAL["fim_de_semana"],
        ))
    return alerts


def detect_nepotismo_sobrenome(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    """Sócios de empresas contratadas com sobrenome igual a servidores."""
    try:
        df = conn.execute("""
            WITH serv AS (
                SELECT
                    servidor_nome,
                    secretaria,
                    SPLIT_PART(TRIM(UPPER(servidor_nome)), ' ', -1) AS sobrenome
                FROM servidores
                WHERE LENGTH(TRIM(servidor_nome)) > 5
            ),
            socios AS (
                SELECT
                    s.socio_nome,
                    e.razao_social AS empresa,
                    e.cnpj,
                    SPLIT_PART(TRIM(UPPER(s.socio_nome)), ' ', -1) AS sobrenome
                FROM empresa_socios s
                JOIN empresas_cnpj e ON s.cnpj = e.cnpj
                WHERE LENGTH(TRIM(s.socio_nome)) > 5
            )
            SELECT
                sv.servidor_nome,
                sv.secretaria,
                sc.socio_nome,
                sc.empresa,
                sc.cnpj,
                sv.sobrenome AS sobrenome_comum
            FROM serv sv
            JOIN socios sc ON sv.sobrenome = sc.sobrenome
            WHERE LENGTH(sv.sobrenome) > 4
            ORDER BY sv.sobrenome
        """).fetchdf()
    except Exception as e:
        log.warning(f"detect_nepotismo_sobrenome: {e}")
        return []

    alerts = []
    for _, row in df.iterrows():
        alerts.append(Alert(
            detector_id="NEP",
            severity="ALTO",
            entity_type="servidor",
            entity_name=row["servidor_nome"],
            description=(
                f"Servidor {row['servidor_nome']} ({row['secretaria']}) compartilha "
                f"sobrenome '{row['sobrenome_comum']}' com sócio {row['socio_nome']} "
                f"da empresa contratada {row['empresa']} (CNPJ: {row['cnpj']}). "
                f"Verificar parentesco e conflito de interesse."
            ),
            exposure_brl=0,
            base_legal=LEGAL["nepotismo"],
        ))
    return alerts


# ─── REGISTRO DE DETECTORES ───────────────────────────────────────────────────

DETECTORS: dict[str, Callable] = {
    "fracionamento":          detect_fracionamento,
    "outlier_salarial":       detect_outlier_salarial,
    "viagem_bloco":           detect_viagem_bloco,
    "concentracao_mercado":   detect_concentracao_mercado,
    "empresa_suspensa":       detect_empresa_suspensa,
    "doacao_contrato":        detect_doacao_to_contrato,
    "fim_de_semana":          detect_fim_de_semana,
    "nepotismo_sobrenome":    detect_nepotismo_sobrenome,
}


# ─── PIPELINE ─────────────────────────────────────────────────────────────────

def run_all_detectors(conn: duckdb.DuckDBPyConnection, detector_ids: list[str] | None = None) -> list[Alert]:
    detectors = (
        {k: v for k, v in DETECTORS.items() if k in detector_ids}
        if detector_ids
        else DETECTORS
    )

    all_alerts = []
    for name, fn in detectors.items():
        try:
            console.print(f"[cyan]▶ {name}...[/cyan]", end=" ")
            alerts = fn(conn)
            console.print(f"[green]{len(alerts)} alertas[/green]")
            all_alerts.extend(alerts)
        except Exception as e:
            console.print(f"[red]ERRO: {e}[/red]")

    return all_alerts


def save_alerts(conn: duckdb.DuckDBPyConnection, alerts: list[Alert]):
    if not alerts:
        return

    rows = [
        {
            "dossie_id": a.dossie_id,
            "detector_id": a.detector_id,
            "severity": a.severity,
            "entity_type": a.entity_type,
            "entity_name": a.entity_name,
            "description": a.description,
            "exposure_brl": a.exposure_brl,
            "base_legal": a.base_legal,
            "evidence": json.dumps(a.evidence, ensure_ascii=False),
            "detected_at": datetime.utcnow().isoformat(),
        }
        for a in alerts
    ]
    df = pd.DataFrame(rows)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            dossie_id VARCHAR,
            detector_id VARCHAR,
            severity VARCHAR,
            entity_type VARCHAR,
            entity_name VARCHAR,
            description VARCHAR,
            exposure_brl DOUBLE,
            base_legal VARCHAR,
            evidence VARCHAR,
            detected_at TIMESTAMP,
            status VARCHAR DEFAULT 'DETECTADO'
        )
    """)

    # Dedup por dossie_id
    try:
        existing = set(conn.execute("SELECT dossie_id FROM alerts").fetchdf()["dossie_id"])
    except Exception:
        existing = set()

    new = df[~df["dossie_id"].isin(existing)]
    if not new.empty:
        conn.execute("INSERT INTO alerts SELECT *, 'DETECTADO' FROM new")
        console.print(f"[green]✓ {len(new)} novos alertas salvos[/green]")


def print_summary(alerts: list[Alert]):
    by_severity = {"CRÍTICO": [], "ALTO": [], "MÉDIO": []}
    for a in alerts:
        by_severity.get(a.severity, []).append(a)

    table = Table(title="ALERTAS DETECTADOS", border_style="red")
    table.add_column("Severidade", style="bold")
    table.add_column("Detector")
    table.add_column("Entidade", max_width=40)
    table.add_column("Exposição", justify="right")
    table.add_column("Descrição", max_width=60)

    colors = {"CRÍTICO": "red", "ALTO": "yellow", "MÉDIO": "cyan"}

    for sev in ["CRÍTICO", "ALTO", "MÉDIO"]:
        for a in by_severity[sev]:
            table.add_row(
                f"[{colors[sev]}]{sev}[/{colors[sev]}]",
                a.detector_id,
                a.entity_name,
                f"R${a.exposure_brl:,.0f}",
                a.description[:80] + "..." if len(a.description) > 80 else a.description,
            )

    console.print(table)
    total_exposure = sum(a.exposure_brl for a in alerts)
    console.print(
        f"\n[bold]Total: {len(alerts)} alertas | "
        f"Exposição total: R${total_exposure:,.2f}[/bold]"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--detector", help="Roda só este detector")
    parser.add_argument("--export-csv", action="store_true")
    args = parser.parse_args()

    conn = duckdb.connect(DB_PATH)

    detector_ids = [args.detector] if args.detector else None
    alerts = run_all_detectors(conn, detector_ids)

    if alerts:
        print_summary(alerts)
        save_alerts(conn, alerts)

        if args.export_csv:
            df = pd.DataFrame([a.__dict__ for a in alerts])
            out = Path("data/alerts_export.csv")
            df.to_csv(out, index=False)
            console.print(f"[green]Exportado: {out}[/green]")

    conn.close()
