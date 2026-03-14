"""
SENTINELA // ENGINE LEGADO DE CRUZAMENTOS

Este modulo continua existindo para triagem tecnica interna e compatibilidade
com rotinas antigas, mas agora:
  - usa o schema real do banco atual;
  - grava campos probatorios no `alerts`;
  - bloqueia por padrao os detectores mais propensos a falso positivo;
  - nunca deve ser usado como saida externa sem passar pela camada operacional.

USO:
    python -m src.core.cross_reference_engine
    python -m src.core.cross_reference_engine --detector fracionamento --allow-internal
    python -m src.core.cross_reference_engine --export-csv
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()
log = logging.getLogger("sentinela.cross")
DB_PATH = "data/sentinela_analytics.duckdb"

COMMON_ACRE_SURNAMES = frozenset(
    {
        "SILVA",
        "SOUZA",
        "SANTOS",
        "OLIVEIRA",
        "LIMA",
        "PEREIRA",
        "COSTA",
        "FERREIRA",
        "ALVES",
        "RODRIGUES",
        "NASCIMENTO",
        "GOMES",
        "ARAUJO",
        "ARAÚJO",
        "ROCHA",
        "MARTINS",
        "CARVALHO",
        "MELO",
        "RIBEIRO",
        "MOURA",
        "BATISTA",
        "NUNES",
        "CAVALCANTE",
        "PINTO",
        "LOPES",
        "MOREIRA",
        "DIAS",
        "MONTEIRO",
        "CASTRO",
        "CAMPOS",
        "MENDES",
        "TEIXEIRA",
        "FREITAS",
        "MACEDO",
        "CUNHA",
        "ANDRADE",
        "RAMOS",
        "SOARES",
        "CARDOSO",
        "FARIAS",
        "MEDEIROS",
        "NETO",
        "FILHO",
        "JUNIOR",
        "SALES",
        "VALE",
        "QUEIROZ",
        "BEZERRA",
        "BARBOSA",
        "CORREIA",
        "AZEVEDO",
        "FIGUEIREDO",
        "NOGUEIRA",
        "KAXINAWA",
        "KAXINAWÁ",
        "YAWANAWA",
        "YAWANAWÁ",
        "ASHANINKA",
        "APURINA",
        "APURINÃ",
        "JAMINAWA",
    }
)
MIN_SOBRENOME_LEN = 6
LIMITE_DISPENSA_BENS_SERVICOS = 50_000.00

LEGAL = {
    "fracionamento": (
        "Lei 14.133/2021, art. 75, I c/c art. 22, §4º. "
        "Padrão compatível com fracionamento exige apuração documental da unidade contratante."
    ),
    "outlier_salarial": (
        "CF art. 37, X. Triagem estatística exige verificação humana de adicionais legais e regimes específicos."
    ),
    "nepotismo": (
        "Súmula Vinculante 13 do STF exige prova de parentesco. Coincidência de sobrenome, sozinha, não comprova nepotismo."
    ),
    "empresa_suspensa": (
        "Lei 14.133/2021, art. 156, e bases CEIS/CNEP da CGU. Sem data contratual materializada, o cruzamento é apenas triagem interna."
    ),
    "fim_de_semana": (
        "Acórdão 2.484/2021-TCU-Plenário. Diária em fim de semana requer contexto do evento e ato formal."
    ),
    "viagem_bloco": (
        "Rastro operacional de diárias em grupo. Exige conferência de portaria, evento e programação."
    ),
    "concentracao_mercado": (
        "Lei 14.133/2021, art. 5º. Concentração elevada é rastro contratual, não prova automática de direcionamento."
    ),
    "doacao_contrato": (
        "Lei 12.846/2013 e dados TSE. O cruzamento é indiciário e depende de cronologia contratual válida."
    ),
}

INTERNAL_ONLY_DEFAULT = {
    "fracionamento",
    "outlier_salarial",
    "empresa_suspensa",
    "doacao_contrato",
    "nepotismo_sobrenome",
}


@dataclass
class Alert:
    detector_id: str
    severity: str
    entity_type: str
    entity_name: str
    description: str
    exposure_brl: float
    base_legal: str
    classe_achado: str = "HIPOTESE_INVESTIGATIVA"
    grau_probatorio: str = "EXPLORATORIO"
    fonte_primaria: str = ""
    uso_externo: str = "REVISAO_INTERNA"
    inferencia_permitida: str = ""
    limite_conclusao: str = "Saída apenas para triagem interna."
    evidence: list[dict] = field(default_factory=list)
    dossie_id: str = ""

    def __post_init__(self) -> None:
        if not self.dossie_id:
            digest = hashlib.sha1(
                f"{self.detector_id}|{self.entity_type}|{self.entity_name}|{self.description}".encode("utf-8")
            ).hexdigest()[:16]
            self.dossie_id = f"{self.detector_id[:4].upper()}_{digest}"


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    tables = set(conn.execute("SHOW TABLES").df()["name"].tolist())
    return table_name in tables


def _build_contract_union(conn: duckdb.DuckDBPyConnection) -> str | None:
    sources: list[str] = []
    if _table_exists(conn, "obras"):
        sources.append(
            """
            SELECT
                empresa_nome,
                secretaria,
                CAST(valor_total AS DOUBLE) AS valor_total,
                CAST(capturado_em AS DATE) AS data_ref,
                empresa_id AS empresa_doc,
                'obras' AS fonte
            FROM obras
            """
        )
    if _table_exists(conn, "licitacoes"):
        sources.append(
            """
            SELECT
                empresa_nome,
                secretaria,
                CAST(valor_total AS DOUBLE) AS valor_total,
                CAST(capturado_em AS DATE) AS data_ref,
                empresa_id AS empresa_doc,
                'licitacoes' AS fonte
            FROM licitacoes
            """
        )
    if not sources:
        return None
    return "\nUNION ALL\n".join(sources)


def detect_fracionamento(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    contract_union = _build_contract_union(conn)
    if not contract_union:
        return []

    df = conn.execute(
        f"""
        WITH contratos AS (
            {contract_union}
        )
        SELECT
            empresa_nome,
            secretaria,
            COUNT(*) AS num_contratos,
            SUM(valor_total) AS valor_agregado,
            MAX(valor_total) AS maior_contrato,
            MIN(data_ref) AS primeiro,
            MAX(data_ref) AS ultimo
        FROM contratos
        WHERE valor_total > 0
          AND valor_total < {LIMITE_DISPENSA_BENS_SERVICOS}
          AND empresa_nome IS NOT NULL
          AND secretaria IS NOT NULL
        GROUP BY 1, 2
        HAVING COUNT(*) >= 3
           AND SUM(valor_total) > {LIMITE_DISPENSA_BENS_SERVICOS}
        ORDER BY valor_agregado DESC
        LIMIT 100
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        n = int(row["num_contratos"] or 0)
        alerts.append(
            Alert(
                detector_id="FRAC",
                severity="CRÍTICO" if n >= 5 else "ALTO",
                entity_type="empresa",
                entity_name=row["empresa_nome"],
                description=(
                    f"{n} contratações abaixo de R$ {LIMITE_DISPENSA_BENS_SERVICOS:,.2f} "
                    f"para {row['empresa_nome']} em {row['secretaria']}, totalizando "
                    f"R$ {float(row['valor_agregado'] or 0):,.2f} entre {row['primeiro']} e {row['ultimo']}."
                ),
                exposure_brl=float(row["valor_agregado"] or 0),
                base_legal=LEGAL["fracionamento"],
                classe_achado="RASTRO_CONTRATUAL",
                grau_probatorio="DOCUMENTAL_PRIMARIO" if n >= 5 else "INDICIARIO",
                fonte_primaria="PORTAL_LOCAL",
                uso_externo="APTO_APURACAO" if n >= 5 else "REVISAO_INTERNA",
                inferencia_permitida="Há padrão contratual compatível com fracionamento e que exige conferência do processo integral.",
                limite_conclusao="O padrão não basta para afirmar dolo ou fracionamento ilícito sem comparar objeto, cronologia e modalidade usada.",
            )
        )
    return alerts


def detect_outlier_salarial(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not _table_exists(conn, "servidores"):
        return []

    df = conn.execute(
        """
        WITH stats AS (
            SELECT
                cargo,
                AVG(CAST(valor_liquido AS DOUBLE)) AS media,
                STDDEV(CAST(valor_liquido AS DOUBLE)) AS desvio,
                COUNT(*) AS n
            FROM servidores
            WHERE valor_liquido IS NOT NULL
              AND CAST(valor_liquido AS DOUBLE) > 0
              AND cargo IS NOT NULL
            GROUP BY 1
            HAVING COUNT(*) >= 30
               AND STDDEV(CAST(valor_liquido AS DOUBLE)) > 0
        )
        SELECT
            s.servidor_nome,
            s.cargo,
            s.secretaria,
            CAST(s.valor_liquido AS DOUBLE) AS salario,
            st.media,
            st.desvio,
            st.n,
            (CAST(s.valor_liquido AS DOUBLE) - st.media) / st.desvio AS zscore
        FROM servidores s
        JOIN stats st ON s.cargo = st.cargo
        WHERE (CAST(s.valor_liquido AS DOUBLE) - st.media) / st.desvio > 3.5
        ORDER BY zscore DESC
        LIMIT 100
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        alerts.append(
            Alert(
                detector_id="SAL",
                severity="ALTO" if float(row["zscore"] or 0) > 5 else "MÉDIO",
                entity_type="servidor",
                entity_name=row["servidor_nome"],
                description=(
                    f"Valor líquido de R$ {float(row['salario'] or 0):,.2f}, "
                    f"{float(row['zscore'] or 0):.1f}σ acima da média do cargo {row['cargo']} "
                    f"(média R$ {float(row['media'] or 0):,.2f}, n={int(row['n'] or 0)})."
                ),
                exposure_brl=max(0.0, float(row["salario"] or 0) - float(row["media"] or 0)),
                base_legal=LEGAL["outlier_salarial"],
                classe_achado="HIPOTESE_INVESTIGATIVA",
                grau_probatorio="INDICIARIO",
                fonte_primaria="FOLHA_LOCAL",
                uso_externo="REVISAO_INTERNA",
                inferencia_permitida="Há discrepância estatística relevante no cargo.",
                limite_conclusao="Não usar externamente sem verificar adicionais legais, acumulação autorizada e erro material de folha.",
            )
        )
    return alerts


def detect_viagem_bloco(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not _table_exists(conn, "diarias"):
        return []

    df = conn.execute(
        """
        SELECT
            data_saida,
            destino,
            secretaria,
            COUNT(*) AS num_servidores,
            SUM(CAST(valor AS DOUBLE)) AS valor_total
        FROM diarias
        WHERE data_saida IS NOT NULL
          AND destino IS NOT NULL
          AND CAST(valor AS DOUBLE) > 0
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 3
        ORDER BY valor_total DESC
        LIMIT 100
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        alerts.append(
            Alert(
                detector_id="DIA",
                severity="ALTO" if int(row["num_servidores"] or 0) >= 5 else "MÉDIO",
                entity_type="grupo",
                entity_name=f"{row['secretaria']} → {row['destino']}",
                description=(
                    f"{int(row['num_servidores'] or 0)} servidores viajaram para {row['destino']} "
                    f"em {row['data_saida']} com total de R$ {float(row['valor_total'] or 0):,.2f}."
                ),
                exposure_brl=float(row["valor_total"] or 0),
                base_legal=LEGAL["viagem_bloco"],
                classe_achado="RASTRO_CONTRATUAL",
                grau_probatorio="INDICIARIO",
                fonte_primaria="PORTAL_DIARIAS",
                uso_externo="REVISAO_INTERNA",
                inferencia_permitida="Há deslocamento coletivo que exige verificação de evento, portaria e motivação.",
                limite_conclusao="Sem programação oficial e ato concessório, o agrupamento não basta para afirmar irregularidade.",
            )
        )
    return alerts


def detect_concentracao_mercado(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not _table_exists(conn, "obras"):
        return []

    df = conn.execute(
        """
        WITH tot AS (
            SELECT secretaria, SUM(CAST(valor_total AS DOUBLE)) AS total_secretaria
            FROM obras
            WHERE valor_total IS NOT NULL
            GROUP BY 1
        )
        SELECT
            o.secretaria,
            o.empresa_nome,
            SUM(CAST(o.valor_total AS DOUBLE)) AS total_empresa,
            t.total_secretaria,
            SUM(CAST(o.valor_total AS DOUBLE)) / NULLIF(t.total_secretaria, 0) AS share
        FROM obras o
        JOIN tot t ON t.secretaria = o.secretaria
        WHERE o.empresa_nome IS NOT NULL
        GROUP BY 1, 2, 4
        HAVING SUM(CAST(o.valor_total AS DOUBLE)) / NULLIF(t.total_secretaria, 0) >= 0.40
        ORDER BY share DESC, total_empresa DESC
        LIMIT 100
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        alerts.append(
            Alert(
                detector_id="OB",
                severity="ALTO",
                entity_type="empresa",
                entity_name=row["empresa_nome"],
                description=(
                    f"{row['empresa_nome']} concentra {float(row['share'] or 0) * 100:.1f}% "
                    f"da exposição de {row['secretaria']}, somando R$ {float(row['total_empresa'] or 0):,.2f}."
                ),
                exposure_brl=float(row["total_empresa"] or 0),
                base_legal=LEGAL["concentracao_mercado"],
                classe_achado="RASTRO_CONTRATUAL",
                grau_probatorio="INDICIARIO",
                fonte_primaria="PORTAL_OBRAS",
                uso_externo="APTO_APURACAO",
                inferencia_permitida="Há concentração contratual relevante em um único recebedor.",
                limite_conclusao="Concentração elevada não prova direcionamento; exige leitura do mercado, objeto e universo de licitantes.",
            )
        )
    return alerts


def detect_empresa_suspensa(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not (_table_exists(conn, "obras") and _table_exists(conn, "cgu_ceis")):
        return []

    df = conn.execute(
        """
        WITH sancoes AS (
            SELECT
                nome_sancionado,
                REGEXP_REPLACE(COALESCE(cnpj_cpf_sancionado, ''), '[^0-9]', '', 'g') AS doc,
                motivo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                orgao_sancionador
            FROM cgu_ceis
        )
        SELECT
            o.empresa_nome,
            o.secretaria,
            SUM(CAST(o.valor_total AS DOUBLE)) AS total_contratos,
            s.motivo_sancao,
            s.data_inicio_sancao,
            s.data_fim_sancao,
            s.orgao_sancionador
        FROM obras o
        JOIN sancoes s
          ON (
              REGEXP_REPLACE(COALESCE(o.empresa_id, ''), '[^0-9]', '', 'g') <> ''
              AND REGEXP_REPLACE(COALESCE(o.empresa_id, ''), '[^0-9]', '', 'g') = s.doc
          )
          OR (
              REGEXP_REPLACE(COALESCE(o.empresa_id, ''), '[^0-9]', '', 'g') = ''
              AND UPPER(TRIM(o.empresa_nome)) = UPPER(TRIM(s.nome_sancionado))
          )
        GROUP BY 1, 2, 4, 5, 6, 7
        ORDER BY total_contratos DESC
        LIMIT 100
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        alerts.append(
            Alert(
                detector_id="CEIS",
                severity="ALTO",
                entity_type="empresa",
                entity_name=row["empresa_nome"],
                description=(
                    f"{row['empresa_nome']} coincide com cadastro CEIS/CNEP ({row['motivo_sancao']}) "
                    f"e soma R$ {float(row['total_contratos'] or 0):,.2f} em contratos no portal local."
                ),
                exposure_brl=float(row["total_contratos"] or 0),
                base_legal=LEGAL["empresa_suspensa"],
                classe_achado="CRUZAMENTO_SANCIONATORIO",
                grau_probatorio="INDICIARIO",
                fonte_primaria="CEIS_CGU + PORTAL_LOCAL",
                uso_externo="REVISAO_INTERNA",
                inferencia_permitida="Há coincidência entre recebedor local e base sancionatória federal.",
                limite_conclusao="Sem data contratual materializada e sem checagem de abrangência, o cruzamento não deve virar peça externa automática.",
            )
        )
    return alerts


def detect_doacao_to_contrato(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not (_table_exists(conn, "tse_doacoes") and _table_exists(conn, "obras")):
        return []

    df = conn.execute(
        """
        SELECT
            d.nm_doador_originario AS doador,
            d.nr_cpf_cnpj_doador_originario AS doc_doador,
            SUM(CAST(REPLACE(REPLACE(d.vr_receita, '.', ''), ',', '.') AS DOUBLE)) AS valor_doado,
            o.empresa_nome,
            SUM(CAST(o.valor_total AS DOUBLE)) AS total_contratos
        FROM tse_doacoes d
        JOIN obras o
          ON REGEXP_REPLACE(COALESCE(d.nr_cpf_cnpj_doador_originario, ''), '[^0-9]', '', 'g')
             = REGEXP_REPLACE(COALESCE(o.empresa_id, ''), '[^0-9]', '', 'g')
        GROUP BY 1, 2, 4
        ORDER BY total_contratos DESC
        LIMIT 100
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        alerts.append(
            Alert(
                detector_id="TSE",
                severity="MÉDIO",
                entity_type="empresa",
                entity_name=row["empresa_nome"],
                description=(
                    f"CNPJ doador do TSE coincide com recebedor local: doação total R$ {float(row['valor_doado'] or 0):,.2f} "
                    f"e contratos somando R$ {float(row['total_contratos'] or 0):,.2f}."
                ),
                exposure_brl=float(row["total_contratos"] or 0),
                base_legal=LEGAL["doacao_contrato"],
                classe_achado="HIPOTESE_INVESTIGATIVA",
                grau_probatorio="INDICIARIO",
                fonte_primaria="TSE + PORTAL_LOCAL",
                uso_externo="REVISAO_INTERNA",
                inferencia_permitida="Há coincidência societária entre doador eleitoral e fornecedor.",
                limite_conclusao="Sem cronologia contratual válida e sem vínculo com agente decisor, o cruzamento é apenas indiciário.",
            )
        )
    return alerts


def detect_fim_de_semana(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not _table_exists(conn, "diarias"):
        return []

    df = conn.execute(
        """
        SELECT
            servidor_nome,
            data_saida,
            CAST(valor AS DOUBLE) AS valor,
            destino,
            secretaria,
            DAYOFWEEK(data_saida) AS dia_semana
        FROM diarias
        WHERE DAYOFWEEK(data_saida) IN (1, 7)
          AND CAST(valor AS DOUBLE) > 0
        ORDER BY valor DESC
        LIMIT 200
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        dia = "Domingo" if int(row["dia_semana"] or 0) == 1 else "Sábado"
        alerts.append(
            Alert(
                detector_id="FDS",
                severity="MÉDIO",
                entity_type="servidor",
                entity_name=row["servidor_nome"],
                description=(
                    f"Diária de R$ {float(row['valor'] or 0):,.2f} em {dia} ({row['data_saida']}) "
                    f"para {row['destino']} pela unidade {row['secretaria']}."
                ),
                exposure_brl=float(row["valor"] or 0),
                base_legal=LEGAL["fim_de_semana"],
                classe_achado="HIPOTESE_INVESTIGATIVA",
                grau_probatorio="INDICIARIO",
                fonte_primaria="PORTAL_DIARIAS",
                uso_externo="REVISAO_INTERNA",
                inferencia_permitida="Há diária em fim de semana que exige contexto do evento e ato concessório.",
                limite_conclusao="Sem prova de ausência de programação oficial, o pagamento em fim de semana não basta para afirmar irregularidade.",
            )
        )
    return alerts


def detect_nepotismo_sobrenome(conn: duckdb.DuckDBPyConnection) -> list[Alert]:
    if not (_table_exists(conn, "servidores") and _table_exists(conn, "empresa_socios") and _table_exists(conn, "empresas_cnpj")):
        return []

    surnames = "', '".join(sorted(COMMON_ACRE_SURNAMES))
    df = conn.execute(
        f"""
        WITH serv AS (
            SELECT
                servidor_nome,
                secretaria,
                cargo,
                SPLIT_PART(TRIM(UPPER(servidor_nome)), ' ', -1) AS sobrenome
            FROM servidores
            WHERE servidor_nome IS NOT NULL
              AND LENGTH(TRIM(servidor_nome)) > 5
        ),
        socios AS (
            SELECT
                s.socio_nome,
                e.razao_social AS empresa,
                e.cnpj,
                SPLIT_PART(TRIM(UPPER(s.socio_nome)), ' ', -1) AS sobrenome
            FROM empresa_socios s
            JOIN empresas_cnpj e ON e.cnpj = s.cnpj
            WHERE s.socio_nome IS NOT NULL
              AND LENGTH(TRIM(s.socio_nome)) > 5
        )
        SELECT
            sv.servidor_nome,
            sv.secretaria,
            sv.cargo,
            sc.socio_nome,
            sc.empresa,
            sc.cnpj,
            sv.sobrenome
        FROM serv sv
        JOIN socios sc ON sv.sobrenome = sc.sobrenome
        WHERE LENGTH(sv.sobrenome) >= {MIN_SOBRENOME_LEN}
          AND sv.sobrenome NOT IN ('{surnames}')
        ORDER BY sv.sobrenome, sv.secretaria
        LIMIT 200
        """
    ).fetchdf()

    alerts: list[Alert] = []
    for _, row in df.iterrows():
        alerts.append(
            Alert(
                detector_id="NEP",
                severity="MÉDIO",
                entity_type="servidor",
                entity_name=row["servidor_nome"],
                description=(
                    f"Triagem interna: sobrenome '{row['sobrenome']}' coincide entre servidor "
                    f"{row['servidor_nome']} ({row['secretaria']}) e sócio {row['socio_nome']} "
                    f"da empresa {row['empresa']} ({row['cnpj']})."
                ),
                exposure_brl=0.0,
                base_legal=LEGAL["nepotismo"],
                classe_achado="HIPOTESE_INVESTIGATIVA",
                grau_probatorio="EXPLORATORIO",
                fonte_primaria="QSA + FOLHA_LOCAL",
                uso_externo="REVISAO_INTERNA",
                inferencia_permitida="Há coincidência de sobrenome que pode orientar busca documental adicional.",
                limite_conclusao="Coincidência de sobrenome não prova parentesco, nepotismo nem conflito de interesse. Não usar externamente.",
            )
        )
    return alerts


DETECTORS: dict[str, Callable[[duckdb.DuckDBPyConnection], list[Alert]]] = {
    "fracionamento": detect_fracionamento,
    "outlier_salarial": detect_outlier_salarial,
    "viagem_bloco": detect_viagem_bloco,
    "concentracao_mercado": detect_concentracao_mercado,
    "empresa_suspensa": detect_empresa_suspensa,
    "doacao_contrato": detect_doacao_to_contrato,
    "fim_de_semana": detect_fim_de_semana,
    "nepotismo_sobrenome": detect_nepotismo_sobrenome,
}


def run_all_detectors(
    conn: duckdb.DuckDBPyConnection,
    detector_ids: list[str] | None = None,
    *,
    allow_internal: bool = False,
) -> list[Alert]:
    selected = {k: v for k, v in DETECTORS.items() if detector_ids is None or k in detector_ids}
    if not allow_internal:
        selected = {k: v for k, v in selected.items() if k not in INTERNAL_ONLY_DEFAULT}

    all_alerts: list[Alert] = []
    for name, fn in selected.items():
        try:
            console.print(f"[cyan]▶ {name}...[/cyan]", end=" ")
            alerts = fn(conn)
            console.print(f"[green]{len(alerts)} alertas[/green]")
            all_alerts.extend(alerts)
        except Exception as exc:
            console.print(f"[red]ERRO: {exc}[/red]")
    return all_alerts


def ensure_alert_columns(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
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
        """
    )
    existing = {row[1] for row in conn.execute("PRAGMA table_info('alerts')").fetchall()}
    extra_columns = {
        "classe_achado": "VARCHAR",
        "grau_probatorio": "VARCHAR",
        "fonte_primaria": "VARCHAR",
        "uso_externo": "VARCHAR",
        "inferencia_permitida": "VARCHAR",
        "limite_conclusao": "VARCHAR",
    }
    for column, dtype in extra_columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE alerts ADD COLUMN {column} {dtype}")


def save_alerts(conn: duckdb.DuckDBPyConnection, alerts: list[Alert]) -> int:
    if not alerts:
        return 0

    ensure_alert_columns(conn)
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
            "detected_at": datetime.now(UTC).isoformat(),
            "status": "DETECTADO",
            "classe_achado": a.classe_achado,
            "grau_probatorio": a.grau_probatorio,
            "fonte_primaria": a.fonte_primaria,
            "uso_externo": a.uso_externo,
            "inferencia_permitida": a.inferencia_permitida,
            "limite_conclusao": a.limite_conclusao,
        }
        for a in alerts
    ]
    df = pd.DataFrame(rows)
    existing = set()
    try:
        existing = set(conn.execute("SELECT dossie_id FROM alerts").fetchdf()["dossie_id"])
    except Exception:
        pass
    new = df[~df["dossie_id"].isin(existing)]
    if new.empty:
        return 0

    conn.register("new_alerts_df", new)
    conn.execute(
        """
        INSERT INTO alerts (
            dossie_id, detector_id, severity, entity_type, entity_name, description,
            exposure_brl, base_legal, evidence, detected_at, status,
            classe_achado, grau_probatorio, fonte_primaria, uso_externo,
            inferencia_permitida, limite_conclusao
        )
        SELECT
            dossie_id, detector_id, severity, entity_type, entity_name, description,
            exposure_brl, base_legal, evidence, CAST(detected_at AS TIMESTAMP), status,
            classe_achado, grau_probatorio, fonte_primaria, uso_externo,
            inferencia_permitida, limite_conclusao
        FROM new_alerts_df
        """
    )
    conn.unregister("new_alerts_df")
    return len(new)


def print_summary(alerts: list[Alert]) -> None:
    by_severity = {"CRÍTICO": [], "ALTO": [], "MÉDIO": []}
    for alert in alerts:
        by_severity.setdefault(alert.severity, []).append(alert)

    table = Table(title="ALERTAS LEGADOS / TRIAGEM INTERNA", border_style="red")
    table.add_column("Severidade", style="bold")
    table.add_column("Detector")
    table.add_column("Entidade", max_width=34)
    table.add_column("Uso")
    table.add_column("Descrição", max_width=72)

    colors = {"CRÍTICO": "red", "ALTO": "yellow", "MÉDIO": "cyan"}
    for severity in ["CRÍTICO", "ALTO", "MÉDIO"]:
        for alert in by_severity.get(severity, []):
            table.add_row(
                f"[{colors.get(severity, 'white')}]{severity}[/{colors.get(severity, 'white')}]",
                alert.detector_id,
                alert.entity_name,
                alert.uso_externo,
                alert.description[:120] + "..." if len(alert.description) > 120 else alert.description,
            )
    console.print(table)
    total_exposure = sum(a.exposure_brl for a in alerts)
    console.print(
        f"\n[bold]Total: {len(alerts)} alertas | Exposição total: R$ {total_exposure:,.2f}[/bold]"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--detector", help="Roda só este detector")
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument(
        "--allow-internal",
        action="store_true",
        help="Inclui detectores internos/mais sensíveis que ficam bloqueados por padrão.",
    )
    args = parser.parse_args()

    conn = duckdb.connect(DB_PATH)
    try:
        detector_ids = [args.detector] if args.detector else None
        alerts = run_all_detectors(conn, detector_ids, allow_internal=args.allow_internal)
        if not alerts:
            console.print("[yellow]Nenhum alerta legado gerado.[/yellow]")
            return 0

        print_summary(alerts)
        saved = save_alerts(conn, alerts)
        console.print(f"[green]✓ {saved} novos alertas salvos[/green]")

        if args.export_csv:
            df = pd.DataFrame([a.__dict__ for a in alerts])
            out = Path("data/alerts_export.csv")
            df.to_csv(out, index=False)
            console.print(f"[green]Exportado: {out}[/green]")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
