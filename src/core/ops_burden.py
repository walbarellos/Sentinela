from __future__ import annotations

import json
from typing import Any

import duckdb
import pandas as pd

from src.core.ops_legal import legal_anchor_payload


BURDEN_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_burden_item (
    burden_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    item_key VARCHAR NOT NULL,
    item_label VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    status_order INTEGER NOT NULL,
    evidence_grade VARCHAR,
    legal_anchors_json JSON,
    source_refs_json JSON,
    rationale VARCHAR,
    next_action VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

BURDEN_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_burden_item AS
SELECT *
FROM ops_case_burden_item
ORDER BY case_id, status_order, item_key
"""

BURDEN_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_burden_summary AS
SELECT
    case_id,
    COUNT(*) AS total_items,
    COUNT(*) FILTER (WHERE status = 'COMPROVADO_DOCUMENTAL') AS comprovado_documental,
    COUNT(*) FILTER (WHERE status = 'PENDENTE_DOCUMENTO') AS pendente_documento,
    COUNT(*) FILTER (WHERE status = 'PENDENTE_ENQUADRAMENTO') AS pendente_enquadramento,
    COUNT(*) FILTER (WHERE status = 'SEM_BASE_ATUAL') AS sem_base_atual
FROM ops_case_burden_item
GROUP BY 1
ORDER BY case_id
"""

STATUS_ORDER = {
    "COMPROVADO_DOCUMENTAL": 10,
    "PENDENTE_DOCUMENTO": 20,
    "PENDENTE_ENQUADRAMENTO": 30,
    "SEM_BASE_ATUAL": 40,
}


def ensure_ops_burden(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(BURDEN_DDL)
    con.execute(BURDEN_VIEW)
    con.execute(BURDEN_SUMMARY_VIEW)


def _has_artifact(artifacts_df: pd.DataFrame, *labels: str) -> bool:
    if artifacts_df.empty:
        return False
    target = {label.lower() for label in labels}
    return artifacts_df["label"].fillna("").str.lower().isin(target).any()


def _artifact_sources(artifacts_df: pd.DataFrame, *labels: str) -> list[str]:
    if artifacts_df.empty:
        return []
    target = {label.lower() for label in labels}
    rows = artifacts_df[artifacts_df["label"].fillna("").str.lower().isin(target)]
    return rows["path"].dropna().astype(str).tolist()


def _inbox_status(inbox_df: pd.DataFrame, *doc_keys: str) -> str | None:
    if inbox_df.empty:
        return None
    target = {key.lower() for key in doc_keys}
    rows = inbox_df[inbox_df["documento_chave"].fillna("").str.lower().isin(target)]
    if rows.empty:
        return None
    statuses = rows["status_documento"].fillna("").astype(str).str.upper().tolist()
    if any(status in {"RECEBIDO", "ANALISADO"} for status in statuses):
        return "RECEBIDO"
    if any(status == "ARQUIVO_NAO_LOCALIZADO" for status in statuses):
        return "ARQUIVO_NAO_LOCALIZADO"
    if any(status == "PENDENTE" for status in statuses):
        return "PENDENTE"
    return statuses[0] if statuses else None


def _inbox_sources(inbox_df: pd.DataFrame, *doc_keys: str) -> list[str]:
    if inbox_df.empty:
        return []
    target = {key.lower() for key in doc_keys}
    rows = inbox_df[inbox_df["documento_chave"].fillna("").str.lower().isin(target)]
    return rows["file_path"].dropna().astype(str).tolist()


def _make_burden_row(
    *,
    case_id: str,
    family: str,
    item_key: str,
    item_label: str,
    status: str,
    evidence_grade: str,
    anchors: list[str],
    source_refs: list[str],
    rationale: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "burden_id": f"{case_id}:{item_key}",
        "case_id": case_id,
        "family": family,
        "item_key": item_key,
        "item_label": item_label,
        "status": status,
        "status_order": STATUS_ORDER[status],
        "evidence_grade": evidence_grade,
        "legal_anchors_json": json.dumps(legal_anchor_payload(anchors), ensure_ascii=False),
        "source_refs_json": json.dumps(source_refs, ensure_ascii=False),
        "rationale": rationale,
        "next_action": next_action,
    }


def _build_rb_burden(case: dict[str, Any], artifacts_df: pd.DataFrame, inbox_df: pd.DataFrame) -> list[dict[str, Any]]:
    numero = str(case["case_id"]).split(":")[-1]
    licitacao_sources = _artifact_sources(artifacts_df, "licitacao_detail", "cpl_publicacao_1554_pdf")
    contract_sources = _artifact_sources(artifacts_df, "contrato_detail")
    rows = [
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="materialidade_contrato",
            item_label="Materialidade contratual publicada",
            status="COMPROVADO_DOCUMENTAL" if contract_sources else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO" if contract_sources else "INDICIARIO",
            anchors=["CF88_ART37_CAPUT", "CF88_ART37_XXI", "L14133_PLANEJAMENTO"],
            source_refs=contract_sources,
            rationale="O caso so pode seguir externamente se o contrato estiver materializado em fonte primaria local.",
            next_action="Congelar ou anexar o detalhe oficial do contrato no caso." if not contract_sources else "Manter cadeia de evidencia do contrato.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="rastro_licitatorio",
            item_label="Origem licitatoria e publicacoes do certame",
            status="COMPROVADO_DOCUMENTAL" if licitacao_sources else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_CORROBORADO" if licitacao_sources else "INDICIARIO",
            anchors=["CF88_ART37_XXI", "L14133_PLANEJAMENTO", "LAI_12527_2011"],
            source_refs=licitacao_sources,
            rationale="Comparacao de contrato com edital e publicacoes depende do rastro licitatorio materializado.",
            next_action="Obter processo/licitação integral e publicacoes oficiais." if not licitacao_sources else "Usar o rastro para comparacao semantica.",
        ),
    ]

    process_key = f"processo_integral_contrato_{numero}"
    process_status = _inbox_status(inbox_df, process_key)
    rows.append(
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="processo_integral_contrato",
            item_label="Processo integral do contrato",
            status="COMPROVADO_DOCUMENTAL" if process_status == "RECEBIDO" else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO" if process_status == "RECEBIDO" else "INDICIARIO",
            anchors=["CF88_ART5_XXXIII", "LAI_12527_2011", "L14133_FISCALIZACAO"],
            source_refs=_inbox_sources(inbox_df, process_key),
            rationale="Processo integral e necessario para confirmar formacao da vontade administrativa e execucao.",
            next_action="Cobrar processo integral ao orgao contratante." if process_status != "RECEBIDO" else "Validar anexos e despachos do processo integral.",
        )
    )

    if numero == "3895":
        consulta_status = _inbox_status(inbox_df, "consulta_ceis_cnep_contratacao_3895")
        rows.extend(
            [
                _make_burden_row(
                    case_id=case["case_id"],
                    family=case["family"],
                    item_key="cruzamento_sancionatorio",
                    item_label="Cruzamento sancionatorio entre contrato e fornecedor",
                    status="COMPROVADO_DOCUMENTAL",
                    evidence_grade="DOCUMENTAL_CORROBORADO",
                    anchors=["CF88_ART37_CAPUT", "L12846_2013", "D11129_2022"],
                    source_refs=_artifact_sources(artifacts_df, "relato_apuracao_3895", "dossie_rb_sus"),
                    rationale="O caso ja nasce de contrato municipal com fornecedor cruzado em base sancionatoria.",
                    next_action="Preservar consulta e cadeia documental do contrato e do CNPJ.",
                ),
                _make_burden_row(
                    case_id=case["case_id"],
                    family=case["family"],
                    item_key="diligencia_integridade_previa",
                    item_label="Comprovacao de consulta de integridade previa",
                    status="COMPROVADO_DOCUMENTAL" if consulta_status == "RECEBIDO" else "PENDENTE_DOCUMENTO",
                    evidence_grade="DOCUMENTAL_PRIMARIO" if consulta_status == "RECEBIDO" else "INDICIARIO",
                    anchors=["CF88_ART37_CAPUT", "LAI_12527_2011", "D11129_2022"],
                    source_refs=_inbox_sources(inbox_df, "consulta_ceis_cnep_contratacao_3895"),
                    rationale="O sistema nao presume falha de due diligence; exige documento de consulta previa ou justificativa.",
                    next_action="Solicitar consulta formal ou despacho equivalente no processo de contratacao." if consulta_status != "RECEBIDO" else "Auditar a data e o resultado da consulta anexada.",
                ),
            ]
        )
    else:
        memoria_status = _inbox_status(inbox_df, "memoria_comparativa_item_3898")
        processo_licit_status = _inbox_status(inbox_df, "processo_licitacao_2274334")
        rows.extend(
            [
                _make_burden_row(
                    case_id=case["case_id"],
                    family=case["family"],
                    item_key="contradicao_item_edital",
                    item_label="Contradicao objetiva entre item contratual e base licitatoria",
                    status="COMPROVADO_DOCUMENTAL",
                    evidence_grade="DOCUMENTAL_CORROBORADO",
                    anchors=["CF88_ART37_XXI", "L14133_PLANEJAMENTO"],
                    source_refs=_artifact_sources(artifacts_df, "relato_apuracao_3898", "dossie_rb_sus", "contrato_detail", "licitacao_detail"),
                    rationale="O caso ja esta materializado como divergencia documental; nao depende de inferencia moral.",
                    next_action="Preservar a comparacao entre contrato, licitacao e publicacoes oficiais.",
                ),
                _make_burden_row(
                    case_id=case["case_id"],
                    family=case["family"],
                    item_key="processo_integral_licitacao",
                    item_label="Processo integral da licitacao-mae",
                    status="COMPROVADO_DOCUMENTAL" if processo_licit_status == "RECEBIDO" else "PENDENTE_DOCUMENTO",
                    evidence_grade="DOCUMENTAL_PRIMARIO" if processo_licit_status == "RECEBIDO" else "INDICIARIO",
                    anchors=["CF88_ART5_XXXIII", "LAI_12527_2011", "L14133_PLANEJAMENTO"],
                    source_refs=_inbox_sources(inbox_df, "processo_licitacao_2274334"),
                    rationale="O processo integral fecha o triangulo edital-proposta-contrato com lastro oficial completo.",
                    next_action="Solicitar processo integral da licitacao 2274334." if processo_licit_status != "RECEBIDO" else "Conferir proposta vencedora e memoria de julgamento.",
                ),
                _make_burden_row(
                    case_id=case["case_id"],
                    family=case["family"],
                    item_key="memoria_comparativa_item",
                    item_label="Memoria comparativa item x edital x proposta",
                    status="COMPROVADO_DOCUMENTAL" if memoria_status == "RECEBIDO" else "PENDENTE_DOCUMENTO",
                    evidence_grade="DOCUMENTAL_CORROBORADO" if memoria_status == "RECEBIDO" else "INDICIARIO",
                    anchors=["CF88_ART37_XXI", "L14133_PLANEJAMENTO"],
                    source_refs=_inbox_sources(inbox_df, "memoria_comparativa_item_3898"),
                    rationale="A memoria comparativa reforca a divergencia sem depender de interpretacao solta do operador.",
                    next_action="Anexar memoria comparativa assinada ou documento equivalente." if memoria_status != "RECEBIDO" else "Usar a memoria como peca central da noticia de fato ou pedido de apuracao.",
                ),
            ]
        )

    return rows


def _build_sesacre_burden(case: dict[str, Any], artifacts_df: pd.DataFrame, inbox_df: pd.DataFrame) -> list[dict[str, Any]]:
    cnpj = str(case["case_id"]).split(":")[-1]
    return [
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="cruzamento_sancao_ativa",
            item_label="Fornecedor cruzado com sancao ativa em base publica",
            status="COMPROVADO_DOCUMENTAL",
            evidence_grade="DOCUMENTAL_CORROBORADO",
            anchors=["CF88_ART37_CAPUT", "L12846_2013", "D11129_2022"],
            source_refs=_artifact_sources(artifacts_df, "dossie_sesacre_top10", "noticia_fato_top10"),
            rationale="O caso materializa o cruzamento entre fornecedor contratado e sancao ativa em base publica; cronologia decisoria e due diligence seguem dependentes do processo integral.",
            next_action="Preservar cadeia de evidencia do cruzamento e buscar processo integral, justificativa e consulta de integridade.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="processo_integral_contratacao",
            item_label="Processo integral de contratacao do fornecedor",
            status="COMPROVADO_DOCUMENTAL" if _inbox_status(inbox_df, f"processo_integral_fornecedor_{cnpj}") == "RECEBIDO" else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO" if _inbox_status(inbox_df, f"processo_integral_fornecedor_{cnpj}") == "RECEBIDO" else "INDICIARIO",
            anchors=["CF88_ART5_XXXIII", "LAI_12527_2011", "L14133_PLANEJAMENTO"],
            source_refs=_inbox_sources(inbox_df, f"processo_integral_fornecedor_{cnpj}"),
            rationale="Sem processo integral o sistema nao conclui sobre a decisao administrativa de contratar apesar das sancoes.",
            next_action="Solicitar processo integral e despacho de contratacao." if _inbox_status(inbox_df, f"processo_integral_fornecedor_{cnpj}") != "RECEBIDO" else "Auditar despacho, parecer e matriz de risco do processo.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="consulta_integridade_previa",
            item_label="Consulta de integridade/sancoes no fluxo de contratacao",
            status="COMPROVADO_DOCUMENTAL" if _inbox_status(inbox_df, f"consulta_ceis_cnep_{cnpj}") == "RECEBIDO" else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO" if _inbox_status(inbox_df, f"consulta_ceis_cnep_{cnpj}") == "RECEBIDO" else "INDICIARIO",
            anchors=["CF88_ART37_CAPUT", "LAI_12527_2011", "D11129_2022"],
            source_refs=_inbox_sources(inbox_df, f"consulta_ceis_cnep_{cnpj}"),
            rationale="O sistema nao presume omissao da administracao; exige documento de consulta ou justificativa formal.",
            next_action="Cobrar comprovacao documental da due diligence no processo." if _inbox_status(inbox_df, f"consulta_ceis_cnep_{cnpj}") != "RECEBIDO" else "Validar data, resultado e autoridade responsavel pela consulta.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="justificativa_manutencao_contratual",
            item_label="Justificativa para manter/celebrar contratacao apesar do risco",
            status="COMPROVADO_DOCUMENTAL" if _inbox_status(inbox_df, f"justificativa_manutencao_contratual_{cnpj}") == "RECEBIDO" else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO" if _inbox_status(inbox_df, f"justificativa_manutencao_contratual_{cnpj}") == "RECEBIDO" else "INDICIARIO",
            anchors=["CF88_ART37_CAPUT", "L14133_PLANEJAMENTO", "LAI_12527_2011"],
            source_refs=_inbox_sources(inbox_df, f"justificativa_manutencao_contratual_{cnpj}"),
            rationale="A noticia de fato externa so deve relatar tolerancia institucional se houver ou faltar justificativa formal verificavel.",
            next_action="Solicitar despacho, parecer ou justificativa formal." if _inbox_status(inbox_df, f"justificativa_manutencao_contratual_{cnpj}") != "RECEBIDO" else "Confrontar justificativa com sancoes e execucao.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="lastro_execucao_pagamento",
            item_label="Execucao, fiscalizacao e pagamentos do contrato",
            status="COMPROVADO_DOCUMENTAL" if _inbox_status(inbox_df, f"execucao_fiscalizacao_pagamentos_{cnpj}") == "RECEBIDO" else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO" if _inbox_status(inbox_df, f"execucao_fiscalizacao_pagamentos_{cnpj}") == "RECEBIDO" else "INDICIARIO",
            anchors=["L14133_FISCALIZACAO", "LAI_12527_2011"],
            source_refs=_inbox_sources(inbox_df, f"execucao_fiscalizacao_pagamentos_{cnpj}"),
            rationale="Fiscalizacao e glosa podem mudar o enquadramento e a gravidade do caso.",
            next_action="Solicitar medicao, fiscalizacao, glosa e comprovantes de pagamento." if _inbox_status(inbox_df, f"execucao_fiscalizacao_pagamentos_{cnpj}") != "RECEBIDO" else "Conferir entrega, glosa e cronologia de pagamento.",
        ),
    ]


def _build_saude_societario_burden(case: dict[str, Any], artifacts_df: pd.DataFrame, inbox_df: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="sobreposicao_societaria",
            item_label="Socio/administrador em empresa de saude com contrato publico",
            status="COMPROVADO_DOCUMENTAL",
            evidence_grade="DOCUMENTAL_CORROBORADO",
            anchors=["CF88_ART37_CAPUT", "L14133_PLANEJAMENTO"],
            source_refs=_artifact_sources(artifacts_df, "dossie_followup", "dossie_gate"),
            rationale="QSA, contrato e estabelecimento de saude ja estao documentados no caso.",
            next_action="Preservar a cadeia de evidencia societaria e contratual.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="cnes_concomitancia_publico_privada",
            item_label="Historico CNES com concomitancia publico-privada",
            status="COMPROVADO_DOCUMENTAL",
            evidence_grade="DOCUMENTAL_PRIMARIO",
            anchors=["CF88_ART37_CAPUT", "CF88_ART37_XVI"],
            source_refs=_artifact_sources(artifacts_df, "dossie_followup", "diligencias"),
            rationale="O caso tem historico CNES e carga concomitante documentada.",
            next_action="Usar o CNES como fato de base, sem extrapolar para ilegalidade automatica.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="compatibilidade_horarios",
            item_label="Compatibilidade juridico-funcional de horarios e cargas",
            status="COMPROVADO_DOCUMENTAL"
            if all(
                _inbox_status(inbox_df, key) == "RECEBIDO"
                for key in [
                    "ficha_funcional_maira",
                    "ficha_funcional_marcos",
                    "escalas_ponto_maira",
                    "escalas_ponto_marcos",
                    "parecer_compatibilidade",
                ]
            )
            else "PENDENTE_DOCUMENTO",
            evidence_grade="DOCUMENTAL_PRIMARIO"
            if all(
                _inbox_status(inbox_df, key) == "RECEBIDO"
                for key in [
                    "ficha_funcional_maira",
                    "ficha_funcional_marcos",
                    "escalas_ponto_maira",
                    "escalas_ponto_marcos",
                    "parecer_compatibilidade",
                ]
            )
            else "INDICIARIO",
            anchors=["CF88_ART37_XVI", "LAI_12527_2011"],
            source_refs=_inbox_sources(
                inbox_df,
                "ficha_funcional_maira",
                "ficha_funcional_marcos",
                "escalas_ponto_maira",
                "escalas_ponto_marcos",
                "parecer_compatibilidade",
            ),
            rationale="A ferramenta nao pode chamar acumulacao ilicita sem ficha funcional, escala e parecer ou documento equivalente.",
            next_action="Cobrar ficha funcional, escalas e parecer de compatibilidade." if not all(
                _inbox_status(inbox_df, key) == "RECEBIDO"
                for key in [
                    "ficha_funcional_maira",
                    "ficha_funcional_marcos",
                    "escalas_ponto_maira",
                    "escalas_ponto_marcos",
                    "parecer_compatibilidade",
                ]
            ) else "Confrontar horarios efetivos e sobreposicao por competencia.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="vedacao_legal_conclusiva",
            item_label="Enquadramento juridico conclusivo de vedacao funcional",
            status="PENDENTE_ENQUADRAMENTO",
            evidence_grade="ANALISE_HUMANA",
            anchors=["CF88_ART37_CAPUT", "CF88_ART37_XVI"],
            source_refs=_artifact_sources(artifacts_df, "dossie_gate", "diligencias"),
            rationale="Mesmo com base documental forte, subsuncao normativa continua dependente de leitura juridica humana.",
            next_action="Submeter o caso a analise juridico-funcional depois que a caixa documental estiver completa.",
        ),
        _make_burden_row(
            case_id=case["case_id"],
            family=case["family"],
            item_key="parentesco_ou_designacao_cruzada",
            item_label="Parentesco vedado ou designacao cruzada documentada",
            status="SEM_BASE_ATUAL",
            evidence_grade="SEM_LASTRO",
            anchors=["CF88_ART37_CAPUT"],
            source_refs=[],
            rationale="O caso nao tem hoje documento publico suficiente para afirmar parentesco vedado, designacao cruzada ou vinculo pessoal proibido.",
            next_action="Nao promover essa tese sem documento nominal, vinculo funcional verificavel e nexo objetivo entre as partes.",
        ),
    ]


def sync_ops_burden(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_burden(con)
    con.execute("DELETE FROM ops_case_burden_item")

    try:
        cases_df = con.execute("SELECT * FROM ops_case_registry ORDER BY case_id").df()
    except duckdb.Error:
        return {"rows_written": 0, "cases": 0}
    if cases_df.empty:
        return {"rows_written": 0, "cases": 0}

    artifacts_df = con.execute("SELECT * FROM ops_case_artifact").df() if "ops_case_artifact" in set(con.execute("SHOW TABLES").df()["name"].tolist()) else pd.DataFrame()
    inbox_df = con.execute("SELECT * FROM ops_case_inbox_document").df() if "ops_case_inbox_document" in set(con.execute("SHOW TABLES").df()["name"].tolist()) else pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for case in json.loads(cases_df.to_json(orient="records", force_ascii=False)):
        case_artifacts = artifacts_df[artifacts_df["case_id"] == case["case_id"]].copy() if not artifacts_df.empty else pd.DataFrame()
        case_inbox = inbox_df[inbox_df["case_id"] == case["case_id"]].copy() if not inbox_df.empty else pd.DataFrame()
        family = case.get("family")
        if family == "rb_sus_contrato":
            rows.extend(_build_rb_burden(case, case_artifacts, case_inbox))
        elif family == "sesacre_sancao":
            rows.extend(_build_sesacre_burden(case, case_artifacts, case_inbox))
        elif family == "saude_societario":
            rows.extend(_build_saude_societario_burden(case, case_artifacts, case_inbox))

    for row in rows:
        con.execute(
            """
            INSERT INTO ops_case_burden_item (
                burden_id, case_id, family, item_key, item_label, status, status_order,
                evidence_grade, legal_anchors_json, source_refs_json, rationale, next_action, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                row["burden_id"],
                row["case_id"],
                row["family"],
                row["item_key"],
                row["item_label"],
                row["status"],
                row["status_order"],
                row["evidence_grade"],
                row["legal_anchors_json"],
                row["source_refs_json"],
                row["rationale"],
                row["next_action"],
            ],
        )

    return {"rows_written": len(rows), "cases": int(cases_df["case_id"].nunique())}
