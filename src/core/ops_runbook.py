from __future__ import annotations

import json
from typing import Any

import duckdb
import pandas as pd

from src.core.ops_legal import legal_anchor_payload


RUNBOOK_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_runbook (
    runbook_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    recommended_mode VARCHAR NOT NULL,
    peca_recomendada VARCHAR NOT NULL,
    destinatario_principal VARCHAR NOT NULL,
    destinatarios_secundarios_json JSON,
    canal_preferencial VARCHAR,
    objetivo_operacional VARCHAR,
    contradicao_central VARCHAR,
    risco_controlado VARCHAR,
    status_resumo VARCHAR,
    next_best_action VARCHAR,
    dossier_minimo_json JSON,
    documentos_requeridos_json JSON,
    legal_anchors_json JSON,
    source_refs_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

RUNBOOK_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_runbook AS
SELECT *
FROM ops_case_runbook
ORDER BY family, case_id
"""

RUNBOOK_STEP_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_runbook_step (
    runbook_step_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    step_order INTEGER NOT NULL,
    phase_label VARCHAR NOT NULL,
    action_label VARCHAR NOT NULL,
    target_orgao VARCHAR,
    deliverable VARCHAR,
    blocking BOOLEAN DEFAULT FALSE,
    status_hint VARCHAR,
    legal_anchors_json JSON,
    source_refs_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

RUNBOOK_STEP_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_runbook_step AS
SELECT *
FROM ops_case_runbook_step
ORDER BY case_id, step_order, action_label
"""


def ensure_ops_runbook(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(RUNBOOK_DDL)
    con.execute(RUNBOOK_VIEW)
    con.execute(RUNBOOK_STEP_DDL)
    con.execute(RUNBOOK_STEP_VIEW)


def _json_load_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        raw = json.loads(value)
    except Exception:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw if item]
    return [str(raw)]


def _artifacts_for_case(con: duckdb.DuckDBPyConnection, case_id: str) -> pd.DataFrame:
    return con.execute(
        """
        SELECT label, kind, path, exists
        FROM ops_case_artifact
        WHERE case_id = ?
        ORDER BY kind, label
        """,
        [case_id],
    ).df()


def _burden_for_case(con: duckdb.DuckDBPyConnection, case_id: str) -> pd.DataFrame:
    return con.execute(
        """
        SELECT item_key, item_label, status, next_action, source_refs_json
        FROM ops_case_burden_item
        WHERE case_id = ?
        ORDER BY status_order, item_key
        """,
        [case_id],
    ).df()


def _contradictions_for_case(con: duckdb.DuckDBPyConnection, case_id: str) -> pd.DataFrame:
    return con.execute(
        """
        SELECT title, rationale, next_action
        FROM ops_case_contradiction
        WHERE case_id = ?
        ORDER BY severity DESC, title
        """,
        [case_id],
    ).df()


def _gate_for_case(con: duckdb.DuckDBPyConnection, case_id: str) -> pd.DataFrame:
    return con.execute(
        """
        SELECT export_mode, allowed, blocking_reason
        FROM ops_case_export_gate
        WHERE case_id = ?
        ORDER BY export_mode
        """,
        [case_id],
    ).df()


def _allowed_mode(gate_df: pd.DataFrame) -> str:
    if gate_df.empty:
        return "NOTA_INTERNA"
    for mode in ["NOTICIA_FATO", "PEDIDO_DOCUMENTAL", "NOTA_INTERNA"]:
        rows = gate_df[gate_df["export_mode"] == mode]
        if not rows.empty and bool(rows.iloc[0]["allowed"]):
            return mode
    return "NOTA_INTERNA"


def _artifact_paths(artifacts_df: pd.DataFrame, *labels: str) -> list[str]:
    if artifacts_df.empty:
        return []
    target = {label.lower() for label in labels}
    rows = artifacts_df[artifacts_df["label"].fillna("").str.lower().isin(target)]
    return rows["path"].dropna().astype(str).tolist()


def _source_union(*groups: list[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for group in groups:
        for item in group:
            if item and item not in seen:
                seen.add(item)
                merged.append(item)
    return merged


def _family_profile(case: pd.Series, mode: str) -> dict[str, Any]:
    family = str(case["family"])
    if family == "rb_sus_contrato":
        return {
            "peca": "NOTICIA_FATO" if mode == "NOTICIA_FATO" else "PEDIDO_APURACAO_PRELIMINAR",
            "principal": "Controladoria Geral do Municipio de Rio Branco",
            "secundarios": [
                "Secretaria Municipal de Saude - SEMSA",
                "Ministerio Publico do Estado do Acre",
            ],
            "canal": "protocolo administrativo ou ouvidoria institucional",
            "anchors": ["CF88_ART37_XXI", "L14133_PLANEJAMENTO", "LAI_12527_2011", "CNMP_RES174_2017"],
        }
    if family == "sesacre_sancao":
        return {
            "peca": "NOTICIA_FATO",
            "principal": "Controladoria-Geral do Estado do Acre",
            "secundarios": [
                "Secretaria de Estado de Saude do Acre - SESACRE",
                "Ministerio Publico do Estado do Acre",
            ],
            "canal": "protocolo administrativo estadual ou ouvidoria institucional",
            "anchors": ["CF88_ART37_CAPUT", "LAI_12527_2011", "L12846_2013", "D11129_2022", "CNMP_RES174_2017"],
        }
    return {
        "peca": "PEDIDO_DOCUMENTAL" if mode == "PEDIDO_DOCUMENTAL" else mode,
        "principal": "Secretaria Municipal de Saude - RH / SEMSA",
        "secundarios": [
            "Secretaria de Estado de Saude do Acre - SESACRE",
            "Controle Interno competente",
        ],
        "canal": "pedido documental com protocolo formal e controle de resposta",
        "anchors": ["CF88_ART5_XXXIII", "CF88_ART37_CAPUT", "CF88_ART37_XVI", "LAI_12527_2011"],
    }


def _requested_documents(case: pd.Series) -> list[str]:
    family = str(case["family"])
    if family == "rb_sus_contrato":
        return [
            "processo integral da licitacao-mae",
            "processo integral do contrato",
            "proposta vencedora e memoria de julgamento",
            "justificativa administrativa do item divergente",
            "parecer da fiscalizacao/atesto relacionado ao item questionado",
        ]
    if family == "sesacre_sancao":
        return [
            "processo integral de contratacao do fornecedor",
            "consulta de integridade ou justificativa equivalente",
            "justificativa de manutencao contratual diante da sancao",
            "atos de fiscalizacao, glosa ou providencias adotadas",
        ]
    return [
        "ficha funcional completa",
        "declaracoes de acumulacao e compatibilidade de horarios",
        "escalas e jornada praticada no periodo relevante",
        "processo integral do contrato 779/2023",
        "atos societarios ou declaracoes internas sobre vinculo com a CEDIMP",
    ]


def _status_summary(mode: str, burden_df: pd.DataFrame) -> str:
    if burden_df.empty:
        return "Sem matriz probatoria suficiente."
    pending_doc = int((burden_df["status"] == "PENDENTE_DOCUMENTO").sum())
    pending_legal = int((burden_df["status"] == "PENDENTE_ENQUADRAMENTO").sum())
    proved = int((burden_df["status"] == "COMPROVADO_DOCUMENTAL").sum())
    if mode == "NOTICIA_FATO":
        return f"Pronto para noticia de fato com {proved} nucleo(s) documental(is), {pending_doc} pendencia(s) documental(is) residual(is) e {pending_legal} dependencia(s) juridica(s)."
    if mode == "PEDIDO_DOCUMENTAL":
        return f"Pronto para pedido documental com {pending_doc} pendencia(s) prioritaria(s) e {proved} item(ns) ja comprovado(s)."
    return "Caso restrito a uso interno ate nova documentacao."


def _dossier_minimo(case: pd.Series, artifacts_df: pd.DataFrame) -> list[str]:
    family = str(case["family"])
    if family == "rb_sus_contrato":
        return _source_union(
            _artifact_paths(artifacts_df, "relato_apuracao_3898"),
            _artifact_paths(artifacts_df, "dossie_rb_sus"),
            _artifact_paths(artifacts_df, "contrato_detail", "licitacao_detail"),
            _artifact_paths(artifacts_df, "cpl_publicacao_1554_pdf", "cpl_publicacao_1640_pdf"),
        )
    if family == "sesacre_sancao":
        return _source_union(
            _artifact_paths(artifacts_df, "relato_apuracao_top10"),
            _artifact_paths(artifacts_df, "dossie_sesacre_top10"),
        )
    return _source_union(
        _artifact_paths(artifacts_df, "dossie_followup", "dossie_gate", "diligencias"),
        _artifact_paths(artifacts_df, "nota_operacional", "bundle"),
    )


def _contradiction_summary(case: pd.Series, contradiction_df: pd.DataFrame) -> str:
    family = str(case["family"])
    if not contradiction_df.empty:
        first = contradiction_df.iloc[0]
        return str(first.get("title") or first.get("rationale") or "")
    if family == "rb_sus_contrato":
        return "Divergencia objetiva entre item contratual e base licitatoria publica."
    if family == "sesacre_sancao":
        return "Fornecedor estadual cruzado com sancao ativa em base publica, com necessidade de confirmar decisao administrativa e due diligence no processo integral."
    return "Coincidencia funcional e societaria em saude exige resposta documental e enquadramento humano."


def _risk_controlled(case: pd.Series) -> str:
    family = str(case["family"])
    if family == "rb_sus_contrato":
        return "O runbook evita salto acusatorio e foca em contradicao documental verificavel."
    if family == "sesacre_sancao":
        return "O runbook trata o caso como noticia de fato tecnica, sem presumir nulidade automatica de todos os contratos."
    return "O runbook limita o uso externo a pedido documental e evita concluir acumulacao ilicita ou impedimento sem resposta oficial."


def _runbook_steps(case: pd.Series, profile: dict[str, Any], mode: str, dossier_minimo: list[str]) -> list[dict[str, Any]]:
    case_id = str(case["case_id"])
    family = str(case["family"])
    requested = _requested_documents(case)
    if family == "rb_sus_contrato":
        raw_steps = [
            ("preparo", "Conferir dossie minimo e congelar artefatos centrais", profile["principal"], "dossie minimo do caso", True, "IMEDIATO"),
            ("protocolo", "Protocolar noticia de fato ou pedido de apuracao preliminar", profile["principal"], profile["peca"], True, "IMEDIATO"),
            ("diligencia", "Requerer processo integral da licitacao e do contrato", "SEMSA / CPL", requested[0], True, "APOS_PROTOCOLO"),
            ("diligencia", "Cobrar justificativa do item divergente", "SEMSA / fiscalizacao contratual", requested[3], False, "APOS_PROTOCOLO"),
            ("analise", "Confrontar resposta oficial com edital, proposta e contrato", "Equipe de analise", "nota tecnica complementar", False, "APOS_RESPOSTA"),
        ]
    elif family == "sesacre_sancao":
        raw_steps = [
            ("preparo", "Conferir dossie estadual e extratos do fornecedor", profile["principal"], "dossie minimo do caso", True, "IMEDIATO"),
            ("protocolo", "Protocolar noticia de fato tecnica", profile["principal"], profile["peca"], True, "IMEDIATO"),
            ("diligencia", "Requerer consulta de integridade e justificativa de manutencao contratual", "SESACRE / controle interno", requested[1], True, "APOS_PROTOCOLO"),
            ("diligencia", "Requerer processo integral de contratacao e atos de fiscalizacao", "SESACRE", requested[0], False, "APOS_PROTOCOLO"),
            ("analise", "Reavaliar alcance, vigencia e providencias adotadas", "Equipe de analise", "nota tecnica complementar", False, "APOS_RESPOSTA"),
        ]
    else:
        raw_steps = [
            ("preparo", "Conferir dossie funcional e historico CNES", profile["principal"], "dossie minimo do caso", True, "IMEDIATO"),
            ("protocolo", "Protocolar pedido documental dirigido", profile["principal"], profile["peca"], True, "IMEDIATO"),
            ("diligencia", "Requerer ficha funcional, declaracoes e escalas", profile["principal"], requested[0], True, "APOS_PROTOCOLO"),
            ("diligencia", "Requerer processo integral do contrato estadual", "SESACRE", requested[3], False, "APOS_PROTOCOLO"),
            ("analise", "Reavaliar compatibilidade de horarios e enquadramento juridico", "Equipe de analise", "nota juridico-funcional", False, "APOS_RESPOSTA"),
        ]
    steps: list[dict[str, Any]] = []
    for idx, (phase, action, orgao, deliverable, blocking, hint) in enumerate(raw_steps, start=1):
        anchors = profile["anchors"]
        steps.append(
            {
                "runbook_step_id": f"{case_id}:runbook:{idx}",
                "case_id": case_id,
                "family": family,
                "step_order": idx,
                "phase_label": phase,
                "action_label": action,
                "target_orgao": orgao,
                "deliverable": deliverable,
                "blocking": blocking,
                "status_hint": hint,
                "legal_anchors_json": json.dumps(legal_anchor_payload(anchors), ensure_ascii=False),
                "source_refs_json": json.dumps(dossier_minimo[:8], ensure_ascii=False),
            }
        )
    return steps


def sync_ops_runbook(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_runbook(con)
    con.execute("DELETE FROM ops_case_runbook_step")
    con.execute("DELETE FROM ops_case_runbook")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    required = {"ops_case_registry", "ops_case_export_gate", "ops_case_burden_item", "ops_case_artifact"}
    if not required.issubset(tables):
        return {"rows_written": 0, "steps_written": 0, "cases": 0}

    cases_df = con.execute("SELECT * FROM ops_case_registry ORDER BY case_id").df()
    rows_written = 0
    steps_written = 0

    for _, case in cases_df.iterrows():
        case_id = str(case["case_id"])
        artifacts_df = _artifacts_for_case(con, case_id)
        burden_df = _burden_for_case(con, case_id)
        contradiction_df = _contradictions_for_case(con, case_id)
        gate_df = _gate_for_case(con, case_id)

        mode = _allowed_mode(gate_df)
        profile = _family_profile(case, mode)
        dossier_minimo = _dossier_minimo(case, artifacts_df)
        source_refs = _source_union(
            dossier_minimo,
            _source_union(*[_json_load_list(v) for v in burden_df["source_refs_json"].tolist()]) if not burden_df.empty else [],
        )
        documents = _requested_documents(case)
        contradiction = _contradiction_summary(case, contradiction_df)
        next_best = str(case.get("proximo_passo") or "")
        con.execute(
            """
            INSERT INTO ops_case_runbook (
                runbook_id, case_id, family, recommended_mode, peca_recomendada,
                destinatario_principal, destinatarios_secundarios_json, canal_preferencial,
                objetivo_operacional, contradicao_central, risco_controlado, status_resumo,
                next_best_action, dossier_minimo_json, documentos_requeridos_json,
                legal_anchors_json, source_refs_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                f"{case_id}:runbook",
                case_id,
                str(case["family"]),
                mode,
                profile["peca"],
                profile["principal"],
                json.dumps(profile["secundarios"], ensure_ascii=False),
                profile["canal"],
                str(case.get("resumo_curto") or ""),
                contradiction,
                _risk_controlled(case),
                _status_summary(mode, burden_df),
                next_best,
                json.dumps(dossier_minimo, ensure_ascii=False),
                json.dumps(documents, ensure_ascii=False),
                json.dumps(legal_anchor_payload(profile["anchors"]), ensure_ascii=False),
                json.dumps(source_refs[:12], ensure_ascii=False),
            ],
        )
        rows_written += 1

        for step in _runbook_steps(case, profile, mode, dossier_minimo):
            con.execute(
                """
                INSERT INTO ops_case_runbook_step (
                    runbook_step_id, case_id, family, step_order, phase_label,
                    action_label, target_orgao, deliverable, blocking, status_hint,
                    legal_anchors_json, source_refs_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    step["runbook_step_id"],
                    step["case_id"],
                    step["family"],
                    step["step_order"],
                    step["phase_label"],
                    step["action_label"],
                    step["target_orgao"],
                    step["deliverable"],
                    step["blocking"],
                    step["status_hint"],
                    step["legal_anchors_json"],
                    step["source_refs_json"],
                ],
            )
            steps_written += 1

    return {"rows_written": rows_written, "steps_written": steps_written, "cases": rows_written}
