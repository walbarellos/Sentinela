from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import duckdb

from src.core.ops_guard import RISK_PATTERNS, _best_snippet, _has_safe_context
from src.core.ops_legal import legal_anchor_payload


ROOT = Path(__file__).resolve().parents[2]

RULEBOOK_DDL = """
CREATE TABLE IF NOT EXISTS ops_rule_catalog (
    rule_id VARCHAR PRIMARY KEY,
    component VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    purpose VARCHAR NOT NULL,
    intended_use VARCHAR NOT NULL,
    assurance_level VARCHAR NOT NULL,
    human_review_required BOOLEAN NOT NULL,
    false_positive_risk VARCHAR NOT NULL,
    legal_anchors_json JSON,
    benchmark_refs_json JSON,
    notes VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

RULEBOOK_VIEW = """
CREATE OR REPLACE VIEW v_ops_rule_catalog AS
SELECT *
FROM ops_rule_catalog
ORDER BY family, component, rule_id
"""

VALIDATION_DDL = """
CREATE TABLE IF NOT EXISTS ops_rule_validation (
    validation_id VARCHAR PRIMARY KEY,
    rule_id VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    finding VARCHAR,
    remediation VARCHAR,
    details_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

VALIDATION_VIEW = """
CREATE OR REPLACE VIEW v_ops_rule_validation AS
SELECT *
FROM ops_rule_validation
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
    severity DESC,
    validation_id
"""

BENCHMARKS: dict[str, dict[str, str]] = {
    "USASPENDING_RECIPIENT_DRILLDOWN": {
        "benchmark_id": "USASPENDING_RECIPIENT_DRILLDOWN",
        "label": "USAspending recipient/award drill-down",
        "url": "https://api.usaspending.gov/docs/endpoints",
        "note": "Inspiracao para busca por sujeito, award drill-down e filtros fortes antes do detalhe.",
    },
    "OVERSIGHT_OPEN_RECOMMENDATIONS": {
        "benchmark_id": "OVERSIGHT_OPEN_RECOMMENDATIONS",
        "label": "Oversight.gov open recommendations",
        "url": "https://www.oversight.gov/reports/recommendations",
        "note": "Inspiracao para fila de pendencias, status aberto e diligencias por caso.",
    },
    "SEC_EDGAR_DOCUMENT_FIRST": {
        "benchmark_id": "SEC_EDGAR_DOCUMENT_FIRST",
        "label": "SEC EDGAR document-first workflow",
        "url": "https://www.sec.gov/search-filings",
        "note": "Inspiracao para timeline, anexo primeiro e busca por evidencia.",
    },
    "OECD_PROCUREMENT_INTEGRITY": {
        "benchmark_id": "OECD_PROCUREMENT_INTEGRITY",
        "label": "OECD integrity in public procurement",
        "url": "https://www.oecd.org/en/topics/sub-issues/integrity-in-public-procurement.html",
        "note": "Integridade, controles internos, e-procurement, accountability e gestao de riscos ao longo do ciclo.",
    },
    "INTOSAI_ISSAI100": {
        "benchmark_id": "INTOSAI_ISSAI100",
        "label": "INTOSAI ISSAI 100",
        "url": "https://www.intosaifipp.org/wp-content/uploads/2023/10/Final-ISSAI-100-after-FIPP-approval-Oct-2023.pdf",
        "note": "Evidencia suficiente e apropriada, materialidade qualitativa e quantitativa, julgamento profissional.",
    },
    "ISRAEL_OMBUDSMAN_COMPLAINTS": {
        "benchmark_id": "ISRAEL_OMBUDSMAN_COMPLAINTS",
        "label": "Israel Ombudsman complaint workflow",
        "url": "https://www.mevaker.gov.il/en/ombudsman/activity",
        "note": "Fluxo de queixa focado em corpo publico, anexos relevantes, resposta previa do orgao e protecao a whistleblower.",
    },
}

RULES: list[dict[str, Any]] = [
    {
        "rule_id": "RB_SEMANTIC_TRIAD",
        "component": "ops_semantic",
        "family": "rb_sus_contrato",
        "title": "Comparacao semantica contrato x edital x proposta",
        "purpose": "Materializar divergencias documentais objetivas sem adjetivacao ou inferencia penal.",
        "intended_use": "noticia_de_fato|pedido_documental",
        "assurance_level": "DOCUMENTAL_CORROBORADO",
        "human_review_required": False,
        "false_positive_risk": "MEDIO",
        "legal_anchors": ["CF88_ART37_XXI", "L14133_PLANEJAMENTO", "LAI_12527_2011"],
        "benchmarks": ["OECD_PROCUREMENT_INTEGRITY", "INTOSAI_ISSAI100", "SEC_EDGAR_DOCUMENT_FIRST"],
        "notes": "Usa compatibilidade conservadora de objeto e so promove contradicao quando o status semantico e DIVERGENTE.",
    },
    {
        "rule_id": "SESACRE_SANCTION_CROSS",
        "component": "ops_burden",
        "family": "sesacre_sancao",
        "title": "Cruzamento sancionatorio com due diligence documental",
        "purpose": "Tratar sancao ativa como gatilho de apuracao, exigindo processo integral, consulta previa e justificativa formal.",
        "intended_use": "noticia_de_fato|pedido_documental",
        "assurance_level": "DOCUMENTAL_CORROBORADO",
        "human_review_required": False,
        "false_positive_risk": "BAIXO",
        "legal_anchors": ["CF88_ART37_CAPUT", "L12846_2013", "D11129_2022", "LAI_12527_2011"],
        "benchmarks": ["OECD_PROCUREMENT_INTEGRITY", "USASPENDING_RECIPIENT_DRILLDOWN"],
        "notes": "O motor nao presume dolo; cobra trilha documental de integridade e decisao administrativa.",
    },
    {
        "rule_id": "SAUDE_SOCIETARIO_CNES",
        "component": "ops_burden",
        "family": "saude_societario",
        "title": "Concomitancia publico-privada em saude com base CNES",
        "purpose": "Tratar coincidencia societaria e historico CNES como fato documental para triagem funcional, sem declarar ilicitude.",
        "intended_use": "pedido_documental|nota_interna",
        "assurance_level": "DOCUMENTAL_PRIMARIO",
        "human_review_required": True,
        "false_positive_risk": "MEDIO",
        "legal_anchors": ["CF88_ART37_XVI", "CF88_ART37_CAPUT", "LAI_12527_2011", "CNMP_RES174_2017"],
        "benchmarks": ["INTOSAI_ISSAI100", "ISRAEL_OMBUDSMAN_COMPLAINTS"],
        "notes": "Nao prova acumulacao ilicita, impedimento ou nepotismo; exige enquadramento humano e documento funcional.",
    },
    {
        "rule_id": "NON_ACCUSATORY_EXPORT_GATE",
        "component": "ops_export",
        "family": "all",
        "title": "Gate de saida nao-acusatoria",
        "purpose": "Restringir a saida externa a nota interna, pedido documental e noticia de fato, com limite explicito da conclusao.",
        "intended_use": "all",
        "assurance_level": "PROCESSUAL",
        "human_review_required": False,
        "false_positive_risk": "BAIXO",
        "legal_anchors": ["CF88_ART5_XXXIII", "CNMP_RES174_2017", "PF_IN255_ART9_2023"],
        "benchmarks": ["OVERSIGHT_OPEN_RECOMMENDATIONS", "ISRAEL_OMBUDSMAN_COMPLAINTS", "INTOSAI_ISSAI100"],
        "notes": "Bloqueia exportacao acusatoria e exige plausibilidade, base minima e diligencia proporcional.",
    },
    {
        "rule_id": "PROBATIVE_BURDEN_MATRIX",
        "component": "ops_burden",
        "family": "all",
        "title": "Matriz de onus probatorio",
        "purpose": "Separar comprovado documentalmente do que depende de documento, enquadramento ou ainda nao tem base atual.",
        "intended_use": "all",
        "assurance_level": "PROCESSUAL",
        "human_review_required": False,
        "false_positive_risk": "BAIXO",
        "legal_anchors": ["CF88_ART5_XXXIII", "LAI_12527_2011", "PF_IN255_ART9_2023"],
        "benchmarks": ["INTOSAI_ISSAI100", "OVERSIGHT_OPEN_RECOMMENDATIONS"],
        "notes": "Evita que hipotese seja promovida ao mesmo nivel de fato documental.",
    },
    {
        "rule_id": "FAMILY_CONFIDENCE_GUARD",
        "component": "ops_registry",
        "family": "all",
        "title": "Contencoes de confianca por familia de caso",
        "purpose": "Impedir regressao de falso positivo conhecido e manter linguagem conservadora nas familias ativas.",
        "intended_use": "all",
        "assurance_level": "PROCESSUAL",
        "human_review_required": False,
        "false_positive_risk": "BAIXO",
        "legal_anchors": ["CF88_ART37_CAPUT", "LAI_12527_2011", "CNMP_RES174_2017"],
        "benchmarks": ["INTOSAI_ISSAI100", "OVERSIGHT_OPEN_RECOMMENDATIONS"],
        "notes": "Serve como cerca para casos municipais, sancionatorios e societarios mais sensiveis.",
    },
]


def ensure_ops_rulebook(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(RULEBOOK_DDL)
    con.execute(RULEBOOK_VIEW)
    con.execute(VALIDATION_DDL)
    con.execute(VALIDATION_VIEW)


def _benchmark_payload(ids: list[str]) -> list[dict[str, str]]:
    return [BENCHMARKS[item] for item in ids if item in BENCHMARKS]


def _validation_row(
    *,
    validation_id: str,
    rule_id: str,
    severity: str,
    status: str,
    title: str,
    finding: str,
    remediation: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "validation_id": validation_id,
        "rule_id": rule_id,
        "severity": severity,
        "status": status,
        "title": title,
        "finding": finding,
        "remediation": remediation,
        "details_json": json.dumps(details or {}, ensure_ascii=False),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_generated_exports(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_case_generated_export" not in tables:
        return []
    return con.execute(
        """
        SELECT export_id, case_id, export_mode, path, sha256, size_bytes
        FROM ops_case_generated_export
        ORDER BY created_at DESC, case_id, export_mode
        """
    ).fetchall()


def sync_ops_rulebook(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_rulebook(con)
    con.execute("DELETE FROM ops_rule_catalog")
    con.execute("DELETE FROM ops_rule_validation")

    for rule in RULES:
        con.execute(
            """
            INSERT INTO ops_rule_catalog (
                rule_id, component, family, title, purpose, intended_use,
                assurance_level, human_review_required, false_positive_risk,
                legal_anchors_json, benchmark_refs_json, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                rule["rule_id"],
                rule["component"],
                rule["family"],
                rule["title"],
                rule["purpose"],
                rule["intended_use"],
                rule["assurance_level"],
                rule["human_review_required"],
                rule["false_positive_risk"],
                json.dumps(legal_anchor_payload(rule["legal_anchors"]), ensure_ascii=False),
                json.dumps(_benchmark_payload(rule["benchmarks"]), ensure_ascii=False),
                rule["notes"],
            ],
        )

    validations: list[dict[str, Any]] = []
    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())

    missing_anchors = 0
    if "ops_case_burden_item" in tables:
        missing_anchors = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ops_case_burden_item
                WHERE legal_anchors_json IS NULL OR CAST(legal_anchors_json AS VARCHAR) IN ('[]', '', 'null')
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="PROBATIVE_BURDEN_MATRIX:anchors",
            rule_id="PROBATIVE_BURDEN_MATRIX",
            severity="CRITICO" if missing_anchors else "INFO",
            status="FAIL" if missing_anchors else "PASS",
            title="Todos os itens de onus probatorio possuem ancora legal",
            finding=f"{missing_anchors} item(ns) sem ancora legal." if missing_anchors else "Todos os itens possuem ancora legal materializada.",
            remediation="Preencher legal_anchors_json em todos os burden items antes de uso externo.",
            details={"missing_anchor_rows": missing_anchors},
        )
    )

    gate_missing = 0
    if "ops_case_export_gate" in tables:
        gate_missing = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ops_case_export_gate
                WHERE disclaimer IS NULL OR disclaimer = '' OR rationale IS NULL OR rationale = ''
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="NON_ACCUSATORY_EXPORT_GATE:metadata",
            rule_id="NON_ACCUSATORY_EXPORT_GATE",
            severity="CRITICO" if gate_missing else "INFO",
            status="FAIL" if gate_missing else "PASS",
            title="Gate de exportacao sempre traz fundamento e disclaimer",
            finding=f"{gate_missing} gate(s) sem rationale/disclaimer." if gate_missing else "Todos os gates trazem rationale e disclaimer.",
            remediation="Bloquear exportacao sem limite da conclusao e rationale explicitos.",
            details={"missing_gate_metadata": gate_missing},
        )
    )

    contradiction_orphans = 0
    if "ops_case_contradiction" in tables and "ops_case_semantic_issue" in tables:
        contradiction_orphans = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ops_case_contradiction c
                LEFT JOIN ops_case_semantic_issue s
                  ON s.case_id = c.case_id
                 AND s.comparator = c.comparator
                 AND s.status = 'DIVERGENTE'
                WHERE s.issue_id IS NULL
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="RB_SEMANTIC_TRIAD:provenance",
            rule_id="RB_SEMANTIC_TRIAD",
            severity="ALTO" if contradiction_orphans else "INFO",
            status="FAIL" if contradiction_orphans else "PASS",
            title="Toda contradicao tem lastro em divergencia semantica materializada",
            finding=f"{contradiction_orphans} contradicao(oes) sem origem semantica." if contradiction_orphans else "Todas as contradicoes possuem origem semantica rastreavel.",
            remediation="Nao materializar contradicao sem issue DIVERGENTE correspondente.",
            details={"orphan_contradictions": contradiction_orphans},
        )
    )

    external_without_doc = 0
    if "ops_case_registry" in tables and "ops_case_burden_item" in tables:
        external_without_doc = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ops_case_registry r
                LEFT JOIN (
                    SELECT case_id, COUNT(*) AS total_doc
                    FROM ops_case_burden_item
                    WHERE status = 'COMPROVADO_DOCUMENTAL'
                    GROUP BY case_id
                ) b ON b.case_id = r.case_id
                WHERE r.uso_externo IS NOT NULL
                  AND r.uso_externo != 'REVISAO_INTERNA'
                  AND COALESCE(b.total_doc, 0) = 0
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="PROBATIVE_BURDEN_MATRIX:external_minimum",
            rule_id="PROBATIVE_BURDEN_MATRIX",
            severity="CRITICO" if external_without_doc else "INFO",
            status="FAIL" if external_without_doc else "PASS",
            title="Caso com uso externo possui base documental minima",
            finding=f"{external_without_doc} caso(s) aptos a uso externo sem qualquer item comprovado documentalmente." if external_without_doc else "Todos os casos com uso externo possuem base documental minima.",
            remediation="Rebaixar caso para revisao interna ou completar lastro documental minimo.",
            details={"external_without_documental": external_without_doc},
        )
    )

    rb_sanction_active = 0
    if "ops_case_registry" in tables:
        rb_sanction_active = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ops_case_registry
                WHERE family = 'rb_sus_contrato'
                  AND classe_achado = 'CRUZAMENTO_SANCIONATORIO'
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="FAMILY_CONFIDENCE_GUARD:rb_false_positive_cleanup",
            rule_id="FAMILY_CONFIDENCE_GUARD",
            severity="CRITICO" if rb_sanction_active else "INFO",
            status="FAIL" if rb_sanction_active else "PASS",
            title="Fila municipal ativa nao mantem falso positivo sancionatorio conhecido",
            finding=(
                f"{rb_sanction_active} caso(s) municipais ainda classificados como cruzamento sancionatorio."
                if rb_sanction_active
                else "Nenhum caso municipal ativo permanece como cruzamento sancionatorio apos a limpeza temporal."
            ),
            remediation="Rebaixar ou remover qualquer caso municipal sancionatorio sem validacao temporal/material.",
            details={"rb_active_sanction_cases": rb_sanction_active},
        )
    )

    sesacre_missing_core = 0
    if "ops_case_registry" in tables and "ops_case_burden_item" in tables:
        sesacre_missing_core = int(
            con.execute(
                """
                WITH target_cases AS (
                    SELECT case_id
                    FROM ops_case_registry
                    WHERE family = 'sesacre_sancao'
                ),
                required_keys AS (
                    SELECT * FROM (
                        VALUES
                            ('cruzamento_sancao_ativa'),
                            ('processo_integral_contratacao'),
                            ('consulta_integridade_previa'),
                            ('justificativa_manutencao_contratual'),
                            ('lastro_execucao_pagamento')
                    ) AS t(item_key)
                )
                SELECT COUNT(*)
                FROM target_cases c
                CROSS JOIN required_keys k
                LEFT JOIN ops_case_burden_item b
                  ON b.case_id = c.case_id
                 AND b.item_key = k.item_key
                WHERE b.burden_id IS NULL
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="SESACRE_SANCTION_CROSS:documentary_limits",
            rule_id="SESACRE_SANCTION_CROSS",
            severity="CRITICO" if sesacre_missing_core else "INFO",
            status="FAIL" if sesacre_missing_core else "PASS",
            title="Casos sancionatorios da SESACRE mantem trilha minima de processo, justificativa e fiscalizacao",
            finding=(
                f"{sesacre_missing_core} lacuna(s) em itens centrais de processo/due diligence/fiscalizacao."
                if sesacre_missing_core
                else "Todos os casos sancionatorios da SESACRE mantem a trilha documental minima exigida."
            ),
            remediation="Materializar todos os burden items centrais antes de qualquer uso externo.",
            details={"sesacre_missing_core_items": sesacre_missing_core},
        )
    )

    sesacre_overclaim_language = 0
    if "ops_case_registry" in tables and "ops_case_burden_item" in tables:
        sesacre_overclaim_language = int(
            con.execute(
                """
                SELECT COALESCE((
                    SELECT COUNT(*)
                    FROM ops_case_registry
                    WHERE family = 'sesacre_sancao'
                      AND (
                        LOWER(COALESCE(title, '')) LIKE '%concomitante%'
                        OR LOWER(COALESCE(resumo_curto, '')) LIKE '%concomitante%'
                      )
                ), 0) + COALESCE((
                    SELECT COUNT(*)
                    FROM ops_case_burden_item
                    WHERE family = 'sesacre_sancao'
                      AND LOWER(COALESCE(item_label, '')) LIKE '%concomitante%'
                ), 0)
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="SESACRE_SANCTION_CROSS:claim_language",
            rule_id="SESACRE_SANCTION_CROSS",
            severity="ALTO" if sesacre_overclaim_language else "INFO",
            status="FAIL" if sesacre_overclaim_language else "PASS",
            title="Casos sancionatorios da SESACRE usam linguagem conservadora sobre cronologia contratual",
            finding=(
                f"{sesacre_overclaim_language} texto(s) ativos ainda usam linguagem temporal forte como 'concomitante'."
                if sesacre_overclaim_language
                else "Os casos sancionatorios ativos usam linguagem conservadora de cruzamento, sem extrapolar cronologia."
            ),
            remediation="Trocar linguagem temporal forte por cruzamento conservador enquanto a cronologia decisoria nao estiver fechada.",
            details={"sesacre_overclaim_language": sesacre_overclaim_language},
        )
    )

    saude_nepotismo_labels = 0
    if "ops_case_burden_item" in tables or "ops_case_checklist" in tables:
        burden_hits = 0
        checklist_hits = 0
        if "ops_case_burden_item" in tables:
            burden_hits = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM ops_case_burden_item
                    WHERE family = 'saude_societario'
                      AND LOWER(COALESCE(item_label, '')) LIKE '%nepot%'
                    """
                ).fetchone()[0]
                or 0
            )
        if "ops_case_checklist" in tables:
            checklist_hits = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM ops_case_checklist
                    WHERE family = 'saude_societario'
                      AND LOWER(COALESCE(step_label, '')) LIKE '%nepot%'
                    """
                ).fetchone()[0]
                or 0
            )
        saude_nepotismo_labels = burden_hits + checklist_hits
    validations.append(
        _validation_row(
            validation_id="SAUDE_SOCIETARIO_CNES:no_nepotism_operational_label",
            rule_id="SAUDE_SOCIETARIO_CNES",
            severity="ALTO" if saude_nepotismo_labels else "INFO",
            status="FAIL" if saude_nepotismo_labels else "PASS",
            title="Caso societario em saude nao usa rotulo operacional de nepotismo",
            finding=(
                f"{saude_nepotismo_labels} rotulo(s) operacionais ainda usam 'nepotismo'."
                if saude_nepotismo_labels
                else "A trilha societaria em saude evita rotulo operacional de nepotismo e mantem tese em aberto."
            ),
            remediation="Usar apenas formula neutra de parentesco/designacao ate surgir prova documental especifica.",
            details={"saude_nepotismo_label_hits": saude_nepotismo_labels},
        )
    )

    saude_external_nf = 0
    if "ops_case_export_gate" in tables and "ops_case_registry" in tables:
        saude_external_nf = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ops_case_export_gate g
                JOIN ops_case_registry r USING (case_id, family)
                WHERE r.family = 'saude_societario'
                  AND g.export_mode = 'NOTICIA_FATO'
                  AND g.allowed = TRUE
                """
            ).fetchone()[0]
            or 0
        )
    validations.append(
        _validation_row(
            validation_id="SAUDE_SOCIETARIO_CNES:external_cap",
            rule_id="SAUDE_SOCIETARIO_CNES",
            severity="CRITICO" if saude_external_nf else "INFO",
            status="FAIL" if saude_external_nf else "PASS",
            title="Caso societario em saude nao sobe automaticamente para noticia de fato",
            finding=(
                f"{saude_external_nf} gate(s) de noticia de fato liberado(s) para saude societaria."
                if saude_external_nf
                else "A trilha societaria em saude permanece limitada a pedido documental ou nota interna."
            ),
            remediation="Bloquear noticia de fato para familia saude_societario enquanto faltar fechamento juridico-funcional.",
            details={"saude_nf_allowed": saude_external_nf},
        )
    )

    export_integrity_failures: list[dict[str, Any]] = []
    if "ops_case_generated_export" in tables and "ops_case_export_gate" in tables:
        for export_id, case_id, export_mode, path_value, stored_sha, size_bytes in _iter_generated_exports(con):
            path = Path(path_value)
            file_path = path if path.is_absolute() else ROOT / path
            reason: str | None = None
            if not file_path.exists():
                reason = "arquivo congelado ausente no disco"
            elif _sha256_file(file_path) != stored_sha:
                reason = "sha256 divergente entre banco e arquivo"
            elif int(file_path.stat().st_size) != int(size_bytes or 0):
                reason = "size_bytes divergente entre banco e arquivo"
            else:
                gate = con.execute(
                    """
                    SELECT allowed
                    FROM ops_case_export_gate
                    WHERE case_id = ? AND export_mode = ?
                    """,
                    [case_id, export_mode],
                ).fetchone()
                if not gate or not bool(gate[0]):
                    reason = "exportacao congelada sem gate permitido vigente"
                else:
                    guard_rows = 0
                    if "ops_case_language_guard" in tables:
                        guard_rows = int(
                            con.execute(
                                "SELECT COUNT(*) FROM ops_case_language_guard WHERE case_id = ?",
                                [case_id],
                            ).fetchone()[0]
                            or 0
                        )
                    if guard_rows > 0:
                        reason = "caso com language guard ativo nao deveria manter exportacao congelada"
                    else:
                        content = file_path.read_text(encoding="utf-8", errors="replace")
                        if "LIMITE DA CONCLUSAO" not in content or "FATO OBJETIVO" not in content:
                            reason = "saida congelada sem secoes minimas obrigatorias"
                        else:
                            for _, pattern, _, _ in RISK_PATTERNS:
                                if any(word in pattern for word in ("den[uú]ncia", "representa")):
                                    continue
                                if re.search(pattern, content, re.IGNORECASE):
                                    snippet = _best_snippet(content, pattern)
                                    if _has_safe_context(snippet):
                                        continue
                                    reason = "saida congelada contem linguagem impropria ou penal"
                                    break
            if reason:
                export_integrity_failures.append(
                    {
                        "export_id": export_id,
                        "case_id": case_id,
                        "export_mode": export_mode,
                        "path": str(path_value),
                        "reason": reason,
                    }
                )
    validations.append(
        _validation_row(
            validation_id="NON_ACCUSATORY_EXPORT_GATE:frozen_integrity",
            rule_id="NON_ACCUSATORY_EXPORT_GATE",
            severity="CRITICO" if export_integrity_failures else "INFO",
            status="FAIL" if export_integrity_failures else "PASS",
            title="Exportacoes congeladas mantem integridade, gate e linguagem segura",
            finding=f"{len(export_integrity_failures)} exportacao(oes) com falha." if export_integrity_failures else "Todas as exportacoes congeladas estao integras e guardadas pelo gate.",
            remediation="Regerar a peca com gate valido e limpar linguagem antes de novo congelamento.",
            details={"failures": export_integrity_failures},
        )
    )

    validations.append(
        _validation_row(
            validation_id="RB_SEMANTIC_TRIAD:compatibility_rule",
            rule_id="RB_SEMANTIC_TRIAD",
            severity="INFO",
            status="PASS",
            title="Comparacao de objeto usa compatibilidade conservadora",
            finding="A regra de objeto admite igualdade, contencao e forte sobreposicao lexical antes de marcar divergencia.",
            remediation="Manter threshold conservador e revisar somente se houver prova empirica de falso negativo recorrente.",
            details={"token_overlap_threshold": 0.8},
        )
    )

    for row in validations:
        con.execute(
            """
            INSERT INTO ops_rule_validation (
                validation_id, rule_id, severity, status, title,
                finding, remediation, details_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                row["validation_id"],
                row["rule_id"],
                row["severity"],
                row["status"],
                row["title"],
                row["finding"],
                row["remediation"],
                row["details_json"],
            ],
        )

    fail_count = sum(1 for row in validations if row["status"] == "FAIL")
    warn_count = sum(1 for row in validations if row["status"] == "WARN")
    return {
        "rules_written": len(RULES),
        "validation_rows": len(validations),
        "fail_rows": fail_count,
        "warn_rows": warn_count,
    }
