from __future__ import annotations

from typing import Any, Callable

import duckdb
import pandas as pd
import streamlit as st

from src.core.ops_registry import sync_ops_case_registry
from src.core.ops_runtime import (
    begin_pipeline_run,
    ensure_ops_runtime,
    finish_pipeline_run,
    refresh_source_cache,
)
from src.ui.ops_shared import DB_PATH, get_rw_db, resolve_artifact_path


def _run_logged_pipeline(
    pipeline: str,
    runner: Callable[[duckdb.DuckDBPyConnection], dict[str, Any]],
) -> dict[str, Any]:
    con = get_rw_db()
    try:
        ensure_ops_runtime(con)
        run_id = begin_pipeline_run(
            con,
            pipeline,
            trigger_mode="streamlit",
            actor="app",
        )
        try:
            stats = runner(con)
            finish_pipeline_run(
                con,
                run_id,
                status="success",
                rows_written=int(stats.get("rows_written", stats.get("cases", stats.get("sources", 0)) or 0)),
                artifacts_written=int(stats.get("artifacts_written", stats.get("artifacts", 0)) or 0),
                details=stats,
            )
            return stats
        except Exception as exc:
            finish_pipeline_run(
                con,
                run_id,
                status="failed",
                error_text=str(exc),
                details={"pipeline": pipeline},
            )
            raise
    finally:
        con.close()


def sync_ops_registry_now() -> dict[str, Any]:
    return _run_logged_pipeline("sync_ops_case_registry", sync_ops_case_registry)


def sync_ops_source_cache_now() -> dict[str, Any]:
    return _run_logged_pipeline("sync_ops_source_cache", refresh_source_cache)


def sync_ops_search_index_now() -> dict[str, Any]:
    from src.core.ops_search import sync_ops_search_index

    return _run_logged_pipeline("sync_ops_search_index", sync_ops_search_index)


def freeze_ops_case_export_now(case_id: str, export_mode: str) -> dict[str, Any]:
    from src.core.ops_export import freeze_case_external_text

    def _runner(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        frozen = freeze_case_external_text(con, case_id=case_id, export_mode=export_mode, actor="app")
        registry_stats = sync_ops_case_registry(con)
        return {
            "rows_written": int(frozen.get("rows_written", 0)),
            "artifacts_written": 1,
            "case_id": case_id,
            "export_mode": export_mode,
            "path": frozen.get("path"),
            "sha256": frozen.get("sha256"),
            "size_bytes": int(frozen.get("size_bytes", 0) or 0),
            "reused": bool(frozen.get("reused")),
            "export_id": frozen.get("export_id"),
            "registry_cases": int(registry_stats.get("cases", 0)),
            "registry_artifacts": int(registry_stats.get("artifacts", 0)),
        }

    pipeline = f"freeze_ops_case_export:{case_id}:{export_mode.lower()}"
    return _run_logged_pipeline(pipeline, _runner)


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_dashboard_data():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_registry" not in tables:
            return None, pd.DataFrame()

        summary_row = con.execute(
            """
            SELECT
                COUNT(*) AS total_cases,
                COUNT(*) FILTER (WHERE uso_externo IS NOT NULL AND uso_externo != 'REVISAO_INTERNA') AS external_ready,
                COUNT(*) FILTER (WHERE estagio_operacional = 'APTO_OFICIO_DOCUMENTAL') AS document_request_ready,
                COALESCE(SUM(valor_referencia_brl), 0) AS total_value_brl,
                MAX(updated_at) AS last_updated
            FROM ops_case_registry
            """
        ).fetchone()
        inbox_summary_row = (0, 0, 0)
        if "ops_case_inbox_document" in tables:
            inbox_summary_row = con.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status_documento IN ('PENDENTE', 'ARQUIVO_NAO_LOCALIZADO')) AS pending_docs,
                    COUNT(*) FILTER (WHERE status_documento = 'RECEBIDO') AS received_docs,
                    COUNT(DISTINCT case_id) AS inbox_cases
                FROM ops_case_inbox_document
                """
            ).fetchone()
        by_stage_df = con.execute(
            """
            SELECT estagio_operacional, COUNT(*) AS total
            FROM ops_case_registry
            GROUP BY 1
            ORDER BY total DESC, estagio_operacional
            """
        ).df()
        by_family_df = con.execute(
            """
            SELECT family, COUNT(*) AS total
            FROM ops_case_registry
            GROUP BY 1
            ORDER BY total DESC, family
            """
        ).df()
        burden_row = (0, 0, 0, 0)
        if "ops_case_burden_item" in tables:
            burden_row = con.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'COMPROVADO_DOCUMENTAL') AS comprovado_documental,
                    COUNT(*) FILTER (WHERE status = 'PENDENTE_DOCUMENTO') AS pendente_documento,
                    COUNT(*) FILTER (WHERE status = 'PENDENTE_ENQUADRAMENTO') AS pendente_enquadramento,
                    COUNT(*) FILTER (WHERE status = 'SEM_BASE_ATUAL') AS sem_base_atual
                FROM ops_case_burden_item
                """
            ).fetchone()
        contradiction_count = 0
        if "ops_case_contradiction" in tables:
            contradiction_count = int(con.execute("SELECT COUNT(*) FROM ops_case_contradiction").fetchone()[0] or 0)
        language_guard_count = 0
        if "ops_case_language_guard" in tables:
            language_guard_count = int(con.execute("SELECT COUNT(*) FROM ops_case_language_guard").fetchone()[0] or 0)
        export_gate_count = 0
        if "ops_case_export_gate" in tables:
            export_gate_count = int(con.execute("SELECT COUNT(*) FROM ops_case_export_gate").fetchone()[0] or 0)
        generated_export_count = 0
        if "ops_case_generated_export" in tables:
            generated_export_count = int(con.execute("SELECT COUNT(*) FROM ops_case_generated_export").fetchone()[0] or 0)
        generated_export_diff_count = 0
        if "ops_case_generated_export_diff" in tables:
            generated_export_diff_count = int(con.execute("SELECT COUNT(*) FROM ops_case_generated_export_diff").fetchone()[0] or 0)
        rule_validation_fail_count = 0
        if "ops_rule_validation" in tables:
            rule_validation_fail_count = int(
                con.execute("SELECT COUNT(*) FROM ops_rule_validation WHERE status = 'FAIL'").fetchone()[0] or 0
            )
        cases_df = con.execute(
            """
            SELECT
                case_id,
                family,
                title,
                subtitle,
                subject_name,
                subject_doc,
                orgao,
                severity,
                classe_achado,
                uso_externo,
                estagio_operacional,
                prioridade,
                valor_referencia_brl,
                artifact_count,
                resumo_curto,
                proximo_passo,
                bundle_path,
                bundle_sha256,
                updated_at
            FROM v_ops_case_registry
            """
        ).df()
    finally:
        con.close()

    summary = {
        "total_cases": int(summary_row[0] or 0),
        "external_ready": int(summary_row[1] or 0),
        "document_request_ready": int(summary_row[2] or 0),
        "total_value_brl": float(summary_row[3] or 0),
        "last_updated": summary_row[4],
        "pending_docs": int(inbox_summary_row[0] or 0),
        "received_docs": int(inbox_summary_row[1] or 0),
        "inbox_cases": int(inbox_summary_row[2] or 0),
        "burden_documental": int(burden_row[0] or 0),
        "burden_pending_doc": int(burden_row[1] or 0),
        "burden_pending_legal": int(burden_row[2] or 0),
        "burden_no_basis": int(burden_row[3] or 0),
        "contradictions": contradiction_count,
        "language_guard": language_guard_count,
        "export_gate": export_gate_count,
        "generated_export": generated_export_count,
        "generated_export_diff": generated_export_diff_count,
        "rule_validation_fail": rule_validation_fail_count,
        "by_stage": by_stage_df.to_dict("records"),
        "by_family": by_family_df.to_dict("records"),
    }
    return summary, cases_df


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_artifacts(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_artifact" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT label, kind, path, exists, size_bytes, sha256, updated_at
            FROM v_ops_case_artifact
            WHERE case_id = ?
            ORDER BY kind, label
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_runtime_data():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_pipeline_run" not in tables or "ops_source_cache" not in tables:
            return pd.DataFrame(), pd.DataFrame()
        runs_df = con.execute(
            """
            SELECT
                pipeline,
                status,
                trigger_mode,
                actor,
                started_at,
                finished_at,
                duration_ms,
                rows_written,
                artifacts_written,
                error_text
            FROM v_ops_pipeline_run_latest
            LIMIT 20
            """
        ).df()
        sources_df = con.execute(
            """
            SELECT
                source_name,
                resource_url,
                status_code,
                etag,
                last_modified,
                ttl_seconds,
                fetched_at,
                expires_at,
                response_sha256,
                body_path
            FROM v_ops_source_cache_latest
            ORDER BY fetched_at DESC, source_name
            """
        ).df()
    finally:
        con.close()
    return runs_df, sources_df


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_timeline(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "v_ops_case_timeline_event" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                event_at,
                phase_order,
                phase_label,
                event_type,
                event_group,
                title,
                detail,
                source_ref,
                path_ref
            FROM v_ops_case_timeline_event
            WHERE case_id = ?
            ORDER BY phase_order, event_at DESC, event_type, title
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_burden(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_burden_item" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                item_label,
                status,
                evidence_grade,
                rationale,
                next_action,
                legal_anchors_json,
                source_refs_json
            FROM v_ops_case_burden_item
            WHERE case_id = ?
            ORDER BY status_order, item_key
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_semantic(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_semantic_issue" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                comparator,
                field_key,
                status,
                severity,
                left_label,
                left_value,
                center_label,
                center_value,
                right_label,
                right_value,
                rationale,
                source_refs_json
            FROM v_ops_case_semantic_issue
            WHERE case_id = ?
            ORDER BY severity DESC, comparator, field_key
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_contradictions(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_contradiction" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT title, severity, status, comparator, rationale, next_action, source_refs_json
            FROM v_ops_case_contradiction
            WHERE case_id = ?
            ORDER BY severity DESC, title
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_checklist(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_checklist" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT step_group, step_label, step_status, blocking, rationale, source_refs_json
            FROM v_ops_case_checklist
            WHERE case_id = ?
            ORDER BY blocking DESC, step_group, step_label
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_language_guard(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_language_guard" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT label, issue_type, severity, snippet, rationale, suggestion
            FROM v_ops_case_language_guard
            WHERE case_id = ?
            ORDER BY severity DESC, label, issue_type
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_export_gate(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_export_gate" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT export_mode, allowed, blocking_reason, rationale, disclaimer
            FROM v_ops_case_export_gate
            WHERE case_id = ?
            ORDER BY export_mode
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_generated_exports(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_generated_export" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                export_mode,
                label,
                path,
                sha256,
                size_bytes,
                actor,
                created_at
            FROM v_ops_case_generated_export
            WHERE case_id = ?
            ORDER BY created_at DESC, export_mode
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_generated_export_diffs(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_generated_export_diff" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                export_mode,
                older_export_id,
                newer_export_id,
                changed,
                added_lines,
                removed_lines,
                summary,
                diff_text,
                updated_at
            FROM v_ops_case_generated_export_diff
            WHERE case_id = ?
            ORDER BY updated_at DESC, export_mode
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_runbook(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_runbook" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                recommended_mode,
                peca_recomendada,
                destinatario_principal,
                destinatarios_secundarios_json,
                canal_preferencial,
                objetivo_operacional,
                contradicao_central,
                risco_controlado,
                status_resumo,
                next_best_action,
                dossier_minimo_json,
                documentos_requeridos_json,
                legal_anchors_json,
                source_refs_json
            FROM v_ops_case_runbook
            WHERE case_id = ?
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_case_runbook_steps(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_runbook_step" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                step_order,
                phase_label,
                action_label,
                target_orgao,
                deliverable,
                blocking,
                status_hint,
                legal_anchors_json,
                source_refs_json
            FROM v_ops_case_runbook_step
            WHERE case_id = ?
            ORDER BY step_order
            """,
            [case_id],
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_ops_inbox_queue() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_inbox_document" not in tables or "ops_case_registry" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                r.case_id,
                r.family,
                r.subject_name,
                r.orgao,
                COUNT(*) AS total_docs,
                COUNT(*) FILTER (WHERE d.status_documento IN ('PENDENTE', 'ARQUIVO_NAO_LOCALIZADO')) AS pending_docs,
                COUNT(*) FILTER (WHERE d.status_documento = 'RECEBIDO') AS received_docs,
                MAX(d.updated_at) AS last_updated
            FROM ops_case_registry r
            JOIN ops_case_inbox_document d ON d.case_id = r.case_id
            GROUP BY 1,2,3,4
            ORDER BY pending_docs DESC, received_docs ASC, last_updated DESC, case_id
            """
        ).df()
    finally:
        con.close()


@st.cache_data(ttl=120, show_spinner=False)
def read_text_artifact(path_value: str) -> str:
    path = resolve_artifact_path(path_value)
    if not path or not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


@st.cache_data(ttl=120, show_spinner=False)
def read_binary_artifact(path_value: str) -> bytes:
    path = resolve_artifact_path(path_value)
    if not path or not path.exists():
        return b""
    return path.read_bytes()
