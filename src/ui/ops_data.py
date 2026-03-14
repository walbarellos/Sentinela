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
                event_type,
                event_group,
                title,
                detail,
                source_ref,
                path_ref
            FROM v_ops_case_timeline_event
            WHERE case_id = ?
            ORDER BY event_at DESC, event_type, title
            """,
            [case_id],
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
