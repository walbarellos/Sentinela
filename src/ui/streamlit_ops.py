from __future__ import annotations

import base64
import json
from io import StringIO
from pathlib import Path
from typing import Any, Callable

import duckdb
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.core.ops_registry import sync_ops_case_registry
from src.core.ops_runtime import (
    begin_pipeline_run,
    ensure_ops_runtime,
    finish_pipeline_run,
    refresh_source_cache,
)


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def get_rw_db():
    return duckdb.connect(str(DB_PATH))


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


def format_brl(value: float | None) -> str:
    if value is None:
        return "R$ 0,00"
    return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_case_label(row: pd.Series) -> str:
    subject = row.get("subject_name") or row.get("title") or row.get("case_id")
    stage = row.get("estagio_operacional") or "SEM_ESTAGIO"
    return f"{subject} [{stage}]"


def resolve_artifact_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


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


def render_artifact_preview(path_value: str | None, kind: str | None = None) -> None:
    path = resolve_artifact_path(path_value)
    if not path_value or not path or not path.exists():
        st.warning("Artefato não localizado no disco.")
        return

    suffix = path.suffix.lower()
    st.caption(f"Preview local: `{path.relative_to(ROOT)}`")

    if suffix in {".md", ".txt"}:
        content = read_text_artifact(path_value)
        if suffix == ".md":
            st.markdown(content)
        else:
            st.code(content, language="text")
        return

    if suffix == ".json":
        content = read_text_artifact(path_value)
        try:
            st.json(json.loads(content))
        except json.JSONDecodeError:
            st.code(content, language="json")
        return

    if suffix == ".csv":
        content = read_text_artifact(path_value)
        try:
            st.dataframe(pd.read_csv(StringIO(content)), use_container_width=True, hide_index=True)
        except Exception:
            st.code(content[:12000], language="csv")
        return

    if suffix in {".html", ".htm"}:
        html_content = read_text_artifact(path_value)
        components.html(html_content, height=820, scrolling=True)
        return

    if suffix == ".pdf":
        payload = read_binary_artifact(path_value)
        if not payload:
            st.warning("PDF vazio ou indisponível.")
            return
        pdf_b64 = base64.b64encode(payload).decode("ascii")
        components.html(
            f'<iframe src="data:application/pdf;base64,{pdf_b64}" width="100%" height="920" style="border:none;"></iframe>',
            height=940,
            scrolling=False,
        )
        return

    st.info(f"Pré-visualização não implementada para `{suffix or kind or 'arquivo'}`.")
    st.code(str(path.relative_to(ROOT)), language="text")


def _apply_case_filters(ops_cases: pd.DataFrame) -> pd.DataFrame:
    st.markdown("#### Filtros")
    filt1, filt2, filt3, filt4 = st.columns(4)
    family_options = ["Todas"] + sorted(v for v in ops_cases["family"].dropna().unique().tolist())
    stage_options = ["Todos"] + sorted(v for v in ops_cases["estagio_operacional"].dropna().unique().tolist())
    orgao_options = ["Todos"] + sorted(v for v in ops_cases["orgao"].dropna().unique().tolist())
    uso_options = ["Todos"] + sorted(v for v in ops_cases["uso_externo"].dropna().unique().tolist())
    family_filter = filt1.selectbox("Família", family_options)
    stage_filter = filt2.selectbox("Estágio", stage_options)
    orgao_filter = filt3.selectbox("Órgão", orgao_options)
    uso_filter = filt4.selectbox("Uso externo", uso_options)
    search = st.text_input("Busca livre", placeholder="case_id, sujeito, resumo, classe do achado...")

    filtered = ops_cases.copy()
    if family_filter != "Todas":
        filtered = filtered[filtered["family"] == family_filter]
    if stage_filter != "Todos":
        filtered = filtered[filtered["estagio_operacional"] == stage_filter]
    if orgao_filter != "Todos":
        filtered = filtered[filtered["orgao"] == orgao_filter]
    if uso_filter != "Todos":
        filtered = filtered[filtered["uso_externo"] == uso_filter]
    if search:
        search_upper = search.upper()
        filtered = filtered[
            filtered.fillna("").astype(str).apply(
                lambda col: col.str.upper().str.contains(search_upper, regex=False)
            ).any(axis=1)
        ]
    return filtered


def _render_overview_tab(summary: dict[str, Any], runs_df: pd.DataFrame, sources_df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Casos", summary["total_cases"])
    col2.metric("Prontos p/ uso externo", summary["external_ready"])
    col3.metric("Pedido documental", summary["document_request_ready"])
    col4.metric("Valor rastreado", format_brl(summary["total_value_brl"]))

    block1, block2 = st.columns([1, 1])
    with block1:
        st.markdown("#### Estágios")
        df_stage = pd.DataFrame(summary["by_stage"])
        if df_stage.empty:
            st.info("Sem estágios materializados.")
        else:
            st.dataframe(df_stage, use_container_width=True, hide_index=True)
    with block2:
        st.markdown("#### Famílias")
        df_family = pd.DataFrame(summary["by_family"])
        if df_family.empty:
            st.info("Sem famílias materializadas.")
        else:
            st.dataframe(df_family, use_container_width=True, hide_index=True)

    st.markdown("#### Saúde operacional")
    runtime_left, runtime_right = st.columns([1, 1])
    with runtime_left:
        if runs_df.empty:
            st.info("Sem execuções registradas ainda.")
        else:
            st.dataframe(runs_df.head(8), use_container_width=True, hide_index=True)
    with runtime_right:
        if sources_df.empty:
            st.info("Sem fontes monitoradas ainda.")
        else:
            healthy = int((sources_df["status_code"].fillna(0) < 400).sum())
            st.metric("Fontes saudáveis", f"{healthy}/{len(sources_df)}")
            st.dataframe(
                sources_df[["source_name", "status_code", "fetched_at", "expires_at"]],
                use_container_width=True,
                hide_index=True,
            )


def _render_case_workbench(filtered: pd.DataFrame) -> None:
    st.markdown(f"#### Casos filtrados: {len(filtered)}")
    if filtered.empty:
        st.info("Nenhum caso encontrado com os filtros atuais.")
        return

    workbench_left, workbench_right = st.columns([1.1, 1.3])
    with workbench_left:
        case_ids = filtered["case_id"].tolist()
        selected_case_id = st.selectbox(
            "Fila de casos",
            case_ids,
            format_func=lambda cid: format_case_label(filtered.loc[filtered["case_id"] == cid].iloc[0]),
        )
        st.dataframe(
            filtered[
                [
                    "subject_name",
                    "orgao",
                    "classe_achado",
                    "estagio_operacional",
                    "prioridade",
                    "valor_referencia_brl",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    selected_case = filtered.loc[filtered["case_id"] == selected_case_id].iloc[0]
    artifacts_df = load_ops_case_artifacts(selected_case_id)

    with workbench_right:
        head_left, head_right = st.columns([1.4, 1])
        with head_left:
            st.markdown(f"### {selected_case['title']}")
            if pd.notna(selected_case["subtitle"]):
                st.caption(str(selected_case["subtitle"]))
            st.markdown(f"**Sujeito:** {selected_case.get('subject_name') or 'N/D'}")
            if pd.notna(selected_case["subject_doc"]):
                st.markdown(f"**Documento:** `{selected_case['subject_doc']}`")
            st.markdown(f"**Classe:** `{selected_case.get('classe_achado') or 'N/D'}`")
            st.markdown(f"**Estágio:** `{selected_case.get('estagio_operacional') or 'N/D'}`")
            st.markdown(f"**Uso externo:** `{selected_case.get('uso_externo') or 'N/D'}`")
            st.markdown(f"**Resumo:** {selected_case.get('resumo_curto') or 'N/D'}")
            st.markdown(f"**Próximo passo:** {selected_case.get('proximo_passo') or 'N/D'}")
        with head_right:
            st.metric("Prioridade", int(selected_case.get("prioridade") or 0))
            st.metric("Artefatos", int(selected_case.get("artifact_count") or 0))
            st.metric("Valor", format_brl(selected_case.get("valor_referencia_brl")))
            if pd.notna(selected_case["bundle_path"]):
                st.caption(f"Bundle: `{selected_case['bundle_path']}`")
            if pd.notna(selected_case["bundle_sha256"]):
                st.code(str(selected_case["bundle_sha256"]), language="text")

        st.markdown("#### Evidências")
        if artifacts_df.empty:
            st.info("Nenhum artefato materializado para este caso.")
            return

        artifacts_left, artifacts_right = st.columns([1, 1.2])
        with artifacts_left:
            st.dataframe(artifacts_df, use_container_width=True, hide_index=True)
        with artifacts_right:
            previewable = artifacts_df[artifacts_df["exists"] & artifacts_df["path"].notna()].copy()
            if previewable.empty:
                st.info("Nenhum artefato local disponível para preview.")
                return
            preview_labels = previewable.apply(lambda row: f"{row['kind']} :: {row['label']}", axis=1).tolist()
            preview_index = st.selectbox(
                "Abrir artefato",
                range(len(previewable)),
                format_func=lambda idx: preview_labels[idx],
            )
            selected_artifact = previewable.iloc[int(preview_index)]
            meta1, meta2, meta3 = st.columns(3)
            meta1.metric("Tipo", str(selected_artifact.get("kind") or "N/D"))
            meta2.metric("Tamanho", f"{int(selected_artifact.get('size_bytes') or 0):,} bytes")
            meta3.metric("Atualizado", str(selected_artifact.get("updated_at") or "N/D"))
            st.code(str(selected_artifact.get("sha256") or ""), language="text")
            render_artifact_preview(str(selected_artifact.get("path") or ""), str(selected_artifact.get("kind") or ""))


def _render_runtime_tab(runs_df: pd.DataFrame, sources_df: pd.DataFrame) -> None:
    left, right = st.columns([1, 1.1])
    with left:
        st.markdown("#### Execuções")
        if runs_df.empty:
            st.info("Sem execuções registradas.")
        else:
            st.dataframe(runs_df, use_container_width=True, hide_index=True)
    with right:
        st.markdown("#### Fontes")
        if sources_df.empty:
            st.info("Sem fontes monitoradas.")
        else:
            st.dataframe(
                sources_df[
                    [
                        "source_name",
                        "status_code",
                        "etag",
                        "last_modified",
                        "fetched_at",
                        "expires_at",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
            preview_sources = sources_df[sources_df["body_path"].notna()].copy()
            if not preview_sources.empty:
                selected_source_idx = st.selectbox(
                    "Abrir snapshot de fonte",
                    range(len(preview_sources)),
                    format_func=lambda idx: str(preview_sources.iloc[int(idx)]["source_name"]),
                )
                selected_source = preview_sources.iloc[int(selected_source_idx)]
                render_artifact_preview(str(selected_source.get("body_path") or ""), "source_snapshot")


def render_ops_page() -> None:
    st.markdown('<div class="main-header"><h1>Casos Operacionais</h1></div>', unsafe_allow_html=True)
    st.caption("Centro operacional investigativo: fila de casos, runtime e evidências no mesmo fluxo.")

    toolbar_left, toolbar_mid, toolbar_right = st.columns([3, 1, 1])
    with toolbar_mid:
        if st.button("🔄 Atualizar Registry", use_container_width=True):
            try:
                stats = sync_ops_registry_now()
                st.cache_data.clear()
                st.success(f"Registry atualizado: {stats['cases']} casos / {stats['artifacts']} artefatos.")
                st.rerun()
            except Exception as exc:
                st.error(f"Falha ao atualizar registry: {exc}")
    with toolbar_right:
        if st.button("🌐 Atualizar Fontes", use_container_width=True):
            try:
                stats = sync_ops_source_cache_now()
                st.cache_data.clear()
                st.success(f"Fontes verificadas: {stats['ok']}/{stats['sources']} OK.")
                st.rerun()
            except Exception as exc:
                st.error(f"Falha ao atualizar fontes: {exc}")

    ops_summary, ops_cases = load_ops_dashboard_data()
    runs_df, sources_df = load_ops_runtime_data()
    if ops_summary is None:
        st.warning("O registry operacional ainda não foi materializado neste banco.")
        st.code(".venv/bin/python scripts/sync_ops_case_registry.py")
        return

    overview_tab, cases_tab, runtime_tab = st.tabs(["Visão Geral", "Bancada de Casos", "Runtime"])
    with overview_tab:
        _render_overview_tab(ops_summary, runs_df, sources_df)
    with cases_tab:
        filtered = _apply_case_filters(ops_cases)
        _render_case_workbench(filtered)
    with runtime_tab:
        _render_runtime_tab(runs_df, sources_df)
