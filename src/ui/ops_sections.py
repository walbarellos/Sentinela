from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from src.ui.ops_burden import render_burden_tab
from src.ui.ops_checklist import render_checklist_tab
from src.ui.ops_data import (
    load_ops_case_artifacts,
    load_ops_case_burden,
    load_ops_case_checklist,
    load_ops_case_contradictions,
    load_ops_case_export_gate,
    load_ops_case_runbook,
    load_ops_case_runbook_steps,
    load_ops_case_language_guard,
    load_ops_case_semantic,
    load_ops_case_timeline,
)
from src.ui.ops_diff import render_artifact_diff
from src.ui.ops_export import render_export_tab
from src.ui.ops_preview import render_artifact_preview
from src.ui.ops_semantic import render_semantic_diff
from src.ui.ops_shared import format_brl, format_case_label, present_external_usage, present_stage_label
from src.ui.ops_timeline import render_timeline_tab


def apply_case_filters(ops_cases: pd.DataFrame) -> pd.DataFrame:
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


def render_overview_tab(summary: dict[str, Any], runs_df: pd.DataFrame, sources_df: pd.DataFrame) -> None:
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Casos", summary["total_cases"])
    col2.metric("Prontos p/ uso externo", summary["external_ready"])
    col3.metric("Pedido documental", summary["document_request_ready"])
    col4.metric("Docs pendentes", summary.get("pending_docs", 0))
    col5.metric("Docs recebidos", summary.get("received_docs", 0))
    col6.metric("Valor rastreado", format_brl(summary["total_value_brl"]))

    burden1, burden2, burden3, burden4 = st.columns(4)
    burden1.metric("Ônus comprovado", summary.get("burden_documental", 0))
    burden2.metric("Ônus pend. doc", summary.get("burden_pending_doc", 0))
    burden3.metric("Ônus pend. jurid.", summary.get("burden_pending_legal", 0))
    burden4.metric("Ônus sem base", summary.get("burden_no_basis", 0))
    extra1, extra2, extra3, extra4, extra5, extra6, extra7 = st.columns(7)
    extra1.metric("Contradições", summary.get("contradictions", 0))
    extra2.metric("Guard linguagem", summary.get("language_guard", 0))
    extra3.metric("Gates export", summary.get("export_gate", 0))
    extra4.metric("Exports congelados", summary.get("generated_export", 0))
    extra5.metric("Diffs congelados", summary.get("generated_export_diff", 0))
    extra6.metric("Falhas de regra", summary.get("rule_validation_fail", 0))
    extra7.metric("Inbox ativa", summary.get("inbox_cases", 0))

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


def render_case_workbench(filtered: pd.DataFrame) -> None:
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
    burden_df = load_ops_case_burden(selected_case_id)
    checklist_df = load_ops_case_checklist(selected_case_id)
    contradiction_df = load_ops_case_contradictions(selected_case_id)
    export_gate_df = load_ops_case_export_gate(selected_case_id)
    runbook_df = load_ops_case_runbook(selected_case_id)
    runbook_steps_df = load_ops_case_runbook_steps(selected_case_id)
    guard_df = load_ops_case_language_guard(selected_case_id)
    semantic_df = load_ops_case_semantic(selected_case_id)
    timeline_df = load_ops_case_timeline(selected_case_id)

    with workbench_right:
        resume_tab, export_tab, checklist_tab, burden_tab, evidence_tab, timeline_tab, diff_tab = st.tabs(["Resumo", "Exportação", "Checklist", "Ônus", "Evidências", "Timeline", "Diff"])
        with resume_tab:
            head_left, head_right = st.columns([1.4, 1])
            with head_left:
                st.markdown(f"### {selected_case['title']}")
                if pd.notna(selected_case["subtitle"]):
                    st.caption(str(selected_case["subtitle"]))
                st.markdown(f"**Sujeito:** {selected_case.get('subject_name') or 'N/D'}")
                if pd.notna(selected_case["subject_doc"]):
                    st.markdown(f"**Documento:** `{selected_case['subject_doc']}`")
                st.markdown(f"**Classe:** `{selected_case.get('classe_achado') or 'N/D'}`")
                st.markdown(f"**Estágio:** `{present_stage_label(selected_case.get('estagio_operacional'))}`")
                st.markdown(f"**Uso externo:** `{present_external_usage(selected_case.get('uso_externo'))}`")
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
                if not contradiction_df.empty:
                    st.metric("Contradições", len(contradiction_df))
                if not guard_df.empty:
                    st.metric("Guard linguagem", len(guard_df))
                if not runbook_steps_df.empty:
                    st.metric("Diligências sugeridas", len(runbook_steps_df))

        with export_tab:
            render_export_tab(selected_case_id, export_gate_df, runbook_df, runbook_steps_df)

        with checklist_tab:
            render_checklist_tab(checklist_df, contradiction_df, guard_df)

        with burden_tab:
            render_burden_tab(burden_df)

        with evidence_tab:
            st.markdown("#### Evidências")
            if artifacts_df.empty:
                st.info("Nenhum artefato materializado para este caso.")
            else:
                artifacts_left, artifacts_right = st.columns([1, 1.2])
                with artifacts_left:
                    st.dataframe(artifacts_df, use_container_width=True, hide_index=True)
                with artifacts_right:
                    previewable = artifacts_df[artifacts_df["exists"] & artifacts_df["path"].notna()].copy()
                    if previewable.empty:
                        st.info("Nenhum artefato local disponível para preview.")
                    else:
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

        with timeline_tab:
            render_timeline_tab(timeline_df)

        with diff_tab:
            semantic_left, semantic_right = st.columns([1.1, 0.9])
            with semantic_left:
                render_semantic_diff(semantic_df)
            with semantic_right:
                if artifacts_df.empty:
                    st.info("Nenhum artefato materializado para este caso.")
                else:
                    render_artifact_diff(artifacts_df)


def render_runtime_tab(runs_df: pd.DataFrame, sources_df: pd.DataFrame) -> None:
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
