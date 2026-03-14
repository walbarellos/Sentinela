import streamlit as st
from src.ui.ops_inbox import render_inbox_tab
from src.ui.ops_data import (
    load_ops_dashboard_data,
    load_ops_runtime_data,
    sync_ops_registry_now,
    sync_ops_source_cache_now,
)
from src.ui.ops_sections import apply_case_filters, render_case_workbench, render_overview_tab, render_runtime_tab


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

    overview_tab, cases_tab, inbox_tab, runtime_tab = st.tabs(["Visão Geral", "Bancada de Casos", "Inbox", "Runtime"])
    with overview_tab:
        render_overview_tab(ops_summary, runs_df, sources_df)
    with cases_tab:
        filtered = apply_case_filters(ops_cases)
        render_case_workbench(filtered)
    with inbox_tab:
        render_inbox_tab(ops_cases)
    with runtime_tab:
        render_runtime_tab(runs_df, sources_df)
