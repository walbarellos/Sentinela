from __future__ import annotations

import json

import pandas as pd
import streamlit as st


def _render_list(payload: str | None, empty_label: str) -> None:
    items = json.loads(payload or "[]")
    if not items:
        st.caption(empty_label)
        return
    for item in items:
        st.markdown(f"- {item}")


def _render_anchors(payload: str | None) -> None:
    anchors = json.loads(payload or "[]")
    if not anchors:
        st.caption("Sem ancora legal materializada.")
        return
    for item in anchors:
        label = str(item.get("label") or "Ancora")
        url = str(item.get("url") or "")
        note = str(item.get("note") or "")
        st.markdown(f"- [{label}]({url})")
        if note:
            st.caption(note)


def render_runbook_tab(runbook_df: pd.DataFrame, runbook_steps_df: pd.DataFrame) -> None:
    st.markdown("#### Runbook operacional")
    if runbook_df.empty:
        st.info("Sem runbook materializado para este caso.")
        return

    row = runbook_df.iloc[0]
    top1, top2, top3, top4 = st.columns(4)
    top1.metric("Peça recomendada", str(row.get("peca_recomendada") or "N/D"))
    top2.metric("Modo liberado", str(row.get("recommended_mode") or "N/D"))
    top3.metric("Canal", str(row.get("canal_preferencial") or "N/D"))
    top4.metric("Passos", int(len(runbook_steps_df)))

    left, right = st.columns([1.1, 1])
    with left:
        st.markdown(f"**Destinatario principal:** {row.get('destinatario_principal') or 'N/D'}")
        st.markdown("**Destinatarios secundarios**")
        _render_list(row.get("destinatarios_secundarios_json"), "Sem destinatarios secundarios.")
        st.markdown(f"**Objetivo operacional:** {row.get('objetivo_operacional') or 'N/D'}")
        st.markdown(f"**Contradicao central:** {row.get('contradicao_central') or 'N/D'}")
        st.markdown(f"**Risco controlado:** {row.get('risco_controlado') or 'N/D'}")
        st.markdown(f"**Resumo de status:** {row.get('status_resumo') or 'N/D'}")
        st.markdown(f"**Proxima melhor acao:** {row.get('next_best_action') or 'N/D'}")
    with right:
        st.markdown("**Dossie minimo a anexar**")
        _render_list(row.get("dossier_minimo_json"), "Sem dossie minimo materializado.")
        st.markdown("**Documentos a requerer**")
        _render_list(row.get("documentos_requeridos_json"), "Sem documentos requeridos materializados.")

    anchors_col, refs_col = st.columns([1, 1])
    with anchors_col:
        st.markdown("**Base normativa oficial**")
        _render_anchors(row.get("legal_anchors_json"))
    with refs_col:
        st.markdown("**Fontes locais do runbook**")
        _render_list(row.get("source_refs_json"), "Sem fontes locais do runbook.")

    st.markdown("#### Sequencia de diligencias")
    if runbook_steps_df.empty:
        st.info("Sem passos materializados.")
        return

    for _, step in runbook_steps_df.iterrows():
        label = f"{int(step['step_order'])}. {step['phase_label']} :: {step['action_label']}"
        if bool(step.get("blocking")):
            label += " [bloqueante]"
        with st.expander(label):
            st.markdown(f"**Orgao alvo:** {step.get('target_orgao') or 'N/D'}")
            st.markdown(f"**Entregavel esperado:** {step.get('deliverable') or 'N/D'}")
            st.markdown(f"**Janela operacional:** {step.get('status_hint') or 'N/D'}")
            st.markdown("**Ancora legal**")
            _render_anchors(step.get("legal_anchors_json"))
            st.markdown("**Fontes locais relacionadas**")
            _render_list(step.get("source_refs_json"), "Sem referencias locais.")
