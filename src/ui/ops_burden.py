from __future__ import annotations

import json

import pandas as pd
import streamlit as st


STATUS_STYLE = {
    "COMPROVADO_DOCUMENTAL": ("Comprovado documental", "green"),
    "PENDENTE_DOCUMENTO": ("Pendente de documento", "orange"),
    "PENDENTE_ENQUADRAMENTO": ("Pendente de enquadramento", "blue"),
    "SEM_BASE_ATUAL": ("Sem base atual", "gray"),
}


def _render_anchors(anchor_payload: str | None) -> None:
    anchors = json.loads(anchor_payload or "[]")
    if not anchors:
        st.caption("Sem ancora legal vinculada.")
        return
    lines = [f"- [{item['label']}]({item['url']})" for item in anchors]
    st.markdown("\n".join(lines))


def _render_sources(source_payload: str | None) -> None:
    sources = json.loads(source_payload or "[]")
    if not sources:
        st.caption("Sem referencia de arquivo local.")
        return
    for source in sources[:8]:
        st.code(str(source), language="text")


def render_burden_tab(burden_df: pd.DataFrame) -> None:
    st.markdown("#### Matriz de onus probatorio")
    if burden_df.empty:
        st.info("Sem matriz de onus probatorio materializada para este caso.")
        return

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Comprovado", int((burden_df["status"] == "COMPROVADO_DOCUMENTAL").sum()))
    m2.metric("Pend. doc", int((burden_df["status"] == "PENDENTE_DOCUMENTO").sum()))
    m3.metric("Pend. juridico", int((burden_df["status"] == "PENDENTE_ENQUADRAMENTO").sum()))
    m4.metric("Sem base", int((burden_df["status"] == "SEM_BASE_ATUAL").sum()))

    for _, row in burden_df.iterrows():
        status_label, status_color = STATUS_STYLE.get(str(row["status"]), (str(row["status"]), "gray"))
        with st.expander(f"{row['item_label']} [{status_label}]"):
            st.caption(f"Grau: {row.get('evidence_grade') or 'N/D'}")
            st.markdown(
                f"<span style='display:inline-block;padding:0.15rem 0.45rem;border-radius:999px;background:{status_color};color:white;font-size:0.8rem'>{status_label}</span>",
                unsafe_allow_html=True,
            )
            st.markdown(f"**Racional:** {row.get('rationale') or 'N/D'}")
            st.markdown(f"**Proxima diligencia:** {row.get('next_action') or 'N/D'}")
            anchor_col, source_col = st.columns([1.2, 1])
            with anchor_col:
                st.markdown("**Base normativa oficial**")
                _render_anchors(row.get("legal_anchors_json"))
            with source_col:
                st.markdown("**Fontes locais**")
                _render_sources(row.get("source_refs_json"))
