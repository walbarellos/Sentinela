from __future__ import annotations

import json

import pandas as pd
import streamlit as st


STATUS_STYLE = {
    "CONCLUIDO": ("Concluido", "green"),
    "PENDENTE": ("Pendente", "orange"),
    "REVISAO_HUMANA": ("Revisao humana", "blue"),
    "BLOQUEADO": ("Bloqueado", "red"),
}


def _render_sources(payload: str | None) -> None:
    refs = json.loads(payload or "[]")
    if not refs:
        st.caption("Sem referencia local.")
        return
    for ref in refs[:8]:
        st.code(str(ref), language="text")


def render_checklist_tab(checklist_df: pd.DataFrame, contradiction_df: pd.DataFrame, guard_df: pd.DataFrame) -> None:
    st.markdown("#### Checklist probatorio")
    if checklist_df.empty:
        st.info("Sem checklist materializado para este caso.")
        return

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Concluido", int((checklist_df["step_status"] == "CONCLUIDO").sum()))
    m2.metric("Pendente", int((checklist_df["step_status"] == "PENDENTE").sum()))
    m3.metric("Revisao humana", int((checklist_df["step_status"] == "REVISAO_HUMANA").sum()))
    m4.metric("Bloqueios", int(checklist_df["blocking"].fillna(False).sum()))

    if not contradiction_df.empty:
        st.markdown("##### Contradicoes objetivas")
        st.dataframe(
            contradiction_df[["title", "severity", "status", "rationale", "next_action"]],
            use_container_width=True,
            hide_index=True,
        )

    if not guard_df.empty:
        st.markdown("##### Gate de linguagem")
        st.warning("Ha saidas com linguagem externa potencialmente impropria. O sistema deve preferir noticia de fato e pedido de apuracao.")
        st.dataframe(
            guard_df[["label", "issue_type", "severity", "suggestion"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("Nenhuma saida operacional materializada foi flagrada pelo gate de linguagem.")

    for _, row in checklist_df.iterrows():
        status_label, badge_color = STATUS_STYLE.get(str(row["step_status"]), (str(row["step_status"]), "gray"))
        with st.expander(f"{row['step_group']} :: {row['step_label']} [{status_label}]"):
            st.markdown(
                f"<span style='display:inline-block;padding:0.15rem 0.45rem;border-radius:999px;background:{badge_color};color:white;font-size:0.8rem'>{status_label}</span>",
                unsafe_allow_html=True,
            )
            st.markdown(f"**Racional operacional:** {row.get('rationale') or 'N/D'}")
            st.markdown("**Fontes locais**")
            _render_sources(row.get("source_refs_json"))
