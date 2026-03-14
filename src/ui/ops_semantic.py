from __future__ import annotations

import json

import pandas as pd
import streamlit as st


STATUS_HELP = {
    "COERENTE": ("Coerente", "green"),
    "DIVERGENTE": ("Divergente", "red"),
    "AUSENTE": ("Ausente", "orange"),
    "INSUFICIENTE": ("Insuficiente", "gray"),
}


def _render_sources(payload: str | None) -> None:
    refs = json.loads(payload or "[]")
    if not refs:
        st.caption("Sem referencia local adicional.")
        return
    for ref in refs[:8]:
        st.code(str(ref), language="text")


def render_semantic_diff(semantic_df: pd.DataFrame) -> None:
    st.markdown("#### Diff semantico")
    if semantic_df.empty:
        st.info("Sem comparacao semantica materializada para este caso.")
        return

    st.caption("Comparacao orientada a contradicoes objetivas entre contrato, edital/publicacao e proposta congelada.")
    status_counts = semantic_df["status"].value_counts(dropna=False).reset_index()
    status_counts.columns = ["status", "total"]
    st.dataframe(status_counts, use_container_width=True, hide_index=True)

    for _, row in semantic_df.iterrows():
        status_label, badge_color = STATUS_HELP.get(str(row["status"]), (str(row["status"]), "gray"))
        with st.expander(f"{row['comparator']} :: {row['field_key']} [{status_label}]"):
            st.markdown(
                f"<span style='display:inline-block;padding:0.15rem 0.45rem;border-radius:999px;background:{badge_color};color:white;font-size:0.8rem'>{status_label}</span>",
                unsafe_allow_html=True,
            )
            comp_df = pd.DataFrame(
                [
                    {"papel": row["left_label"], "valor": row["left_value"]},
                    {"papel": row["center_label"], "valor": row["center_value"]},
                    {"papel": row["right_label"], "valor": row["right_value"]},
                ]
            )
            st.dataframe(comp_df, use_container_width=True, hide_index=True)
            st.markdown(f"**Racional:** {row.get('rationale') or 'N/D'}")
            st.markdown("**Fontes locais usadas**")
            _render_sources(row.get("source_refs_json"))
