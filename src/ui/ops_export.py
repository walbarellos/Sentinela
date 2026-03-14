from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from src.core.ops_export import build_case_external_text
from src.ui.ops_runbook import render_runbook_tab
from src.ui.ops_data import (
    freeze_ops_case_export_now,
    load_ops_case_generated_export_diffs,
    load_ops_case_generated_exports,
)
from src.ui.ops_shared import DB_PATH


MODE_LABELS = {
    "NOTA_INTERNA": "Nota interna",
    "PEDIDO_DOCUMENTAL": "Pedido documental",
    "NOTICIA_FATO": "Notícia de fato",
}


def _format_mode(value: str) -> str:
    return MODE_LABELS.get(value, value)


def render_export_tab(
    case_id: str,
    gate_df: pd.DataFrame,
    runbook_df: pd.DataFrame | None = None,
    runbook_steps_df: pd.DataFrame | None = None,
) -> None:
    st.markdown("#### Exportação segura")
    if gate_df.empty:
        st.info("Gate de exportação ainda não foi materializado para este caso.")
        return

    st.dataframe(gate_df, use_container_width=True, hide_index=True)
    if runbook_df is not None and not runbook_df.empty:
        with st.expander("Encaminhamento operacional", expanded=False):
            steps_df = runbook_steps_df if runbook_steps_df is not None else pd.DataFrame()
            render_runbook_tab(runbook_df, steps_df, compact=True)

    frozen_df = load_ops_case_generated_exports(case_id)
    diff_df = load_ops_case_generated_export_diffs(case_id)
    if frozen_df.empty:
        st.caption("Nenhuma exportação controlada foi congelada para este caso ainda.")
    else:
        st.markdown("##### Exportações congeladas")
        st.dataframe(frozen_df, use_container_width=True, hide_index=True)
    if not diff_df.empty:
        st.markdown("##### Diferenças entre versões congeladas")
        st.dataframe(
            diff_df.drop(columns=["diff_text"]),
            use_container_width=True,
            hide_index=True,
        )
        selected_diff_idx = st.selectbox(
            "Abrir diff congelado",
            range(len(diff_df)),
            format_func=lambda idx: f"{diff_df.iloc[int(idx)]['export_mode']} :: {diff_df.iloc[int(idx)]['summary']}",
        )
        st.code(str(diff_df.iloc[int(selected_diff_idx)]["diff_text"] or ""), language="diff")

    allowed_df = gate_df[gate_df["allowed"] == True].copy()  # noqa: E712
    if allowed_df.empty:
        st.warning("Nenhum modo de exportação está liberado para este caso.")
        return

    export_mode = st.selectbox(
        "Modo permitido",
        allowed_df["export_mode"].tolist(),
        format_func=_format_mode,
    )

    preview_key = f"ops_export_preview:{case_id}:{export_mode}"
    if st.button("Gerar texto seguro", use_container_width=True):
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            payload = build_case_external_text(con, case_id=case_id, export_mode=export_mode)
        finally:
            con.close()
        st.session_state[preview_key] = payload["text"]

    if preview_key in st.session_state:
        st.text_area("Texto gerado", value=st.session_state[preview_key], height=360)
        st.download_button(
            label="Baixar .txt",
            data=str(st.session_state[preview_key]).encode("utf-8"),
            file_name=f"{case_id.replace(':', '_')}__{export_mode.lower()}.txt",
            mime="text/plain",
            use_container_width=True,
        )
        if st.button("Congelar exportação controlada", type="primary", use_container_width=True):
            try:
                result = freeze_ops_case_export_now(case_id, export_mode)
                st.cache_data.clear()
                action = "reaproveitada" if result.get("reused") else "congelada"
                st.success(
                    f"Exportação {action}: {result.get('path')} / sha256 {result.get('sha256')}"
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Falha ao congelar exportação: {exc}")
