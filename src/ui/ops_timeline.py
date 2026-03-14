from __future__ import annotations

import pandas as pd
import streamlit as st


def render_timeline_tab(timeline_df: pd.DataFrame) -> None:
    st.markdown("#### Timeline documental")
    if timeline_df.empty:
        st.info("Sem eventos materializados para este caso.")
        return

    top1, top2 = st.columns([1, 1.6])
    phase_options = ["Todas"] + timeline_df["phase_label"].dropna().unique().tolist()
    with top1:
        phase_filter = st.selectbox("Fase", phase_options, key="ops_timeline_phase")
    with top2:
        event_options = ["Todos"] + sorted(timeline_df["event_type"].dropna().unique().tolist())
        event_filter = st.selectbox("Tipo de evento", event_options, key="ops_timeline_event")

    filtered = timeline_df.copy()
    if phase_filter != "Todas":
        filtered = filtered[filtered["phase_label"] == phase_filter]
    if event_filter != "Todos":
        filtered = filtered[filtered["event_type"] == event_filter]

    phase_counts = (
        filtered.groupby(["phase_order", "phase_label"], dropna=False)
        .size()
        .reset_index(name="total")
        .sort_values(["phase_order", "phase_label"])
    )
    st.dataframe(phase_counts[["phase_label", "total"]], use_container_width=True, hide_index=True)

    for phase in phase_counts.to_dict("records"):
        phase_df = filtered[filtered["phase_label"] == phase["phase_label"]].copy()
        with st.expander(f"{phase['phase_label']} ({int(phase['total'])})", expanded=len(phase_counts) <= 2):
            st.dataframe(
                phase_df[["event_at", "event_type", "event_group", "title", "detail", "source_ref", "path_ref"]],
                use_container_width=True,
                hide_index=True,
            )
