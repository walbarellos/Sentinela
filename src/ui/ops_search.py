from __future__ import annotations

import re

import duckdb
import pandas as pd
import streamlit as st

from src.ui.ops_preview import render_artifact_preview
from src.ui.ops_shared import DB_PATH


def _best_window(content: str, terms: list[str], radius: int = 220) -> tuple[str, int]:
    lowered = content.lower()
    positions: list[int] = []
    for term in terms:
        positions.extend(match.start() for match in re.finditer(re.escape(term.lower()), lowered))
    if not positions:
        snippet = content[: radius * 2].replace("\n", " ").strip()
        return snippet, 0

    best_score = -1
    best_start = 0
    best_end = min(len(content), radius * 2)
    for pos in positions:
        start = max(0, pos - radius)
        end = min(len(content), pos + radius)
        window = lowered[start:end]
        score = sum(window.count(term.lower()) for term in terms)
        if score > best_score:
            best_score = score
            best_start = start
            best_end = end
    snippet = content[best_start:best_end].replace("\n", " ").strip()
    return snippet, best_score


def _highlight(snippet: str, terms: list[str]) -> str:
    highlighted = snippet
    for term in sorted(set(terms), key=len, reverse=True):
        highlighted = re.sub(
            re.escape(term),
            lambda m: f"**{m.group(0)}**",
            highlighted,
            flags=re.IGNORECASE,
        )
    return highlighted


@st.cache_data(ttl=30, show_spinner=False)
def load_search_index() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_artifact_text_index" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                i.case_id,
                r.family,
                r.subject_name,
                r.orgao,
                i.source_type,
                i.event_type,
                i.label,
                i.kind,
                i.path,
                i.suffix,
                i.text_chars,
                i.line_count,
                i.content_text,
                i.updated_at
            FROM ops_artifact_text_index i
            LEFT JOIN ops_case_registry r ON r.case_id = i.case_id
            ORDER BY i.updated_at DESC, i.case_id, i.label
            """
        ).df()
    finally:
        con.close()


def render_search_tab() -> None:
    st.markdown("#### Busca textual")
    index_df = load_search_index()
    if index_df.empty:
        st.info("Índice textual ainda não materializado.")
        st.code(".venv/bin/python scripts/sync_ops_search_index.py")
        return

    top1, top2, top3 = st.columns(3)
    top1.metric("Docs indexados", len(index_df))
    top2.metric("Casos com texto", int(index_df["case_id"].nunique()))
    top3.metric("Famílias", int(index_df["family"].fillna("N/D").nunique()))

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    query = col1.text_input("Consulta", placeholder="empresa, processo, item, radiologia, sanção...")
    family_options = ["Todas"] + sorted(v for v in index_df["family"].dropna().unique().tolist())
    suffix_options = ["Todos"] + sorted(v for v in index_df["suffix"].dropna().unique().tolist())
    source_options = ["Todos"] + sorted(v for v in index_df["source_type"].dropna().unique().tolist())
    family_filter = col2.selectbox("Família", family_options, key="ops_search_family")
    suffix_filter = col3.selectbox("Tipo", suffix_options, key="ops_search_type")
    source_filter = col4.selectbox("Origem", source_options, key="ops_search_source")

    col5, col6, col7 = st.columns([1, 1, 1.4])
    event_options = ["Todos"] + sorted(v for v in index_df["event_type"].dropna().unique().tolist())
    orgao_options = ["Todos"] + sorted(v for v in index_df["orgao"].dropna().unique().tolist())
    event_filter = col5.selectbox("Evento", event_options, key="ops_search_event")
    orgao_filter = col6.selectbox("Órgão", orgao_options, key="ops_search_orgao")
    case_filter = col7.text_input("Case ID", placeholder="rb:contrato:3898")

    filtered = index_df.copy()
    if family_filter != "Todas":
        filtered = filtered[filtered["family"] == family_filter]
    if suffix_filter != "Todos":
        filtered = filtered[filtered["suffix"] == suffix_filter]
    if source_filter != "Todos":
        filtered = filtered[filtered["source_type"] == source_filter]
    if event_filter != "Todos":
        filtered = filtered[filtered["event_type"] == event_filter]
    if orgao_filter != "Todos":
        filtered = filtered[filtered["orgao"] == orgao_filter]
    if case_filter.strip():
        filtered = filtered[filtered["case_id"].str.contains(case_filter.strip(), case=False, regex=False)]

    if not query or len(query.strip()) < 3:
        st.caption("Digite pelo menos 3 caracteres para pesquisar no índice local.")
        st.dataframe(
            filtered[["case_id", "family", "orgao", "source_type", "event_type", "label", "suffix", "text_chars", "updated_at"]].head(20),
            width="stretch",
            hide_index=True,
        )
        return

    terms = [term.strip().lower() for term in query.split() if term.strip()]
    mask = filtered["content_text"].fillna("").str.lower().apply(lambda text: all(term in text for term in terms))
    results = filtered[mask].copy()
    if results.empty:
        st.warning("Nenhum trecho encontrado no índice local.")
        return

    snippet_data = results["content_text"].fillna("").apply(lambda text: _best_window(text, terms))
    results["snippet"] = snippet_data.apply(lambda item: item[0])
    results["hit_score"] = snippet_data.apply(lambda item: item[1])
    results["snippet_md"] = results["snippet"].apply(lambda text: _highlight(text, terms))
    exact_phrase = query.strip().lower()
    results["exact_phrase"] = results["content_text"].fillna("").str.lower().str.contains(exact_phrase, regex=False)
    results = results.sort_values(["exact_phrase", "hit_score", "updated_at"], ascending=[False, False, False])
    st.caption(f"Resultados: {len(results)}")
    st.dataframe(
        results[["case_id", "family", "orgao", "source_type", "event_type", "label", "suffix", "hit_score", "snippet", "updated_at"]],
        width="stretch",
        hide_index=True,
    )

    preview_idx = st.selectbox(
        "Abrir resultado",
        range(len(results)),
        format_func=lambda idx: f"{results.iloc[int(idx)]['case_id']} :: {results.iloc[int(idx)]['label']}",
        key="ops_search_preview_result"
    )
    selected = results.iloc[int(preview_idx)]
    st.markdown(selected["snippet_md"])
    render_artifact_preview(str(selected["path"]), str(selected["kind"]))
