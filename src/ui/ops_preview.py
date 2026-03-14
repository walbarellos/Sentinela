from __future__ import annotations

import base64
import json
from io import StringIO

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.ui.ops_data import read_binary_artifact, read_text_artifact
from src.ui.ops_shared import ROOT, resolve_artifact_path


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
