from __future__ import annotations

import difflib
import json
from io import StringIO
from pathlib import Path

import pandas as pd
import streamlit as st

from src.ui.ops_data import read_text_artifact
from src.ui.ops_shared import ROOT, resolve_artifact_path


def _html_to_text(content: str) -> str:
    try:
        from bs4 import BeautifulSoup

        return BeautifulSoup(content, "html.parser").get_text("\n", strip=True)
    except Exception:
        return content


@st.cache_data(ttl=120, show_spinner=False)
def artifact_to_diff_text(path_value: str) -> str | None:
    path = resolve_artifact_path(path_value)
    if not path or not path.exists():
        return None

    suffix = path.suffix.lower()
    if suffix in {".md", ".txt"}:
        return read_text_artifact(path_value)

    if suffix == ".json":
        content = read_text_artifact(path_value)
        try:
            return json.dumps(json.loads(content), ensure_ascii=False, indent=2, sort_keys=True)
        except json.JSONDecodeError:
            return content

    if suffix == ".csv":
        content = read_text_artifact(path_value)
        try:
            df = pd.read_csv(StringIO(content))
            return df.to_csv(index=False)
        except Exception:
            return content

    if suffix in {".html", ".htm"}:
        return _html_to_text(read_text_artifact(path_value))

    if suffix == ".pdf":
        txt_fallback = Path(path).with_suffix(".txt")
        if txt_fallback.exists():
            rel = str(txt_fallback.relative_to(ROOT))
            return read_text_artifact(rel)
        return None

    return None


def render_artifact_diff(artifacts_df: pd.DataFrame) -> None:
    st.markdown("#### Diff documental")
    previewable = artifacts_df[artifacts_df["exists"] & artifacts_df["path"].notna()].copy()
    if len(previewable) < 2:
        st.info("O diff precisa de pelo menos dois artefatos locais.")
        return

    labels = previewable.apply(lambda row: f"{row['kind']} :: {row['label']}", axis=1).tolist()
    left_col, right_col = st.columns(2)
    with left_col:
        left_idx = st.selectbox("Artefato A", range(len(previewable)), format_func=lambda idx: labels[int(idx)], key="ops_diff_left")
    with right_col:
        right_idx = st.selectbox("Artefato B", range(len(previewable)), format_func=lambda idx: labels[int(idx)], index=min(1, len(previewable)-1), key="ops_diff_right")

    if int(left_idx) == int(right_idx):
        st.info("Selecione dois artefatos diferentes.")
        return

    left_row = previewable.iloc[int(left_idx)]
    right_row = previewable.iloc[int(right_idx)]
    left_text = artifact_to_diff_text(str(left_row["path"]))
    right_text = artifact_to_diff_text(str(right_row["path"]))

    if left_text is None or right_text is None:
        st.warning("Um dos artefatos não tem texto extraível para diff. Prefira `md`, `txt`, `json`, `csv`, `html` ou PDF com `.txt` espelho.")
        return

    left_suffix = Path(str(left_row["path"])).suffix.lower()
    right_suffix = Path(str(right_row["path"])).suffix.lower()
    if left_suffix == right_suffix == ".json":
        try:
            left_json = json.loads(left_text)
            right_json = json.loads(right_text)
            if isinstance(left_json, dict) and isinstance(right_json, dict):
                left_keys = set(left_json.keys())
                right_keys = set(right_json.keys())
                s1, s2, s3 = st.columns(3)
                s1.metric("Chaves A", len(left_keys))
                s2.metric("Chaves B", len(right_keys))
                s3.metric("Chaves divergentes", len(left_keys ^ right_keys))
                if left_keys ^ right_keys:
                    diff_len = max(len(left_keys - right_keys), len(right_keys - left_keys))
                    st.dataframe(
                        pd.DataFrame(
                            {
                                "apenas_em_A": sorted(left_keys - right_keys)[:diff_len],
                                "apenas_em_B": sorted(right_keys - left_keys)[:diff_len],
                            }
                        ),
                        width='stretch',
                        hide_index=True,
                    )
        except Exception:
            pass

    if left_suffix == right_suffix == ".csv":
        try:
            left_df = pd.read_csv(StringIO(left_text))
            right_df = pd.read_csv(StringIO(right_text))
            s1, s2, s3, s4 = st.columns(4)
            s1.metric("Linhas A", len(left_df))
            s2.metric("Linhas B", len(right_df))
            s3.metric("Cols A", len(left_df.columns))
            s4.metric("Cols B", len(right_df.columns))
            col_diff = sorted(set(left_df.columns) ^ set(right_df.columns))
            if col_diff:
                st.caption("Colunas divergentes")
                st.code(", ".join(col_diff[:80]), language="text")
        except Exception:
            pass

    left_lines = left_text.splitlines()
    right_lines = right_text.splitlines()
    diff = list(
        difflib.unified_diff(
            left_lines,
            right_lines,
            fromfile=str(left_row["path"]),
            tofile=str(right_row["path"]),
            lineterm="",
            n=2,
        )
    )
    st.caption(f"Linhas A: {len(left_lines)} | Linhas B: {len(right_lines)} | Diff: {len(diff)} linhas")
    if not diff:
        st.success("Sem diferença textual detectada no recorte normalizado.")
        return
    st.code("\n".join(diff[:1200]), language="diff")
