from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd
import streamlit as st

from src.core.ops_inbox import get_case_inbox_spec, run_case_workflow, sync_ops_inbox, upload_case_inbox_document
from src.ui.ops_shared import DB_PATH


@st.cache_data(ttl=30, show_spinner=False)
def load_case_inbox_documents(case_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_inbox_document" not in tables:
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                destino,
                eixo,
                documento_chave,
                categoria_documental,
                descricao_documento,
                status_documento,
                protocolo,
                recebido_em,
                file_path,
                file_exists,
                size_bytes,
                notas,
                updated_at
            FROM v_ops_case_inbox_document
            WHERE case_id = ?
            ORDER BY destino, eixo, documento_chave
            """,
            [case_id],
        ).df()
    finally:
        con.close()


def sync_case_inbox_now(case_id: str) -> dict[str, int | str | None]:
    con = duckdb.connect(str(DB_PATH))
    try:
        return sync_ops_inbox(con, case_id=case_id)
    finally:
        con.close()


def render_inbox_tab(cases_df: pd.DataFrame) -> None:
    st.markdown("#### Inbox Operacional")
    available_cases = cases_df[cases_df["case_id"].apply(lambda cid: get_case_inbox_spec(str(cid)) is not None)].copy()
    if available_cases.empty:
        st.info("Nenhum caso com caixa operacional configurada.")
        return

    case_id = st.selectbox(
        "Caso com caixa de respostas",
        available_cases["case_id"].tolist(),
        format_func=lambda cid: f"{available_cases.loc[available_cases['case_id'] == cid, 'subject_name'].iloc[0]} [{cid}]",
    )

    toolbar_left, toolbar_mid, toolbar_right = st.columns([1, 1, 1.4])
    with toolbar_left:
        if st.button("📥 Sincronizar Inbox", use_container_width=True):
            stats = sync_case_inbox_now(case_id)
            st.cache_data.clear()
            st.success(f"Inbox sincronizada: {stats['rows_written']} linhas.")
            st.rerun()
    with toolbar_mid:
        if st.button("▶️ Rerodar Workflow", use_container_width=True):
            try:
                result = run_case_workflow(case_id)
                st.cache_data.clear()
                st.success(f"Workflow concluído: {len(result['steps'])} etapas.")
                with st.expander("Saída resumida do workflow", expanded=False):
                    for step in result["steps"]:
                        st.markdown(f"**{' '.join(step['command'])}**")
                        if step["stdout"]:
                            st.code(step["stdout"], language="text")
                        if step["stderr"]:
                            st.code(step["stderr"], language="text")
                st.rerun()
            except Exception as exc:
                st.error(f"Falha ao rerodar workflow: {exc}")
    with toolbar_right:
        st.caption("Upload salva no diretório oficial do caso e atualiza o índice da caixa.")

    inbox_df = load_case_inbox_documents(case_id)
    if inbox_df.empty:
        st.warning("A caixa ainda não foi sincronizada para este caso.")
    else:
        st.dataframe(inbox_df, use_container_width=True, hide_index=True)

    st.markdown("#### Anexar resposta oficial")
    if inbox_df.empty:
        st.info("Sincronize a inbox para habilitar o upload guiado.")
        return

    pending_options = inbox_df.apply(
        lambda row: f"{row['documento_chave']} :: {row['destino']} :: {row['categoria_documental']}",
        axis=1,
    ).tolist()
    selected_idx = st.selectbox("Documento-alvo", range(len(inbox_df)), format_func=lambda idx: pending_options[int(idx)])
    selected_row = inbox_df.iloc[int(selected_idx)]

    form_left, form_right = st.columns([1, 1])
    with form_left:
        protocolo = st.text_input("Protocolo", value=str(selected_row.get("protocolo") or ""))
        recebido_em = st.date_input("Recebido em", value=date.today())
    with form_right:
        notas = st.text_area("Notas", value=str(selected_row.get("notas") or ""), height=110)
        uploaded = st.file_uploader("Arquivo", key=f"upload:{case_id}:{selected_row['documento_chave']}")

    if st.button("Salvar resposta", type="primary"):
        if uploaded is None:
            st.error("Selecione um arquivo antes de salvar.")
            return
        try:
            result = upload_case_inbox_document(
                case_id=case_id,
                documento_chave=str(selected_row["documento_chave"]),
                filename=uploaded.name,
                payload=uploaded.getvalue(),
                protocolo=protocolo,
                notas=notas,
                recebido_em=recebido_em,
            )
            sync_case_inbox_now(case_id)
            st.cache_data.clear()
            st.success(f"Arquivo salvo: {result['file_relpath']}")
            st.rerun()
        except Exception as exc:
            st.error(f"Falha ao salvar resposta: {exc}")
