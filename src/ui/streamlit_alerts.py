from __future__ import annotations

import duckdb
import streamlit as st


def render_alerts_page(db: duckdb.DuckDBPyConnection) -> None:
    st.markdown('<div class="main-header"><h1>Alertas Legados em Quarentena</h1></div>', unsafe_allow_html=True)
    st.warning(
        "Esta aba mostra saídas legadas do `cross_reference_engine`. "
        "Elas não são a camada principal do produto e não devem ser usadas para emissão externa. "
        "Para trabalho probatório, use `📂 OPERAÇÕES`."
    )
    st.caption(
        "Os alertas abaixo são apenas triagem técnica. Qualquer exportação, notícia de fato ou pedido documental "
        "deve nascer do gate operacional e não desta tabela bruta."
    )
    views = {row[0] for row in db.execute("SHOW TABLES").fetchall()}
    source_name = "v_alerts_legacy_quarantine" if "v_alerts_legacy_quarantine" in views else "alerts"
    columns = {row[1] for row in db.execute(f"PRAGMA table_info('{source_name}')").fetchall()}
    status_expr = "COALESCE(detector_status, 'LEGADO_SEM_CLASSIFICACAO')" if "detector_status" in columns else "'LEGADO_SEM_CLASSIFICACAO'"
    classe_expr = "COALESCE(classe_achado, 'N/D')" if "classe_achado" in columns else "'N/D'"
    grau_expr = "COALESCE(grau_probatorio, 'N/D')" if "grau_probatorio" in columns else "'N/D'"
    uso_expr = "COALESCE(uso_externo, 'REVISAO_INTERNA')" if "uso_externo" in columns else "'REVISAO_INTERNA'"
    limite_expr = "COALESCE(limite_conclusao, '')" if "limite_conclusao" in columns else "''"
    df = db.execute(
        f"""
        SELECT
            d_id,
            severity,
            detector_id,
            {status_expr} AS detector_status,
            entity_name,
            description,
            base_legal,
            {classe_expr} AS classe_achado,
            {grau_expr} AS grau_probatorio,
            {uso_expr} AS uso_externo,
            {limite_expr} AS limite_conclusao
        FROM (SELECT row_number() OVER() AS d_id, * FROM {source_name}) alerts
        ORDER BY severity DESC, detected_at DESC
        """
    ).df()

    if df.empty:
        st.info("Nenhum alerta legado encontrado.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Linhas em quarentena", len(df))
    c2.metric("Detectores aposentados", int((df["detector_status"] == "APOSENTADO").sum()))
    c3.metric("Cobertos em ops", int((df["detector_status"] == "COBERTO_OPS").sum()))

    for _, row in df.iterrows():
        with st.container():
            st.markdown(
                f"""
                <div style="background:rgba(6,20,35,0.8); border-left:5px solid {'#ff2244' if row['severity']=='CRÍTICO' else '#ffaa00'}; padding:20px; border-radius:2px;">
                    <div style="font-family:monospace; color:#00c8ff; font-size:12px;">LEGACY // {row['detector_id']} // {row['detector_status']} // {row['uso_externo']}</div>
                    <div style="font-size:20px; font-weight:700; color:#fff; margin:5px 0;">{row['entity_name']}</div>
                    <div style="color:#cce8f4; margin-bottom:15px;">{row['description']}</div>
                    <div style="font-size:12px; color:#a9d8ee;">Classe: {row['classe_achado']} | Grau: {row['grau_probatorio']}</div>
                    <div style="font-size:11px; opacity:0.6; border-top:1px solid #222; padding-top:10px;">{row['base_legal']}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.info(row["limite_conclusao"] or "Saída legada sem limite formal registrado.")
        st.divider()
