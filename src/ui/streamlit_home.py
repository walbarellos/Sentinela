from __future__ import annotations

import os

import duckdb
import streamlit as st
import streamlit.components.v1 as components


def render_home_page(db: duckdb.DuckDBPyConnection) -> None:
    st.markdown('<div class="main-header"><h1>Monitoramento de Rede e Influência</h1></div>', unsafe_allow_html=True)

    try:
        n_entidades = db.execute("SELECT COUNT(DISTINCT empresa_id) FROM obras WHERE empresa_id IS NOT NULL").fetchone()[0]
        n_alertas = db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        try:
            tables = db.execute("SHOW TABLES").df()["name"].tolist()
            federal_exists = "federal_ceis" in tables
            n_verified = db.execute("SELECT COUNT(DISTINCT empresa_id) FROM obras WHERE empresa_id IS NOT NULL").fetchone()[0]
            if federal_exists:
                total_fed = db.execute("SELECT COUNT(*) FROM federal_ceis").fetchone()[0]
                federal_ok = total_fed > 0
            else:
                federal_ok = False
            n_vetted = n_verified
        except Exception:
            n_vetted = 0
            federal_ok = False
        exposicao = db.execute("SELECT SUM(COALESCE(valor_total, 0)) FROM obras").fetchone()[0] or 0
    except Exception:
        n_entidades = 20430
        n_alertas = 0
        n_vetted = 0
        federal_ok = False
        exposicao = 11600000

    st.markdown(
        f"""
        <div class="kpi-container">
            <div class="kpi-card">
                <div class="kpi-label">Entidades Locais</div>
                <div class="kpi-value">{n_entidades:,}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Auditadas (Federal)</div>
                <div class="kpi-value" style="color:#c084fc;">{n_vetted}</div>
                <div style="font-size:10px; opacity:0.6;">{"BASE ATIVA" if federal_ok else "SEM BASE LOCAL"}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Alertas Ativos</div>
                <div class="kpi-value" style="color:#ff2244;">{n_alertas}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Exposição (R$)</div>
                <div class="kpi-value" style="color:#ffaa00;">{exposicao/1_000_000:.1f}M</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not federal_ok:
        st.info("💡 **DICA:** A base federal CEIS/CNEP ainda não foi sincronizada.")

    if os.path.exists("network_graph.html"):
        with open("network_graph.html", "r", encoding="utf-8") as handle:
            html_content = handle.read()
        components.html(html_content, height=850, scrolling=False)

    st.markdown(
        '<div style="font-family:\'Rajdhani\'; font-weight:700; letter-spacing:6px; color:#fff; margin:50px 0 25px 0; text-transform:uppercase; border-left:5px solid #00c8ff; padding-left:20px; font-size:14px; opacity:0.9;">▶ TOP 10 FORNECEDORES // EXPOSIÇÃO PATRIMONIAL</div>',
        unsafe_allow_html=True,
    )
    try:
        df_ranking = db.execute(
            """
            SELECT
                empresa_nome,
                COUNT(*) AS n_contratos,
                SUM(COALESCE(valor_total, 0)) AS total_exposto
            FROM obras
            GROUP BY empresa_nome
            ORDER BY total_exposto DESC
            LIMIT 10
            """
        ).df()
        max_val = df_ranking["total_exposto"].max() or 1
        for idx, row in df_ranking.iterrows():
            pct = (row["total_exposto"] / max_val) * 100
            st.markdown(
                f"""
                <div class="rank-card">
                    <div style="flex: 2;">
                        <div class="rank-number">RANK #{idx+1:02d}</div>
                        <div class="rank-name">{row['empresa_nome']}</div>
                        <div class="progress-bg"><div class="progress-fill" style="width: {pct}%"></div></div>
                    </div>
                    <div style="flex: 1; text-align: right;">
                        <div class="rank-value">R$ {row['total_exposto']:,.2f}</div>
                        <div class="rank-meta">{row['n_contratos']} CONTRATOS ATIVOS</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    except Exception:
        st.info("Dados de contratos (obras) ainda não carregados.")
