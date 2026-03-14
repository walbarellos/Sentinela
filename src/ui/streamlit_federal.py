from __future__ import annotations

import duckdb
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def render_federal_page(db: duckdb.DuckDBPyConnection) -> None:
    st.markdown('<div class="main-header"><h1>Escudo de Auditoria Federal</h1></div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="federal-shield">
            <div style="font-size:40px;">🛡️</div>
            <div>
                <div style="font-weight:700; color:#fff; font-size:18px;">INTEGRAÇÃO CGU / PORTAL DA TRANSPARÊNCIA</div>
                <div style="font-size:13px; opacity:0.7;">Cruzamento automático de Listas Negras Federais (CEIS/CNEP) com Contratos Municipais de Rio Branco.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Resumo de Sanções")
        try:
            df_stats = db.execute("SELECT tipo_sancao, COUNT(*) AS total FROM federal_ceis GROUP BY 1 ORDER BY 2 DESC LIMIT 5").df()
            if df_stats.empty:
                fig = go.Figure()
                fig.add_annotation(text="✅ Nenhuma sanção encontrada para as empresas locais", x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#00ff88"))
                fig.update_layout(template="plotly_dark", paper_bgcolor="#0a0e1a", plot_bgcolor="#0a0e1a", height=300)
                st.plotly_chart(fig, use_container_width=True)
            else:
                fig = px.bar(df_stats, x="tipo_sancao", y="total", color="total", color_continuous_scale=["#ff6b6b", "#ff0000"], template="plotly_dark")
                fig.update_layout(paper_bgcolor="#0a0e1a", plot_bgcolor="#0a0e1a", height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.warning("Base CEIS não populada. Use `python portal_transparencia_integrator.py --bulk-sancoes`.")

    with col2:
        st.subheader("🔍 Status das Empresas Locais")
        n_local = db.execute("SELECT COUNT(DISTINCT empresa_id) FROM obras").fetchone()[0]
        try:
            total_federal = db.execute("SELECT COUNT(*) FROM federal_ceis").fetchone()[0]
        except Exception:
            total_federal = 0

        if total_federal == 0:
            st.warning(f"⚠️ O sistema identificou **{n_local} empresas** locais, mas a base federal está vazia.")
        else:
            st.info(f"O sistema verificou **{n_local} empresas** contratadas por Rio Branco contra uma base de **{total_federal:,} sanções federais**.")
            try:
                n_matches = db.execute(
                    """
                    SELECT COUNT(DISTINCT o.empresa_id)
                    FROM obras o
                    INNER JOIN federal_ceis fc
                      ON REGEXP_REPLACE(o.empresa_id::VARCHAR,'[^0-9]','') =
                         REGEXP_REPLACE(fc.cnpj,'[^0-9]','')
                    """
                ).fetchone()[0]
                if n_matches == 0:
                    st.success("✅ RESULTADO: Nenhuma das empresas locais possui sanções vigentes no CEIS/CNEP.")
                else:
                    st.error(f"🔴 ALERTA CRÍTICO: {n_matches} empresa(s) com contratos ativos possuem sanções federais!")
            except Exception:
                st.info("Aguardando sincronização para cruzamento.")

    st.markdown(
        '<div style="font-family:\'Rajdhani\'; font-weight:700; letter-spacing:4px; color:#fff; margin:40px 0 20px 0; text-transform:uppercase; border-left:4px solid #c084fc; padding-left:15px;">📄 Últimos Registros Federativos // Radar CGU</div>',
        unsafe_allow_html=True,
    )
    try:
        df_ceis = db.execute("SELECT nome, cnpj, tipo_sancao, orgao_sancionador FROM federal_ceis ORDER BY row_number() OVER() DESC LIMIT 10").df()
        if df_ceis.empty:
            st.write("Sem registros federais carregados.")
        else:
            for _, row in df_ceis.iterrows():
                st.markdown(
                    f"""
                    <div style="background: rgba(192, 132, 252, 0.03);
                                border: 1px solid rgba(192, 132, 252, 0.1);
                                border-left: 3px solid #c084fc;
                                padding: 15px 20px; margin-bottom: 10px; border-radius: 2px;">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                            <div>
                                <div style="font-family:'Share Tech Mono'; font-size:10px; color:#c084fc; letter-spacing:2px;">SANCIONADO // {row['cnpj']}</div>
                                <div style="font-size:16px; font-weight:700; color:#fff; margin:4px 0;">{row['nome']}</div>
                                <div style="font-size:11px; color:#8eb8d4; opacity:0.8;">Órgão: {row['orgao_sancionador']}</div>
                            </div>
                            <div style="text-align:right;">
                                <div style="background:rgba(255,43,74,0.1); color:#ff2b4a; padding:4px 10px; border-radius:2px; font-family:'Share Tech Mono'; font-size:11px; border:1px solid rgba(255,43,74,0.2);">
                                    {row['tipo_sancao']}
                                </div>
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    except Exception:
        st.write("A base federal ainda não foi populada.")
