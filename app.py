import streamlit as st
import pandas as pd
import duckdb
import streamlit.components.v1 as components
import os
import json

st.set_page_config(page_title="SENTINELA // COMMAND CENTER", layout="wide", initial_sidebar_state="expanded")

# --- DATABASE ---
@st.cache_resource
def get_db():
    return duckdb.connect("./data/sentinela_analytics.duckdb", read_only=True)

db = get_db()

# --- LUXURY CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@300;400;600;700&display=swap');
    
    .stApp { background-color: #020408; color: #8eb8d4; }
    
    /* Centralizar tudo */
    .main-header { text-align: center; margin-bottom: 40px; }
    .main-header h1 { font-family: 'Rajdhani', sans-serif; font-weight: 700; letter-spacing: 5px; color: #fff; text-transform: uppercase; }
    
    /* KPI Cards de Luxo */
    .kpi-container { display: flex; justify-content: space-around; text-align: center; margin-bottom: 50px; padding: 20px; background: rgba(255,255,255,0.02); border-radius: 4px; border: 1px solid rgba(0,200,255,0.05); }
    .kpi-card { flex: 1; }
    .kpi-label { font-family: 'Rajdhani', sans-serif; font-size: 12px; letter-spacing: 3px; color: #00c8ff; opacity: 0.8; text-transform: uppercase; }
    .kpi-value { font-family: 'Share Tech Mono', monospace; font-size: 42px; font-weight: 400; color: #fff; text-shadow: 0 0 20px rgba(0,200,255,0.3); margin-top: 5px; }
    
    /* Iframe Styling */
    iframe { border: none !important; border-radius: 8px; box-shadow: 0 20px 50px rgba(0,0,0,0.5); }
    
    /* Sidebar */
    .css-1d391kg { background-color: #060d14 !important; }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.title("🛡️ SENTINELA")
    st.caption("INTELLIGENCE UNIT // V5.1")
    st.divider()
    page = st.radio("NAVEGAÇÃO INTERNA", ["🏠 CENTRO DE COMANDO", "🚩 ALERTAS CRÍTICOS", "👥 RASTREIO DE PESSOAL"])
    st.divider()
    st.info("Sistema operando em modo de escrutínio total. Dados cruzados de 14 fontes públicas.")

# --- PAGES ---

if page == "🏠 CENTRO DE COMANDO":
    st.markdown('<div class="main-header"><h1>Rede de Influência e Conexões</h1></div>', unsafe_allow_html=True)
    
    # KPIs Estilizados
    entidades = "20,430"
    alertas = str(db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0])
    exposicao = "R$ 11.6M"
    
    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-card">
            <div class="kpi-label">Entidades Mapeadas</div>
            <div class="kpi-value">{entidades}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Alertas de Risco</div>
            <div class="kpi-value">{alertas}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Exposição Financeira</div>
            <div class="kpi-value" style="color: #ffaa00;">{exposicao}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # GRAFO DINÂMICO
    if os.path.exists("network_graph.html"):
        with open("network_graph.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=850, scrolling=False)
    else:
        st.warning("Gerando motor gráfico... aguarde 2 segundos.")

elif page == "🚩 ALERTAS CRÍTICOS":
    st.subheader("🚩 DOSSIÊ DE ANOMALIAS")
    df = db.execute("SELECT severity, detector_id, entity_name, description, base_legal FROM alerts ORDER BY severity DESC").df()
    st.table(df)

elif page == "👥 RASTREIO DE PESSOAL":
    st.subheader("👥 BUSCA AVANÇADA")
    nome = st.text_input("DIGITE O NOME PARA INICIAR O CROSS-REFERENCE")
    if nome:
        res = db.execute(f"SELECT * FROM rb_servidores_mass WHERE servidor ILIKE '%{nome}%' LIMIT 50").df()
        st.dataframe(res)
