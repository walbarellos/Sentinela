# app.py
import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime
from insights_engine import generate_insights_for_obras, generate_insights_for_servidores, _fmt_brl, generate_insights_for_diarias

st.set_page_config(page_title="SENTINELA // COMMAND CENTER", layout="wide", initial_sidebar_state="expanded")

# --- CSS: CYBER AUDIT THEME ---
st.markdown("""
    <style>
    .stApp { background-color: #0e1117; color: #e0e0e0; }
    [data-testid="stSidebar"] { background-color: #05070a; border-right: 1px solid #1f2937; }
    .threat-card {
        background: linear-gradient(145deg, #161b22, #0d1117);
        border: 1px solid #30363d; border-left: 5px solid #30363d;
        padding: 20px; border-radius: 4px; margin-bottom: 15px;
    }
    .status-critico { border-left-color: #f85149 !important; }
    .status-alto { border-left-color: #db6d28 !important; }
    .status-medio { border-left-color: #d29922 !important; }
    .badge-data { background: #000; color: #39ff14; padding: 2px 8px; border-radius: 3px; border: 1px solid #1f2937; font-family: monospace; }
    h1, h2, h3 { text-transform: uppercase; letter-spacing: -1px; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_db():
    return duckdb.connect("./data/sentinela_analytics.duckdb", read_only=True)

db = get_db()

# --- SIDEBAR HUD ---
with st.sidebar:
    st.title("🛡️ SENTINELA")
    st.markdown("<small style='font-family:monospace; color:#8b949e;'>ACRE GOV UNIT v3.2</small>", unsafe_allow_html=True)
    st.divider()
    
    # Disclaimer Jurídico
    st.caption("⚠️ **AVISO LEGAL:** Este sistema identifica ANOMALIAS ESTATÍSTICAS e INDÍCIOS que requerem validação humana. Os dados são públicos (LAI), mas a interpretação requer análise de atos administrativos. Evite imputação de crime sem prova cabal.")
    
    st.divider()
    if 'page' not in st.session_state: st.session_state.page = "radar"
    if st.button("📡 RADAR DE OBRAS"): st.session_state.page = "radar"
    if st.button("👥 VÍNCULOS PESSOAL"): st.session_state.page = "pessoal"
    if st.button("✈️ RASTREIO DIÁRIAS"): st.session_state.page = "diarias"
    st.divider()
    min_n = st.slider("Amostra Mínima (N)", 1, 20, 5)
    min_exp = st.number_input("Exposição Mínima (R$)", value=100000)

# --- FUNÇÃO GENÉRICA DE RENDERIZAÇÃO DE INSIGHTS ---
def render_insights(insights, title_id):
    col_list, col_det = st.columns([1, 1.3])
    with col_list:
        st.markdown(f"**SINAIS:** <span class='badge-data'>{len(insights)}</span>", unsafe_allow_html=True)
        for i in insights:
            sev = f"status-{i.severidade.lower()}"
            with st.container():
                st.markdown(f"""<div class="threat-card {sev}">
                    <b style="float:right;">{i.severidade}</b>
                    <small>ID: {i.id}</small>
                    <h4 style="color:#58a6ff;">{i.titulo}</h4>
                    <p style="font-size:0.85em; color:#8b949e;">{i.descricao}</p>
                </div>""", unsafe_allow_html=True)
                if st.button("DECODIFICAR", key=f"btn_{title_id}_{i.id}"):
                    st.session_state[f"sel_{title_id}"] = i.id
    with col_det:
        sel = st.session_state.get(f"sel_{title_id}")
        ins = next((x for x in insights if x.id == sel), None) if sel else None
        if ins:
            st.markdown(f"### 📁 DOSSIÊ {ins.id}")
            
            # Checklist de Validação Humana
            with st.expander("📋 CHECKLIST DE VALIDAÇÃO (AUDITORIA)", expanded=True):
                st.info("Siga estes passos antes de qualquer conclusão:")
                if "SAL_" in ins.id:
                    st.write("- [ ] Identificar nome e código da rubrica em 'Outras Verbas'.")
                    st.write("- [ ] Verificar se há decisão judicial ou pagamento retroativo.")
                    st.write("- [ ] Checar se o valor líquido alto coincide com IR alto (indica remuneração).")
                    st.write("- [ ] Validar subteto municipal (Subsídio do Prefeito).")
                else:
                    st.write("- [ ] Verificar nexo público (Certificado/Relatório de Viagem).")
                    st.write("- [ ] Conferir base legal (Decreto Municipal de Diárias).")
                    st.write("- [ ] Validar economicidade (Por que comitiva em vez de instrutor local?).")
            
            st.markdown(f"<div class='threat-card' style='border-left:none;'><b>DETALHES DA ANOMALIA:</b><br>{ins.descricao}</div>", unsafe_allow_html=True)
            st.dataframe(pd.DataFrame(ins.evidencias), use_container_width=True)
        else: st.info("AGUARDANDO SELEÇÃO")

# --- PÁGINAS ---
if st.session_state.page == "radar":
    st.subheader("📡 Radar: Obras Públicas")
    try:
        df = db.execute("SELECT * FROM obras").df()
        if df.empty:
            st.info("RASTREAMENTO ATIVO // Nenhuma obra capturada ainda. Execute o crawler correspondente.")
        else:
            insights = generate_insights_for_obras(df, min_exposicao=min_exp, min_n_secretaria=min_n)
            render_insights(insights, "obras")
    except Exception as e:
        st.error(f"DATABASE OFFLINE or ERROR: {e}")

elif st.session_state.page == "pessoal":
    st.subheader("👥 Inteligência: Pessoal & Salários")
    try:
        df_s = db.execute("SELECT * FROM rb_servidores_mass").df()
        if df_s.empty:
            st.warning("RADAR LIMPO // Execute 'python src/ingest/riobranco_servidores_mass.py' para carregar.")
        else:
            insights_s = generate_insights_for_servidores(df_s)
            render_insights(insights_s, "servidores")
    except Exception as e:
        st.error(f"DATABASE ERROR: {e}")

elif st.session_state.page == "diarias":
    st.subheader("✈️ Rastreio: Diárias")
    try:
        df_d = db.execute("SELECT * FROM diarias").df()
        if df_d.empty:
            st.info("RADAR ATIVO // Aguardando carga via 'riobranco_diarias.py'.")
        else:
            insights_d = generate_insights_for_diarias(df_d)
            render_insights(insights_d, "diarias")
    except Exception as e:
        st.error(f"DATABASE ERROR: {e}")
