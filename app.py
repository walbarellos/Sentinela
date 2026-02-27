# app.py
import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime
from insights_engine import generate_insights_for_obras, generate_insights_for_servidores, _fmt_brl, generate_insights_for_diarias
import plotly.express as px

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
        transition: transform 0.2s;
    }
    .threat-card:hover { transform: translateY(-2px); border-color: #58a6ff; }
    .status-critico { border-left-color: #f85149 !important; }
    .status-alto { border-left-color: #db6d28 !important; }
    .status-medio { border-left-color: #d29922 !important; }
    .badge-data { background: #000; color: #39ff14; padding: 2px 8px; border-radius: 3px; border: 1px solid #1f2937; font-family: monospace; font-size: 0.85em; margin-right: 5px;}
    .badge-flag { background: #1f2937; color: #c9d1d9; padding: 2px 6px; border-radius: 10px; font-size: 0.75em; margin-right: 4px; border: 1px solid #30363d;}
    h1, h2, h3 { text-transform: uppercase; letter-spacing: -1px; font-family: 'Inter', sans-serif; }
    .kpi-box { background: #161b22; padding: 20px; border-radius: 6px; border: 1px solid #30363d; text-align: center; }
    .kpi-value { font-size: 2em; font-weight: bold; color: #58a6ff; font-family: monospace; }
    .kpi-label { font-size: 0.85em; color: #8b949e; text-transform: uppercase; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_db():
    return duckdb.connect("./data/sentinela_analytics.duckdb", read_only=True)

db = get_db()

# --- SIDEBAR HUD ---
with st.sidebar:
    st.title("🛡️ SENTINELA")
    st.markdown("<small style='font-family:monospace; color:#8b949e;'>ACRE GOV UNIT v4.0</small>", unsafe_allow_html=True)
    st.divider()
    
    # Disclaimer Jurídico
    st.caption("⚠️ **AVISO LEGAL:** Este sistema identifica ANOMALIAS ESTATÍSTICAS e INDÍCIOS que requerem validação humana. Os dados são públicos (LAI), mas a interpretação requer análise de atos administrativos. Evite imputação de crime sem prova documental.")
    
    st.divider()
    if 'page' not in st.session_state: st.session_state.page = "home"
    
    st.markdown("### NAVEGAÇÃO")
    if st.button("👁️ VISÃO GERAL (HOME)", use_container_width=True): st.session_state.page = "home"
    if st.button("👥 VÍNCULOS & FOLHA", use_container_width=True): st.session_state.page = "pessoal"
    if st.button("✈️ RASTREIO DIÁRIAS", use_container_width=True): st.session_state.page = "diarias"
    if st.button("📡 CONTRATOS & OBRAS", use_container_width=True): st.session_state.page = "radar"
    
    st.divider()
    st.markdown("### PARÂMETROS GLOBAIS")
    min_n = st.slider("Amostra Mínima (N)", 1, 20, 5)
    min_exp = st.number_input("Exposição Mínima (R$)", value=100000)

# --- FUNÇÃO GENÉRICA DE RENDERIZAÇÃO DE INSIGHTS ---
def render_insights(insights, title_id):
    if not insights:
        st.success("Nenhuma anomalia detectada com os parâmetros atuais.")
        return

    col_list, col_det = st.columns([1, 1.5])
    
    with col_list:
        st.markdown(f"### SINAIS DETECTADOS: <span style='color:#58a6ff'>{len(insights)}</span>", unsafe_allow_html=True)
        st.markdown("<hr style='margin-top:0; margin-bottom:15px; border-color:#30363d;'>", unsafe_allow_html=True)
        
        # Simulando virtualização/scroll na coluna
        with st.container(height=800):
            for i in insights:
                sev = f"status-{i.severidade.lower()}"
                
                # Gera flags baseadas na descrição de forma robusta
                flags = []
                if "IR" in i.descricao: flags.append("IR Relevante")
                
                # Só tenta extrair multiplicador se for anomalia estatística (que usa o padrão 'X.Yx')
                if "x" in i.descricao and i.tipo == "ANOMALIA ESTATÍSTICA":
                    try:
                        # Extrai o valor numérico antes do 'x'
                        parts = i.descricao.split("x")[0].split("**")
                        if len(parts) > 1:
                            val_str = parts[-1].replace(",", ".")
                            if float(val_str) > 5:
                                flags.append("Alto Desvio")
                    except:
                        pass
                
                if "agregado" in i.descricao: flags.append("Agrupamento")
                
                flags_html = "".join([f"<span class='badge-flag'>{f}</span>" for f in flags])
                
                with st.container():
                    st.markdown(f"""
                    <div class="threat-card {sev}">
                        <div style="display:flex; justify-content:space-between; align-items:baseline;">
                            <span class='badge-data'>{i.id.split('_')[0]}</span>
                            <b style="color:{'#f85149' if i.severidade=='CRITICO' else '#db6d28' if i.severidade=='ALTO' else '#d29922'}; font-size:0.8em;">{i.severidade}</b>
                        </div>
                        <h4 style="color:#e0e0e0; margin:10px 0 5px 0; font-size:1.1em;">{i.titulo.replace('Indício: ', '')}</h4>
                        <div style="margin-bottom:10px;">{flags_html}</div>
                        <p style="font-size:0.9em; color:#8b949e; margin-bottom:15px;">Exposição: <b style='color:#58a6ff'>{_fmt_brl(i.exposicao)}</b></p>
                    </div>""", unsafe_allow_html=True)
                    
                    if st.button(f"ABRIR DOSSIÊ", key=f"btn_{title_id}_{i.id}", use_container_width=True):
                        st.session_state[f"sel_{title_id}"] = i.id

    with col_det:
        sel = st.session_state.get(f"sel_{title_id}")
        ins = next((x for x in insights if x.id == sel), None) if sel else None
        
        if ins:
            st.markdown(f"## 📁 DOSSIÊ DE AUDITORIA: `{ins.id}`")
            st.markdown("<hr style='margin-top:0; border-color:#30363d;'>", unsafe_allow_html=True)
            
            # Workflow Status
            st.markdown("""
            <div style='display:flex; gap:10px; margin-bottom:20px; font-size:0.85em; font-family:monospace;'>
                <span style='background:#f8514940; color:#f85149; padding:4px 8px; border-radius:4px;'>1. DETECTADO</span> →
                <span style='background:#30363d; color:#8b949e; padding:4px 8px; border-radius:4px;'>2. EM ANÁLISE</span> →
                <span style='background:#30363d; color:#8b949e; padding:4px 8px; border-radius:4px;'>3. LAI SOLICITADA</span> →
                <span style='background:#30363d; color:#8b949e; padding:4px 8px; border-radius:4px;'>4. CONCLUÍDO</span>
            </div>
            """, unsafe_allow_html=True)

            # Painel de Resumo
            st.markdown(f"""
            <div style='background:#161b22; padding:20px; border-radius:6px; border:1px solid #30363d; margin-bottom:20px;'>
                <h4 style='margin-top:0; color:#58a6ff;'>HIPÓTESE INVESTIGATIVA</h4>
                <p style='color:#c9d1d9; font-size:1.1em;'>{ins.descricao}</p>
                <small style='color:#8b949e;'>Fonte Primária: {ins.fontes[0]}</small>
            </div>
            """, unsafe_allow_html=True)
            
            # Checklist Oficial
            st.markdown("### 📋 PROTOCOLO DE VALIDAÇÃO")
            if "SAL_" in ins.id:
                st.checkbox("Identificar natureza e base legal da rubrica predominante ('Outras Verbas').", key=f"chk1_{ins.id}")
                st.checkbox("Verificar existência de processo judicial, portaria de acerto retroativo ou rescisão.", key=f"chk2_{ins.id}")
                st.checkbox("Confrontar valor bruto com o subteto municipal (Subsídio do Prefeito) do mês correspondente.", key=f"chk3_{ins.id}")
                st.checkbox("Anotar se o desconto de IR e Previdência é compatível com parcela remuneratória.", key=f"chk4_{ins.id}")
            else:
                st.checkbox("Localizar portaria de concessão da diária no Diário Oficial.", key=f"chk1_{ins.id}")
                st.checkbox("Verificar existência do evento/curso e certificados de participação dos envolvidos.", key=f"chk2_{ins.id}")
                st.checkbox("Avaliar justificativa de economicidade para viagem em grupo vs. contratação in loco.", key=f"chk3_{ins.id}")
                st.checkbox("Checar se o período pago abrange finais de semana sem programação oficial do evento.", key=f"chk4_{ins.id}")

            # Evidências (Tabela Isolada)
            st.markdown("### 📎 REGISTROS EXTRAÍDOS (EVIDÊNCIA BRUTA)")
            df_evid = pd.DataFrame(ins.evidencias)
            st.dataframe(df_evid, use_container_width=True, hide_index=True)
            
            # Ações Rápidas
            st.markdown("<br>", unsafe_allow_html=True)
            cols_action = st.columns(3)
            cols_action[0].button("📄 Gerar Rascunho LAI", use_container_width=True)
            cols_action[1].button("📌 Fixar Dossiê", use_container_width=True)
            cols_action[2].button("✅ Marcar Explicado", use_container_width=True)

        else: 
            st.info("👈 Selecione um sinal no painel esquerdo para abrir o dossiê analítico.")

# --- PÁGINAS ---
if st.session_state.page == "home":
    st.header("👁️ VISÃO GERAL DO SISTEMA")
    
    # Busca dados macro
    try:
        total_serv = db.execute("SELECT COUNT(*) FROM rb_servidores_mass").fetchone()[0]
        total_diarias = db.execute("SELECT COUNT(*) FROM diarias").fetchone()[0]
        total_obras = db.execute("SELECT COUNT(*) FROM obras").fetchone()[0]
    except:
        total_serv = total_diarias = total_obras = 0

    col1, col2, col3, col4 = st.columns(4)
    with col1: st.markdown(f"<div class='kpi-box'><div class='kpi-label'>Linhas Analisadas</div><div class='kpi-value'>{total_serv + total_diarias + total_obras:,}</div></div>", unsafe_allow_html=True)
    with col2: st.markdown(f"<div class='kpi-box'><div class='kpi-label'>Servidores Mapeados</div><div class='kpi-value'>{total_serv:,}</div></div>", unsafe_allow_html=True)
    with col3: st.markdown(f"<div class='kpi-box'><div class='kpi-label'>Diárias Rastreadas</div><div class='kpi-value'>{total_diarias:,}</div></div>", unsafe_allow_html=True)
    with col4: st.markdown(f"<div class='kpi-box'><div class='kpi-label'>Alertas Ativos</div><div class='kpi-value' style='color:#f85149'>70+</div></div>", unsafe_allow_html=True)

    st.markdown("### PRIORIDADES DE INVESTIGAÇÃO")
    st.info("Navegue pelos módulos no menu lateral para acessar os painéis de triagem e dossiês detalhados.")
    
elif st.session_state.page == "radar":
    st.header("📡 Radar: Obras Públicas")
    try:
        df = db.execute("SELECT * FROM obras").df()
        if df.empty:
            st.info("RASTREAMENTO ATIVO // Nenhuma obra capturada ainda. Execute o crawler correspondente.")
        else:
            insights = generate_insights_for_obras(df, min_exposicao=min_exp, min_n_secretaria=min_n)
            render_insights(insights, "obras")
    except Exception as e:
        st.error(f"DATABASE ERROR: {e}")

elif st.session_state.page == "pessoal":
    st.header("👥 Inteligência: Pessoal & Salários")
    try:
        df_s = db.execute("SELECT * FROM rb_servidores_mass").df()
        if df_s.empty:
            st.warning("RADAR LIMPO // Execute o coletor para carregar.")
        else:
            insights_s = generate_insights_for_servidores(df_s)
            render_insights(insights_s, "servidores")
    except Exception as e:
        st.error(f"DATABASE ERROR: {e}")

elif st.session_state.page == "diarias":
    st.header("✈️ Rastreio: Diárias")
    try:
        df_d = db.execute("SELECT * FROM diarias").df()
        if df_d.empty:
            st.info("RADAR ATIVO // Aguardando carga via coletor.")
        else:
            insights_d = generate_insights_for_diarias(df_d)
            render_insights(insights_d, "diarias")
    except Exception as e:
        st.error(f"DATABASE ERROR: {e}")
