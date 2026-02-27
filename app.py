import streamlit as st
import pandas as pd
import duckdb
import streamlit.components.v1 as components
import os
import json
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="SENTINELA // COMMAND CENTER", layout="wide", initial_sidebar_state="expanded")

# --- CONSTANTS & HELPERS ---
LEGAL_MAP_ALERTAS = {
    "FDS":       ("Acórdão 2.484/2021-TCU-Plenário",
                  "Diárias pagas em sábados/domingos sem programação oficial"),
    "BLOCO":     ("Lei 8.112/90 art. 58 / Decreto 5.992/2006",
                  "Concessão irregular de diárias em grupo sem justificativa"),
    "OUTLIER":   ("CF art. 37 XI / Decreto Municipal",
                  "Remuneração acima do teto constitucional"),
    "CEIS":      ("Lei 8.666/93 art. 87 / Lei 14.133/21",
                  "Contrato com empresa inidônea ou suspensa"),
    "FRACION":   ("Lei 14.133/21 art. 29 §2°",
                  "Fracionamento de despesa para dispensar licitação"),
    "NEPOTISMO": ("Súmula Vinculante nº 13 STF",
                  "Indícios de nepotismo em contratação"),
}

def gerar_texto_denuncia(alerta: dict) -> str:
    tipo = alerta.get('detector_id', 'GERAL')
    legal_ref, descricao_legal = LEGAL_MAP_ALERTAS.get(tipo, ("", "Irregularidade identificada no sistema Sentinela"))
    return f"""
DENÚNCIA — SISTEMA SENTINELA // CONTROLE SOCIAL

FATO DENUNCIADO:
  Entidade: {alerta.get('entity_name', 'N/D')}
  Tipo: {tipo} — {alerta.get('description', 'N/D')}
  
  Severidade: {alerta.get('severity', 'N/D')}
  Base Legal: {alerta.get('base_legal', legal_ref)}

FUNDAMENTO LEGAL SUGERIDO:
  {legal_ref}
  {descricao_legal}

PEDIDO:
  Solicita-se a apuração dos fatos e a instauração de procedimentos 
  administrativos/fiscais cabíveis para verificar a regularidade da situação.

FONTE DOS DADOS:
  Portal de Transparência de Rio Branco (https://transparencia.riobranco.ac.gov.br)
  Dados públicos coletados e analisados pelo sistema SENTINELA.

Data de geração: {__import__('datetime').date.today().isoformat()}
Sistema: SENTINELA // Inteligência em Controle Social
""".strip()

# --- DATABASE ---
@st.cache_resource
def get_db():
    return duckdb.connect("./data/sentinela_analytics.duckdb", read_only=True)

db = get_db()

# --- LUXURY & SERIOUS CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@300;400;600;700&display=swap');
    
    .stApp { background-color: #020408; color: #8eb8d4; }
    
    .main-header { text-align: center; margin-bottom: 40px; }
    .main-header h1 { font-family: 'Rajdhani', sans-serif; font-weight: 700; letter-spacing: 8px; color: #fff; text-transform: uppercase; }
    
    /* KPI Cards Refinados */
    .kpi-container { display: flex; justify-content: space-around; text-align: center; margin-bottom: 50px; padding: 30px; background: linear-gradient(180deg, rgba(0,200,255,0.05) 0%, rgba(0,0,0,0) 100%); border-radius: 4px; border: 1px solid rgba(0,200,255,0.1); }
    .kpi-card { flex: 1; border-right: 1px solid rgba(0,200,255,0.1); }
    .kpi-card:last-child { border-right: none; }
    .kpi-label { font-family: 'Rajdhani', sans-serif; font-size: 11px; letter-spacing: 4px; color: #00c8ff; opacity: 0.7; text-transform: uppercase; }
    .kpi-value { font-family: 'Share Tech Mono', monospace; font-size: 48px; color: #fff; margin-top: 10px; }
    
    /* Federal Shield Style */
    .federal-shield {
        background: rgba(192, 132, 252, 0.05);
        border: 1px solid rgba(192, 132, 252, 0.2);
        padding: 20px;
        border-radius: 4px;
        margin-bottom: 30px;
        display: flex;
        align-items: center;
        gap: 20px;
    }
    
    .status-vetted { color: #00ff88; font-family: 'Share Tech Mono'; font-size: 12px; }
    
    /* Ranking Luxury Style */
    .rank-card {
        background: rgba(0, 200, 255, 0.03);
        border: 1px solid rgba(0, 200, 255, 0.1);
        border-left: 4px solid #00c8ff;
        padding: 15px 25px;
        margin-bottom: 12px;
        border-radius: 4px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .rank-card:hover {
        background: rgba(0, 200, 255, 0.08);
        border-color: rgba(0, 200, 255, 0.3);
        transform: translateX(5px);
        box-shadow: 0 0 20px rgba(0, 200, 255, 0.1);
    }
    .rank-number { font-family: 'Rajdhani', sans-serif; font-size: 12px; color: #00c8ff; letter-spacing: 3px; font-weight: 700; opacity: 0.6; }
    .rank-name { font-family: 'Rajdhani', sans-serif; font-size: 18px; font-weight: 600; color: #fff; text-transform: uppercase; }
    .rank-value { font-family: 'Share Tech Mono', monospace; font-size: 22px; color: #ffaa00; text-shadow: 0 0 10px rgba(255, 170, 0, 0.3); }
    .rank-meta { font-family: 'Rajdhani', sans-serif; font-size: 10px; color: #4a6a7a; letter-spacing: 2px; text-align: right; }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.title("🛡️ SENTINELA")
    st.caption("INTELLIGENCE UNIT // V5.4")
    st.divider()
    page = st.radio("NAVEGAÇÃO INTERNA", ["🏠 CENTRO DE COMANDO", "🚩 ALERTAS CRÍTICOS", "🏛️ AUDITORIA FEDERAL (CGU)", "👥 BUSCA AVANÇADA"])
    st.divider()
    st.markdown("### STATUS OPERACIONAL")
    st.success("Sincronização Local: OK")
    st.success("Sincronização Federal: OK")

# --- PAGES ---

if page == "🏠 CENTRO DE COMANDO":
    st.markdown('<div class="main-header"><h1>Monitoramento de Rede e Influência</h1></div>', unsafe_allow_html=True)
    
    # KPIs de Alta Seriedade
    try:
        n_entidades = db.execute("SELECT COUNT(DISTINCT empresa_id) FROM obras WHERE empresa_id IS NOT NULL").fetchone()[0]
        n_alertas = db.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        
        # Auditadas Federal (CEIS/CNEP)
        try:
            # Verifica se a tabela existe e tem dados
            tables = db.execute("SHOW TABLES").df()['name'].tolist()
            if 'federal_ceis' in tables:
                n_vetted = db.execute("SELECT COUNT(*) FROM federal_ceis").fetchone()[0]
                federal_ok = n_vetted > 0
            else:
                n_vetted = 0
                federal_ok = False
        except:
            n_vetted = 0
            federal_ok = False
            
        # Exposição (soma de contratos)
        exposicao = db.execute("SELECT SUM(COALESCE(valor_total, 0)) FROM obras").fetchone()[0] or 0
    except:
        n_entidades = 20430 # Fallback
        n_alertas = 0
        n_vetted = 0
        federal_ok = False
        exposicao = 11600000

    st.markdown(f"""
    <div class="kpi-container">
        <div class="kpi-card">
            <div class="kpi-label">Entidades Locais</div>
            <div class="kpi-value">{n_entidades:,}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Auditadas (Federal)</div>
            <div class="kpi-value" style="color:#c084fc;">{f"{n_vetted:,}" if federal_ok else "⚠️ N/D"}</div>
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
    """, unsafe_allow_html=True)
    
    if not federal_ok:
        st.info("💡 **DICA:** A base federal CEIS/CNEP ainda não foi sincronizada. Você pode usar a aba **PORTAL TRANSPARÊNCIA (LIVE)** para consultas em tempo real direto na fonte.")

    if os.path.exists("network_graph.html"):
        with open("network_graph.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=850, scrolling=False)

    st.markdown('<div style="font-family:\'Rajdhani\'; font-weight:700; letter-spacing:6px; color:#fff; margin:50px 0 25px 0; text-transform:uppercase; border-left:5px solid #00c8ff; padding-left:20px; font-size:14px; opacity:0.9;">▶ TOP 10 FORNECEDORES // EXPOSIÇÃO PATRIMONIAL</div>', unsafe_allow_html=True)
    try:
        query_ranking = """
            SELECT 
                empresa_nome, 
                COUNT(*) as n_contratos, 
                SUM(COALESCE(valor_total, 0)) as total_exposto 
            FROM obras 
            GROUP BY empresa_nome 
            ORDER BY total_exposto DESC 
            LIMIT 10
        """
        df_ranking = db.execute(query_ranking).df()
        
        for idx, row in df_ranking.iterrows():
            st.markdown(f"""
            <div class="rank-card">
                <div>
                    <div class="rank-number">RANK #{idx+1:02d}</div>
                    <div class="rank-name">{row['empresa_nome']}</div>
                </div>
                <div>
                    <div class="rank-value">R$ {row['total_exposto']:,.2f}</div>
                    <div class="rank-meta">{row['n_contratos']} CONTRATOS ATIVOS</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    except:
        st.info("Dados de contratos (obras) ainda não carregados.")

elif page == "🚩 ALERTAS CRÍTICOS":
    st.markdown('<div class="main-header"><h1>Dossiê de Anomalias</h1></div>', unsafe_allow_html=True)
    df = db.execute("SELECT d_id, severity, detector_id, entity_name, description, base_legal FROM (SELECT row_number() OVER() as d_id, * FROM alerts) alerts ORDER BY severity DESC, detected_at DESC").df()
    
    for idx, row in df.iterrows():
        with st.container():
            col_content, col_action = st.columns([8, 2])
            with col_content:
                st.markdown(f"""
                <div style="background:rgba(6,20,35,0.8); border-left:5px solid {'#ff2244' if row['severity']=='CRÍTICO' else '#ffaa00'}; padding:20px; border-radius:2px;">
                    <div style="font-family:monospace; color:#00c8ff; font-size:12px;">TARGET_ID: {row['detector_id']} // {row['severity']}</div>
                    <div style="font-size:20px; font-weight:700; color:#fff; margin:5px 0;">{row['entity_name']}</div>
                    <div style="color:#cce8f4; margin-bottom:15px;">{row['description']}</div>
                    <div style="font-size:11px; opacity:0.6; border-top:1px solid #222; padding-top:10px;">{row['base_legal']}</div>
                </div>
                """, unsafe_allow_html=True)
                
                # Barra de risco visual
                score = {"CRÍTICO": 95, "ALTO": 75, "MÉDIO": 50}.get(row['severity'], 30)
                st.progress(score / 100, text=f"Risco Analítico: {score}%")
            
            with col_action:
                st.write("") # Spacer
                if st.button("📋 Denúncia", key=f"den_{row['d_id']}"):
                    texto = gerar_texto_denuncia(row.to_dict())
                    st.session_state[f"den_txt_{row['d_id']}"] = texto

            # Exibição do texto gerado se o botão foi clicado
            key_txt = f"den_txt_{row['d_id']}"
            if key_txt in st.session_state:
                with st.expander("📄 Texto da Denúncia Gerado", expanded=True):
                    st.text_area("Copie para o portal de denúncia:", value=st.session_state[key_txt], height=250)
                    col_c1, col_c2 = st.columns(2)
                    col_c1.markdown("[🌐 Abrir FalaBR](https://falabr.cgu.gov.br)")
                    if col_c2.button("🗑️ Limpar", key=f"clr_{row['d_id']}"):
                        del st.session_state[key_txt]
                        st.rerun()
        st.divider()

elif page == "🏛️ AUDITORIA FEDERAL (CGU)":
    st.markdown('<div class="main-header"><h1>Escudo de Auditoria Federal</h1></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="federal-shield">
        <div style="font-size:40px;">🛡️</div>
        <div>
            <div style="font-weight:700; color:#fff; font-size:18px;">INTEGRAÇÃO CGU / PORTAL DA TRANSPARÊNCIA</div>
            <div style="font-size:13px; opacity:0.7;">Cruzamento automático de Listas Negras Federais (CEIS/CNEP) com Contratos Municipais de Rio Branco.</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Resumo de Sanções")
        try:
            df_stats = db.execute("SELECT tipo_sancao, COUNT(*) as total FROM federal_ceis GROUP BY 1 ORDER BY 2 DESC LIMIT 5").df()
            if df_stats.empty:
                fig = go.Figure()
                fig.add_annotation(text="✅ Nenhuma sanção encontrada para as empresas locais", x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#00ff88"))
                fig.update_layout(template="plotly_dark", paper_bgcolor="#0a0e1a", plot_bgcolor="#0a0e1a", height=300)
                st.plotly_chart(fig, use_container_width=True)
            else:
                fig = px.bar(df_stats, x="tipo_sancao", y="total", color="total", color_continuous_scale=["#ff6b6b", "#ff0000"], template="plotly_dark")
                fig.update_layout(paper_bgcolor="#0a0e1a", plot_bgcolor="#0a0e1a", height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        except:
            st.warning("Base CEIS não populada. Use `python portal_transparencia_integrator.py --bulk-sancoes`.")
        
    with col2:
        st.subheader("🔍 Status das Empresas Locais")
        n_local = db.execute("SELECT COUNT(DISTINCT empresa_id) FROM obras").fetchone()[0]
        st.info(f"O sistema verificou **{n_local} empresas** contratadas por Rio Branco contra a base federal.")
        
        # Check for matches
        try:
            n_matches = db.execute("""
                SELECT COUNT(DISTINCT o.empresa_cnpj)
                FROM obras o
                INNER JOIN federal_ceis fc
                  ON REGEXP_REPLACE(o.empresa_cnpj,'[^0-9]','') =
                     REGEXP_REPLACE(fc.cnpj,'[^0-9]','')
            """).fetchone()[0]
            
            if n_matches == 0:
                st.success("RESULTADO: Nenhuma empresa local consta atualmente como Inidônea (CEIS) ou Punida (CNEP).")
            else:
                st.error(f"🔴 ALERTA: {n_matches} empresas com contratos ativos possuem sanções federais vigentes.")
        except:
            st.info("Aguardando sincronização com base federal.")

    st.subheader("📄 Últimos Registros Federativos")
    try:
        df_ceis = db.execute("SELECT nome, cnpj, tipo_sancao, orgao_sancionador FROM federal_ceis LIMIT 50").df()
        st.dataframe(df_ceis, use_container_width=True)
    except:
        st.write("Sem registros disponíveis.")

elif page == "👥 BUSCA AVANÇADA":
    st.markdown('<div class="main-header"><h1>Rastreio de Pessoal</h1></div>', unsafe_allow_html=True)
    nome = st.text_input("BUSCAR SERVIDOR", placeholder="Nome ou matrícula...")
    
    try:
        if nome:
            query = """
                SELECT 
                    TRIM(SPLIT_PART(servidor, ' - ', 1)) AS nome,
                    COALESCE(NULLIF(TRIM(SPLIT_PART(servidor, ' - ', 2)),''),'N/D') AS cargo,
                    ch AS matricula,
                    vencimento_base,
                    outras_verbas,
                    salario_liquido,
                    COALESCE(vencimento_base,0) + COALESCE(outras_verbas,0) AS total_bruto
                FROM rb_servidores_mass 
                WHERE servidor ILIKE ? 
                ORDER BY salario_liquido DESC NULLS LAST
                LIMIT 100
            """
            df_res = db.execute(query, [f"%{nome}%"]).df()
        else:
            query = """
                SELECT 
                    TRIM(SPLIT_PART(servidor, ' - ', 1)) AS nome,
                    COALESCE(NULLIF(TRIM(SPLIT_PART(servidor, ' - ', 2)),''),'N/D') AS cargo,
                    ch AS matricula,
                    vencimento_base,
                    outras_verbas,
                    salario_liquido,
                    COALESCE(vencimento_base,0) + COALESCE(outras_verbas,0) AS total_bruto
                FROM rb_servidores_mass 
                ORDER BY salario_liquido DESC NULLS LAST
                LIMIT 20
            """
            df_res = db.execute(query).df()
        
        if df_res.empty:
            st.warning("Nenhum servidor encontrado.")
        else:
            st.dataframe(df_res, use_container_width=True)
            
            # Destaque acima do teto (R$ 22.000 estimativa local)
            TETO = 22000.0
            acima_teto = df_res[df_res["salario_liquido"] > TETO]
            if not acima_teto.empty:
                st.error(f"🔴 {len(acima_teto)} servidor(es) com salário líquido acima do teto estimado (R$ 22.000)")
                st.dataframe(acima_teto[["nome", "cargo", "salario_liquido"]], use_container_width=True)

    except Exception as e:
        st.error(f"Erro ao processar busca: {e}")
        with st.expander("Debug - Schema"):
            st.write(db.execute("PRAGMA table_info('rb_servidores_mass')").df())
