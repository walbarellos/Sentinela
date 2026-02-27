"""
sentinela_patch.py — Diagnóstico + Patches para todos os 5 problemas identificados
===================================================================================

IMAGEM 1 — Network Graph: Labels sobrepostas, ilegível, sem interatividade real
IMAGEM 2 — Dashboard: "Auditadas Federal: 0", R$0,00 em todas entidades, grafo pequeno
IMAGEM 3 — Alertas: Sem botão de denúncia, sem score visual, sem agrupamento por alvo
IMAGEM 4 — Escudo Federal: Gráfico completamente preto (bug de tema Plotly/Altair)
IMAGEM 5 — Rastreio Pessoal: DuckDB BinderException — coluna "secretaria" inexistente

Aplique substituindo os blocos correspondentes em app.py.
"""

# ══════════════════════════════════════════════════════════════════════════════
# PATCH 1 — IMAGEM 5: FIX CRÍTICO — DuckDB BinderException "secretaria"
# ══════════════════════════════════════════════════════════════════════════════
# PROBLEMA: Query referencia coluna "secretaria" e "cargo" que não existem.
# Colunas reais de rb_servidores_mass:
#   outras_verbas | servidor | vencimento_base | salario_liquido | ch
# O campo "servidor" provavelmente contém "NOME - CARGO" concatenado.
# SOLUÇÃO: parse o campo "servidor" para extrair nome e cargo dinamicamente.

RASTREIO_PESSOAL_QUERY_CORRIGIDA = """
-- SUBSTITUIR no app.py seção "BUSCA AVANÇADA / RASTREIO DE PESSOAL"
-- Query antiga (linha ~146): SELECT split_part(servidor, '-', 2) as nome, cargo, secretaria, salario_liquido FROM rb_servidores_mass
-- Query nova (compatível com schema real):

SELECT
    -- Extrai nome: tudo antes do primeiro ' - '
    TRIM(SPLIT_PART(servidor, ' - ', 1))                   AS nome,
    -- Extrai cargo: tudo depois do primeiro ' - ' (pode ser vazio)
    COALESCE(NULLIF(TRIM(SPLIT_PART(servidor, ' - ', 2)), ''), 'N/D') AS cargo,
    -- Extrai CH/matrícula se existir no campo
    ch                                                      AS matricula,
    vencimento_base                                         AS remuneracao_base,
    salario_liquido,
    outras_verbas,
    -- Calcula total real
    COALESCE(vencimento_base, 0) + COALESCE(outras_verbas, 0) AS total_bruto
FROM rb_servidores_mass
WHERE
    -- Filtro de busca por nome (case-insensitive)
    LOWER(servidor) LIKE LOWER(CONCAT('%', ?, '%'))
ORDER BY salario_liquido DESC NULLS LAST
LIMIT 200
"""

# Implementação correta da seção Rastreio de Pessoal para app.py:
RASTREIO_PESSOAL_STREAMLIT = '''
# ── SEÇÃO: RASTREIO DE PESSOAL (substitui bloco com bug) ──────────────────

st.title("👤 RASTREIO DE PESSOAL")
busca = st.text_input("BUSCAR SERVIDOR", placeholder="Nome ou matrícula...")

if busca:
    try:
        query = """
            SELECT
                TRIM(SPLIT_PART(servidor, ' - ', 1))                        AS nome,
                COALESCE(NULLIF(TRIM(SPLIT_PART(servidor, ' - ', 2)),''),'N/D') AS cargo,
                ch                                                            AS matricula,
                vencimento_base,
                outras_verbas,
                salario_liquido,
                COALESCE(vencimento_base,0) + COALESCE(outras_verbas,0)      AS total_bruto
            FROM rb_servidores_mass
            WHERE LOWER(servidor) LIKE LOWER(?)
            ORDER BY salario_liquido DESC NULLS LAST
            LIMIT 200
        """
        df = db.execute(query, [f"%{busca}%"]).df()
        if df.empty:
            st.warning("Nenhum servidor encontrado.")
        else:
            st.success(f"{len(df)} servidor(es) encontrado(s)")
            st.dataframe(df, use_container_width=True)

            # Destaca salários acima do teto constitucional (~R$22.000 em RB)
            TETO = 22000.0
            acima_teto = df[df["salario_liquido"] > TETO]
            if not acima_teto.empty:
                st.error(f"🔴 {len(acima_teto)} servidor(es) acima do teto constitucional (CF art. 37, XI)")
                st.dataframe(acima_teto[["nome","cargo","salario_liquido","total_bruto"]])
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        # Debug: mostra colunas reais da tabela
        with st.expander("Debug — Colunas reais na tabela"):
            try:
                cols = db.execute("DESCRIBE rb_servidores_mass").df()
                st.dataframe(cols)
            except:
                st.write("Tabela rb_servidores_mass não encontrada")
else:
    # Resumo geral quando não há busca
    try:
        stats = db.execute("""
            SELECT
                COUNT(*)                        AS total_servidores,
                AVG(salario_liquido)            AS media_liquido,
                MAX(salario_liquido)            AS maior_salario,
                SUM(salario_liquido)            AS folha_total,
                COUNT(*) FILTER (WHERE salario_liquido > 22000) AS acima_teto
            FROM rb_servidores_mass
        """).df()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Servidores",  f"{int(stats['total_servidores'][0]):,}")
        col2.metric("Salário Médio",     f"R$ {stats['media_liquido'][0]:,.2f}")
        col3.metric("Maior Salário",     f"R$ {stats['maior_salario'][0]:,.2f}")
        col4.metric("Folha Total",       f"R$ {stats['folha_total'][0]:,.0f}")
        if stats['acima_teto'][0] > 0:
            st.error(f"⚠️ {int(stats['acima_teto'][0])} servidores com salário líquido acima de R$22.000")
    except Exception as e:
        st.info("Execute `python3 src/ingest/riobranco_servidores_mass.py` para carregar dados.")
'''


# ══════════════════════════════════════════════════════════════════════════════
# PATCH 2 — IMAGEM 4: Fix gráfico preto no Escudo de Auditoria Federal
# ══════════════════════════════════════════════════════════════════════════════
# PROBLEMA: Plotly/Altair renderiza com fundo preto porque não está configurado
# para tema dark, resultando em gráfico invisível (texto preto em fundo preto).
# SOLUÇÃO: Usar plotly com template "plotly_dark" explícito + fallback para
# st.bar_chart nativo do Streamlit que respeita o tema automaticamente.

ESCUDO_FEDERAL_CHART_FIX = '''
# ── SEÇÃO: ESCUDO DE AUDITORIA FEDERAL — Chart Fix ───────────────────────

import plotly.graph_objects as go
import plotly.express as px

# Substituir o bloco de chart que gera gráfico preto
def render_sancoes_chart(df_sancoes):
    """
    Renderiza gráfico de sanções com tema dark correto.
    df_sancoes deve ter: nome (str), tipo_sancao (str), data_inicio_sancao (str)
    """
    if df_sancoes is None or df_sancoes.empty:
        # PROBLEMA ORIGINAL: Plotly sem dados + sem template = retângulo preto
        # SOLUÇÃO: Mostrar estado vazio informativo
        fig = go.Figure()
        fig.add_annotation(
            text="✅ Nenhuma sanção encontrada para as empresas locais",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#00ff88"),
        )
        fig.update_layout(
            template="plotly_dark",          # ← LINHA CRÍTICA — estava faltando
            paper_bgcolor="#0a0e1a",
            plot_bgcolor="#0a0e1a",
            height=250,
        )
        st.plotly_chart(fig, use_container_width=True)
        return

    # Se há dados: bar chart por tipo de sanção
    contagem = df_sancoes.groupby("tipo_sancao").size().reset_index(name="qtd")
    fig = px.bar(
        contagem,
        x="tipo_sancao", y="qtd",
        color="qtd",
        color_continuous_scale=["#ff6b6b", "#ff0000"],
        title="Sanções por Tipo",
        template="plotly_dark",              # ← CORREÇÃO DO BUG
        labels={"tipo_sancao": "Tipo", "qtd": "Qtd"},
    )
    fig.update_layout(
        paper_bgcolor="#0a0e1a",
        plot_bgcolor="#0a0e1a",
        font_color="#e0e0e0",
        title_font_color="#ff6b6b",
        showlegend=False,
        height=300,
    )
    st.plotly_chart(fig, use_container_width=True)


# TAMBÉM corrigir o indicador "Auditadas (Federal): 0"
# Isso ocorre porque o token CGU não está configurado.
# Adicionar verificação de status:

def render_federal_status():
    token = os.environ.get("CGU_API_TOKEN", "")
    if not token:
        st.warning(
            "⚠️ Token CGU não configurado. "
            "Execute: `export CGU_API_TOKEN=seu-token`  "
            "Obtenha em: portaldatransparencia.gov.br/api-de-dados/cadastrar-email"
        )
        return 0

    # Verifica quantas empresas locais foram auditadas contra CEIS/CNEP
    try:
        auditadas = db.execute("""
            SELECT COUNT(DISTINCT o.empresa_cnpj)
            FROM obras o
            WHERE o.empresa_cnpj IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM federal_ceis fc
                  WHERE REGEXP_REPLACE(o.empresa_cnpj,'[^0-9]','') =
                        REGEXP_REPLACE(fc.cnpj,'[^0-9]','')
              )
        """).fetchone()[0]
        return auditadas
    except:
        # Tabela federal_ceis ainda não existe — dados não foram baixados
        return 0
'''


# ══════════════════════════════════════════════════════════════════════════════
# PATCH 3 — IMAGEM 1: Network Graph — Labels sobrepostas + melhorias
# ══════════════════════════════════════════════════════════════════════════════
# PROBLEMA: Edge labels ("INVESTIGADO", "VETTING: OK") em todas as arestas
# criam sobreposição total em grafos com +20 nós. Completamente ilegível.
# SOLUÇÃO: Labels nas arestas somente no hover; labels nos nós sempre visíveis
# mas colapsados; filtro por tipo de alerta; nós dimensionados por valor total.

NETWORK_GRAPH_IMPROVEMENTS = {
    "edge_labels": "Mover para tooltip (hover), NÃO renderizar por padrão",
    "node_size": "Dimensionar pelo valor_total dos contratos (escala log)",
    "colors": {
        "INVESTIGADO": "#ff3333",    # vermelho — investigado
        "VETTING_OK":  "#00d4aa",    # verde — limpo
        "SANCIONADO":  "#ff8800",    # laranja — sanção federal
        "DOADOR_TSE":  "#cc44ff",    # roxo — doação eleitoral
    },
    "filters": ["Todos", "Apenas Investigados", "Apenas Sancionados", "Alto Risco"],
    "fix_overlap": "Aumentar charge force de -300 para -600 para separar nós",
    "legend": "Adicionar legenda de cores no canto inferior direito",
}

# HTML/JS melhorado para o network graph — substitui sentinela_network_graph.html
NETWORK_GRAPH_HTML_PATCH = """
// PATCH para sentinela_network_graph.html
// Adicionar ANTES do simulation.force("charge"):

// ── Fix 1: Labels nas arestas SOMENTE no hover ──────────────────────────
// Remover: .text(d => d.label) de linkLabels
// Substituir por:
const linkLabelGroup = svg.append("g").attr("class", "link-labels");
// Remover renderização estática de labels nas arestas
// Mostrar apenas no mouseover do link:
link.on("mouseover", function(event, d) {
    tooltip.style("opacity", 1)
        .html(`<b>${d.label}</b><br>${d.tipo || ""}`)
        .style("left", (event.pageX + 10) + "px")
        .style("top", (event.pageY - 28) + "px");
}).on("mouseout", () => tooltip.style("opacity", 0));

// ── Fix 2: Aumentar repulsão entre nós para evitar sobreposição ─────────
// Substituir:
simulation.force("charge", d3.forceManyBody().strength(-300))
// Por:
simulation.force("charge", d3.forceManyBody().strength(-800).distanceMax(400))

// ── Fix 3: Tamanho do nó proporcional ao valor total ────────────────────
// Substituir nodeRadius fixo:
const nodeRadius = d => {
    const base = 8;
    if (!d.valor_total || d.valor_total === 0) return base;
    return Math.min(base + Math.log10(d.valor_total + 1) * 4, 35);
};

// ── Fix 4: Adicionar filtro por tipo de risco ────────────────────────────
// Antes do SVG, no HTML:
// <div id="filter-controls">
//   <button onclick="filterNodes('all')">Todos</button>
//   <button onclick="filterNodes('investigado')">Investigados</button>
//   <button onclick="filterNodes('sancionado')">Sancionados</button>
// </div>
function filterNodes(tipo) {
    node.style("opacity", d => {
        if (tipo === 'all') return 1;
        if (tipo === 'investigado') return d.flags?.includes('INVESTIGADO') ? 1 : 0.1;
        if (tipo === 'sancionado')  return d.flags?.includes('CEIS') ? 1 : 0.1;
        return 1;
    });
    link.style("opacity", d => {
        if (tipo === 'all') return 0.6;
        return (d.source.flags?.includes(tipo.toUpperCase()) || 
                d.target.flags?.includes(tipo.toUpperCase())) ? 0.8 : 0.05;
    });
}
"""


# ══════════════════════════════════════════════════════════════════════════════
# PATCH 4 — IMAGEM 3: Alertas Críticos — Botão de Denúncia + Score Visual
# ══════════════════════════════════════════════════════════════════════════════
# PROBLEMA: Alertas mostram informação mas não têm ação direta.
# Para fazer uma denúncia você precisa: copiar texto, abrir outro portal,
# redigir manualmente. Isso torna o sistema apenas "leitura" quando deveria
# ser "leitura → ação".
# SOLUÇÃO: Botão "📋 Gerar Denúncia" que monta documento completo com:
# - Fato (quem, quando, quanto, destino)
# - Base legal automatica (LEGAL_MAP)
# - Órgão competente pré-selecionado (TCE-AC, CGU, MPF)
# - Texto formatado para LAI ou protocolo de denúncia

ALERTAS_CRITICOS_DENUNCIA = '''
# ── SEÇÃO: ALERTAS CRÍTICOS — com botão de denúncia ─────────────────────

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

ORGAOS_DENUNCIA = {
    "TCE-AC":   "https://www.tce.ac.gov.br/denuncia",
    "CGU":      "https://falabr.cgu.gov.br",
    "MPF":      "http://www.mpf.mp.br/servicos/denuncias-e-reclamacoes",
    "MP-AC":    "https://www.mpac.mp.br/denuncia",
    "LAI":      "https://falabr.cgu.gov.br/web/publico/pedido-informacao",
}

def gerar_texto_denuncia(alerta: dict, tipo: str = "FDS") -> str:
    legal_ref, descricao_legal = LEGAL_MAP_ALERTAS.get(tipo, ("", ""))
    return f"""
DENÚNCIA — SISTEMA SENTINELA // CONTROLE SOCIAL

FATO DENUNCIADO:
  Tipo: {tipo} — {descricao_legal}
  
  Servidor: {alerta.get('servidor_nome', 'N/D')}
  Data: {alerta.get('data_viagem', alerta.get('data', 'N/D'))}
  Valor: R$ {alerta.get('valor_diaria', alerta.get('valor', 0)):,.2f}
  Origem: {alerta.get('origem', 'N/D')}
  Destino: {alerta.get('destino', 'N/D')}
  Secretaria: {alerta.get('secretaria', 'N/D')}

FUNDAMENTO LEGAL:
  {legal_ref}
  {descricao_legal}

PEDIDO:
  Solicita-se a apuração dos fatos, verificação da autorização prévia e
  da programação oficial que justifique a viagem na data indicada,
  bem como a instauração de tomada de contas especial se confirmada
  a irregularidade.

FONTE DOS DADOS:
  Portal de Transparência de Rio Branco (https://transparencia.riobranco.ac.gov.br)
  Dados públicos coletados via Lei de Acesso à Informação (Lei 12.527/2011)

Data de geração: {__import__('datetime').date.today().isoformat()}
Sistema: SENTINELA // Controle Social de Gastos Públicos (Código Aberto)
""".strip()


# No loop de renderização de alertas, adicionar:
def render_alerta_com_denuncia(alerta, tipo_tag, severidade):
    """Substitui o card simples de alerta por card com ações."""
    cor_border = {"ALTO": "#ff3333", "MÉDIO": "#ff8800", "BAIXO": "#ffcc00"}.get(severidade, "#888")
    
    with st.container():
        # Badge de severidade visual (não só texto)
        col_badge, col_content, col_action = st.columns([1, 8, 2])
        
        with col_badge:
            emoji = {"ALTO": "🔴", "MÉDIO": "🟡", "BAIXO": "🟢"}.get(severidade, "⚪")
            st.markdown(f"## {emoji}")
        
        with col_content:
            st.markdown(f"**TARGET_ID: {tipo_tag} // {severidade}**")
            st.markdown(f"### {alerta.get('servidor_nome', 'SERVIDOR')}")
            st.markdown(f"*{alerta.get('descricao', '')}*")
            
            # Barra de risco visual (0-100)
            score = {"ALTO": 85, "MÉDIO": 55, "BAIXO": 25}.get(severidade, 50)
            st.progress(score / 100, text=f"Risco: {score}%")
        
        with col_action:
            if st.button("📋 Denúncia", key=f"den_{alerta.get('id', id(alerta))}"):
                texto = gerar_texto_denuncia(alerta, tipo_tag)
                st.session_state[f"denuncia_texto_{tipo_tag}"] = texto
        
        # Mostrar texto de denúncia se botão foi clicado
        key = f"denuncia_texto_{tipo_tag}"
        if key in st.session_state:
            with st.expander("📄 Texto da Denúncia Gerado", expanded=True):
                st.text_area("Copie e cole no portal de denúncia:", 
                           value=st.session_state[key], height=300, key=f"ta_{key}")
                col_copy, col_portal = st.columns(2)
                with col_copy:
                    if st.button("🗑️ Limpar", key=f"clear_{key}"):
                        del st.session_state[key]
                        st.rerun()
                with col_portal:
                    st.markdown("[🌐 Abrir FalaBR (CGU)](https://falabr.cgu.gov.br)")
        
        st.divider()
'''


# ══════════════════════════════════════════════════════════════════════════════
# PATCH 5 — IMAGEM 2: Dashboard principal — Fix "Auditadas Federal: 0" + valores
# ══════════════════════════════════════════════════════════════════════════════
# PROBLEMA: "AUDITADAS (FEDERAL): 0" porque a tabela federal_ceis está vazia
# (token não configurado ou dados não baixados ainda).
# "R$0,00" nos cards de empresa porque valor_total não está sendo somado.
# SOLUÇÃO: Mostrar estado real + fallback informativo + fix da query de valores.

DASHBOARD_KPI_FIX = '''
# ── KPIs do Dashboard — substitui o bloco de métricas com zeros ──────────

def render_kpis(db):
    col1, col2, col3, col4 = st.columns(4)
    
    # KPI 1: Entidades locais
    try:
        n_entidades = db.execute("""
            SELECT COUNT(DISTINCT empresa_cnpj) FROM obras
            WHERE empresa_cnpj IS NOT NULL
        """).fetchone()[0]
    except:
        n_entidades = 0
    
    # KPI 2: Auditadas federal (com fallback se tabela não existe)
    try:
        n_auditadas = db.execute("""
            SELECT COUNT(DISTINCT o.empresa_cnpj)
            FROM obras o
            INNER JOIN federal_ceis fc
              ON REGEXP_REPLACE(o.empresa_cnpj,'[^0-9]','') =
                 REGEXP_REPLACE(fc.cnpj,'[^0-9]','')
        """).fetchone()[0]
        federal_ok = True
    except Exception:
        # Tabela federal_ceis não existe — dados não foram baixados
        n_auditadas = 0
        federal_ok = False
    
    # KPI 3: Alertas ativos (conta todos os tipos)
    try:
        n_alertas = db.execute("""
            SELECT
                (SELECT COUNT(*) FROM v_diarias_fds) +
                (SELECT COUNT(*) FROM v_diarias_bloco) +
                (SELECT COUNT(*) FROM v_salarios_outlier)
            AS total
        """).fetchone()[0]
    except:
        n_alertas = 0
    
    # KPI 4: Exposição total (soma dos contratos com flag)
    try:
        exposicao = db.execute("""
            SELECT COALESCE(SUM(valor_total), 0)
            FROM obras
            WHERE valor_total > 0
        """).fetchone()[0]
    except:
        exposicao = 0
    
    col1.metric("ENTIDADES LOCAIS",    f"{n_entidades:,}")
    col2.metric(
        "AUDITADAS (FEDERAL)",
        str(n_auditadas) if federal_ok else "⚠️ N/A",
        help="Execute --bulk-sancoes para popular dados federais" if not federal_ok else None
    )
    col3.metric("ALERTAS ATIVOS",      f"{n_alertas:,}")
    col4.metric("EXPOSIÇÃO (R$)",       f"{exposicao/1_000_000:.1f}M")
    
    if not federal_ok:
        st.warning(
            "⚠️ Base federal CEIS/CNEP não carregada. "
            "Execute: `python portal_transparencia_integrator.py --bulk-sancoes`"
        )

# ── FIX: Query para valores das empresas no painel esquerdo ─────────────
# PROBLEMA: R$0,00 em todas as entidades
# A query não estava somando valor_total corretamente
QUERY_EMPRESAS_RANKING = """
SELECT
    empresa_nome,
    empresa_cnpj,
    COUNT(*)        AS n_contratos,
    SUM(COALESCE(valor_total, 0)) AS valor_total_agregado,
    MAX(data_contrato) AS ultimo_contrato
FROM obras
WHERE empresa_cnpj IS NOT NULL
  AND empresa_nome IS NOT NULL
GROUP BY empresa_nome, empresa_cnpj
ORDER BY valor_total_agregado DESC
LIMIT 50
"""
# Substitua a query de listagem de empresas por esta versão
# que usa SUM(COALESCE(valor_total, 0)) em vez de valor_total direto
'''


# ══════════════════════════════════════════════════════════════════════════════
# RESUMO: O QUE APLICAR E ONDE
# ══════════════════════════════════════════════════════════════════════════════
APLICACAO = """
╔══════════════════════════════════════════════════════════════════════════════╗
║  SENTINELA — GUIA DE APLICAÇÃO DOS PATCHES                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  PATCH 1 — app.py ~linha 146 (BinderException "secretaria")                ║
║  Substituir: SELECT split_part(servidor,'-',2) as nome, cargo, secretaria  ║
║  Por: RASTREIO_PESSOAL_STREAMLIT (colado acima)                             ║
║  Impacto: FIX CRÍTICO — seção inteira estava quebrada                       ║
║                                                                              ║
║  PATCH 2 — app.py na seção ESCUDO FEDERAL                                   ║
║  Substituir: bloco de chart/figure sem template                             ║
║  Por: render_sancoes_chart() com template="plotly_dark"                     ║
║  Impacto: Gráfico invisível → visível                                       ║
║                                                                              ║
║  PATCH 3 — sentinela_network_graph.html                                     ║
║  Aplicar: NETWORK_GRAPH_HTML_PATCH                                          ║
║  charge: -300 → -800; edge labels: static → hover only                     ║
║  Impacto: Legibilidade radicalmente melhor                                  ║
║                                                                              ║
║  PATCH 4 — app.py na seção ALERTAS CRÍTICOS                                 ║
║  Substituir: card simples por render_alerta_com_denuncia()                  ║
║  Adicionar: gerar_texto_denuncia() e LEGAL_MAP_ALERTAS                      ║
║  Impacto: Sistema passa de "leitura" para "leitura → ação"                  ║
║                                                                              ║
║  PATCH 5 — app.py na seção KPIs do Dashboard                               ║
║  Substituir: bloco de métricas por render_kpis()                            ║
║  Adicionar: QUERY_EMPRESAS_RANKING com SUM(COALESCE(valor_total,0))         ║
║  Impacto: "Auditadas: 0" e "R$0,00" → valores reais ou status correto      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

if __name__ == "__main__":
    print(APLICACAO)
