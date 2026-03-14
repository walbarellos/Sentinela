from __future__ import annotations

from datetime import date

import duckdb
import streamlit as st


LEGAL_MAP_ALERTAS = {
    "FDS": ("Acórdão 2.484/2021-TCU-Plenário", "Diárias pagas em sábados/domingos sem programação oficial"),
    "BLOCO": ("Lei 8.112/90 art. 58 / Decreto 5.992/2006", "Concessão irregular de diárias em grupo sem justificativa"),
    "OUTLIER": ("CF art. 37 XI / Decreto Municipal", "Remuneração acima do teto constitucional"),
    "CEIS": ("Lei 8.666/93 art. 87 / Lei 14.133/21", "Contrato com empresa inidônea ou suspensa"),
    "FRACION": ("Lei 14.133/21 art. 29 §2°", "Fracionamento de despesa para dispensar licitação"),
    "NEPOTISMO": ("Súmula Vinculante nº 13 STF", "Indícios de nepotismo em contratação"),
}


def gerar_texto_relato(alerta: dict) -> str:
    tipo = alerta.get("detector_id", "GERAL")
    legal_ref, descricao_legal = LEGAL_MAP_ALERTAS.get(tipo, ("", "Irregularidade identificada no sistema Sentinela"))
    return f"""
RELATO PARA APURACAO PRELIMINAR — SISTEMA SENTINELA // CONTROLE SOCIAL

FATO OBJETIVO OBSERVADO:
  Entidade: {alerta.get('entity_name', 'N/D')}
  Tipo: {tipo} — {alerta.get('description', 'N/D')}

  Severidade: {alerta.get('severity', 'N/D')}
  Base Legal: {alerta.get('base_legal', legal_ref)}

FUNDAMENTO LEGAL SUGERIDO:
  {legal_ref}
  {descricao_legal}

PEDIDO:
  Solicita-se a apuracao dos fatos, a verificacao documental e, se for o caso,
  a instauracao dos procedimentos administrativos/fiscais cabiveis para avaliar a regularidade da situacao.

LIMITE DA CONCLUSAO:
  Este texto nao imputa culpa, fraude, crime ou dolo. Ele organiza um fato objetivo
  para noticia de fato, auditoria ou apuracao pelos orgaos competentes.

FONTE DOS DADOS:
  Portal de Transparência de Rio Branco (https://transparencia.riobranco.ac.gov.br)
  Dados públicos coletados e analisados pelo sistema SENTINELA.

Data de geração: {date.today().isoformat()}
Sistema: SENTINELA // Inteligência em Controle Social
""".strip()


def render_alerts_page(db: duckdb.DuckDBPyConnection) -> None:
    st.markdown('<div class="main-header"><h1>Dossiê de Anomalias</h1></div>', unsafe_allow_html=True)
    df = db.execute(
        """
        SELECT d_id, severity, detector_id, entity_name, description, base_legal
        FROM (SELECT row_number() OVER() AS d_id, * FROM alerts) alerts
        ORDER BY severity DESC, detected_at DESC
        """
    ).df()

    for _, row in df.iterrows():
        with st.container():
            col_content, col_action = st.columns([8, 2])
            with col_content:
                st.markdown(
                    f"""
                    <div style="background:rgba(6,20,35,0.8); border-left:5px solid {'#ff2244' if row['severity']=='CRÍTICO' else '#ffaa00'}; padding:20px; border-radius:2px;">
                        <div style="font-family:monospace; color:#00c8ff; font-size:12px;">TARGET_ID: {row['detector_id']} // {row['severity']}</div>
                        <div style="font-size:20px; font-weight:700; color:#fff; margin:5px 0;">{row['entity_name']}</div>
                        <div style="color:#cce8f4; margin-bottom:15px;">{row['description']}</div>
                        <div style="font-size:11px; opacity:0.6; border-top:1px solid #222; padding-top:10px;">{row['base_legal']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                score = {"CRÍTICO": 95, "ALTO": 75, "MÉDIO": 50}.get(row["severity"], 30)
                st.progress(score / 100, text=f"Risco Analítico: {score}%")
            with col_action:
                st.write("")
                if st.button("📋 Relato", key=f"den_{row['d_id']}"):
                    st.session_state[f"den_txt_{row['d_id']}"] = gerar_texto_relato(row.to_dict())

            key_txt = f"den_txt_{row['d_id']}"
            if key_txt in st.session_state:
                with st.expander("📄 Texto do Relato Gerado", expanded=True):
                    st.text_area("Copie para noticia de fato, auditoria ou protocolo oficial:", value=st.session_state[key_txt], height=280)
                    col_c1, col_c2 = st.columns(2)
                    col_c1.markdown("[🌐 Abrir FalaBR](https://falabr.cgu.gov.br)")
                    if col_c2.button("🗑️ Limpar", key=f"clr_{row['d_id']}"):
                        del st.session_state[key_txt]
                        st.rerun()
        st.divider()
