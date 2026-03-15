from __future__ import annotations

import duckdb
import streamlit as st


def render_people_page(db: duckdb.DuckDBPyConnection) -> None:
    st.markdown('<div class="main-header"><h1>Rastreio de Pessoal</h1></div>', unsafe_allow_html=True)
    nome = st.text_input("BUSCAR SERVIDOR", placeholder="Nome ou matrícula...")

    try:
        if nome:
            query = """
                SELECT
                    TRIM(SPLIT_PART(servidor, '-', 2)) AS nome,
                    COALESCE(NULLIF(TRIM(cargo),''),'N/D') AS cargo,
                    ch AS matricula,
                    vencimento_base,
                    outras_verbas,
                    salario_liquido,
                    COALESCE(vencimento_base,0) + COALESCE(outras_verbas,0) AS total_bruto
                FROM rb_servidores_mass
                WHERE servidor ILIKE ? OR cargo ILIKE ?
                ORDER BY salario_liquido DESC NULLS LAST
                LIMIT 100
            """
            df_res = db.execute(query, [f"%{nome}%", f"%{nome}%"]).df()
        else:
            query = """
                SELECT
                    TRIM(SPLIT_PART(servidor, '-', 2)) AS nome,
                    COALESCE(NULLIF(TRIM(cargo),''),'N/D') AS cargo,
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
            st.dataframe(df_res, width='stretch')
            teto = 22000.0
            acima_teto = df_res[df_res["salario_liquido"] > teto]
            if not acima_teto.empty:
                st.error(f"🔴 {len(acima_teto)} servidor(es) com salário líquido acima do teto estimado (R$ 22.000)")
                st.dataframe(acima_teto[["nome", "cargo", "salario_liquido"]], width='stretch')
    except Exception as exc:
        st.error(f"Erro ao processar busca: {exc}")
        with st.expander("Debug - Schema"):
            st.write(db.execute("PRAGMA table_info('rb_servidores_mass')").df())
