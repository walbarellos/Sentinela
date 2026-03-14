from __future__ import annotations

from pathlib import Path

import duckdb
import streamlit as st


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
PAGES = ["🏠 CENTRO DE COMANDO", "📂 OPERAÇÕES", "🧪 ALERTAS LEGADOS (QUARENTENA)", "🏛️ AUDITORIA FEDERAL (CGU)", "👥 BUSCA AVANÇADA"]


@st.cache_data(ttl=30, show_spinner=False)
def load_operational_status() -> dict[str, object]:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        status = {
            "registry": "indisponivel",
            "sources": "indisponivel",
            "healthy_sources": 0,
            "total_sources": 0,
        }
        if "ops_case_registry" in tables:
            total_cases = con.execute("SELECT COUNT(*) FROM ops_case_registry").fetchone()[0]
            status["registry"] = "ok" if total_cases > 0 else "vazio"
            status["total_cases"] = int(total_cases or 0)
        if "ops_source_cache" in tables:
            row = con.execute(
                """
                SELECT
                    COUNT(*) AS total_sources,
                    COUNT(*) FILTER (WHERE status_code < 400) AS healthy_sources
                FROM v_ops_source_cache_latest
                """
            ).fetchone()
            total_sources = int(row[0] or 0)
            healthy_sources = int(row[1] or 0)
            status["sources"] = "ok" if total_sources > 0 and healthy_sources == total_sources else "parcial"
            status["total_sources"] = total_sources
            status["healthy_sources"] = healthy_sources
        if "ops_pipeline_run" in tables:
            latest = con.execute(
                """
                SELECT pipeline, status, started_at
                FROM v_ops_pipeline_run_latest
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
            if latest:
                status["latest_pipeline"] = latest[0]
                status["latest_status"] = latest[1]
                status["latest_started_at"] = latest[2]
        return status
    finally:
        con.close()


def render_sidebar() -> str:
    status = load_operational_status()
    with st.sidebar:
        st.title("🛡️ SENTINELA")
        st.caption("INTELLIGENCE UNIT // V5.4")
        st.divider()
        page = st.radio("NAVEGAÇÃO INTERNA", PAGES)
        st.divider()
        st.markdown("### STATUS OPERACIONAL")

        if status["registry"] == "ok":
            st.success(f"Registry operacional: {status.get('total_cases', 0)} casos")
        elif status["registry"] == "vazio":
            st.warning("Registry operacional vazio")
        else:
            st.info("Registry operacional indisponível")

        if status["sources"] == "ok":
            st.success(f"Fontes monitoradas: {status['healthy_sources']}/{status['total_sources']}")
        elif status["sources"] == "parcial":
            st.warning(f"Fontes monitoradas: {status['healthy_sources']}/{status['total_sources']}")
        else:
            st.info("Fontes monitoradas indisponíveis")

        latest_pipeline = status.get("latest_pipeline")
        latest_status = status.get("latest_status")
        if latest_pipeline and latest_status:
            st.caption(f"Última execução: {latest_pipeline} [{latest_status}]")

    return page
