import streamlit as st
import duckdb
from pathlib import Path
from src.ui.streamlit_alerts import render_alerts_page
from src.ui.streamlit_federal import render_federal_page
from src.ui.streamlit_home import render_home_page
from src.ui.streamlit_ops import render_ops_page
from src.ui.streamlit_people import render_people_page
from src.ui.streamlit_sidebar import render_sidebar
from src.ui.streamlit_theme import apply_global_theme

st.set_page_config(page_title="SENTINELA // COMMAND CENTER", layout="wide", initial_sidebar_state="expanded")

# --- DATABASE ---
ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

def get_db():
    return duckdb.connect(str(DB_PATH), read_only=True)

apply_global_theme()
page = render_sidebar()

# --- PAGES ---

if page == "🏠 CENTRO DE COMANDO":
    db = get_db()
    try:
        render_home_page(db)
    finally:
        db.close()

elif page == "📂 OPERAÇÕES":
    render_ops_page()

elif page == "🧪 ALERTAS LEGADOS (QUARENTENA)":
    db = get_db()
    try:
        render_alerts_page(db)
    finally:
        db.close()

elif page == "🏛️ AUDITORIA FEDERAL (CGU)":
    db = get_db()
    try:
        render_federal_page(db)
    finally:
        db.close()

elif page == "👥 BUSCA AVANÇADA":
    db = get_db()
    try:
        render_people_page(db)
    finally:
        db.close()
