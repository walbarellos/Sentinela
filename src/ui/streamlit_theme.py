from __future__ import annotations

import streamlit as st


GLOBAL_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@300;400;600;700&display=swap');

    .stApp { background-color: #020408; color: #8eb8d4; }

    .main-header { text-align: center; margin-bottom: 40px; }
    .main-header h1 { font-family: 'Rajdhani', sans-serif; font-weight: 700; letter-spacing: 8px; color: #fff; text-transform: uppercase; }

    .kpi-container { display: flex; justify-content: space-around; text-align: center; margin-bottom: 50px; padding: 30px; background: linear-gradient(180deg, rgba(0,200,255,0.05) 0%, rgba(0,0,0,0) 100%); border-radius: 4px; border: 1px solid rgba(0,200,255,0.1); }
    .kpi-card { flex: 1; border-right: 1px solid rgba(0,200,255,0.1); }
    .kpi-card:last-child { border-right: none; }
    .kpi-label { font-family: 'Rajdhani', sans-serif; font-size: 11px; letter-spacing: 4px; color: #00c8ff; opacity: 0.7; text-transform: uppercase; }
    .kpi-value { font-family: 'Share Tech Mono', monospace; font-size: 48px; color: #fff; margin-top: 10px; }

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

    .rank-card {
        background: rgba(0, 200, 255, 0.02);
        border: 1px solid rgba(0, 200, 255, 0.08);
        border-left: 4px solid #00c8ff;
        padding: 20px 30px;
        margin-bottom: 15px;
        border-radius: 2px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1);
        position: relative;
        overflow: hidden;
    }
    .rank-card:hover {
        background: rgba(0, 200, 255, 0.06);
        border-color: rgba(0, 200, 255, 0.3);
        transform: scale(1.01) translateX(10px);
        box-shadow: -10px 0 30px rgba(0, 200, 255, 0.1);
    }
    .rank-card::after {
        content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 100%;
        background: linear-gradient(90deg, transparent, rgba(0,200,255,0.05), transparent);
        transform: translateX(-100%); transition: 0.6s;
    }
    .rank-card:hover::after { transform: translateX(100%); }

    .rank-number { font-family: 'Share Tech Mono', monospace; font-size: 14px; color: #00c8ff; letter-spacing: 4px; font-weight: 700; opacity: 0.5; margin-bottom: 5px; }
    .rank-name { font-family: 'Rajdhani', sans-serif; font-size: 20px; font-weight: 700; color: #fff; text-transform: uppercase; letter-spacing: 1px; }
    .rank-value { font-family: 'Share Tech Mono', monospace; font-size: 24px; color: #ffaa00; text-shadow: 0 0 15px rgba(255, 170, 0, 0.4); }
    .rank-meta { font-family: 'Rajdhani', sans-serif; font-size: 11px; color: #4a6a7a; letter-spacing: 2px; text-align: right; margin-top: 5px; }

    .progress-bg { width: 100%; height: 2px; background: rgba(255,255,255,0.05); margin-top: 10px; }
    .progress-fill { height: 100%; background: #00c8ff; box-shadow: 0 0 10px #00c8ff; }
</style>
"""


def apply_global_theme() -> None:
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
