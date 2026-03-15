
import pandas as pd
import numpy as np
import logging
from src.core.orange_detector import compute_orange_anomalies

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Sentinel.TestRobust")

def test_orange_logic():
    # Seed Fixo
    np.random.seed(42)
    
    # Gerando dados fictícios
    candidates = [
        {"SQ": 1, "NM": "Laranja Obvio", "CARGO": "VEREADOR", "VR": 150000, "VOTOS": 2},
        {"SQ": 2, "NM": "Candidato Real", "CARGO": "VEREADOR", "VR": 10000, "VOTOS": 1500},
        {"SQ": 100, "NM": "Senador Rico Zero Voto", "CARGO": "SENADOR", "VR": 500000, "VOTOS": 0}
    ]
    
    # Adiciona 30 vereadores para a curva proporcional
    for i in range(5, 35):
        candidates.append({
            "SQ": i, "NM": f"Cand {i}", "CARGO": "VEREADOR", 
            "VR": np.random.randint(10000, 40000), 
            "VOTOS": np.random.randint(500, 2000)
        })

    df_rec = pd.DataFrame([{"SQ_CANDIDATO": c["SQ"], "NM_CANDIDATO": c["NM"], "DS_CARGO": c["CARGO"], "VR_RECEITA": c["VR"]} for c in candidates])
    df_vot = pd.DataFrame([{"SQ_CANDIDATO": c["SQ"], "QT_VOTOS": c["VOTOS"]} for c in candidates])

    results = compute_orange_anomalies(df_rec, df_vot)
    
    log.info(f"Robust Results:\n{results[['NM_CANDIDATO', 'DS_CARGO', 'VR_RECEITA_TOTAL', 'QT_VOTOS', 'score_anomalia']]}")
    
    # Verificações
    assert "Laranja Obvio" in results['NM_CANDIDATO'].values
    assert "Senador Rico Zero Voto" in results['NM_CANDIDATO'].values
    assert results[results['NM_CANDIDATO'] == "Senador Rico Zero Voto"].iloc[0]['VR_RECEITA_TOTAL'] == 500000
    
    log.info("🛡️ Robust Orange Logic (including Senator case): PASS")

if __name__ == "__main__":
    test_orange_logic()
