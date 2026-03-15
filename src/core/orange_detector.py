
import pandas as pd
import numpy as np
from typing import Optional

def compute_orange_anomalies(df_receitas: pd.DataFrame, df_votos: pd.DataFrame) -> pd.DataFrame:
    """
    Lógica de detecção com classificação semântica vetorizada.
    """
    if df_receitas.empty:
        return pd.DataFrame()

    # 1. Agregação
    receitas_sum = df_receitas.groupby(['SQ_CANDIDATO', 'NM_CANDIDATO', 'DS_CARGO'])['VR_RECEITA'].sum().reset_index()
    receitas_sum.columns = ['SQ_CANDIDATO', 'NM_CANDIDATO', 'DS_CARGO', 'VR_RECEITA_TOTAL']
    votos_sum = df_votos.groupby('SQ_CANDIDATO')['QT_VOTOS'].sum().reset_index()

    # 2. Merge Left (Foco em quem declarou receita)
    merged = pd.merge(receitas_sum, votos_sum, on='SQ_CANDIDATO', how='left').fillna(0)
    
    # 3. Estatísticas por Cargo
    merged['cargo_count'] = merged.groupby('DS_CARGO')['SQ_CANDIDATO'].transform('count')
    merged['receita_pct'] = merged.groupby('DS_CARGO')['VR_RECEITA_TOTAL'].rank(pct=True)
    merged['votos_pct'] = merged.groupby('DS_CARGO')['QT_VOTOS'].rank(pct=True)
    merged['score_anomalia'] = (merged['receita_pct'] - merged['votos_pct']).round(3)
    
    # 4. Classificação Vetorizada (Performance + Semântica)
    merged['nivel_alerta'] = 'ANOMALIA_ESTATISTICA'
    merged.loc[merged['QT_VOTOS'] < 50, 'nivel_alerta'] = 'ALTO_RISCO'
    merged.loc[merged['QT_VOTOS'] == 0, 'nivel_alerta'] = 'VERIFICAR_REGISTRO'
    
    # 5. Filtros
    mask_prop = (merged['receita_pct'] >= 0.60) & (merged['votos_pct'] <= 0.20) & (merged['VR_RECEITA_TOTAL'] > 15000) & (merged['cargo_count'] >= 5)
    mask_abso = (merged['VR_RECEITA_TOTAL'] > 50000) & (merged['QT_VOTOS'] < 50)
    
    # União explícita: Proporcional primeiro, Absoluto depois (sem sobrescrever anomalias já detectadas)
    prop_df = merged[mask_prop].copy()
    abso_df = merged[mask_abso].copy()
    abso_only = abso_df[~abso_df['SQ_CANDIDATO'].isin(prop_df['SQ_CANDIDATO'])]
    
    anomalies = pd.concat([prop_df, abso_only])
    return anomalies.sort_values('score_anomalia', ascending=False)
