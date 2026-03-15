
import pandas as pd
import duckdb
from pathlib import Path

TSE_DATA_DIR = Path('data/tse')
DB_PATH = 'data/sentinela_analytics.duckdb'

def mask_cpf(val):
    """Oculta CPFs para segurança jurídica, mantém CNPJs."""
    s = str(val).replace('.','').replace('-','')
    if len(s) == 11:
        return f"{s[:3]}.***.***-{s[-2:]}"
    return val

def hunt_operators():
    anomalous_sqs = []
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        # Foco em ALTO_RISCO e ANOMALIA (votos > 0)
        df = con.execute("SELECT SQ_CANDIDATO FROM ops_tse_orange_history WHERE nivel_alerta <> 'VERIFICAR_REGISTRO'").fetchdf()
        if not df.empty: anomalous_sqs = df['SQ_CANDIDATO'].tolist()
    finally:
        con.close()

    if not anomalous_sqs: return

    all_suppliers = []
    for year in [2018, 2020, 2022]:
        p = TSE_DATA_DIR / f"despesas_contratadas_candidatos_{year}_AC.csv"
        if not p.exists(): continue
        df = pd.read_csv(p, encoding='latin-1', sep=';', on_bad_lines='skip', low_memory=False)
        df.columns = [c.upper() for c in df.columns]
        df['SQ_CANDIDATO'] = pd.to_numeric(df['SQ_CANDIDATO'], errors='coerce')
        df_anom = df[df['SQ_CANDIDATO'].isin(anomalous_sqs)].copy()
        if not df_anom.empty: all_suppliers.append(df_anom)

    if not all_suppliers: return
    full_df = pd.concat(all_suppliers)
    
    # Normalização de Moeda e CPF
    full_df['VR_DESPESA_CONTRATADA'] = full_df['VR_DESPESA_CONTRATADA'].astype(str).str.replace(',', '.').astype(float, errors='ignore')
    full_df['NR_CPF_CNPJ_FORNECEDOR'] = full_df['NR_CPF_CNPJ_FORNECEDOR'].apply(mask_cpf)

    # 1. Clusters (Rede)
    freq = full_df.groupby(['NM_FORNECEDOR', 'NR_CPF_CNPJ_FORNECEDOR']).size().reset_index(name='n_candidatos')
    print("\n--- CLUSTERS DE FORNECEDORES (RISCO CONFIRMADO) ---")
    print(freq[freq['n_candidatos'] > 1].sort_values('n_candidatos', ascending=False).to_string(index=False))
    
    # 2. Prestadores Técnicos
    acc = full_df[full_df['DS_DESPESA'].str.contains('CONTAB|ASSESSORIA|CONSULTORIA', na=False, case=False)].copy()
    if not acc.empty:
        print("\n--- PRESTADORES TÉCNICOS ---")
        summary = acc.groupby(['NM_FORNECEDOR', 'DS_DESPESA'])['VR_DESPESA_CONTRATADA'].sum().reset_index()
        print(summary.sort_values('VR_DESPESA_CONTRATADA', ascending=False).head(20).to_string(index=False))

if __name__ == "__main__":
    hunt_operators()
