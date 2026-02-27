import duckdb
import pandas as pd
import zipfile
import os
import glob
from pathlib import Path

DB_PATH = "./data/sentinela_analytics.duckdb"
DATA_DIR = "./data/federal"

def load_dataset(con, zip_path, table_name):
    print(f"Lendo {zip_path}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_file = z.namelist()[0]
            # DuckDB pode ler CSV diretamente de ZIP, mas vamos via pandas para garantir encoding (Latin-1)
            with z.open(csv_file) as f:
                df = pd.read_csv(f, sep=';', encoding='latin-1', dtype=str)
                # Normaliza colunas
                df.columns = [c.lower().replace(' ', '_').replace('/', '_') for c in df.columns]
                
                # Se for CEIS/CNEP, mapear campos críticos para compatibilidade com app.py
                if 'cpf_ou_cnpj_do_sancionado' in df.columns:
                    df = df.rename(columns={'cpf_ou_cnpj_do_sancionado': 'cnpj', 'nome_ou_razão_social_do_sancionado': 'nome'})
                
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                print(f"✓ {table_name}: {len(df)} registros carregados.")
    except Exception as e:
        print(f"Erro ao carregar {zip_path}: {e}")

def main():
    con = duckdb.connect(DB_PATH)
    
    mapping = {
        "ceis": "federal_ceis",
        "cnep": "federal_cnep",
        "ceaf": "federal_ceaf",
        "cepim": "federal_cepim"
    }
    
    for prefix, table in mapping.items():
        files = glob.glob(f"{DATA_DIR}/{prefix}_*.zip")
        if files:
            load_dataset(con, files[0], table)
        else:
            print(f"Arquivo para {prefix} não encontrado.")
            
    con.close()

if __name__ == "__main__":
    main()
