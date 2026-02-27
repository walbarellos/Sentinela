import duckdb
import pandas as pd
import zipfile
import glob
import os
from pathlib import Path

DB_PATH = "./data/sentinela_analytics.duckdb"
DATA_DIR = "./data/federal"

def load_local_dataset(con, zip_path, table_name):
    print(f"Processando {zip_path}...")
    try:
        # Verifica se o arquivo é realmente um ZIP (o head mostrou HTML antes, vamos validar)
        if not zipfile.is_zipfile(zip_path):
            print(f"! Erro: {zip_path} não é um arquivo ZIP válido (pode ser um erro de download).")
            return False
            
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_file = [name for name in z.namelist() if name.endswith('.csv')][0]
            with z.open(csv_file) as f:
                # O portal da transparência usa Latin-1 e separador ;
                df = pd.read_csv(f, sep=';', encoding='latin-1', dtype=str)
                # Normaliza colunas para o que o app.py espera
                # app.py espera: nome, cnpj, tipo_sancao, orgao_sancionador
                
                mapping = {
                    'CPF ou CNPJ do Sancionado': 'cnpj',
                    'Nome ou Razão Social do Sancionado': 'nome',
                    'Tipo de Sanção': 'tipo_sancao',
                    'Órgão Sancionador': 'orgao_sancionador',
                    'Data Início Sanção': 'data_inicio_sancao',
                    'Data Fim Sanção': 'data_fim_sancao'
                }
                
                # Renomeia colunas se existirem
                df = df.rename(columns=mapping)
                
                # Seleciona apenas as colunas necessárias ou todas
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                print(f"✓ {table_name}: {len(df)} registros carregados.")
                return True
    except Exception as e:
        print(f"Erro ao processar {zip_path}: {e}")
        return False

def main():
    con = duckdb.connect(DB_PATH)
    
    # Mapeamento de arquivos para tabelas
    # O arquivo baixado tem nome como ceis_csv_202602.zip
    found = False
    
    for prefix, table in [("ceis", "federal_ceis"), ("cnep", "federal_cnep")]:
        files = glob.glob(f"{DATA_DIR}/{prefix}_*.zip")
        if files:
            if load_local_dataset(con, files[0], table):
                found = True
    
    if not found:
        print("
[!] Nenhum dado federal foi carregado.")
        print("Causa provável: Os arquivos em data/federal/ são inválidos (HTML em vez de ZIP).")
        print("Ação sugerida: Você precisará de um token da CGU para baixar via API ou baixar manualmente os CSVs do Portal da Transparência.")
    
    con.close()

if __name__ == "__main__":
    main()
