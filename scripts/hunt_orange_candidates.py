
import os
import requests
import pandas as pd
import zipfile
import io
import duckdb
import hashlib
import logging
from pathlib import Path
from src.core.orange_detector import compute_orange_anomalies

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("Sentinel.ProjectA")

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele"
DB_PATH = Path('data/sentinela_analytics.duckdb')
TSE_DATA_DIR = Path('data/tse')

TSE_CATALOG = {
    "votos": {
        "pattern": "votacao_candidato_munzona/votacao_candidato_munzona_{year}.zip",
        "csv": "votacao_candidato_munzona_{year}_AC.csv"
    },
    "receitas": {
        "pattern": "prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{year}.zip",
        "csv": "receitas_candidatos_{year}_AC.csv",
        "alt_pattern": "receitas_candidatos/receitas_candidatos_{year}.zip"
    }
}

def get_file_hash(path: Path) -> str:
    sha256 = hashlib.sha256()
    sha256.update(path.read_bytes())
    return sha256.hexdigest()

def download_tse_with_cache(year, category):
    spec = TSE_CATALOG[category]
    csv_filename = spec['csv'].format(year=year)
    local_csv = TSE_DATA_DIR / csv_filename
    
    if local_csv.exists():
        log.info(f"Usando cache local para {category} {year}")
        df = pd.read_csv(local_csv, encoding='latin-1', sep=';', on_bad_lines='skip', low_memory=False)
        return df, get_file_hash(local_csv)
    
    urls = []
    if spec.get('pattern'): urls.append(f"{BASE_URL}/{spec['pattern'].format(year=year)}")
    if spec.get('alt_pattern'): urls.append(f"{BASE_URL}/{spec['alt_pattern'].format(year=year)}")
    
    for url in urls:
        try:
            log.info(f"Downloading {category} {year} via stream...")
            with requests.get(url, timeout=300, stream=True) as r:
                if r.status_code != 200: continue
                buf = io.BytesIO()
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk: buf.write(chunk)
                buf.seek(0)
                with zipfile.ZipFile(buf) as zf:
                    target = next((n for n in zf.namelist() if csv_filename.lower() in n.lower()), None)
                    if not target: target = next((n for n in zf.namelist() if "_AC.csv" in n.upper()), None)
                    if target:
                        raw_bytes = zf.read(target)
                        TSE_DATA_DIR.mkdir(parents=True, exist_ok=True)
                        local_csv.write_bytes(raw_bytes)
                        file_hash = hashlib.sha256(raw_bytes).hexdigest()
                        log.info(f"Cache salvo: {local_csv}")
                        return pd.read_csv(local_csv, encoding='latin-1', sep=';', on_bad_lines='skip', low_memory=False), file_hash
        except Exception as e:
            log.warning(f"Erro no download {url}: {e}")
    return None, None

def process_and_store_year(year, con):
    log.info(f"--- Ciclo Auditoria {year} ---")
    df_votos, hash_v = download_tse_with_cache(year, "votos")
    df_receitas, hash_r = download_tse_with_cache(year, "receitas")
    
    if df_votos is None or df_receitas is None:
        log.error(f"Pulo operacional para {year}")
        return

    df_votos.columns = [c.upper() for c in df_votos.columns]
    df_receitas.columns = [c.upper() for c in df_receitas.columns]

    # Guards
    if 'VR_RECEITA' not in df_receitas.columns or ('QT_VOTOS_NOMINAIS' not in df_votos.columns and 'QT_VOTOS' not in df_votos.columns):
        log.error(f"Schema incompatível para {year}. Colunas: {list(df_receitas.columns)}")
        return

    voto_col = 'QT_VOTOS_NOMINAIS' if 'QT_VOTOS_NOMINAIS' in df_votos.columns else 'QT_VOTOS'
    df_receitas['VR_RECEITA'] = df_receitas['VR_RECEITA'].astype(str).str.replace(',', '.').astype(float)
    df_votos['QT_VOTOS'] = df_votos[voto_col].fillna(0).astype(float)

    anomalies = compute_orange_anomalies(df_receitas, df_votos)
    if not anomalies.empty:
        anomalies['ANO'] = year
        anomalies['SOURCE_HASH_RECEITAS'] = hash_r
        anomalies['SOURCE_HASH_VOTOS'] = hash_v
        con.register("df_temp", anomalies)
        con.execute("""
            INSERT INTO ops_tse_orange_history (
                SQ_CANDIDATO, NM_CANDIDATO, DS_CARGO, VR_RECEITA_TOTAL, QT_VOTOS, 
                cargo_count, receita_pct, votos_pct, score_anomalia, 
                nivel_alerta, ANO, SOURCE_HASH_RECEITAS, SOURCE_HASH_VOTOS
            ) 
            SELECT 
                SQ_CANDIDATO, NM_CANDIDATO, DS_CARGO, VR_RECEITA_TOTAL, QT_VOTOS, 
                cargo_count, receita_pct, votos_pct, score_anomalia, 
                nivel_alerta, ANO, SOURCE_HASH_RECEITAS, SOURCE_HASH_VOTOS 
            FROM df_temp
        """)
        log.info(f"Gravadas {len(anomalies)} anomalias para {year}.")

def main():
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ops_tse_orange_history (
                SQ_CANDIDATO DOUBLE, NM_CANDIDATO VARCHAR, DS_CARGO VARCHAR, 
                VR_RECEITA_TOTAL DOUBLE, QT_VOTOS DOUBLE, cargo_count INTEGER,
                receita_pct DOUBLE, votos_pct DOUBLE, score_anomalia DOUBLE,
                nivel_alerta VARCHAR, ANO INTEGER, 
                SOURCE_HASH_RECEITAS VARCHAR, SOURCE_HASH_VOTOS VARCHAR
            )
        """)
        processed_years = con.execute("SELECT DISTINCT ANO FROM ops_tse_orange_history").fetchdf()['ANO'].tolist()
        for y in [2018, 2020, 2022]:
            if y not in processed_years: process_and_store_year(y, con)
    finally:
        con.close()

if __name__ == "__main__":
    main()
