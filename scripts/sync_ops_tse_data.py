
import logging
import duckdb
import pandas as pd
from src.ingest.tse_connector import TseConnector
from src.core.normalizer import normalize_cpf, normalize_name

# Configura log para vermos o progresso real
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("Sentinel.TSE")

DB_PATH = 'data/sentinela_analytics.duckdb'

def sync_tse_to_duckdb():
    # 1. Baixar/Processar dados do TSE via Conector existente
    # Vamos focar em 2022 (tem CPF aberto) e 2024 (atual)
    connector = TseConnector(data_dir="./data/tse")
    pessoas_dict = connector.run(anos=[2022, 2024])
    
    if not pessoas_dict:
        log.error("Nenhum dado retornado do TSE.")
        return

    # 2. Converter para DataFrame plano para o DuckDB
    rows = []
    for cpf, p in pessoas_dict.items():
        # Ignorar sequenciais da LGPD se não tivermos o nome
        if cpf.startswith("seq:") and not p.nome_canonico:
            continue
            
        for cand in p.candidaturas:
            rows.append({
                "cpf": cpf,
                "nome": p.nome_canonico,
                "ano": cand.ano,
                "cargo": cand.cargo,
                "partido": cand.partido,
                "situacao": cand.situacao,
                "uf": cand.uf
            })
    
    df_tse = pd.DataFrame(rows)
    log.info(f"Normalizados {len(df_tse)} registros de candidaturas.")

    # 3. Persistir no DuckDB
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("DROP TABLE IF EXISTS ops_tse_candidatos")
        con.register("df_tse_temp", df_tse)
        con.execute("CREATE TABLE ops_tse_candidatos AS SELECT * FROM df_tse_temp")
        log.info("Tabela ops_tse_candidatos criada com sucesso.")
    finally:
        con.close()

if __name__ == "__main__":
    sync_tse_to_duckdb()
