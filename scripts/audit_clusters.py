
import duckdb
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("Sentinel.ClusterAudit")

DB_PATH = 'data/sentinela_analytics.duckdb'

def run_cluster_audit():
    con = duckdb.connect(DB_PATH)
    try:
        log.info("Iniciando detecção de Clusters de Controle (Redes de Sócios)...")
        
        # Identifica nomes que aparecem como sócios em múltiplas empresas
        query = """
            SELECT 
                socio_nome,
                COUNT(DISTINCT cnpj) as qtd_empresas,
                list(DISTINCT cnpj) as lista_cnpjs
            FROM empresa_socios
            GROUP BY 1
            HAVING qtd_empresas > 1
            ORDER BY qtd_empresas DESC
        """
        clusters = con.execute(query).fetchdf()
        
        if clusters.empty:
            log.info("Nenhum cluster de sócios detectado na amostra atual.")
            return

        log.info(f"Detectados {len(clusters)} operadores de rede (sócios em múltiplas empresas).")
        
        print("\n=== RANKING DE OPERADORES DE REDE (CLUSTERS) ===")
        print(clusters[['socio_nome', 'qtd_empresas']].head(10).to_string(index=False))
        
        # Para cada CNPJ no cluster, vamos verificar se tem contratos públicos
        for _, row in clusters.head(5).iterrows():
            cnpjs = row['lista_cnpjs']
            contratos = con.execute("""
                SELECT 
                    cnpj, 
                    razao_social, 
                    SUM(total_valor_brl) as valor_total
                FROM trace_norte_rede_empresas
                WHERE cnpj IN (SELECT unnest(?))
                GROUP BY 1, 2
            """, [cnpjs]).fetchdf()
            
            if not contratos.empty:
                print(f"\nOperador: {row['socio_nome']} (Controla {row['qtd_empresas']} empresas)")
                print(contratos.to_string(index=False))

    finally:
        con.close()

if __name__ == "__main__":
    run_cluster_audit()
