import duckdb
import pandas as pd
from pathlib import Path
import logging

log = logging.getLogger("Sentinela.DB")

class AnalyticsDB:
    def __init__(self, db_path="./data/sentinela_analytics.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Cria ou atualiza as tabelas canônicas."""
        # Cria a tabela se não existir (versão completa com 7 colunas)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obras (
                id VARCHAR PRIMARY KEY,
                nome TEXT,
                valor_total DOUBLE,
                empresa_id VARCHAR,
                empresa_nome TEXT,
                secretaria TEXT,
                capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS diarias (
                id VARCHAR PRIMARY KEY,
                servidor_nome TEXT,
                destino TEXT,
                data_saida DATE,
                data_retorno DATE,
                valor DOUBLE,
                motivo TEXT,
                secretaria TEXT,
                capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Migração: Se a tabela for antiga e não tiver a coluna empresa_nome, adiciona.
        cols = self.conn.execute("PRAGMA table_info('obras')").fetchall()
        col_names = [c[1] for c in cols]
        if "empresa_nome" not in col_names:
            log.info("Migrando banco: adicionando coluna 'empresa_nome'...")
            self.conn.execute("ALTER TABLE obras ADD COLUMN empresa_nome TEXT DEFAULT 'Empresa Desconhecida'")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entidades (
                id VARCHAR PRIMARY KEY,
                nome TEXT,
                tipo VARCHAR,
                capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS feedback (
                entidade_id VARCHAR,
                tipo_alerta VARCHAR,
                label VARCHAR,
                usuario VARCHAR,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (entidade_id, tipo_alerta)
            );
        """)
        log.info("Esquema DuckDB verificado e atualizado.")

    def upsert_obra(self, data: dict):
        df = pd.DataFrame([data])
        self.conn.execute("""
            INSERT OR REPLACE INTO obras (id, nome, valor_total, empresa_id, empresa_nome, secretaria, capturado_em)
            SELECT id, nome, valor_total, empresa_id, empresa_nome, secretaria, capturado_em FROM df
        """)

    def upsert_diaria(self, data: dict):
        df = pd.DataFrame([data])
        self.conn.execute("""
            INSERT OR REPLACE INTO diarias (id, servidor_nome, destino, data_saida, data_retorno, valor, motivo, secretaria, capturado_em)
            SELECT id, servidor_nome, destino, data_saida, data_retorno, valor, motivo, secretaria, capturado_em FROM df
        """)

    def close(self):
        self.conn.close()
