"""
portal_transparencia_integrator.py
===================================
Integrador completo do Portal da Transparência do Governo Federal
para o sistema Sentinela.

Fonte: https://api.portaldatransparencia.gov.br/v3/api-docs
Spec verificado ao vivo em 27/02/2026.
"""

from __future__ import annotations
import os
import time
import logging
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Iterator, Any
import httpx
import duckdb
import pandas as pd

# CONFIGURAÇÃO
BASE_URL = "https://api.portaldatransparencia.gov.br"
DB_PATH = Path("data/sentinela_analytics.duckdb")
DATA_DIR = Path("data/federal")

log = logging.getLogger("sentinela.federal")
logging.basicConfig(level=logging.INFO)

def get_token() -> str:
    return os.environ.get("CGU_API_TOKEN", "").strip()

class PortalClient:
    def __init__(self, token: str | None = None):
        self.token = token or get_token()
        self._client = httpx.Client(base_url=BASE_URL, headers={"chave-api-dados": self.token}, timeout=30)
    def close(self): self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()

def download_bulk(dataset_key: str, ano: int | None = None, mes: int | None = None, dest_dir: Path = DATA_DIR) -> Path | None:
    BULK_DATASETS = {
        "ceis_csv": ("https://portaldatransparencia.gov.br/download-de-dados/ceis", "CEIS"),
        "cnep_csv": ("https://portaldatransparencia.gov.br/download-de-dados/cnep", "CNEP"),
        "ceaf_csv": ("https://portaldatransparencia.gov.br/download-de-dados/ceaf", "CEAF"),
        "cepim_csv": ("https://portaldatransparencia.gov.br/download-de-dados/cepim", "CEPIM"),
    }
    url, desc = BULK_DATASETS[dataset_key]
    dest_dir.mkdir(parents=True, exist_ok=True)
    fname = dest_dir / f"{dataset_key}.zip"
    log.info(f"Baixando {desc} → {fname}")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(fname, "wb") as f:
            for chunk in resp.iter_bytes(): f.write(chunk)
    return fname

class CrossReferencePipeline:
    def __init__(self, db_path: Path = DB_PATH):
        self.conn = duckdb.connect(str(db_path))
    
    def detect_empresa_sancionada_ativa(self) -> pd.DataFrame:
        return self.conn.execute("""
            SELECT o.empresa_nome, o.empresa_id, o.valor_total, c.tipo_sancao, 'CEIS' as fonte
            FROM obras o
            JOIN federal_ceis c ON REGEXP_REPLACE(o.empresa_id::VARCHAR, '[^0-9]', '', 'g') = REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
        """).df()

    def run_all(self):
        results = {"empresa_sancionada": self.detect_empresa_sancionada_ativa()}
        for k, v in results.items():
            if not v.empty: print(f"\n🔴 {k.upper()}: {len(v)} indícios encontrados!")
            else: print(f"✓ {k}: limpo")
        return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk-sancoes", action="store_true")
    parser.add_argument("--cross", action="store_true")
    args = parser.parse_args()

    if args.bulk_sancoes:
        for ds in ["ceis_csv", "cnep_csv", "ceaf_csv", "cepim_csv"]: download_bulk(ds)
    
    if args.cross:
        pipe = CrossReferencePipeline()
        pipe.run_all()

if __name__ == "__main__":
    main()
