import requests
from bs4 import BeautifulSoup
import re
import logging
from pathlib import Path
import pandas as pd
from src.core.analytics_db import AnalyticsDB

log = logging.getLogger("Sentinela.Obras")

class RioBrancoObrasParser:
    BASE_URL = "https://transparencia.riobranco.ac.gov.br/obra/ver/"
    
    def __init__(self):
        self.db = AnalyticsDB()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Sentinela/3.0"})

    def _parse_valor(self, text):
        """Converte 'R$ 170.615,96' para float 170615.96"""
        try:
            clean = re.sub(r"[^\d,]", "", text).replace(",", ".")
            return float(clean)
        except:
            return 0.0

    def get_obra_detail(self, obra_id):
        log.info(f"Extraindo detalhes da obra {obra_id}...")
        url = f"{self.BASE_URL}{obra_id}/"
        r = self.session.get(url)
        if r.status_code != 200:
            log.error(f"Falha ao acessar obra {obra_id}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        
        # O portal de Rio Branco usa tabelas para os detalhes
        rows = soup.find_all("tr")
        data = {"id": str(obra_id)}
        
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2: continue
            
            label = cols[0].get_text(strip=True).lower()
            value_td = cols[1]
            value_text = value_td.get_text(strip=True)
            
            if "nome:" in label:
                data["nome"] = value_text
            elif "empresa licitada" in label:
                link = value_td.find("a")
                if link:
                    # Extrai ID da empresa do link '/pessoa/ver/7131/'
                    match = re.search(r"/ver/(\d+)/", link['href'])
                    data["empresa_id"] = match.group(1) if match else None
                    data["empresa_nome"] = value_text
            elif "custo total:" in label:
                data["valor_total"] = self._parse_valor(value_text)
            elif "secretaria fiscalizadora:" in label:
                data["secretaria"] = value_text

        # Salva no DuckDB
        if "nome" in data:
            self.db.upsert_obra({
                "id": data["id"],
                "nome": data["nome"],
                "valor_total": data.get("valor_total", 0.0),
                "empresa_id": data.get("empresa_id", ""),
                "empresa_nome": data.get("empresa_nome", "Empresa Desconhecida"),
                "secretaria": data.get("secretaria", ""),
                "capturado_em": pd.Timestamp.now()
            })
            log.info(f"✅ Obra {obra_id} salva: {data['nome']}")
            return data
        return None

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = RioBrancoObrasParser()
    # Testando com os IDs que você achou no log
    parser.get_obra_detail("256017")
    parser.get_obra_detail("256020")
