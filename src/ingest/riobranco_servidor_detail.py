import logging
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

log = logging.getLogger("Sentinela.ServidorDetail")

class RioBrancoServidorDetail:
    BASE = "https://transparencia.riobranco.ac.gov.br"

    def __init__(self, session: requests.Session or None = None):
        self.s = session or requests.Session()
        self.s.headers.update({"User-Agent": "Sentinela/3.0"})

    def fetch(self, servidor_id: str) -> dict:
        url = f"{self.BASE}/servidor/ver/{servidor_id}/"
        r = self.s.get(url, timeout=30)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        data: dict[str, str] = {}
        text = soup.get_text("
", strip=True)

        # Mapeamento de metadados
        for key in ["Nome", "Cargo", "Lotação", "Vínculo", "Secretaria"]:
            m = re.search(rf"{key}\s*[:\-]\s*(.+)", text)
            if m: data[key.lower()] = m.group(1).strip()

        # Extração de tabelas salariais
        try:
            tables = pd.read_html(r.text)
            main_table = max(tables, key=lambda df: df.shape[0] * df.shape[1]) if tables else None
        except:
            main_table = None

        data["servidor_id"] = str(servidor_id)
        data["url"] = url
        data["capturado_em"] = datetime.now().isoformat()
        return {"meta": data, "table": main_table}

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bot = RioBrancoServidorDetail()
    res = bot.fetch("2893204")
    print(res["meta"])
