import logging
import re
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger("Sentinela.ServidorDetail")


def _extract_field(text: str, *labels: str) -> str:
    for label in labels:
        match = re.search(rf"(?im){label}\s*[:\-–]\s*(.+)", text)
        if match:
            return match.group(1).strip()
    return ""


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
        text = soup.get_text("\n", strip=True)

        # Mantém um conjunto pequeno de campos canônicos para o sync de lotação.
        field_map = {
            "nome": ("Nome",),
            "cargo": ("Cargo",),
            "lotacao": ("Lotação", "Lotacao"),
            "vinculo": ("Vínculo", "Vinculo"),
            "secretaria": ("Secretaria",),
            "unidade": ("Unidade",),
        }
        for key, labels in field_map.items():
            value = _extract_field(text, *labels)
            if value:
                data[key] = value

        # Extração da maior tabela HTML como apoio para debugging/análises futuras.
        try:
            tables = pd.read_html(r.text)
            main_table: Any = max(tables, key=lambda df: df.shape[0] * df.shape[1]) if tables else None
        except Exception:
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
