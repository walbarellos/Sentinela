import logging
import re
from io import StringIO
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.ingest.riobranco_http import fetch_html

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
        self.s.headers.update(
            {
                "User-Agent": "Sentinela/3.0",
                "Accept-Encoding": "identity",
            }
        )

    @staticmethod
    def _canonical_key(label: str) -> str:
        normalized = label.strip().rstrip(":").lower()
        normalized = normalized.replace("ç", "c").replace("ã", "a").replace("á", "a")
        normalized = normalized.replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        normalized = normalized.replace("/", "_")
        normalized = re.sub(r"\s+", "_", normalized)
        return normalized

    @staticmethod
    def _derive_org_fields(lotacao: str) -> tuple[str, str]:
        if not lotacao:
            return "", ""
        parts = [part.strip() for part in lotacao.split(" - ") if part.strip()]
        descricao = " - ".join(parts[1:]) if len(parts) >= 2 else parts[0]
        primary = parts[1] if len(parts) >= 2 else parts[0]
        normalized = descricao.upper()
        if "SEMSA" in normalized or "SAUDE" in normalized or "FUNDO MUNICIPAL DE SAUDE" in normalized:
            return "SEMSA", descricao
        if "SECRETARIA" in primary.upper():
            return primary, descricao
        return "", descricao

    def fetch(self, servidor_id: str) -> dict:
        url = f"{self.BASE}/servidor/ver/{servidor_id}/"
        html = fetch_html(self.s, url, timeout=30)

        soup = BeautifulSoup(html, "html.parser")
        data: dict[str, str] = {}
        text = soup.get_text("\n", strip=True)

        table = soup.find("table", class_="table table-striped")
        if table is not None:
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(" ", strip=True)
                value = cells[1].get_text(" ", strip=True)
                if not value:
                    continue
                data[self._canonical_key(label)] = value

        field_map = {
            "matricula_contrato": ("matricula/contrato",),
            "nome": ("pessoa", "nome"),
            "cargo": ("cargo",),
            "lotacao": ("lotacao", "lotação"),
            "vinculo": ("modalidade", "vinculo", "vínculo"),
            "tipo_de_folha": ("tipo_de_folha",),
            "admissao": ("admissao", "admissão"),
        }
        for key, labels in field_map.items():
            if key in data:
                continue
            value = _extract_field(text, *labels)
            if value:
                data[key] = value

        secretaria, unidade = self._derive_org_fields(data.get("lotacao", ""))
        if secretaria and "secretaria" not in data:
            data["secretaria"] = secretaria
        if unidade and "unidade" not in data:
            data["unidade"] = unidade

        # Extração da maior tabela HTML como apoio para debugging/análises futuras.
        try:
            tables = pd.read_html(StringIO(html))
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
