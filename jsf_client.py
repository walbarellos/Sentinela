import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, Optional, List
import requests
from bs4 import BeautifulSoup

@dataclass
class JSFPage:
    html: str
    viewstate: str

class JSFClient:
    """Motor de Elite para Portais JSF/PrimeFaces (v4.1)."""
    def __init__(self, base_url: str, form_id: str = "Formulario"):
        self.base_url = base_url.rstrip("/") + "/"
        self.form_id = form_id
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "Sentinela/4.1 (Intelligence Unit)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        })
        self._page: Optional[JSFPage] = None

    def get(self) -> JSFPage:
        r = self.s.get(self.base_url)
        r.raise_for_status()
        vs = BeautifulSoup(r.text, "html.parser").find("input", {"name": "javax.faces.ViewState"})["value"]
        self._page = JSFPage(html=r.text, viewstate=vs)
        return self._page

    def set_select_pair(self, state: Dict, base_id: str, value: str):
        """Define o valor para o ID real e para o widget de input do PrimeFaces."""
        state[base_id] = value
        state[f"{base_id}_input"] = value
        # Tenta IDs compostos comuns (ex: j_idt77:j_idt79 -> j_idt77:j_idt84_input)
        # Esse mapeamento exato virá do HAR do usuário.

    def download_file(self, trigger_id: str, payload_overrides: Dict) -> requests.Response:
        """Executa o POST de download garantindo os cookies de segurança."""
        if not self._page: self.get()
        
        # O SEGREDO: Cookie que autoriza o download no PrimeFaces
        self.s.cookies.set("primefaces.download", "true", domain="transparencia.riobranco.ac.gov.br", path="/")
        
        state = {
            self.form_id: self.form_id,
            "javax.faces.ViewState": self._page.viewstate,
            trigger_id: trigger_id  # O gatilho do botão
        }
        state.update(payload_overrides)
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": self.base_url,
            "Origin": "https://transparencia.riobranco.ac.gov.br"
        }
        
        return self.s.post(self.base_url, data=state, headers=headers)

    def discover_button_by_label(self, label: str) -> str:
        """Encontra o ID do botão Mojarra baseado no texto (ex: JSON, CSV)."""
        soup = BeautifulSoup(self._page.html, "html.parser")
        for el in soup.find_all(["a", "button"]):
            text = (el.get_text() + el.get("title", "")).upper()
            onclick = el.get("onclick", "")
            if label.upper() in text and "mojarra.jsfcljs" in onclick:
                match = re.search(r"\{\s*'([^']+)'", onclick)
                if match: return match.group(1)
        raise RuntimeError(f"Gatilho '{label}' não localizado no HTML.")
