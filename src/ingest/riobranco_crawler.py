import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import time
import os
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class RioBrancoCrawler:
    BASE_URL = "https://transparencia.riobranco.ac.gov.br/despesa/"
    
    def __init__(self, data_dir="./data/riobranco"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Sentinela/2.0 (Crawler Massivo Anticorrupção)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        })
        self.viewstate = None

    def _init_session(self):
        log.info("Iniciando sessão e obtendo ViewState...")
        r = self.session.get(self.BASE_URL)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml-xml")
        vs_input = soup.find("input", {"name": "javax.faces.ViewState"})
        if not vs_input:
            raise RuntimeError("ViewState não encontrado no HTML inicial.")
        self.viewstate = vs_input["value"]
        log.info(f"ViewState capturado: {self.viewstate[:20]}...")

    def get_unidades(self) -> dict:
        """Emula o Autocomplete do PrimeFaces para extrair todas as unidades (IDs e Nomes)."""
        if not self.viewstate:
            self._init_session()

        log.info("Solicitando lista completa de Unidades via AJAX...")
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "Formulario:acunidade:acunidade",
            "javax.faces.partial.execute": "Formulario:acunidade:acunidade",
            "javax.faces.partial.render": "Formulario:acunidade:acunidade",
            "Formulario:acunidade:acunidade": "",
            "Formulario:acunidade:acunidade_query": " ", # Espaço para puxar tudo
            "Formulario": "Formulario",
            "javax.faces.ViewState": self.viewstate
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.BASE_URL,
        }
        
        r = self.session.post(self.BASE_URL, data=payload, headers=headers)
        r.raise_for_status()
        
        unidades = {}
        try:
            root = ET.fromstring(r.text)
            for update in root.findall(".//update"):
                if "acunidade" in update.get("id", "") and update.text:
                    soup = BeautifulSoup(update.text, "html.parser")
                    for li in soup.find_all("li", class_="ui-autocomplete-item"):
                        uid = li.get("data-item-value")
                        nome = li.get("data-item-label")
                        if uid and nome:
                            unidades[uid] = nome
        except Exception as e:
            log.error(f"Erro ao parsear XML do Autocomplete: {e}")
            
        log.info(f"Extraídas {len(unidades)} unidades administrativas.")
        return unidades

    def download_csv_unidade(self, unidade_id: str, unidade_nome: str, ano: str = "2024") -> Optional[Path]:
        """Fluxo JSF Profissional: itemSelect -> btnProcurar -> Exportar"""
        log.info(f"Baixando dados da unidade: {unidade_nome} ({unidade_id})")
        
        headers_ajax = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.BASE_URL,
        }

        # PASSO 1: Disparar itemSelect para a unidade
        payload_select = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "Formulario:acunidade:acunidade",
            "javax.faces.partial.execute": "Formulario:acunidade:acunidade",
            "javax.faces.behavior.event": "itemSelect",
            "Formulario": "Formulario",
            "Formulario:acunidade:acunidade_input": unidade_nome,
            "Formulario:acunidade:acunidade_hinput": unidade_id,
            "javax.faces.ViewState": self.viewstate
        }
        r_select = self.session.post(self.BASE_URL, data=payload_select, headers=headers_ajax)
        try:
            root = ET.fromstring(r_select.text)
            for update in root.findall(".//update"):
                if update.get("id") == "javax.faces.ViewState":
                    self.viewstate = update.text
        except Exception: pass

        # PASSO 2: Clicar em "Procurar"
        payload_procurar = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "Formulario:j_idt77:btnProcurar",
            "javax.faces.partial.execute": "Formulario",
            "javax.faces.partial.render": "Formulario",
            "Formulario:j_idt77:btnProcurar": "Formulario:j_idt77:btnProcurar",
            "Formulario": "Formulario",
            "Formulario:j_idt73": "POR_NATUREZA",
            "Formulario:j_idt77:j_idt79": "2492234",
            "Formulario:j_idt77:j_idt84_input": "2492234",
            "Formulario:acunidade:acunidade_input": unidade_nome,
            "Formulario:acunidade:acunidade_hinput": unidade_id,
            "javax.faces.ViewState": self.viewstate
        }
        r_ajax = self.session.post(self.BASE_URL, data=payload_procurar, headers=headers_ajax)
        try:
            root = ET.fromstring(r_ajax.text)
            for update in root.findall(".//update"):
                if update.get("id") == "javax.faces.ViewState":
                    self.viewstate = update.text
        except Exception: pass

        # PASSO 3: Exportar a Grid Filtrada
        payload_export = {
            "Formulario": "Formulario",
            "Formulario:j_idt73": "POR_NATUREZA",
            "Formulario:j_idt77:j_idt79": "2492234",
            "Formulario:j_idt77:j_idt84_input": "2492234",
            "Formulario:acunidade:acunidade_input": unidade_nome,
            "Formulario:acunidade:acunidade_hinput": unidade_id,
            "javax.faces.ViewState": self.viewstate,
            "Formulario:j_idt87:j_idt101": "Formulario:j_idt87:j_idt101"
        }
        
        headers_export = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": self.BASE_URL,
        }
        r_csv = self.session.post(self.BASE_URL, data=payload_export, headers=headers_export)
        
        if r_csv.status_code == 200 and "attachment" in r_csv.headers.get("Content-Disposition", ""):
            file_path = self.data_dir / f"despesa_unidade_{unidade_id}.csv"
            file_path.write_bytes(r_csv.content)
            log.info(f"  -> Salvo: {file_path.name}")
            return file_path
        else:
            log.warning(f"  -> Falhou o download da unidade {unidade_id}.")
            return None

    def run(self):
        log.info("=== Iniciando Coleta Massiva (Rio Branco) ===")
        unidades = self.get_unidades()
        
        if not unidades:
            log.error("Nenhuma unidade encontrada. Abortando.")
            return
            
        sucessos = 0
        # Limitando a 5 para testar o crawler. Depois tiramos o limite.
        for uid, nome in list(unidades.items())[:5]:
            csv_path = self.download_csv_unidade(uid, nome)
            if csv_path:
                sucessos += 1
            time.sleep(1) # Respeito ao servidor
            
        log.info(f"Coleta concluída! {sucessos}/5 arquivos baixados.")

if __name__ == "__main__":
    crawler = RioBrancoCrawler()
    crawler.run()
