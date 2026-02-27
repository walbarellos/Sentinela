import requests
from bs4 import BeautifulSoup
import re
import logging
import xml.etree.ElementTree as ET
from src.ingest.riobranco_obras import RioBrancoObrasParser
from jsf_client import JSFClient

log = logging.getLogger("Sentinela.ObrasList")

class RioBrancoObrasList:
    URL = "https://transparencia.riobranco.ac.gov.br/obra/"
    
    def __init__(self):
        self.client = JSFClient(self.URL)
        self.parser = RioBrancoObrasParser()

    def get_all_obra_ids(self):
        log.info("Iniciando captura massiva de IDs (Botão TODOS)...")
        
        # 1. GET Inicial
        self.client.get()
        
        # 2. Descobrir botão TODOS
        btn_id = None
        try:
            btn_id = self.client.discover_button_by_label("TODOS")
        except:
            # Fallback ID common in this portal for "TODOS" button in tables
            btn_id = "Formulario:j_idt83:j_idt102:j_idt118"
            
        log.info(f"Usando botão: {btn_id}")

        # 3. POST AJAX para ativar o modo "TODOS"
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": btn_id,
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "Formulario",
            btn_id: btn_id,
            "Formulario": "Formulario",
            "javax.faces.ViewState": self.client._page.viewstate
        }
        
        r_ajax = self.client.s.post(self.URL, data=payload, headers={"Faces-Request": "partial/ajax"})
        
        # 4. Parse do XML de Resposta
        ids = set()
        try:
            soup_xml = BeautifulSoup(r_ajax.text, "xml")
            update_node = soup_xml.find("update", {"id": "Formulario"})
            if update_node:
                html_content = update_node.text
                soup_tab = BeautifulSoup(html_content, "html.parser")
                links = soup_tab.find_all("a", href=re.compile(r"/obra/ver/\d+/"))
                for link in links:
                    match = re.search(r"/ver/(\d+)/", link['href'])
                    if match:
                        ids.add(match.group(1))
        except Exception as e:
            log.error(f"Erro ao processar resposta AJAX: {e}")
            # Fallback para a primeira página
            soup_init = BeautifulSoup(self.client._page.html, "html.parser")
            links = soup_init.find_all("a", href=re.compile(r"/obra/ver/\d+/"))
            for link in links:
                match = re.search(r"/ver/(\d+)/", link['href'])
                if match: ids.add(match.group(1))
        
        log.info(f"Sucesso! {len(ids)} IDs de obras capturados.")
        return list(ids)

    def run_mass_import(self):
        ids = self.get_all_obra_ids()
        count = 0
        for oid in ids:
            if self.parser.get_obra_detail(oid):
                count += 1
        log.info(f"🚀 Sentinela finalizou: {count} obras reais de Rio Branco agora estão no DuckDB.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    collector = RioBrancoObrasList()
    collector.run_mass_import()
