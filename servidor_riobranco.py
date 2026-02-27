import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import logging
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Sentinela.Servidor")

class ServidorRioBranco:
    BASE_URL = "https://transparencia.riobranco.ac.gov.br/servidor/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Sentinela/3.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        })
        self.viewstate = None

    def _init_session(self):
        r = self.session.get(self.BASE_URL)
        soup = BeautifulSoup(r.text, "html.parser")
        vs_input = soup.find("input", {"name": "javax.faces.ViewState"})
        if vs_input:
            self.viewstate = vs_input["value"]
            log.info("Sessão inicializada.")

    def baixar_competencia(self, ano_id, mes_val, filename="servidores.json"):
        if not self.viewstate: self._init_session()
        
        # 1. Update AJAX para o Mês (simula onchange)
        payload_ajax = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "Formulario:j_idt77",
            "javax.faces.partial.execute": "Formulario:j_idt77",
            "javax.faces.partial.render": "Formulario",
            "javax.faces.event": "change",
            "Formulario": "Formulario",
            "Formulario:j_idt73": ano_id,
            "Formulario:j_idt77": mes_val,
            "javax.faces.ViewState": self.viewstate
        }
        
        headers_ajax = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        r_ajax = self.session.post(self.BASE_URL, data=payload_ajax, headers=headers_ajax)
        
        try:
            root = ET.fromstring(r_ajax.text)
            for update in root.findall(".//update"):
                if update.get("id") == "javax.faces.ViewState":
                    self.viewstate = update.text
                    log.info("ViewState atualizado.")
        except: pass

        # 2. Comando de Exportação JSON (Simulando mojarra.jsfcljs)
        log.info(f"Exportando AnoID={ano_id} Mes={mes_val}...")
        payload_export = {
            "Formulario": "Formulario",
            "Formulario:j_idt73": ano_id,
            "Formulario:j_idt77": mes_val,
            "Formulario:j_idt115": "MENSAL",
            "Formulario:j_idt119": "SERVIDORES",
            "Formulario:j_idt125": "TODOS",
            "javax.faces.ViewState": self.viewstate,
            "Formulario:j_idt80:j_idt96": "Formulario:j_idt80:j_idt96" # Campo de disparo
        }
        
        headers_export = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": self.BASE_URL,
            "Origin": "https://transparencia.riobranco.ac.gov.br"
        }
        
        r_json = self.session.post(self.BASE_URL, data=payload_export, headers=headers_export)
        
        if "attachment" in r_json.headers.get("Content-Disposition", ""):
            with open(filename, "wb") as f:
                f.write(r_json.content)
            log.info(f"✅ Sucesso: {filename}")
            return True
        else:
            log.error("Servidor recusou exportação. Payload ou ViewState podem estar desalinhados.")
            return False

if __name__ == "__main__":
    bot = ServidorRioBranco()
    bot.baixar_competencia("2492234", "1", "servidores_2025_01.json")
