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
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest"
        })
        self.viewstate = None

    def _init_session(self):
        r = self.session.get(self.BASE_URL)
        soup = BeautifulSoup(r.text, "html.parser")
        self.viewstate = soup.find("input", {"name": "javax.faces.ViewState"})["value"]

    def baixar_competencia(self, ano_id, mes_val, filename="servidor.json"):
        """
        ano_id: O valor do option (ex: 2492234 para 2025)
        mes_val: O valor do mês (1 a 12)
        """
        if not self.viewstate: self._init_session()
        
        log.info(f"Configurando competência: AnoID={ano_id}, Mês={mes_val}...")
        
        # 1. Update AJAX para o Mês (Simula onchange)
        payload_mes = {
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
            "Faces-Request": "partial/ajax"
        }
        
        r = self.session.post(self.BASE_URL, data=payload_mes, headers=headers_ajax)
        
        # Captura novo ViewState
        try:
            root = ET.fromstring(r.text)
            for update in root.findall(".//update"):
                if update.get("id") == "javax.faces.ViewState":
                    self.viewstate = update.text
        except: pass

        # 2. Comando de Exportação JSON
        log.info("Disparando exportação JSON...")
        payload_export = {
            "Formulario": "Formulario",
            "Formulario:j_idt73": ano_id,
            "Formulario:j_idt77": mes_val,
            "Formulario:j_idt115": "MENSAL", # Padrao
            "Formulario:j_idt119": "SERVIDORES", # Padrao
            "Formulario:j_idt125": "TODOS", # Padrao
            "javax.faces.ViewState": self.viewstate,
            "Formulario:j_idt80:j_idt96": "Formulario:j_idt80:j_idt96" # Botão JSON
        }
        
        r_json = self.session.post(self.BASE_URL, data=payload_export)
        
        if "attachment" in r_json.headers.get("Content-Disposition", ""):
            with open(filename, "wb") as f:
                f.write(r_json.content)
            log.info(f"✅ Arquivo salvo: {filename}")
            return True
        else:
            log.error("Falha ao exportar. Verifique os IDs.")
            return False

if __name__ == "__main__":
    bot = ServidorRioBranco()
    # Exemplo: Janeiro (1) de 2025 (ID 2492234 obtido nos testes anteriores)
    bot.baixar_competencia("2492234", "1", "servidor_2025_01.json")
