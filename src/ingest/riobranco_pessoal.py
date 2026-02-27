# src/ingest/riobranco_pessoal.py
import requests
from bs4 import BeautifulSoup
import re
import logging
from src.core.analytics_db import AnalyticsDB

log = logging.getLogger("Sentinela.Pessoal")

class pessoalParser:
    def __init__(self):
        self.db = AnalyticsDB()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Sentinela/3.0"})

    def get_servidor_detail(self, servidor_id):
        url = f"https://transparencia.riobranco.ac.gov.br/servidor/ver/{servidor_id}/"
        r = self.session.get(url)
        if r.status_code != 200: return None
        
        soup = BeautifulSoup(r.text, "html.parser")
        data = {"id": str(servidor_id), "tipo": "PF"}
        
        # Extrai Nome e CPF (se houver)
        nome_tag = soup.find("h2") # Geralmente o nome está no topo
        if nome_tag: data["nome"] = nome_tag.get_text(strip=True)
        
        # Procura por tabelas de remuneração
        # (Ajustar conforme o HTML real do 2.txt)
        return data

    def get_diaria_detail(self, diaria_id):
        url = f"https://transparencia.riobranco.ac.gov.br/diaria/ver/{diaria_id}/"
        r = self.session.get(url)
        if r.status_code != 200: return None
        
        soup = BeautifulSoup(r.text, "html.parser")
        # Extrai valor, destino e motivo
        return {"id": diaria_id, "fonte": "DIARIA"}
