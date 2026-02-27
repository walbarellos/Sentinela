import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import logging
from pathlib import Path
from datetime import datetime
from jsf_client import JSFClient
from src.core.analytics_db import AnalyticsDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("Sentinela.Diarias")

class RioBrancoDiariasCrawler:
    BASE_URL = "https://transparencia.riobranco.ac.gov.br/diaria/"
    
    def __init__(self):
        self.db = AnalyticsDB()
        self.client = JSFClient(self.BASE_URL)

    def _sync_state(self, year_id="2873896"):
        """Sincroniza o estado para o ano desejado."""
        self.client.get()
        log.info(f"Sincronizando Exercício ID: {year_id}")
        
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "Formulario:j_idt73:j_idt75",
            "javax.faces.partial.execute": "Formulario:j_idt73:j_idt75",
            "javax.faces.partial.render": "Formulario",
            "javax.faces.event": "change",
            "Formulario": "Formulario",
            "Formulario:j_idt73:j_idt75": year_id,
            "javax.faces.ViewState": self.client._page.viewstate
        }
        r = self.client.s.post(self.BASE_URL, data=payload, headers={"Faces-Request": "partial/ajax"})
        
        soup = BeautifulSoup(r.text, "xml")
        vs = soup.find("update", {"id": "javax.faces.ViewState"})
        if vs:
            self.client._page.viewstate = vs.text

    def _search(self):
        """Dispara o botão Pesquisar para processar os filtros."""
        log.info("Disparando Pesquisa...")
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "Formulario:j_idt132",
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "Formulario",
            "Formulario:j_idt132": "Formulario:j_idt132",
            "Formulario": "Formulario",
            "javax.faces.ViewState": self.client._page.viewstate
        }
        r = self.client.s.post(self.BASE_URL, data=payload, headers={"Faces-Request": "partial/ajax"})
        soup = BeautifulSoup(r.text, "xml")
        vs = soup.find("update", {"id": "javax.faces.ViewState"})
        if vs:
            self.client._page.viewstate = vs.text

    def fetch_and_save(self, year_id="2873896"):
        log.info(f"Iniciando captura de Diárias (Exercício ID {year_id})...")
        
        self._sync_state(year_id)
        self._search()

        # ID do botão CSV para Diárias (validado via script)
        trigger_id = "Formulario:j_idt83:j_idt97"
        
        payload = {
            "Formulario": "Formulario",
            "Formulario:j_idt73:j_idt75": year_id,
            "Formulario:j_idt112": "", # Localidade
            "Formulario:j_idt116": "", # Tipo
            "Formulario:j_idt120": "", # Meio Transporte
        }

        r = self.client.download_file(trigger_id, payload)
        
        if "attachment" not in r.headers.get("Content-Disposition", ""):
            log.error("Falha no download do CSV de Diárias.")
            return None

        log.info(f"CSV de Diárias recebido ({len(r.content)} bytes).")
        
        # Parse CSV
        df = pd.read_csv(io.BytesIO(r.content), encoding="iso-8859-1")
        log.info(f"Colunas originais: {df.columns.tolist()}")
        
        # Normalização de colunas
        def clean_col(c):
            import re
            c = str(c).strip().lower()
            # Artefatos comuns de ISO-8859-1 mal interpretado
            replacements = {
                ' ': '_', 'í': 'i', 'ã': 'a', 'ú': 'u', 'é': 'e', 'ó': 'o', 'á': 'a', 'ç': 'c',
                'a£': 'a', 'a\xad': 'i', 'a¡': 'a', 'aº': 'u', 'a©': 'e', 'a³': 'o', 'ãº': 'u',
                'ã\xad': 'i', 'ã¡': 'a', 'ã©': 'e', 'ã³': 'o', 'ã±': 'n'
            }
            for old, new in replacements.items(): c = c.replace(old, new)
            return re.sub(r'[^a-z0-9_]', '', c)

        df.columns = [clean_col(c) for c in df.columns]
        log.info(f"Colunas limpas: {df.columns.tolist()}")
        
        # Mapeamento de colunas para o banco
        # Originais limpas: ['numero', 'data', 'tipo', 'pessoa', 'itinerario', 'motivo', 'meio_de_transporte', 'valor', 'saida', 'retorno', 'empenho']
        col_map = {
            'pessoa': 'servidor_nome',
            'itinerario': 'destino',
            'saida': 'data_saida',
            'retorno': 'data_retorno'
        }
        
        # Conversão de tipos
        valor_col = 'valor'
        if valor_col in df.columns:
            df['valor_limpo'] = df[valor_col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.extract(r'([\d\.]+)')[0].astype(float)
        else:
            df['valor_limpo'] = 0.0
        
        # Datas (Formato 26/02/2026)
        for col in ['saida', 'retorno']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

        # Persistência
        count = 0
        for _, row in df.iterrows():
            # Gerar um ID único (Hash de servidor + data + valor)
            import hashlib
            s_nome = str(row.get('pessoa', 'Desconhecido'))
            d_saida = str(row.get('saida'))
            v_val = str(row.get('valor_limpo', 0.0))
            row_id = hashlib.md5(f"{s_nome}{d_saida}{v_val}".encode()).hexdigest()
            
            self.db.upsert_diaria({
                "id": row_id,
                "servidor_nome": s_nome,
                "destino": str(row.get('itinerario', '')),
                "data_saida": row.get('saida'),
                "data_retorno": row.get('retorno'),
                "valor": float(row.get('valor_limpo', 0.0)),
                "motivo": str(row.get('motivo', '')),
                "secretaria": "Prefeitura de Rio Branco", # CSV não traz unidade gestora explícita às vezes
                "capturado_em": datetime.now()
            })
            count += 1
            
        log.info(f"✅ Sucesso: {count} diárias salvas no DuckDB.")
        return df

if __name__ == "__main__":
    RioBrancoDiariasCrawler().fetch_and_save()
