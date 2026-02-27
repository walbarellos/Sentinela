import logging
import requests
import pandas as pd
import io
from bs4 import BeautifulSoup
from src.ingest.riobranco_jsf import extract_viewstate
from src.core.analytics_db import AnalyticsDB
from jsf_client import JSFClient

log = logging.getLogger("Sentinela.ServidoresMass")

class RioBrancoServidoresMass:
    URL = "https://transparencia.riobranco.ac.gov.br/servidor/"

    def __init__(self):
        self.db = AnalyticsDB()
        self.client = JSFClient(self.URL)

    def _sync_state(self, year_id=None, month_id=None):
        """Sincroniza o estado do servidor para o ano e mês desejados."""
        self.client.get()
        
        if year_id:
            log.info(f"Sincronizando Ano ID: {year_id}")
            payload = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "Formulario:j_idt73",
                "javax.faces.partial.execute": "Formulario:j_idt73",
                "javax.faces.partial.render": "Formulario",
                "javax.faces.event": "change",
                "Formulario": "Formulario",
                "Formulario:j_idt73": year_id,
                "javax.faces.ViewState": self.client._page.viewstate
            }
            r = self.client.s.post(self.URL, data=payload, headers={"Faces-Request": "partial/ajax"})
            self._update_viewstate(r.text)

        if month_id:
            log.info(f"Sincronizando Mês ID: {month_id}")
            payload = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "Formulario:j_idt77",
                "javax.faces.partial.execute": "Formulario:j_idt77",
                "javax.faces.partial.render": "Formulario",
                "javax.faces.event": "change",
                "Formulario": "Formulario",
                "Formulario:j_idt73": year_id or "",
                "Formulario:j_idt77": month_id,
                "javax.faces.ViewState": self.client._page.viewstate
            }
            r = self.client.s.post(self.URL, data=payload, headers={"Faces-Request": "partial/ajax"})
            self._update_viewstate(r.text)

    def _update_viewstate(self, xml_text):
        soup = BeautifulSoup(xml_text, "xml")
        vs = soup.find("update", {"id": "javax.faces.ViewState"})
        if vs:
            self.client._page.viewstate = vs.text

    def fetch_and_save(self, year_id="2873896", month_id=""):
        log.info(f"Iniciando captura CSV para AnoID={year_id} MesID={month_id}...")
        
        self._sync_state(year_id, month_id)

        # O ID do botão CSV foi validado via HAR e teste
        trigger_id = "Formulario:j_idt80:j_idt94"
        
        payload = {
            "Formulario:j_idt73": year_id,
            "Formulario:j_idt77": month_id,
            "Formulario:j_idt115": "TODOS",
            "Formulario:j_idt119": "TODOS",
            "Formulario:j_idt125": "TODOS",
        }

        r = self.client.download_file(trigger_id, payload)
        
        if "attachment" not in r.headers.get("Content-Disposition", ""):
            log.error("Falha no download do CSV. O servidor não retornou um arquivo.")
            return None

        log.info(f"CSV recebido ({len(r.content)} bytes). Processando...")
        
        # O CSV usa encoding ISO-8859-1 (Latin1)
        df = pd.read_csv(io.BytesIO(r.content), encoding="iso-8859-1")
        
        # Normalização de colunas (Remoção de acentos e caracteres especiais)
        def clean_column(c):
            c = str(c).strip().lower()
            # Substituições manuais para artefatos comuns de encoding
            replacements = {
                ' ': '_', 'í': 'i', 'ã': 'a', 'ú': 'u', 'é': 'e', 'ó': 'o', 'á': 'a', 'ç': 'c',
                'a£': 'a', 'a\xad': 'i', 'a¡': 'a', 'aº': 'u', 'a©': 'e'
            }
            for old, new in replacements.items():
                c = c.replace(old, new)
            # Remove qualquer caractere não alfanumérico restante exceto underscore
            import re
            c = re.sub(r'[^a-z0-9_]', '', c)
            return c

        df.columns = [clean_column(c) for c in df.columns]
        
        log.info(f"Colunas detectadas: {list(df.columns)}")

        # Conversão numérica
        numeric_cols = ['vencimento_base', 'outras_verbas', 'salario_bruto', 'descontos', 'salario_liquido']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

        # Adicionar metadados
        df['ano_id'] = year_id
        df['mes_id'] = month_id if month_id else "TODOS"

        # Persistência
        table_name = "rb_servidores_mass"
        self.db.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.db.conn.register("df_temp", df)
        self.db.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_temp")
        
        log.info(f"✅ Sucesso: {len(df)} registros salvos na tabela '{table_name}'.")
        return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    # Por padrão, pega o ano atual (2026) conforme mapeado
    RioBrancoServidoresMass().fetch_and_save()
