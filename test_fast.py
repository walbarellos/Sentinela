import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Sentinela.Fast")

def download_direto(ano_id, mes_val):
    url = "https://transparencia.riobranco.ac.gov.br/servidor/"
    session = requests.Session()
    session.headers.update({"User-Agent": "Sentinela/3.0"})
    
    # 1. Pega ViewState
    r = session.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    vs = soup.find("input", {"name": "javax.faces.ViewState"})["value"]
    
    # 2. POST de exportacao direta
    payload = {
        "Formulario": "Formulario",
        "Formulario:j_idt73": ano_id,
        "Formulario:j_idt77": mes_val,
        "Formulario:j_idt115": "MENSAL",
        "Formulario:j_idt119": "SERVIDORES",
        "Formulario:j_idt125": "TODOS",
        "javax.faces.ViewState": vs,
        "Formulario:j_idt80:j_idt96": "Formulario:j_idt80:j_idt96"
    }
    
    log.info(f"Tentando exportação direta: {ano_id}/{mes_val}...")
    r2 = session.post(url, data=payload)
    
    if "attachment" in r2.headers.get("Content-Disposition", ""):
        with open("servidor_teste.json", "wb") as f:
            f.write(r2.content)
        log.info("✅ SUCESSO na exportação direta!")
        return True
    else:
        log.error("Falha na exportação direta.")
        return False

if __name__ == "__main__":
    download_direto("2492234", "1")
