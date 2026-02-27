import requests
from bs4 import BeautifulSoup
import re

BASE_URL = "https://transparencia.riobranco.ac.gov.br/despesa/"

r = requests.get(BASE_URL)
r.raise_for_status()
html = r.text

print("==== BUSCANDO UNIDADES ====")
soup = BeautifulSoup(html, "lxml")

selects = soup.find_all("select")
for s in selects:
    print(f"Select: {s.get('name')}, id: {s.get('id')}, options: {len(s.find_all('option'))}")

print("==== BUSCANDO SCRIPTS WIDGETBUILDER ====")
for script in soup.find_all("script"):
    if script.string and "acunidade" in script.string:
        print("Script contendo acunidade encontrado!")
        print(script.string.strip()[:300])
