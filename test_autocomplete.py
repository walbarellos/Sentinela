import requests
from bs4 import BeautifulSoup
import re

BASE_URL = "https://transparencia.riobranco.ac.gov.br/despesa/"

session = requests.Session()
r = session.get(BASE_URL)
soup = BeautifulSoup(r.text, "lxml-xml")

viewstate = soup.find("input", {"name": "javax.faces.ViewState"})["value"]

print("Extraindo Unidades via AJAX Autocomplete...")

payload_unidades = {
    "javax.faces.partial.ajax": "true",
    "javax.faces.source": "Formulario:acunidade:acunidade",
    "javax.faces.partial.execute": "Formulario:acunidade:acunidade",
    "javax.faces.partial.render": "Formulario:acunidade:acunidade",
    "Formulario:acunidade:acunidade": "",
    "Formulario:acunidade:acunidade_query": " ", # Um espaço costuma trazer tudo
    "Formulario": "Formulario",
    "javax.faces.ViewState": viewstate
}

headers = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Faces-Request": "partial/ajax", # Muito importante no PrimeFaces
    "X-Requested-With": "XMLHttpRequest",
    "Referer": BASE_URL,
}

r_ajax = session.post(BASE_URL, data=payload_unidades, headers=headers)

if "partial-response" in r_ajax.text:
    print("Sucesso! Resposta AJAX recebida.")
    # A resposta é um XML com um <extension> contendo JSON (geralmente) ou <li> em HTML escaped
    # Vamos extrair e imprimir para ver o formato
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(r_ajax.text)
        for ext in root.findall(".//extension"):
            if ext.get("type") == "primefaces.args":
                print("Args encontrados:", ext.text)
        for update in root.findall(".//update"):
            if "acunidade" in update.get("id"):
                print("Update do autocomplete:", update.text[:300])
    except Exception as e:
        print("Erro ao dar parse no XML de resposta:", e)
        print("Raw:", r_ajax.text[:500])
else:
    print("Falhou a requisição AJAX:")
    print(r_ajax.text[:500])
