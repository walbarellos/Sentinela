import requests
from bs4 import BeautifulSoup

BASE_URL = "https://transparencia.riobranco.ac.gov.br/despesa/"

session = requests.Session()

# 1. GET inicial
r = session.get(BASE_URL)
r.raise_for_status()

soup = BeautifulSoup(r.text, "lxml")

viewstate = soup.find("input", {"name": "javax.faces.ViewState"})
viewstate_value = viewstate["value"]

print("ViewState:", viewstate_value)

# 2. POST para gerar CSV
payload = {
    "Formulario": "Formulario",
    "Formulario:j_idt73": "POR_NATUREZA",
    "Formulario:j_idt77:j_idt79": "2492234",
    "Formulario:j_idt77:j_idt84_filter": "",
    "Formulario:j_idt77:j_idt84_input": "2492234",
    "Formulario:acunidade:acunidade_input": "Gabinete do Prefeito - GABPREF",
    "Formulario:acunidade:acunidade_hinput": "5",
    "Formulario:acnd:acnd_input": "3.3.20.41.00.00.00 - Contribuições",
    "Formulario:acnd:acnd_hinput": "1891529",
    "javax.faces.ViewState": viewstate_value,
    "Formulario:j_idt87:j_idt101": "Formulario:j_idt87:j_idt101"
}

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": BASE_URL,
    "Origin": "https://transparencia.riobranco.ac.gov.br"
}

response = session.post(BASE_URL, data=payload, headers=headers)

if "attachment" in response.headers.get("Content-Disposition", ""):
    with open("despesa.csv", "wb") as f:
        f.write(response.content)
    print("Arquivo salvo como despesa.csv")
else:
    print("Falhou:")
    print(response.text[:1000])
