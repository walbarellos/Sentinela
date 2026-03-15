"""
diag_rb_contrato.py — cole a saída de volta para fechar o sync.
Lê: fluxo JSF real de /contrato/, schema CSV exportado, campos do detalhe.

Uso: .venv/bin/python scripts/diag_rb_contrato.py
"""
from __future__ import annotations
import re, sys, csv, requests
from io import StringIO
from pathlib import Path
from bs4 import BeautifulSoup

BASE   = "https://transparencia.riobranco.ac.gov.br"
URL_C  = f"{BASE}/contrato/"
SEMSA_ID = "4065"  # confirmado na sessão anterior

s = requests.Session()
s.headers.update({"User-Agent": "Sentinela/3.0"})

# ── 1. ViewState + form_id + exercícios disponíveis ──────────────────────────
r = s.get(URL_C, timeout=20); r.raise_for_status()
html = r.text

vs_m = re.search(r'javax\.faces\.ViewState[^>]*value="([^"]+)"', html)
vs   = vs_m.group(1) if vs_m else ""

form_m = re.search(r'<form[^>]+id="(j_idt\d+)"', html)
form_id = form_m.group(1) if form_m else "j_idt35"

exerc_m = re.search(
    r'<select[^>]+id="([^"]*[Ee]xercicio[^"]*)"[^>]*>(.*?)</select>',
    html, re.DOTALL)
if exerc_m:
    exerc_id = exerc_m.group(1)
    exerc_opts = re.findall(
        r'<option[^>]+value="([^"]+)"[^>]*>([^<]+)</option>',
        exerc_m.group(2))
else:
    exerc_id = ""
    exerc_opts = []

print(f"form_id   : {form_id}")
print(f"ViewState : {vs[:40]}...")
print(f"exerc_id  : {exerc_id}")
print(f"exercícios disponíveis: {exerc_opts}")

# ── 2. Campo de secretaria/unidade: id do autocomplete ───────────────────────
ac_m = re.search(
    r'id="([^"]*(?:[Ss]ecretar|[Uu]nidade|acSecretar|acUnidade)[^"]*)"',
    html)
ac_id = ac_m.group(1) if ac_m else ""
print(f"\nautocomplete_id: {ac_id}")

# Descobre também o input hidden que guarda o value selecionado
hidden_m = re.search(rf'id="({re.escape(ac_id)}_hinput)"', html) if ac_id else None
hidden_id = hidden_m.group(1) if hidden_m else (ac_id + "_hinput" if ac_id else "")
print(f"hidden_id      : {hidden_id}")

# ── 3. Botão de pesquisa / export ─────────────────────────────────────────────
btn_m = re.search(
    r'id="([^"]*(?:[Pp]esquisar|btnProcurar|[Bb]uscar)[^"]*)"', html)
btn_id = btn_m.group(1) if btn_m else ""
print(f"btn_pesquisar  : {btn_id}")

exp_m = re.search(
    r'id="([^"]*(?:[Ee]xport|[Cc]sv|[Cc]sv|[Dd]ownload|[Ee]xcel)[^"]*)"', html)
exp_id = exp_m.group(1) if exp_m else ""
print(f"btn_export     : {exp_id}")

# ── 4. Busca real: 2024 + SEMSA, tenta export CSV ─────────────────────────────
# Descobre o value de 2024 nos exercícios
ano = 2024
ano_val = str(ano)
for val, label in exerc_opts:
    if str(ano) in label:
        ano_val = val; break

payload: dict = {
    form_id: form_id,
    "javax.faces.ViewState": vs,
}
if exerc_id:
    payload[exerc_id] = ano_val
if hidden_id:
    payload[hidden_id] = SEMSA_ID
if ac_id:
    payload[ac_id] = "SEMSA"
if btn_id:
    payload[btn_id] = btn_id

print(f"\nPayload busca: {list(payload.keys())}")

r2 = s.post(URL_C, data=payload, timeout=30)
print(f"POST status  : {r2.status_code} | bytes: {len(r2.content)}")

# Tenta parsear como CSV
for enc in ("utf-8", "utf-8-sig", "latin1"):
    try:
        text = r2.content.decode(enc)
        sample = "\n".join(text.splitlines()[:3])
        if "<html" not in sample.lower() and "<" not in sample[:20]:
            print(f"\n=== CSV ({enc}) — primeiras 3 linhas ===")
            reader = csv.reader(StringIO(text), delimiter=";")
            for i, row in enumerate(reader):
                if i > 5: break
                print(row)
            break
    except Exception:
        continue
else:
    # Tentou CSV mas parece HTML — imprime headers e primeiras linhas da tabela
    soup = BeautifulSoup(r2.text, "html.parser")
    trs = soup.find_all("tr")
    print(f"\n=== HTML resposta — {len(trs)} <tr> encontrados ===")
    for tr in trs[:6]:
        print([td.get_text(strip=True) for td in tr.find_all(["td","th"])])

# ── 5. Detalhe de um contrato: confirma campos fornecedor/CNPJ ───────────────
det_url = f"{BASE}/contrato/ver/2129156/"
r3 = s.get(det_url, timeout=20)
soup3 = BeautifulSoup(r3.text, "html.parser")
text3 = soup3.get_text("\n", strip=True)
print(f"\n=== Detalhe {det_url} ===")
for kw in ("Contratad", "Fornecedor", "CNPJ", "CPF", "Empresa",
           "Razão", "Objeto", "Valor", "Vigência"):
    for line in text3.splitlines():
        if kw.lower() in line.lower() and len(line) < 200:
            print(f"  {line.strip()}")
            break
