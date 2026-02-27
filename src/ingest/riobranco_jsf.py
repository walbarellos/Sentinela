import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

def extract_viewstate(html: str) -> str or None:
    soup = BeautifulSoup(html, "html.parser")
    vs = soup.find("input", {"name": "javax.faces.ViewState"})
    return vs["value"] if vs and vs.has_attr("value") else None

def parse_partial_xml_updates(xml_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        if xml_text.startswith("<?xml"):
            xml_text = xml_text[xml_text.find("?>")+2:].strip()
        root = ET.fromstring(f"<root>{xml_text}</root>")
        for upd in root.findall(".//update"):
            uid = upd.get("id")
            if uid: out[uid] = upd.text or ""
    except Exception: pass
    return out

def find_button_id_by_label(html: str, label: str) -> str or None:
    """Busca ID de botões Mojarra ou PrimeFaces pelo texto."""
    soup = BeautifulSoup(html, "html.parser")
    # Procura em links, botões e inputs
    candidates = soup.find_all(["a", "button", "input"])
    
    for node in candidates:
        # Pega texto do nó ou o atributo 'value' (para inputs)
        txt = (node.get_text() or node.get("value") or "").strip().upper()
        if label.upper() not in txt:
            continue
            
        onclick = node.get("onclick", "") or ""
        
        # 1. Tenta padrão PrimeFaces.ab({s:'ID',...})
        pf_match = re.search(r"PrimeFaces\.ab\(\{\s*s\s*:\s*'([^']+)'", onclick)
        if pf_match: return pf_match.group(1)
        
        # 2. Tenta padrão mojarra.jsfcljs(...,{'ID':'ID'},...)
        mj_match = re.search(r"\{\s*'([^']+)'\s*:\s*'[^']+'\s*\}", onclick)
        if mj_match: return mj_match.group(1)
        
        # 3. Fallback: ID do próprio elemento se tiver onclick
        if onclick and node.get("id"): return node.get("id")

    return None
