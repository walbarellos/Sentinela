import logging
import requests
from src.ingest.riobranco_jsf import (
    extract_viewstate,
    parse_partial_xml_updates,
    find_mojarra_button_id_by_label,
    find_all_ver_ids_in_html,
)

log = logging.getLogger("Sentinela.ServidorList")

class RioBrancoServidorList:
    BASE_URL = "https://transparencia.riobranco.ac.gov.br/servidor/"

    def __init__(self, session: requests.Session or None = None):
        self.s = session or requests.Session()
        self.s.headers.update({
            "User-Agent": "Sentinela/3.0",
        })

    def fetch_all_ids(self) -> list[str]:
        log.info("Acessando portal de pessoal...")
        r = self.s.get(self.BASE_URL, headers={"Accept": "text/html"})
        r.raise_for_status()
        vs = extract_viewstate(r.text)
        if not vs:
            raise RuntimeError("Não consegui capturar ViewState do /servidor/")

        btn_id = find_mojarra_button_id_by_label(r.text, "TODOS")
        if not btn_id:
            log.warning("Botão 'TODOS' não encontrado. Extraindo IDs da primeira página.")
            return find_all_ver_ids_in_html(r.text, "servidor")

        log.info(f"Botão 'TODOS' mapeado: {btn_id}. Solicitando carga completa...")

        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": btn_id,
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "Formulario",
            btn_id: btn_id,
            "Formulario": "Formulario",
            "javax.faces.ViewState": vs,
        }
        
        headers = {
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": self.BASE_URL,
        }

        rx = self.s.post(self.BASE_URL, data=payload, headers=headers)
        rx.raise_for_status()

        updates = parse_partial_xml_updates(rx.text)
        html_tab = updates.get("Formulario") or "
".join(updates.values())

        ids = find_all_ver_ids_in_html(html_tab, "servidor")
        if not ids:
            ids = find_all_ver_ids_in_html(r.text, "servidor")

        log.info(f"Sucesso operacional: {len(ids)} IDs capturados.")
        return ids

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    bot = RioBrancoServidorList()
    ids = bot.fetch_all_ids()
    print(f"Total: {len(ids)} IDs. Exemplo: {ids[:5]}")
