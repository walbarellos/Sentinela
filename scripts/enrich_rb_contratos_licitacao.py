from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

from src.ingest.riobranco_http import fetch_html
import sync_rb_contratos as rb_contratos_sync

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
BASE = "https://transparencia.riobranco.ac.gov.br"
AUTO_UPDATE_CONFIDENCE = 75
PDF_CACHE = ROOT / "data" / "cache" / "rb_contratos_pdf"
NUMERIC_BRL_RE = re.compile(r"^\d{1,3}(?:\.\d{3})*,\d{2}$")

# Processo do contrato -> licitacao-mae confirmada no portal
PROCESS_HINTS = {
    "3006": {
        "licitacao_id": "2274334",
        "ano_licitacao": 2023,
        "descricao": "SEMSA material e servicos graficos (processo 3006/2023)",
    },
    "3799": {
        "licitacao_id": "2364182",
        "ano_licitacao": 2024,
        "descricao": "Aquisição de Gêneros Alimentícios para UAA/CAPS/Divisão de Saúde",
    },
}

DDL_MATCH = """
CREATE TABLE IF NOT EXISTS rb_contratos_licitacao_match (
    row_id VARCHAR PRIMARY KEY,
    contrato_row_id VARCHAR,
    numero_contrato VARCHAR,
    numero_processo VARCHAR,
    licitacao_id VARCHAR,
    licitacao_url VARCHAR,
    proposta_id VARCHAR,
    proposta_url VARCHAR,
    pessoa_id VARCHAR,
    pessoa_url VARCHAR,
    fornecedor VARCHAR,
    cnpj VARCHAR,
    exact_price_matches INTEGER,
    matched_items INTEGER,
    contract_items INTEGER,
    confidence INTEGER,
    status VARCHAR,
    method VARCHAR,
    raw_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


@dataclass
class ProposalItem:
    descricao: str
    descricao_norm: str
    quantidade: float
    valor_unitario: float
    valor_total: float
    lote_numero: str


@dataclass
class Proposal:
    proposta_id: str
    proposta_url: str
    pessoa_id: str
    pessoa_url: str
    fornecedor: str
    cnpj: str
    items: list[ProposalItem]


def norm_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", (value or "").upper())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^A-Z0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def parse_brl(value: object) -> float:
    text = str(value or "")
    text = re.sub(r"[^\d,.-]", "", text)
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def short_id(prefix: str, *parts: object) -> str:
    return rb_contratos_sync.short_id(prefix, *parts)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Sentinela/3.0",
            "Accept-Encoding": "identity",
        }
    )
    return session


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_MATCH)
    PDF_CACHE.mkdir(parents=True, exist_ok=True)


def fetch_contract_json(session: requests.Session, detail_url: str) -> dict:
    html = fetch_html(session, detail_url, timeout=30)
    viewstates = re.findall(r'name="javax\.faces\.ViewState"[^>]+value="([^"]+)"', html)
    if not viewstates:
        raise RuntimeError(f"ViewState ausente em {detail_url}")
    response = session.post(
        detail_url,
        data={
            "Formulario": "Formulario",
            "Formulario:j_idt72:j_idt88": "Formulario:j_idt72:j_idt88",
            "javax.faces.ViewState": viewstates[0],
        },
        timeout=120,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": detail_url,
            "Origin": BASE,
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        raise RuntimeError(f"JSON vazio em {detail_url}")
    return payload[0]


def extract_report_pdf_control(detail_html: str) -> tuple[str, str]:
    viewstates = re.findall(
        r'name="javax\.faces\.ViewState"[^>]+value="([^"]+)"',
        detail_html,
    )
    viewstate = viewstates[0] if viewstates else ""
    control_match = re.search(
        r"mojarra\.jsfcljs\(document\.getElementById\('Formulario'\),\{'([^']+)':'[^']+'\},'_blank'\)",
        detail_html,
    )
    control = control_match.group(1) if control_match else "Formulario:j_idt72:j_idt78"
    return viewstate, control


def fetch_report_pdf(session: requests.Session, *, detail_url: str, detail_html: str) -> Path:
    cache_path = PDF_CACHE / f"bridge_{short_id('PDF', detail_url)}.pdf"
    if cache_path.exists():
        return cache_path

    viewstate, control = extract_report_pdf_control(detail_html)
    if not viewstate or not control:
        raise RuntimeError("controle PDF/ViewState não encontrado no detalhe do contrato")

    response = session.post(
        detail_url,
        data={
            "Formulario": "Formulario",
            control: control,
            "javax.faces.ViewState": viewstate,
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": detail_url,
            "Origin": BASE,
        },
        timeout=120,
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise RuntimeError("exportacao PDF do detalhe nao retornou PDF valido")
    cache_path.write_bytes(response.content)
    return cache_path


def parse_contract_items_from_report_text(text: str) -> list[dict]:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    start_idx = 0
    for idx, line in enumerate(lines):
        upper = line.upper()
        if upper in {"VALOR TOTAL", "UNITÁRIO TOTAL"} or upper.endswith("UNITÁRIO TOTAL"):
            start_idx = idx + 1

    items: list[dict] = []
    idx = start_idx
    while idx < len(lines):
        line = lines[idx]
        if line.startswith("Webpublico - Sistema"):
            break

        desc_lines: list[str] = []
        while (
            idx < len(lines)
            and lines[idx].lower() != "null"
            and not NUMERIC_BRL_RE.match(lines[idx])
            and not lines[idx].startswith("Webpublico - Sistema")
        ):
            desc_lines.append(lines[idx])
            idx += 1

        if not desc_lines:
            idx += 1
            continue

        if idx < len(lines) and lines[idx].lower() == "null":
            idx += 1

        numbers: list[str] = []
        while idx < len(lines) and len(numbers) < 3:
            if NUMERIC_BRL_RE.match(lines[idx]):
                numbers.append(lines[idx])
            elif lines[idx].startswith("Webpublico - Sistema"):
                break
            idx += 1

        if len(numbers) != 3:
            continue

        quantidade = parse_brl(numbers[0])
        valor_unitario = parse_brl(numbers[1])
        valor_total = parse_brl(numbers[2])
        if quantidade > 0 and valor_total > 0 and valor_unitario == 0:
            valor_unitario = round(valor_total / quantidade, 4)

        descricao = " ".join(desc_lines)
        items.append(
            {
                "descricao": descricao,
                "descricao_norm": norm_text(descricao),
                "quantidade": quantidade,
                "valor_unitario": valor_unitario,
                "valor_total": valor_total,
            }
        )
    return items


def fetch_contract_items_from_report_pdf(session: requests.Session, detail_url: str) -> list[dict]:
    detail_html = fetch_html(session, detail_url, timeout=30)
    pdf_path = fetch_report_pdf(session, detail_url=detail_url, detail_html=detail_html)
    proc = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_contract_items_from_report_text(proc.stdout)


def parse_contract_items(contract_json: dict) -> list[dict]:
    raw_items = (contract_json or {}).get("Itens") or {}
    items: list[dict] = []
    for entry in raw_items.values():
        descricao = str(entry.get("Item", "")).strip()
        items.append(
            {
                "descricao": descricao,
                "descricao_norm": norm_text(descricao),
                "quantidade": float(entry.get("Quantidade", 0) or 0),
                "valor_unitario": float(entry.get("Valor Unitário", 0) or 0),
                "valor_total": float(entry.get("Valor Total", 0) or 0),
            }
        )
    return items


def fetch_contract_items(session: requests.Session, detail_url: str) -> tuple[list[dict], str]:
    try:
        contract_json = fetch_contract_json(session, detail_url)
        items = parse_contract_items(contract_json)
        if items:
            return items, "detail_json"
    except Exception:
        pass

    items = fetch_contract_items_from_report_pdf(session, detail_url)
    return items, "detail_report_pdf"


def parse_licitacao_proposals(session: requests.Session, licitacao_id: str) -> list[Proposal]:
    lic_url = f"{BASE}/licitacao/ver/{licitacao_id}/"
    html = fetch_html(session, lic_url, timeout=30)
    soup = BeautifulSoup(html, "html.parser")
    tab = soup.find("div", {"id": "tabPropostasELances"})
    if tab is None:
        raise RuntimeError(f"tabPropostasELances ausente em {lic_url}")

    proposals: list[Proposal] = []
    for tr in tab.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        proposta_link = tr.find("a", href=re.compile(r"/proposta-fornecedor/ver/\d+/"))
        pessoa_link = tds[2].find("a", href=re.compile(r"/pessoa/ver/\d+/"))
        if not proposta_link or not pessoa_link:
            continue

        proposta_href = proposta_link["href"]
        proposta_url = proposta_href if proposta_href.startswith("http") else f"{BASE}{proposta_href}"
        proposta_id_match = re.search(r"/proposta-fornecedor/ver/(\d+)/", proposta_href)
        proposta_id = proposta_id_match.group(1) if proposta_id_match else ""

        pessoa_href = pessoa_link["href"]
        pessoa_url = pessoa_href if pessoa_href.startswith("http") else f"{BASE}{pessoa_href}"
        pessoa_id_match = re.search(r"/pessoa/ver/(\d+)/", pessoa_href)
        pessoa_id = pessoa_id_match.group(1) if pessoa_id_match else ""

        fornecedor = re.sub(r"\s+", " ", tds[2].get_text(" ", strip=True))
        cnpj = parse_pessoa_cnpj(session, pessoa_url)
        items = parse_proposal_items(session, proposta_url)

        proposals.append(
            Proposal(
                proposta_id=proposta_id,
                proposta_url=proposta_url,
                pessoa_id=pessoa_id,
                pessoa_url=pessoa_url,
                fornecedor=fornecedor,
                cnpj=cnpj,
                items=items,
            )
        )
    return proposals


def parse_pessoa_cnpj(session: requests.Session, pessoa_url: str) -> str:
    html = fetch_html(session, pessoa_url, timeout=30)
    soup = BeautifulSoup(html, "html.parser")
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != 2:
            continue
        label = norm_text(tds[0].get_text(" ", strip=True))
        if label == "CPF CPNJ":
            return clean_doc(tds[1].get_text(" ", strip=True))
    return ""


def parse_proposal_items(session: requests.Session, proposta_url: str) -> list[ProposalItem]:
    html = fetch_html(session, proposta_url, timeout=30)
    soup = BeautifulSoup(html, "html.parser")
    items: list[ProposalItem] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        lot_link = tr.find("a", href=re.compile(r"/proposta-fornecedor/lote/ver/\d+/"))
        if not lot_link or len(tds) < 5:
            continue
        lot_href = lot_link["href"]
        lot_url = lot_href if lot_href.startswith("http") else f"{BASE}{lot_href}"
        lot_numero = re.sub(r"\s+", " ", tds[1].get_text(" ", strip=True))
        lot_items = parse_lot_items(session, lot_url, lot_numero)
        items.extend(lot_items)
    return items


def parse_lot_items(session: requests.Session, lot_url: str, lot_numero: str) -> list[ProposalItem]:
    html = fetch_html(session, lot_url, timeout=30)
    soup = BeautifulSoup(html, "html.parser")
    out: list[ProposalItem] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        descricao = re.sub(r"\s+", " ", tds[1].get_text(" ", strip=True))
        out.append(
            ProposalItem(
                descricao=descricao,
                descricao_norm=norm_text(descricao),
                quantidade=parse_brl(tds[2].get_text(" ", strip=True)),
                valor_unitario=parse_brl(tds[3].get_text(" ", strip=True)),
                valor_total=parse_brl(tds[4].get_text(" ", strip=True)),
                lote_numero=lot_numero,
            )
        )
    return out


def iter_candidate_items(contract_desc_norm: str, proposal_items: list[ProposalItem]) -> list[tuple[int, ProposalItem]]:
    candidates: list[tuple[int, ProposalItem]] = []
    for item in proposal_items:
        proposal_desc_norm = item.descricao_norm
        if proposal_desc_norm == contract_desc_norm:
            candidates.append((2, item))
            continue
        if len(proposal_desc_norm) >= 5 and proposal_desc_norm in contract_desc_norm:
            candidates.append((1, item))
            continue
        if len(contract_desc_norm) >= 5 and contract_desc_norm in proposal_desc_norm:
            candidates.append((1, item))
    return candidates


def score_contract_against_proposal(contract_items: list[dict], proposal: Proposal) -> dict:
    matched_items = 0
    exact_price_matches = 0
    close_price_matches = 0
    best_matches: list[dict] = []
    rel_diffs: list[float] = []
    for contract_item in contract_items:
        candidates = iter_candidate_items(contract_item["descricao_norm"], proposal.items)
        if not candidates:
            continue

        _, proposal_item = min(
            candidates,
            key=lambda entry: (
                -entry[0],
                abs(entry[1].valor_unitario - contract_item["valor_unitario"]),
                abs(entry[1].quantidade - contract_item["quantidade"]),
            ),
        )
        matched_items += 1
        diff_abs = abs(proposal_item.valor_unitario - contract_item["valor_unitario"])
        diff_rel = diff_abs / max(contract_item["valor_unitario"], 0.01)
        rel_diffs.append(diff_rel)
        if (
            contract_item["valor_unitario"] > 0
            and proposal_item.valor_unitario > 0
            and diff_abs < 0.011
        ):
            exact_price_matches += 1
        if diff_abs <= max(2.0, contract_item["valor_unitario"] * 0.15):
            close_price_matches += 1
        best_matches.append(
            {
                "contract_item": contract_item["descricao"],
                "proposal_item": proposal_item.descricao,
                "contract_unit": contract_item["valor_unitario"],
                "proposal_unit": proposal_item.valor_unitario,
                "contract_qty": contract_item["quantidade"],
                "proposal_qty": proposal_item.quantidade,
                "diff_abs": diff_abs,
                "diff_rel": diff_rel,
                "lote_numero": proposal_item.lote_numero,
            }
        )

    contract_count = max(len(contract_items), 1)
    coverage = matched_items / contract_count
    close_coverage = close_price_matches / contract_count
    sorted_rels = sorted(rel_diffs)
    median_rel_diff = sorted_rels[len(sorted_rels) // 2] if sorted_rels else 999.0

    confidence = round(45 + 20 * coverage + 10 * close_coverage - 5 * min(median_rel_diff, 5))
    if exact_price_matches == contract_count and contract_count > 0:
        confidence = 97
    elif coverage == 1.0 and close_coverage >= 0.60 and median_rel_diff <= 0.15:
        confidence = max(confidence, 84)
    elif coverage >= 0.80 and close_coverage >= 0.40 and median_rel_diff <= 0.40:
        confidence = max(confidence, 78)
    confidence = max(0, min(confidence, 97))
    return {
        "matched_items": matched_items,
        "exact_price_matches": exact_price_matches,
        "close_price_matches": close_price_matches,
        "contract_items": contract_count,
        "coverage": coverage,
        "median_rel_diff": median_rel_diff,
        "confidence": confidence,
        "matches": best_matches,
    }


def choose_best_proposal(contract_items: list[dict], proposals: list[Proposal]) -> tuple[Proposal | None, dict | None, list[tuple]]:
    scores: list[tuple] = []
    for proposal in proposals:
        score = score_contract_against_proposal(contract_items, proposal)
        scores.append(
            (
                score["confidence"],
                score["exact_price_matches"],
                score["close_price_matches"],
                score["matched_items"],
                -score["median_rel_diff"],
                proposal,
                score,
            )
        )
    scores.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4], item[5].fornecedor), reverse=True)
    if not scores:
        return None, None, []
    best = scores[0]
    runner_up = scores[1] if len(scores) > 1 else None
    best_score = best[6]
    if best_score["matched_items"] <= 0:
        return None, None, scores
    if runner_up:
        runner_score = runner_up[6]
        if (
            best_score["confidence"] == runner_score["confidence"]
            and best_score["matched_items"] == runner_score["matched_items"]
            and best_score["close_price_matches"] == runner_score["close_price_matches"]
            and abs(best_score["median_rel_diff"] - runner_score["median_rel_diff"]) < 0.001
        ):
            return None, None, scores
    if best_score["confidence"] < 45:
        return None, None, scores
    return best[5], best_score, scores


def upsert_match(
    con: duckdb.DuckDBPyConnection,
    *,
    contrato_row_id: str,
    numero_contrato: str,
    numero_processo: str,
    licitacao_id: str,
    proposal: Proposal | None,
    score: dict | None,
    ranking: list[tuple],
) -> None:
    licitacao_url = f"{BASE}/licitacao/ver/{licitacao_id}/"
    status = "matched" if proposal and score and score["confidence"] >= AUTO_UPDATE_CONFIDENCE else "review"
    fornecedor = proposal.fornecedor if proposal else ""
    cnpj = proposal.cnpj if proposal else ""
    proposta_id = proposal.proposta_id if proposal else ""
    proposta_url = proposal.proposta_url if proposal else ""
    pessoa_id = proposal.pessoa_id if proposal else ""
    pessoa_url = proposal.pessoa_url if proposal else ""
    exact_price_matches = int(score["exact_price_matches"]) if score else 0
    matched_items = int(score["matched_items"]) if score else 0
    contract_items = int(score["contract_items"]) if score else 0
    confidence = int(score["confidence"]) if score else 0
    raw_json = json.dumps(
        {
            "ranking": [
                {
                    "fornecedor": item[5].fornecedor,
                    "cnpj": item[5].cnpj,
                    "confidence": item[0],
                    "exact_price_matches": item[1],
                    "close_price_matches": item[2],
                    "matched_items": item[3],
                    "median_rel_diff": -item[4],
                }
                for item in ranking[:5]
            ],
            "matches": score["matches"] if score else [],
        },
        ensure_ascii=False,
    )
    row_id = short_id("RBLIC", contrato_row_id, licitacao_id)
    con.execute(
        """
        INSERT INTO rb_contratos_licitacao_match (
            row_id, contrato_row_id, numero_contrato, numero_processo, licitacao_id, licitacao_url,
            proposta_id, proposta_url, pessoa_id, pessoa_url, fornecedor, cnpj,
            exact_price_matches, matched_items, contract_items, confidence,
            status, method, raw_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (row_id) DO UPDATE SET
            proposta_id = excluded.proposta_id,
            proposta_url = excluded.proposta_url,
            pessoa_id = excluded.pessoa_id,
            pessoa_url = excluded.pessoa_url,
            fornecedor = excluded.fornecedor,
            cnpj = excluded.cnpj,
            exact_price_matches = excluded.exact_price_matches,
            matched_items = excluded.matched_items,
            contract_items = excluded.contract_items,
            confidence = excluded.confidence,
            status = excluded.status,
            method = excluded.method,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        [
            row_id,
            contrato_row_id,
            numero_contrato,
            numero_processo,
            licitacao_id,
            licitacao_url,
            proposta_id,
            proposta_url,
            pessoa_id,
            pessoa_url,
            fornecedor,
            cnpj,
            exact_price_matches,
            matched_items,
            contract_items,
            confidence,
            status,
            "licitacao_item_price_bridge",
            raw_json,
            datetime.now(),
        ],
    )


def update_contract(con: duckdb.DuckDBPyConnection, *, contrato_row_id: str, fornecedor: str, cnpj: str) -> None:
    con.execute(
        """
        UPDATE rb_contratos
        SET
            fornecedor = CASE WHEN (fornecedor IS NULL OR fornecedor = '') AND ? <> '' THEN ? ELSE fornecedor END,
            cnpj = CASE WHEN (cnpj IS NULL OR cnpj = '') AND ? <> '' THEN ? ELSE cnpj END
        WHERE row_id = ?
        """,
        [fornecedor, fornecedor, cnpj, cnpj, contrato_row_id],
    )


def load_pending_contracts(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    return con.execute(
        """
        SELECT row_id, numero_contrato, numero_processo, detail_url
        FROM rb_contratos
        WHERE sus = TRUE
          AND numero_processo IN ({placeholders})
          AND ((cnpj IS NULL OR cnpj = '') OR (fornecedor IS NULL OR fornecedor = ''))
        ORDER BY CAST(numero_contrato AS INTEGER)
        """.format(placeholders=", ".join("?" for _ in PROCESS_HINTS)),
        list(PROCESS_HINTS),
    ).fetchall()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enriquece contratos SUS de Rio Branco via ponte licitacao -> proposta -> fornecedor."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db_path))
    ensure_tables(con)
    rb_contratos_sync.ensure_tables(con)

    session = build_session()
    pending = load_pending_contracts(con)
    if args.limit is not None:
        pending = pending[: args.limit]
    print(f"contratos_pendentes_licitacao={len(pending)}")

    proposal_cache: dict[str, list[Proposal]] = {}
    updated = 0
    for contrato_row_id, numero_contrato, numero_processo, detail_url in pending:
        hint = PROCESS_HINTS.get(str(numero_processo))
        if not hint:
            continue
        licitacao_id = hint["licitacao_id"]
        if licitacao_id not in proposal_cache:
            proposal_cache[licitacao_id] = parse_licitacao_proposals(session, licitacao_id)
        contract_items, source_kind = fetch_contract_items(session, detail_url)
        proposal, score, ranking = choose_best_proposal(contract_items, proposal_cache[licitacao_id])
        upsert_match(
            con,
            contrato_row_id=contrato_row_id,
            numero_contrato=str(numero_contrato),
            numero_processo=str(numero_processo),
            licitacao_id=licitacao_id,
            proposal=proposal,
            score=score,
            ranking=ranking,
        )
        if proposal and score:
            print(
                f"{numero_contrato}: {proposal.fornecedor} {proposal.cnpj} "
                f"| source={source_kind} "
                f"| exact={score['exact_price_matches']} close={score['close_price_matches']} matched={score['matched_items']}/{score['contract_items']} "
                f"| median_rel={score['median_rel_diff']:.2f} confidence={score['confidence']}"
            )
            if not args.dry_run and score["confidence"] >= AUTO_UPDATE_CONFIDENCE:
                update_contract(
                    con,
                    contrato_row_id=contrato_row_id,
                    fornecedor=proposal.fornecedor,
                    cnpj=proposal.cnpj,
                )
                updated += 1
        else:
            print(f"{numero_contrato}: ambiguous source={source_kind}")

    if not args.dry_run:
        rb_contratos_sync.build_views(con)
        n_insights = rb_contratos_sync.build_insights(con)
        print(f"updated={updated}")
        print(f"insights={n_insights}")
    con.close()


if __name__ == "__main__":
    main()
