from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import duckdb
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[2]

SEMANTIC_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_semantic_issue (
    issue_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    comparator VARCHAR NOT NULL,
    field_key VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    left_label VARCHAR,
    left_value VARCHAR,
    center_label VARCHAR,
    center_value VARCHAR,
    right_label VARCHAR,
    right_value VARCHAR,
    rationale VARCHAR,
    source_refs_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

SEMANTIC_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_semantic_issue AS
SELECT *
FROM ops_case_semantic_issue
ORDER BY case_id, severity DESC, comparator, field_key
"""


def ensure_ops_semantic(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(SEMANTIC_DDL)
    con.execute(SEMANTIC_VIEW)


def _resolve_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


def _read_text(path_value: str | None) -> str:
    path = _resolve_path(path_value)
    if not path or not path.exists():
        return ""
    suffix = path.suffix.lower()
    if suffix in {".html", ".htm"}:
        raw = path.read_text(encoding="utf-8", errors="replace")
        return BeautifulSoup(raw, "html.parser").get_text("\n", strip=True)
    if suffix == ".pdf":
        txt = path.with_suffix(".txt")
        if txt.exists():
            return txt.read_text(encoding="utf-8", errors="replace")
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value


def _extract_label_table(soup: BeautifulSoup) -> dict[str, str]:
    payload: dict[str, str] = {}
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) != 2:
            continue
        left = cells[0].get_text(" ", strip=True).replace(":", "").strip()
        right = cells[1].get_text(" ", strip=True).strip()
        if left and right:
            payload[left] = right
    return payload


def _extract_contract_html(path_value: str | None) -> dict[str, Any]:
    path = _resolve_path(path_value)
    if not path or not path.exists():
        return {}
    soup = BeautifulSoup(path.read_text(encoding="utf-8", errors="replace"), "html.parser")
    labels = _extract_label_table(soup)
    items: list[str] = []
    for row in soup.select("div#tabItens table tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        item_name = cells[0].get_text(" ", strip=True)
        if item_name:
            items.append(item_name)
    return {
        "numero_contrato": labels.get("Número do Contrato"),
        "numero_processo": labels.get("Número do Processo"),
        "objeto": labels.get("Objeto"),
        "valor": labels.get("Valor"),
        "itens": items,
        "text": soup.get_text("\n", strip=True),
    }


def _extract_licitacao_html(path_value: str | None) -> dict[str, Any]:
    path = _resolve_path(path_value)
    if not path or not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(raw, "html.parser")
    labels = _extract_label_table(soup)
    text = soup.get_text("\n", strip=True)
    proposal_suppliers: list[str] = []
    for row in soup.select("div#tabPropostasELances table tr"):
        cells = row.find_all("td")
        if len(cells) >= 3:
            supplier = cells[2].get_text(" ", strip=True)
            if supplier:
                proposal_suppliers.append(supplier)
    return {
        "processo_compra": labels.get("Processo de Compra"),
        "pregao_numero": re.search(r"PREG[AÃ]O ELETR[ÔO]NICO SRP N[ºO]\s*([0-9/.-]+)", text, re.I).group(1)
        if re.search(r"PREG[AÃ]O ELETR[ÔO]NICO SRP N[ºO]\s*([0-9/.-]+)", text, re.I)
        else None,
        "licitacoes_e": re.search(r"\b(10[0-9]{5,})\b", text).group(1) if re.search(r"\b(10[0-9]{5,})\b", text) else None,
        "objeto": next((line for line in text.splitlines() if "Contratação de empresa especializada" in line), ""),
        "proposal_suppliers": proposal_suppliers,
        "text": text,
    }


def _extract_publication(path_value: str | None) -> dict[str, Any]:
    text = _read_text(path_value)
    return {
        "pregao_numero": re.search(r"PREG[AÃ]O ELETR[ÔO]NICO SRP N[ºO]\s*([0-9/.-]+)", text, re.I).group(1)
        if re.search(r"PREG[AÃ]O ELETR[ÔO]NICO SRP N[ºO]\s*([0-9/.-]+)", text, re.I)
        else None,
        "licitacoes_e": re.search(r"\b(10[0-9]{5,})\b", text).group(1) if re.search(r"\b(10[0-9]{5,})\b", text) else None,
        "objeto": next((line for line in text.splitlines() if "Objeto:" in line or "que tem como objeto" in line.lower()), ""),
        "text": text,
    }


def _extract_dossie_findings(path_value: str | None) -> dict[str, Any]:
    text = _read_text(path_value)
    findings: dict[str, Any] = {"text": text}
    match = re.search(
        r"Item `1`: (?P<item>.+?) \| qtd .*? \| propostas `(?P<propostas>True|False)` \| edital `(?P<edital>True|False)`",
        text,
        re.I,
    )
    if match:
        findings.update(
            {
                "item": match.group("item"),
                "propostas": match.group("propostas").lower() == "true",
                "edital": match.group("edital").lower() == "true",
            }
        )
    return findings


def _issue(
    *,
    case_id: str,
    comparator: str,
    field_key: str,
    status: str,
    severity: str,
    left_label: str,
    left_value: str | None,
    center_label: str,
    center_value: str | None,
    right_label: str,
    right_value: str | None,
    rationale: str,
    source_refs: list[str],
) -> dict[str, Any]:
    return {
        "issue_id": f"{case_id}:{comparator}:{field_key}",
        "case_id": case_id,
        "comparator": comparator,
        "field_key": field_key,
        "status": status,
        "severity": severity,
        "left_label": left_label,
        "left_value": left_value,
        "center_label": center_label,
        "center_value": center_value,
        "right_label": right_label,
        "right_value": right_value,
        "rationale": rationale,
        "source_refs_json": json.dumps(source_refs, ensure_ascii=False),
    }


def _artifact_map(con: duckdb.DuckDBPyConnection, case_id: str) -> dict[str, str]:
    rows = con.execute(
        """
        SELECT label, path
        FROM ops_case_artifact
        WHERE case_id = ? AND exists AND path IS NOT NULL
        """,
        [case_id],
    ).fetchall()
    return {str(label): str(path) for label, path in rows}


def _build_rb_semantic(case_id: str, artifacts: dict[str, str]) -> list[dict[str, Any]]:
    if "contrato_detail" not in artifacts or "licitacao_detail" not in artifacts:
        return []

    contract = _extract_contract_html(artifacts.get("contrato_detail"))
    licitacao = _extract_licitacao_html(artifacts.get("licitacao_detail"))
    edital = _extract_publication(artifacts.get("cpl_publicacao_1554_pdf") or artifacts.get("cpl_publicacao_1554_html"))
    retificacao = _extract_publication(artifacts.get("cpl_publicacao_1640_pdf") or artifacts.get("cpl_publicacao_1640_html"))
    dossie = _extract_dossie_findings(artifacts.get("dossie_rb_sus"))

    if not contract or not licitacao:
        return []

    issues: list[dict[str, Any]] = []
    process_ok = str(licitacao.get("processo_compra") or "").startswith(str(contract.get("numero_processo") or ""))
    issues.append(
        _issue(
            case_id=case_id,
            comparator="contrato_x_licitacao",
            field_key="numero_processo",
            status="COERENTE" if process_ok else "DIVERGENTE",
            severity="MEDIO" if process_ok else "ALTO",
            left_label="Contrato",
            left_value=contract.get("numero_processo"),
            center_label="Licitacao",
            center_value=licitacao.get("processo_compra"),
            right_label="Publicacao",
            right_value=edital.get("pregao_numero"),
            rationale="O numero do processo do contrato deve apontar para a licitacao-mae materializada no portal.",
            source_refs=[artifacts["contrato_detail"], artifacts["licitacao_detail"], artifacts.get("cpl_publicacao_1554_pdf") or artifacts.get("cpl_publicacao_1554_html")],
        )
    )

    object_ok = _normalize_text(contract.get("objeto")) == _normalize_text(edital.get("objeto") or retificacao.get("objeto") or licitacao.get("objeto"))
    issues.append(
        _issue(
            case_id=case_id,
            comparator="contrato_x_edital",
            field_key="objeto",
            status="COERENTE" if object_ok else "DIVERGENTE",
            severity="MEDIO" if object_ok else "ALTO",
            left_label="Contrato",
            left_value=contract.get("objeto"),
            center_label="Edital",
            center_value=edital.get("objeto") or retificacao.get("objeto"),
            right_label="Licitacao",
            right_value=licitacao.get("objeto"),
            rationale="Objeto contratual deve permanecer aderente ao texto publicado do certame.",
            source_refs=[artifacts["contrato_detail"], artifacts["licitacao_detail"], artifacts.get("cpl_publicacao_1554_pdf") or artifacts.get("cpl_publicacao_1554_html"), artifacts.get("cpl_publicacao_1640_pdf") or artifacts.get("cpl_publicacao_1640_html")],
        )
    )

    contract_items = [item for item in contract.get("itens", []) if item and "item" not in item.lower()]
    critical_item = next((item for item in contract_items if "perfurocortante" in _normalize_text(item)), contract_items[0] if contract_items else "")
    edital_text = " ".join(filter(None, [edital.get("text"), retificacao.get("text"), licitacao.get("text")]))
    item_in_edital = _normalize_text(critical_item) in _normalize_text(edital_text) if critical_item else False
    issues.append(
        _issue(
            case_id=case_id,
            comparator="item_x_edital",
            field_key="item_critico",
            status="COERENTE" if item_in_edital else "DIVERGENTE",
            severity="MEDIO" if item_in_edital else "ALTO",
            left_label="Item contratual",
            left_value=critical_item,
            center_label="Edital/retificacao",
            center_value="presente" if item_in_edital else "ausente",
            right_label="Base usada",
            right_value=edital.get("pregao_numero") or retificacao.get("pregao_numero"),
            rationale="Item contratual critico deve aparecer nas pecas publicadas do edital ou da retificacao.",
            source_refs=[artifacts["contrato_detail"], artifacts["licitacao_detail"], artifacts.get("cpl_publicacao_1554_pdf") or artifacts.get("cpl_publicacao_1554_html"), artifacts.get("cpl_publicacao_1640_pdf") or artifacts.get("cpl_publicacao_1640_html")],
        )
    )

    if dossie.get("item") and _normalize_text(dossie.get("item")) == _normalize_text(critical_item):
        proposal_status = "COERENTE" if dossie.get("propostas") else "DIVERGENTE"
        proposal_value = "presente" if dossie.get("propostas") else "ausente"
        proposal_rationale = "O dossie consolidado do caso registra a presenca ou ausencia do item nas propostas congeladas."
        proposal_sources = [artifacts.get("dossie_rb_sus", "")]
    else:
        proposal_status = "INSUFICIENTE"
        proposal_value = "sem proposta congelada localmente"
        proposal_rationale = "Nao ha artefato local suficiente para afirmar presenca do item nas propostas."
        proposal_sources = [artifacts["licitacao_detail"]]
    issues.append(
        _issue(
            case_id=case_id,
            comparator="item_x_propostas",
            field_key="item_critico",
            status=proposal_status,
            severity="ALTO" if proposal_status == "DIVERGENTE" else "MEDIO",
            left_label="Item contratual",
            left_value=critical_item,
            center_label="Propostas",
            center_value=proposal_value,
            right_label="Fornecedores mapeados",
            right_value=", ".join(licitacao.get("proposal_suppliers", [])[:3]) or "sem fornecedor mapeado",
            rationale=proposal_rationale,
            source_refs=proposal_sources,
        )
    )
    return issues


def sync_ops_semantic_analysis(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_semantic(con)
    con.execute("DELETE FROM ops_case_semantic_issue")

    try:
        case_rows = con.execute(
            """
            SELECT case_id, family
            FROM ops_case_registry
            WHERE family = 'rb_sus_contrato'
            ORDER BY case_id
            """
        ).fetchall()
    except duckdb.Error:
        return {"rows_written": 0, "cases": 0}

    issues: list[dict[str, Any]] = []
    for case_id, _family in case_rows:
        artifacts = _artifact_map(con, case_id)
        issues.extend(_build_rb_semantic(case_id, artifacts))

    for row in issues:
        con.execute(
            """
            INSERT INTO ops_case_semantic_issue (
                issue_id, case_id, comparator, field_key, status, severity,
                left_label, left_value, center_label, center_value, right_label,
                right_value, rationale, source_refs_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                row["issue_id"],
                row["case_id"],
                row["comparator"],
                row["field_key"],
                row["status"],
                row["severity"],
                row["left_label"],
                row["left_value"],
                row["center_label"],
                row["center_value"],
                row["right_label"],
                row["right_value"],
                row["rationale"],
                row["source_refs_json"],
            ],
        )

    return {"rows_written": len(issues), "cases": len(case_rows)}
