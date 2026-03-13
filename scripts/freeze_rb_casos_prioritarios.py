from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

from src.ingest.riobranco_http import fetch_html
import enrich_rb_contratos_ocr as contract_ocr

DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "evidencias_rb_sus_prioritarios"
CASE_DIRS = {
    "3895": OUT_DIR / "caso_3895",
    "3898": OUT_DIR / "caso_3898",
}
TIMEOUT = 120


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path.rstrip("/")).name or "index"
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in name)


def ensure_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Sentinela/3.0",
            "Accept-Encoding": "identity",
        }
    )
    return session


def fetch_case_rows(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, object]]:
    rows = con.execute(
        """
        SELECT
            numero_contrato,
            numero_processo,
            ano,
            secretaria,
            objeto,
            valor_referencia_brl,
            fornecedor,
            cnpj,
            detail_url
        FROM rb_contratos
        WHERE numero_contrato IN ('3895', '3898')
        ORDER BY numero_contrato
        """
    ).fetchall()
    cases: dict[str, dict[str, object]] = {}
    for row in rows:
        cases[str(row[0])] = {
            "numero_contrato": str(row[0]),
            "numero_processo": str(row[1] or ""),
            "ano": int(row[2] or 0),
            "secretaria": str(row[3] or ""),
            "objeto": str(row[4] or ""),
            "valor_referencia_brl": float(row[5] or 0),
            "fornecedor": str(row[6] or ""),
            "cnpj": str(row[7] or ""),
            "detail_url": str(row[8] or ""),
        }
    if "3895" not in cases or "3898" not in cases:
        raise SystemExit("Casos 3895/3898 nao encontrados em rb_contratos.")
    return cases


def write_text(path: Path, content: str) -> dict[str, object]:
    path.write_text(content, encoding="utf-8")
    return {
        "path": str(path.relative_to(ROOT)),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def write_bytes(path: Path, content: bytes) -> dict[str, object]:
    path.write_bytes(content)
    return {
        "path": str(path.relative_to(ROOT)),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def fetch_contract_bundle(session: requests.Session, case: dict[str, object], target_dir: Path) -> list[dict[str, object]]:
    detail_url = str(case["detail_url"])
    detail_html = fetch_html(session, detail_url, timeout=TIMEOUT)
    files: list[dict[str, object]] = []
    detail_path = target_dir / f"contrato_{case['numero_contrato']}_detail.html"
    meta = write_text(detail_path, detail_html)
    meta.update({"source_url": detail_url, "kind": "contract_detail_html"})
    files.append(meta)

    pdf_url, arquivo_id = contract_ocr.extract_contract_pdf_url(detail_html)
    if pdf_url:
        pdf_path = target_dir / f"contrato_{case['numero_contrato']}_anexo_{arquivo_id or slug_from_url(pdf_url)}.pdf"
        response = session.get(pdf_url, timeout=TIMEOUT)
        response.raise_for_status()
        meta = write_bytes(pdf_path, response.content)
        meta.update({"source_url": pdf_url, "kind": "contract_attachment_pdf", "arquivo_id": arquivo_id})
        files.append(meta)

    report_path = target_dir / f"contrato_{case['numero_contrato']}_relatorio.pdf"
    contract_ocr.fetch_report_pdf(
        session,
        detail_url=detail_url,
        detail_html=detail_html,
        target_path=report_path,
    )
    meta = {
        "path": str(report_path.relative_to(ROOT)),
        "sha256": sha256_file(report_path),
        "size_bytes": report_path.stat().st_size,
        "source_url": detail_url,
        "kind": "contract_report_pdf",
    }
    files.append(meta)
    return files


def fetch_html_page(session: requests.Session, url: str, target_path: Path, kind: str) -> dict[str, object]:
    html = fetch_html(session, url, timeout=TIMEOUT)
    meta = write_text(target_path, html)
    meta.update({"source_url": url, "kind": kind})
    return meta


def fetch_publicacao_bundle(session: requests.Session, publicacao_id: str, target_dir: Path) -> list[dict[str, object]]:
    url = f"https://cpl.riobranco.ac.gov.br/publicacao/{publicacao_id}"
    html = fetch_html(session, url, timeout=TIMEOUT)
    files: list[dict[str, object]] = []
    html_path = target_dir / f"cpl_publicacao_{publicacao_id}.html"
    meta = write_text(html_path, html)
    meta.update({"source_url": url, "kind": "cpl_publicacao_html"})
    files.append(meta)

    soup = BeautifulSoup(html, "html.parser")
    for index, anchor in enumerate(soup.find_all("a", href=True), start=1):
        href = anchor["href"]
        if not (href.lower().endswith(".pdf") or "/notices/" in href):
            continue
        pdf_url = href if href.startswith("http") else f"https://cpl.riobranco.ac.gov.br{href}"
        response = session.get(pdf_url, timeout=TIMEOUT)
        response.raise_for_status()
        pdf_path = target_dir / f"cpl_publicacao_{publicacao_id}_{index}.pdf"
        meta = write_bytes(pdf_path, response.content)
        meta.update({"source_url": pdf_url, "kind": "cpl_publicacao_pdf"})
        files.append(meta)
    return files


def build_manifest(cases: dict[str, dict[str, object]], evidence: dict[str, list[dict[str, object]]]) -> str:
    payload = {
        "generated_at": datetime.now().isoformat(),
        "database": str(DB_PATH.relative_to(ROOT)),
        "cases": cases,
        "evidence": evidence,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def build_index_md(cases: dict[str, dict[str, object]], evidence: dict[str, list[dict[str, object]]]) -> str:
    lines = [
        "# Evidencias Congeladas - Casos Prioritarios SUS Rio Branco",
        "",
        f"Gerado em `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`.",
        "",
        "Este indice preserva localmente as paginas e PDFs usados nos dois casos prioritarios municipais.",
        "",
    ]
    for numero in ("3895", "3898"):
        case = cases[numero]
        lines.extend(
            [
                f"## Caso {numero}",
                "",
                f"- Processo: `{case['numero_processo']}`",
                f"- Secretaria: `{case['secretaria']}`",
                f"- Objeto: {case['objeto']}",
                f"- Fornecedor: {case['fornecedor'] or '(nao resolvido)'}",
                f"- CNPJ: `{case['cnpj'] or ''}`",
                "",
                "### Arquivos",
                "",
            ]
        )
        for item in evidence[numero]:
            lines.append(
                f"- `{item['kind']}`: `{item['path']}` | sha256 `{item['sha256']}` | origem {item['source_url']}"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    cases = fetch_case_rows(con)
    con.close()

    session = ensure_session()
    evidence: dict[str, list[dict[str, object]]] = {"3895": [], "3898": []}

    for numero, case in cases.items():
        target_dir = CASE_DIRS[numero]
        target_dir.mkdir(parents=True, exist_ok=True)
        evidence[numero].extend(fetch_contract_bundle(session, case, target_dir))

    licitacao_url = "https://transparencia.riobranco.ac.gov.br/licitacao/ver/2274334/"
    evidence["3898"].append(
        fetch_html_page(
            session,
            licitacao_url,
            CASE_DIRS["3898"] / "licitacao_2274334_detail.html",
            "licitacao_detail_html",
        )
    )
    for publicacao_id in ("1554", "1640"):
        evidence["3898"].extend(fetch_publicacao_bundle(session, publicacao_id, CASE_DIRS["3898"]))

    manifest_path = OUT_DIR / "manifest_rb_sus_prioritarios.json"
    manifest_path.write_text(build_manifest(cases, evidence), encoding="utf-8")

    index_path = OUT_DIR / "README.md"
    index_path.write_text(build_index_md(cases, evidence), encoding="utf-8")

    print(index_path)
    print(manifest_path)
    for numero in ("3895", "3898"):
        for item in evidence[numero]:
            print(item["path"])


if __name__ == "__main__":
    main()
