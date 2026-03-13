from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
import tempfile
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
PDF_CACHE = ROOT / "data" / "cache" / "rb_contratos_pdf"
PUBLIC_CNPJ_PREFIXES = ("04034583",)
CNPJ_RE = re.compile(r"\d{2}[\.,\-]?\d{3}[\.,\-]?\d{3}[\/\-]?\d{4}[\.,\-]?\d{2}")

DDL_OCR = """
CREATE TABLE IF NOT EXISTS rb_contratos_pdf_ocr (
    row_id VARCHAR PRIMARY KEY,
    contrato_row_id VARCHAR,
    numero_contrato VARCHAR,
    arquivo_id VARCHAR,
    detail_url VARCHAR,
    pdf_url VARCHAR,
    pdf_path VARCHAR,
    fornecedor_ocr VARCHAR,
    cnpj_ocr VARCHAR,
    source_kind VARCHAR,
    status VARCHAR,
    error_msg VARCHAR,
    text_excerpt VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def short_id(prefix: str, *parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{hashlib.md5(raw.encode('utf-8')).hexdigest()[:16]}"


def valid_cnpj(digits: str) -> bool:
    if not digits or len(digits) != 14 or len(set(digits)) == 1:
        return False

    def calc(base: str, factors: list[int]) -> str:
        total = sum(int(d) * f for d, f in zip(base, factors))
        remainder = total % 11
        return "0" if remainder < 2 else str(11 - remainder)

    d1 = calc(digits[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = calc(digits[:12] + d1, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return digits[-2:] == d1 + d2


def normalize_cnpj_candidate(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if valid_cnpj(digits):
        return digits

    candidates: set[str] = set()
    if len(digits) > 14:
        for idx in range(len(digits)):
            candidate = digits[:idx] + digits[idx + 1 :]
            if valid_cnpj(candidate):
                candidates.add(candidate)
    if len(digits) < 14:
        for idx in range(len(digits) + 1):
            candidate = digits[:idx] + "0" + digits[idx:]
            if valid_cnpj(candidate):
                candidates.add(candidate)
    if len(candidates) == 1:
        return next(iter(candidates))
    return ""


def is_public_cnpj(digits: str) -> bool:
    return any(digits.startswith(prefix) for prefix in PUBLIC_CNPJ_PREFIXES)


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_OCR)
    PDF_CACHE.mkdir(parents=True, exist_ok=True)


def cleanup_invalid_cnpjs(con: duckdb.DuckDBPyConnection) -> int:
    cleaned = 0
    rows = con.execute(
        """
        SELECT row_id, cnpj
        FROM rb_contratos
        WHERE cnpj IS NOT NULL AND cnpj <> ''
        """
    ).fetchall()
    for row_id, cnpj in rows:
        digits = re.sub(r"\D", "", str(cnpj or ""))
        if digits and not valid_cnpj(digits):
            con.execute("UPDATE rb_contratos SET cnpj = '' WHERE row_id = ?", [row_id])
            cleaned += 1

    ocr_rows = con.execute(
        """
        SELECT row_id, cnpj_ocr
        FROM rb_contratos_pdf_ocr
        WHERE cnpj_ocr IS NOT NULL AND cnpj_ocr <> ''
        """
    ).fetchall()
    for row_id, cnpj in ocr_rows:
        digits = re.sub(r"\D", "", str(cnpj or ""))
        if digits and not valid_cnpj(digits):
            con.execute(
                """
                UPDATE rb_contratos_pdf_ocr
                SET cnpj_ocr = '', status = 'no_match'
                WHERE row_id = ?
                """,
                [row_id],
            )
    return cleaned


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Sentinela/3.0",
            "Accept-Encoding": "identity",
        }
    )
    return session


def fetch_pdf(session: requests.Session, pdf_url: str, target_path: Path) -> Path:
    response = session.get(pdf_url, timeout=120)
    response.raise_for_status()
    target_path.write_bytes(response.content)
    return target_path


def extract_contract_pdf_url(detail_html: str) -> tuple[str, str]:
    soup = BeautifulSoup(detail_html, "html.parser")
    rows = soup.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) != 2:
            continue
        file_link = row.find("a", href=True)
        file_type = rb_contratos_sync.fix_text(cells[1].get_text(" ", strip=True)).upper()
        if not file_link or "/arquivo?id=" not in file_link.get("href", ""):
            continue
        href = file_link["href"]
        file_url = href if href.startswith("http") else f"{BASE}{href}"
        file_id = re.search(r"id=(\d+)", href)
        arquivo_id = file_id.group(1) if file_id else ""
        if file_type == "CONTRATO":
            return file_url, arquivo_id
    for row in rows:
        cells = row.find_all("td")
        if len(cells) != 2:
            continue
        file_link = row.find("a", href=True)
        file_type = rb_contratos_sync.fix_text(cells[1].get_text(" ", strip=True)).upper()
        if file_link and "/arquivo?id=" in file_link.get("href", "") and file_type == "EXTRATO DO CONTRATO":
            href = file_link["href"]
            file_url = href if href.startswith("http") else f"{BASE}{href}"
            file_id = re.search(r"id=(\d+)", href)
            arquivo_id = file_id.group(1) if file_id else ""
            return file_url, arquivo_id
    return "", ""


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


def fetch_report_pdf(
    session: requests.Session,
    *,
    detail_url: str,
    detail_html: str,
    target_path: Path,
) -> Path:
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
    target_path.write_bytes(response.content)
    return target_path


def ocr_pdf_text(pdf_path: Path, pages: int) -> str:
    with tempfile.TemporaryDirectory(prefix="rbocr_") as temp_dir:
        prefix = Path(temp_dir) / "page"
        subprocess.run(
            [
                "pdftoppm",
                "-f",
                "1",
                "-l",
                str(pages),
                "-png",
                str(pdf_path),
                str(prefix),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        chunks: list[str] = []
        for image_path in sorted(Path(temp_dir).glob("page-*.png")):
            proc = subprocess.run(
                [
                    "tesseract",
                    str(image_path),
                    "stdout",
                    "-l",
                    "por",
                    "--psm",
                    "6",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            chunks.append(proc.stdout)
        return "\n".join(chunks)


def extract_supplier_and_cnpj(ocr_text: str) -> tuple[str, str]:
    text = rb_contratos_sync.fix_text(ocr_text)
    normalized = re.sub(r"\s+", " ", text)
    normalized_ascii = rb_contratos_sync.normalize_text(text)

    def has_identifier_context(haystack: str, start: int, end: int) -> bool:
        window = haystack[max(0, start - 96) : min(len(haystack), end + 96)]
        markers = ("CNPJ", "INSCRIT", "CONTRATADA", "EMPRESA", "PESSOA JUR")
        return any(marker in window for marker in markers)

    supplier_patterns = [
        r"CONTRATADA.{0,40}?(?:EMPRESA|FIRMA)\s*[:\-]?\s*([A-Z0-9 .&/\-]{5,140}?)(?:\s+NA FORMA ABAIXO|\s*,?\s*PESSOA JUR|\s*,?\s*INSCRIT)",
        r"DO OUTRO LADO.{0,40}?(?:EMPRESA|FIRMA)\s+([A-Z0-9 .&/\-]{5,140}?)(?:\s*,?\s*PESSOA JUR|\s*,?\s*INSCRIT|\s+NA FORMA ABAIXO)",
        r"A EMPRESA\s+([A-Z0-9 .&/\-]{5,140}?)(?:\s*,?\s*INSCRIT|\s+NA FORMA ABAIXO)",
        r"CONTRATADA,\s*([A-Z0-9 .&/\-]{5,140}?)(?:\s*,?\s*PESSOA JUR|\s*,?\s*INSCRIT)",
    ]
    fornecedor = ""
    upper_text = normalized_ascii
    for pattern in supplier_patterns:
        match = re.search(pattern, upper_text)
        if match:
            fornecedor = re.sub(r"\s+", " ", match.group(1)).strip(" ,.-")
            break

    cnpj_candidates: list[str] = []
    for match in CNPJ_RE.finditer(normalized_ascii):
        candidate = match.group(0)
        if not has_identifier_context(normalized_ascii, *match.span()) and not fornecedor:
            continue
        normalized_candidate = normalize_cnpj_candidate(candidate)
        if normalized_candidate and not is_public_cnpj(normalized_candidate):
            cnpj_candidates.append(normalized_candidate)

    if not cnpj_candidates:
        for match in re.finditer(r"\d[\d\.\,\-\/ ]{12,24}\d", normalized_ascii):
            candidate = match.group(0)
            if not has_identifier_context(normalized_ascii, *match.span()) and not fornecedor:
                continue
            normalized_candidate = normalize_cnpj_candidate(candidate)
            if normalized_candidate and not is_public_cnpj(normalized_candidate):
                cnpj_candidates.append(normalized_candidate)

    chosen_cnpj = ""
    if cnpj_candidates:
        if fornecedor:
            anchor = upper_text.find("CONTRATADA")
            if anchor == -1:
                anchor = upper_text.find(fornecedor[:30])
            if anchor != -1:
                nearby = []
                for candidate in cnpj_candidates:
                    pos = upper_text.find(candidate[:8])
                    distance = abs(pos - anchor) if pos != -1 else 10**9
                    nearby.append((distance, candidate))
                nearby.sort()
                chosen_cnpj = nearby[0][1]
            else:
                chosen_cnpj = cnpj_candidates[0]
        else:
            chosen_cnpj = cnpj_candidates[0]

    return fornecedor, chosen_cnpj


def run_ocr_with_retry(pdf_path: Path, pages: int) -> tuple[str, str, str]:
    attempts = [max(1, pages)]
    if max(attempts) < 4:
        attempts.append(4)
    best_fornecedor = ""
    best_cnpj = ""
    best_text = ""
    for attempt_pages in attempts:
        ocr_text = ocr_pdf_text(pdf_path, pages=attempt_pages)
        fornecedor, cnpj = extract_supplier_and_cnpj(ocr_text)
        if (fornecedor and not best_fornecedor) or (cnpj and not best_cnpj) or not best_text:
            best_fornecedor = fornecedor or best_fornecedor
            best_cnpj = cnpj or best_cnpj
            best_text = ocr_text
        if best_fornecedor and best_cnpj:
            break
    return best_fornecedor, best_cnpj, best_text


def upsert_ocr_result(
    con: duckdb.DuckDBPyConnection,
    *,
    contrato_row_id: str,
    numero_contrato: str,
    arquivo_id: str,
    detail_url: str,
    pdf_url: str,
    pdf_path: str,
    fornecedor: str,
    cnpj: str,
    source_kind: str,
    status: str,
    error_msg: str,
    text_excerpt: str,
) -> None:
    row_id = short_id("RBOCR", contrato_row_id, arquivo_id or pdf_url)
    con.execute(
        """
        INSERT INTO rb_contratos_pdf_ocr (
            row_id, contrato_row_id, numero_contrato, arquivo_id, detail_url, pdf_url, pdf_path,
            fornecedor_ocr, cnpj_ocr, source_kind, status, error_msg, text_excerpt, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (row_id) DO UPDATE SET
            contrato_row_id = excluded.contrato_row_id,
            numero_contrato = excluded.numero_contrato,
            arquivo_id = excluded.arquivo_id,
            detail_url = excluded.detail_url,
            pdf_url = excluded.pdf_url,
            pdf_path = excluded.pdf_path,
            fornecedor_ocr = excluded.fornecedor_ocr,
            cnpj_ocr = excluded.cnpj_ocr,
            source_kind = excluded.source_kind,
            status = excluded.status,
            error_msg = excluded.error_msg,
            text_excerpt = excluded.text_excerpt,
            updated_at = excluded.updated_at
        """,
        [
            row_id,
            contrato_row_id,
            numero_contrato,
            arquivo_id,
            detail_url,
            pdf_url,
            pdf_path,
            fornecedor,
            cnpj,
            source_kind,
            status,
            error_msg,
            text_excerpt,
            datetime.now(),
        ],
    )


def update_contract_row(
    con: duckdb.DuckDBPyConnection,
    *,
    row_id: str,
    fornecedor: str,
    cnpj: str,
) -> None:
    con.execute(
        """
        UPDATE rb_contratos
        SET
            fornecedor = CASE
                WHEN (fornecedor IS NULL OR fornecedor = '') AND ? <> '' THEN ?
                ELSE fornecedor
            END,
            cnpj = CASE
                WHEN (cnpj IS NULL OR cnpj = '') AND ? <> '' THEN ?
                ELSE cnpj
            END
        WHERE row_id = ?
        """,
        [fornecedor, fornecedor, cnpj, cnpj, row_id],
    )


def iter_contracts(con: duckdb.DuckDBPyConnection, limit: int | None) -> list[tuple]:
    sql = """
        SELECT row_id, numero_contrato, detail_url
        FROM rb_contratos
        WHERE detail_url IS NOT NULL AND detail_url <> ''
          AND (
                cnpj IS NULL OR cnpj = ''
                OR fornecedor IS NULL OR fornecedor = ''
              )
        ORDER BY valor_referencia_brl DESC, numero_contrato
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return con.execute(sql).fetchall()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enriquece contratos de Rio Branco via OCR dos PDFs anexos."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--limit", type=int, default=None, help="Limita contratos analisados.")
    parser.add_argument("--pages", type=int, default=2, help="Páginas máximas de OCR por contrato.")
    parser.add_argument("--dry-run", action="store_true", help="Não grava updates em rb_contratos.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect(str(args.db_path))
    ensure_tables(con)
    rb_contratos_sync.ensure_tables(con)
    cleaned = cleanup_invalid_cnpjs(con)

    session = build_session()
    rows = iter_contracts(con, args.limit)
    print(f"contratos_para_enriquecer={len(rows)}")
    if cleaned:
        print(f"invalid_cnpjs_reset={cleaned}")

    updated = 0
    for contrato_row_id, numero_contrato, detail_url in rows:
        detail_html = fetch_html(session, detail_url, timeout=30)
        pdf_url, arquivo_id = extract_contract_pdf_url(detail_html)
        source_kind = "attachment_pdf_ocr"
        if pdf_url:
            pdf_path = PDF_CACHE / f"{arquivo_id or short_id('PDF', contrato_row_id)}.pdf"
        else:
            pdf_path = PDF_CACHE / f"report_{short_id('PDF', contrato_row_id)}.pdf"
            source_kind = "detail_report_pdf_ocr"
        try:
            if pdf_url:
                fetch_pdf(session, pdf_url, pdf_path)
            else:
                fetch_report_pdf(
                    session,
                    detail_url=detail_url,
                    detail_html=detail_html,
                    target_path=pdf_path,
                )
            fornecedor, cnpj, ocr_text = run_ocr_with_retry(pdf_path, pages=args.pages)
            excerpt = re.sub(r"\s+", " ", ocr_text[:1200]).strip()
            status = "ok" if cnpj else "no_match"
            upsert_ocr_result(
                con,
                contrato_row_id=contrato_row_id,
                numero_contrato=numero_contrato,
                arquivo_id=arquivo_id,
                detail_url=detail_url,
                pdf_url=pdf_url,
                pdf_path=str(pdf_path),
                fornecedor=fornecedor,
                cnpj=cnpj,
                source_kind=source_kind,
                status=status,
                error_msg="",
                text_excerpt=excerpt,
            )
            if not args.dry_run and (fornecedor or cnpj):
                update_contract_row(con, row_id=contrato_row_id, fornecedor=fornecedor, cnpj=cnpj)
                updated += 1
            print(
                f"{numero_contrato or contrato_row_id}: status={status} fornecedor={fornecedor[:60]!r} cnpj={cnpj or '-'}"
            )
        except Exception as exc:
            upsert_ocr_result(
                con,
                contrato_row_id=contrato_row_id,
                numero_contrato=numero_contrato,
                arquivo_id=arquivo_id,
                detail_url=detail_url,
                pdf_url=pdf_url,
                pdf_path=str(pdf_path),
                fornecedor="",
                cnpj="",
                source_kind=source_kind,
                status="error",
                error_msg=str(exc)[:400],
                text_excerpt="",
            )
            print(f"{numero_contrato or contrato_row_id}: error={exc}")

    if not args.dry_run:
        rb_contratos_sync.build_views(con)
        n_insights = rb_contratos_sync.build_insights(con)
        n_with_cnpj = con.execute(
            "SELECT COUNT(*) FROM rb_contratos WHERE cnpj <> ''"
        ).fetchone()[0]
        print(f"updated={updated}")
        print(f"rb_contratos_com_cnpj={n_with_cnpj}")
        print(f"insights={n_insights}")

    con.close()


if __name__ == "__main__":
    main()
