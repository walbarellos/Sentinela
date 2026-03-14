from __future__ import annotations

import hashlib
import json
from io import StringIO
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]

TEXT_INDEX_DDL = """
CREATE TABLE IF NOT EXISTS ops_artifact_text_index (
    index_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    event_type VARCHAR,
    source_id VARCHAR NOT NULL,
    label VARCHAR,
    kind VARCHAR,
    path VARCHAR,
    suffix VARCHAR,
    text_sha256 VARCHAR,
    text_chars INTEGER,
    line_count INTEGER,
    content_text VARCHAR,
    metadata_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

TEXT_INDEX_VIEW = """
CREATE OR REPLACE VIEW v_ops_artifact_text_index AS
SELECT *
FROM ops_artifact_text_index
ORDER BY case_id, source_type, label, path
"""


def _resolve_relpath(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()


def _html_to_text(content: str) -> str:
    try:
        from bs4 import BeautifulSoup

        return BeautifulSoup(content, "html.parser").get_text("\n", strip=True)
    except Exception:
        return content


def _extract_text(path: Path) -> tuple[str | None, dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix in {".md", ".txt"}:
        return path.read_text(encoding="utf-8", errors="replace"), {"mode": "text"}

    if suffix == ".json":
        raw = path.read_text(encoding="utf-8", errors="replace")
        try:
            return json.dumps(json.loads(raw), ensure_ascii=False, indent=2, sort_keys=True), {"mode": "json"}
        except json.JSONDecodeError:
            return raw, {"mode": "json_raw"}

    if suffix == ".csv":
        raw = path.read_text(encoding="utf-8", errors="replace")
        try:
            df = pd.read_csv(StringIO(raw))
            return df.to_csv(index=False), {"mode": "csv", "rows": str(len(df)), "cols": str(len(df.columns))}
        except Exception:
            return raw, {"mode": "csv_raw"}

    if suffix in {".html", ".htm"}:
        return _html_to_text(path.read_text(encoding="utf-8", errors="replace")), {"mode": "html_text"}

    if suffix == ".pdf":
        txt_fallback = path.with_suffix(".txt")
        if txt_fallback.exists():
            return txt_fallback.read_text(encoding="utf-8", errors="replace"), {"mode": "pdf_txt_fallback"}
        return None, {"mode": "pdf_without_text"}

    return None, {"mode": "unsupported"}


def ensure_ops_search_index(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(TEXT_INDEX_DDL)
    con.execute("ALTER TABLE ops_artifact_text_index ADD COLUMN IF NOT EXISTS event_type VARCHAR")
    con.execute(TEXT_INDEX_VIEW)


def sync_ops_search_index(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_search_index(con)
    con.execute("DELETE FROM ops_artifact_text_index")

    candidates: list[dict[str, str]] = []

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_case_artifact" in tables:
        for row in con.execute(
            """
            SELECT artifact_id, case_id, label, kind, path
            FROM ops_case_artifact
            WHERE exists AND path IS NOT NULL
            """
        ).fetchall():
            candidates.append(
                {
                    "index_id": f"artifact:{row[0]}",
                    "case_id": row[1],
                    "source_type": "artifact",
                    "event_type": "ARTIFACT_INDEXED",
                    "source_id": row[0],
                    "label": row[2],
                    "kind": row[3],
                    "path": row[4],
                }
            )

    if "ops_case_inbox_document" in tables:
        for row in con.execute(
            """
            SELECT inbox_doc_id, case_id, documento_chave, categoria_documental, file_path
            FROM ops_case_inbox_document
            WHERE file_exists AND file_path IS NOT NULL
            """
        ).fetchall():
            candidates.append(
                {
                    "index_id": f"inbox:{row[0]}",
                    "case_id": row[1],
                    "source_type": "inbox",
                    "event_type": "INBOX_DOCUMENT",
                    "source_id": row[0],
                    "label": row[2],
                    "kind": row[3],
                    "path": row[4],
                }
            )

    indexed = 0
    for item in candidates:
        path = _resolve_relpath(item["path"])
        if not path or not path.exists():
            continue
        content_text, meta = _extract_text(path)
        if not content_text:
            continue
        con.execute(
            """
            INSERT INTO ops_artifact_text_index (
                index_id, case_id, source_type, event_type, source_id, label, kind, path, suffix,
                text_sha256, text_chars, line_count, content_text, metadata_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                item["index_id"],
                item["case_id"],
                item["source_type"],
                item["event_type"],
                item["source_id"],
                item["label"],
                item["kind"],
                item["path"],
                path.suffix.lower(),
                _sha256_text(content_text),
                len(content_text),
                len(content_text.splitlines()),
                content_text,
                json.dumps(meta, ensure_ascii=False),
            ],
        )
        indexed += 1

    return {"indexed_docs": indexed, "candidates": len(candidates)}
