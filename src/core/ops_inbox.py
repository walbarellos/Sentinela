from __future__ import annotations

import csv
import hashlib
import os
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.core.ops_runtime import begin_pipeline_run, ensure_ops_runtime, finish_pipeline_run
from src.core.ops_timeline import ensure_ops_timeline


ROOT = Path(__file__).resolve().parents[2]
ENTREGA_DIR = (
    ROOT
    / "docs"
    / "Claude-march"
    / "patch_claude"
    / "claude_update"
    / "patch"
    / "entrega_denuncia_atual"
)

INBOX_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_inbox_document (
    inbox_doc_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    destino VARCHAR,
    eixo VARCHAR,
    documento_chave VARCHAR,
    categoria_documental VARCHAR,
    descricao_documento VARCHAR,
    status_documento VARCHAR,
    protocolo VARCHAR,
    recebido_em DATE,
    file_path VARCHAR,
    file_exists BOOLEAN,
    file_sha256 VARCHAR,
    size_bytes BIGINT,
    notas VARCHAR,
    source_index_path VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

INBOX_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_inbox_document AS
SELECT *
FROM ops_case_inbox_document
ORDER BY case_id, destino, eixo, documento_chave
"""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_ops_inbox(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(INBOX_DDL)
    con.execute(INBOX_VIEW)
    ensure_ops_timeline(con)


def inbox_specs() -> dict[str, dict[str, Any]]:
    return {
        "cedimp:saude_societario:13325100000130": {
            "index_csv": ENTREGA_DIR / "cedimp_respostas" / "cedimp_respostas_index.csv",
            "upload_dir": ENTREGA_DIR / "cedimp_respostas" / "anexos",
            "base_dir": ENTREGA_DIR,
            "workflow_commands": [
                [".venv/bin/python", "scripts/sync_vinculo_societario_saude_respostas.py"],
                [".venv/bin/python", "scripts/sync_vinculo_societario_saude_maturidade.py"],
                [".venv/bin/python", "scripts/export_vinculo_societario_saude_respostas.py"],
                [".venv/bin/python", "scripts/validate_cedimp_quality.py"],
                [".venv/bin/python", "scripts/sync_ops_case_registry.py"],
            ],
        }
    }


def get_case_inbox_spec(case_id: str) -> dict[str, Any] | None:
    return inbox_specs().get(case_id)


def load_case_inbox_index(case_id: str) -> list[dict[str, Any]]:
    spec = get_case_inbox_spec(case_id)
    if not spec:
        return []
    index_csv = spec["index_csv"]
    if not index_csv.exists():
        return []
    with index_csv.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def sync_ops_inbox(con: duckdb.DuckDBPyConnection, case_id: str | None = None) -> dict[str, Any]:
    ensure_ops_runtime(con)
    ensure_ops_inbox(con)
    specs = inbox_specs()
    target_case_ids = [case_id] if case_id else list(specs)
    rows_written = 0

    for current_case_id in target_case_ids:
        spec = specs.get(current_case_id)
        if not spec:
            continue
        base_dir = spec["base_dir"]
        index_csv = spec["index_csv"]
        rows = load_case_inbox_index(current_case_id)
        con.execute("DELETE FROM ops_case_inbox_document WHERE case_id = ?", [current_case_id])
        for row in rows:
            relpath = (row.get("file_relpath") or "").strip() or None
            file_path = base_dir / relpath if relpath else None
            exists = bool(file_path and file_path.exists())
            con.execute(
                """
                INSERT INTO ops_case_inbox_document (
                    inbox_doc_id, case_id, destino, eixo, documento_chave,
                    categoria_documental, descricao_documento, status_documento,
                    protocolo, recebido_em, file_path, file_exists, file_sha256,
                    size_bytes, notas, source_index_path, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    f"{current_case_id}:{row.get('documento_chave')}",
                    current_case_id,
                    row.get("destino"),
                    row.get("eixo"),
                    row.get("documento_chave"),
                    row.get("categoria_documental"),
                    row.get("descricao_documento"),
                    "ARQUIVO_NAO_LOCALIZADO" if relpath and not exists else row.get("status_documento"),
                    row.get("protocolo"),
                    (row.get("recebido_em") or "").strip() or None,
                    str(file_path.relative_to(ROOT)) if file_path else None,
                    exists,
                    _sha256_file(file_path) if exists and file_path else None,
                    file_path.stat().st_size if exists and file_path else None,
                    row.get("notas"),
                    str(index_csv.relative_to(ROOT)),
                ],
            )
            rows_written += 1

    return {"case_id": case_id, "rows_written": rows_written}


def upload_case_inbox_document(
    *,
    case_id: str,
    documento_chave: str,
    filename: str,
    payload: bytes,
    protocolo: str | None = None,
    notas: str | None = None,
    recebido_em: date | None = None,
) -> dict[str, Any]:
    spec = get_case_inbox_spec(case_id)
    if not spec:
        raise ValueError("Caso sem caixa de entrada configurada.")

    upload_dir = spec["upload_dir"]
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in filename)
    target = upload_dir / f"{documento_chave}__{safe_name}"
    target.write_bytes(payload)

    rows = load_case_inbox_index(case_id)
    matched = False
    for row in rows:
        if row.get("documento_chave") != documento_chave:
            continue
        row["status_documento"] = "RECEBIDO"
        row["protocolo"] = protocolo or row.get("protocolo") or ""
        row["recebido_em"] = (recebido_em or date.today()).isoformat()
        row["file_relpath"] = str(target.relative_to(spec["base_dir"]))
        row["notas"] = notas or row.get("notas") or ""
        matched = True
        break
    if not matched:
        raise ValueError("Documento-chave não encontrado no índice da caixa.")

    index_csv = spec["index_csv"]
    with index_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    return {
        "file_relpath": str(target.relative_to(ROOT)),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def run_case_workflow(case_id: str) -> dict[str, Any]:
    spec = get_case_inbox_spec(case_id)
    if not spec or not spec.get("workflow_commands"):
        raise ValueError("Caso sem workflow operacional configurado.")

    con = duckdb.connect(str(ROOT / "data" / "sentinela_analytics.duckdb"))
    ensure_ops_runtime(con)
    run_id = begin_pipeline_run(
        con,
        pipeline=f"ops_case_workflow:{case_id}",
        trigger_mode="streamlit",
        actor="app",
        details={"case_id": case_id},
    )
    con.close()

    steps: list[dict[str, Any]] = []
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not current_pythonpath else f"{ROOT}:{current_pythonpath}"
    try:
        for command in spec["workflow_commands"]:
            started = datetime.now()
            result = subprocess.run(
                command,
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )
            steps.append(
                {
                    "command": command,
                    "returncode": result.returncode,
                    "stdout": result.stdout[-4000:],
                    "stderr": result.stderr[-4000:],
                    "duration_s": round((datetime.now() - started).total_seconds(), 2),
                }
            )
            if result.returncode != 0:
                con = duckdb.connect(str(ROOT / "data" / "sentinela_analytics.duckdb"))
                try:
                    ensure_ops_runtime(con)
                    finish_pipeline_run(
                        con,
                        run_id,
                        status="failed",
                        details={"case_id": case_id, "steps": steps},
                        error_text=f"Falha em {' '.join(command)} :: {result.stderr[-500:]}",
                    )
                finally:
                    con.close()
                raise RuntimeError(f"Falha em {' '.join(command)} :: {result.stderr[-500:]}")

        con = duckdb.connect(str(ROOT / "data" / "sentinela_analytics.duckdb"))
        try:
            ensure_ops_runtime(con)
            finish_pipeline_run(
                con,
                run_id,
                status="success",
                details={"case_id": case_id, "steps": steps},
                rows_written=len(steps),
            )
        finally:
            con.close()
        return {"case_id": case_id, "steps": steps, "status": "success"}
    except Exception:
        raise
