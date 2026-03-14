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
from src.core.ops_search import ensure_ops_search_index, sync_ops_search_index
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
CEDIMP_CASE_ID = "cedimp:saude_societario:13325100000130"
INDEX_FIELDS = [
    "case_key",
    "cnpj",
    "razao_social",
    "destino",
    "eixo",
    "documento_chave",
    "categoria_documental",
    "descricao_documento",
    "status_documento",
    "protocolo",
    "recebido_em",
    "file_relpath",
    "notas",
]

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


def _write_index_csv(index_csv: Path, rows: list[dict[str, Any]]) -> None:
    index_csv.parent.mkdir(parents=True, exist_ok=True)
    with index_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=INDEX_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _write_case_readme(path: Path, title: str, body: list[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join([f"# {title}", "", *body, ""]), encoding="utf-8")


def ensure_ops_inbox(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(INBOX_DDL)
    con.execute(INBOX_VIEW)
    ensure_ops_timeline(con)
    ensure_ops_search_index(con)


def _cedimp_spec() -> dict[str, Any]:
    return {
        "case_id": CEDIMP_CASE_ID,
        "index_csv": ENTREGA_DIR / "cedimp_respostas" / "cedimp_respostas_index.csv",
        "upload_dir": ENTREGA_DIR / "cedimp_respostas" / "anexos",
        "base_dir": ENTREGA_DIR,
        "workflow_commands": [
            [".venv/bin/python", "scripts/sync_vinculo_societario_saude_respostas.py"],
            [".venv/bin/python", "scripts/sync_vinculo_societario_saude_maturidade.py"],
            [".venv/bin/python", "scripts/export_vinculo_societario_saude_respostas.py"],
            [".venv/bin/python", "scripts/validate_cedimp_quality.py"],
            [".venv/bin/python", "scripts/sync_ops_case_registry.py"],
            [".venv/bin/python", "scripts/sync_ops_timeline.py"],
        ],
    }


def _rb_case_rows(case_id: str) -> list[dict[str, Any]]:
    numero = case_id.split(":")[-1]
    rows = [
        {
            "case_key": case_id,
            "cnpj": "",
            "razao_social": f"Contrato {numero}",
            "destino": "SEMSA",
            "eixo": "contrato_municipal",
            "documento_chave": f"processo_integral_contrato_{numero}",
            "categoria_documental": "PROCESSO_CONTRATO",
            "descricao_documento": f"Processo integral do contrato {numero}, com despacho, termo, anexos e fiscais.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": "",
            "razao_social": f"Contrato {numero}",
            "destino": "SEMSA",
            "eixo": "execucao_contrato",
            "documento_chave": f"notas_fiscais_atestos_{numero}",
            "categoria_documental": "EXECUCAO_ATESTO",
            "descricao_documento": f"Notas fiscais, atestos, glosas e comprovantes de pagamento do contrato {numero}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": "",
            "razao_social": f"Contrato {numero}",
            "destino": "SEMSA",
            "eixo": "contrato_municipal",
            "documento_chave": f"parecer_juridico_{numero}",
            "categoria_documental": "PARECER_JURIDICO",
            "descricao_documento": f"Parecer jurídico e manifestação de controle do contrato {numero}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": "",
            "razao_social": f"Contrato {numero}",
            "destino": "CPL_RIO_BRANCO",
            "eixo": "licitacao_municipal",
            "documento_chave": f"habilitacao_fornecedor_{numero}",
            "categoria_documental": "HABILITACAO_EMPRESA",
            "descricao_documento": f"Documentos de habilitação do fornecedor vinculado ao contrato {numero}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": "",
            "razao_social": f"Contrato {numero}",
            "destino": "CONTROLADORIA_MUNICIPAL",
            "eixo": "controle_interno",
            "documento_chave": f"manifestacao_controle_{numero}",
            "categoria_documental": "MANIFESTACAO_CONTROLE",
            "descricao_documento": f"Manifestação do controle interno sobre o contrato {numero} e sua execução.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
    ]
    if numero == "3895":
        rows.append(
            {
                "case_key": case_id,
                "cnpj": "37306014000148",
                "razao_social": "NORTE DISTRIBUIDORA DE PRODUTOS LTDA",
                "destino": "SEMSA",
                "eixo": "sancao_fornecedor",
                "documento_chave": "consulta_ceis_cnep_contratacao_3895",
                "categoria_documental": "CONSULTA_SANCAO",
                "descricao_documento": "Comprovação documental da consulta de integridade/sanções efetuada antes da contratação da NORTE.",
                "status_documento": "PENDENTE",
                "protocolo": "",
                "recebido_em": "",
                "file_relpath": "",
                "notas": "",
            }
        )
    if numero == "3898":
        rows.extend(
            [
                {
                    "case_key": case_id,
                    "cnpj": "",
                    "razao_social": "Contrato 3898",
                    "destino": "CPL_RIO_BRANCO",
                    "eixo": "licitacao_municipal",
                    "documento_chave": "processo_licitacao_2274334",
                    "categoria_documental": "PROCESSO_LICITACAO",
                    "descricao_documento": "Processo integral da licitação 2274334 / PE SRP 141/2023 com propostas e mapas comparativos.",
                    "status_documento": "PENDENTE",
                    "protocolo": "",
                    "recebido_em": "",
                    "file_relpath": "",
                    "notas": "",
                },
                {
                    "case_key": case_id,
                    "cnpj": "",
                    "razao_social": "Contrato 3898",
                    "destino": "CPL_RIO_BRANCO",
                    "eixo": "divergencia_documental",
                    "documento_chave": "memoria_comparativa_item_3898",
                    "categoria_documental": "MEMORIA_COMPARATIVA",
                    "descricao_documento": "Memória comparativa entre item contratado, edital, retificação e proposta vencedora do item divergente.",
                    "status_documento": "PENDENTE",
                    "protocolo": "",
                    "recebido_em": "",
                    "file_relpath": "",
                    "notas": "",
                },
            ]
        )
    return rows


def _sesacre_case_rows(case_id: str) -> list[dict[str, Any]]:
    cnpj = case_id.split(":")[-1]
    return [
        {
            "case_key": case_id,
            "cnpj": cnpj,
            "razao_social": "",
            "destino": "SESACRE",
            "eixo": "contratacao_estadual",
            "documento_chave": f"processo_integral_fornecedor_{cnpj}",
            "categoria_documental": "PROCESSO_CONTRATACAO",
            "descricao_documento": f"Processo integral de contratação da SESACRE para o fornecedor {cnpj}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": cnpj,
            "razao_social": "",
            "destino": "SESACRE",
            "eixo": "contratacao_estadual",
            "documento_chave": f"ata_ou_contrato_vigente_{cnpj}",
            "categoria_documental": "ATA_CONTRATO",
            "descricao_documento": f"Ata, contrato ou instrumento vigente que embasa a despesa com o fornecedor {cnpj}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": cnpj,
            "razao_social": "",
            "destino": "SESACRE",
            "eixo": "integridade_fornecedor",
            "documento_chave": f"justificativa_manutencao_contratual_{cnpj}",
            "categoria_documental": "JUSTIFICATIVA_CONTRATACAO",
            "descricao_documento": f"Justificativa para manutenção/celebração contratual apesar do cruzamento sancionatório do fornecedor {cnpj}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": cnpj,
            "razao_social": "",
            "destino": "SESACRE",
            "eixo": "integridade_fornecedor",
            "documento_chave": f"consulta_ceis_cnep_{cnpj}",
            "categoria_documental": "CONSULTA_SANCAO",
            "descricao_documento": f"Consulta documental de integridade/sanções do fornecedor {cnpj} no fluxo de contratação.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": cnpj,
            "razao_social": "",
            "destino": "SESACRE",
            "eixo": "execucao_estadual",
            "documento_chave": f"execucao_fiscalizacao_pagamentos_{cnpj}",
            "categoria_documental": "EXECUCAO_FISCALIZACAO",
            "descricao_documento": f"Fiscalização, medição, glosas e pagamentos vinculados ao fornecedor {cnpj}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
        {
            "case_key": case_id,
            "cnpj": cnpj,
            "razao_social": "",
            "destino": "SESACRE",
            "eixo": "habilitacao_empresa",
            "documento_chave": f"habilitacao_empresa_{cnpj}",
            "categoria_documental": "HABILITACAO_EMPRESA",
            "descricao_documento": f"Documentos de habilitação e qualificação da empresa {cnpj}.",
            "status_documento": "PENDENTE",
            "protocolo": "",
            "recebido_em": "",
            "file_relpath": "",
            "notas": "",
        },
    ]


def _rb_case_spec(case_id: str) -> dict[str, Any]:
    numero = case_id.split(":")[-1]
    case_dir = ENTREGA_DIR / "rb_sus_respostas" / f"contrato_{numero}"
    return {
        "case_id": case_id,
        "index_csv": case_dir / "rb_sus_respostas_index.csv",
        "upload_dir": case_dir / "anexos",
        "base_dir": ENTREGA_DIR,
        "seed_rows": _rb_case_rows(case_id),
        "readme_path": case_dir / "README.md",
        "readme_title": f"RB SUS - Caixa do contrato {numero}",
        "readme_body": [
            "Pasta de respostas oficiais do caso municipal priorizado.",
            "",
            "- Preencha ou atualize `rb_sus_respostas_index.csv`.",
            "- Anexos recebidos devem ficar em `anexos/`.",
            "- Depois rerode o workflow do caso pela aba `📂 OPERAÇÕES`.",
        ],
        "workflow_commands": [
            [".venv/bin/python", "scripts/sync_ops_inbox.py", "--case-id", case_id],
            [".venv/bin/python", "scripts/sync_ops_case_registry.py"],
            [".venv/bin/python", "scripts/sync_ops_timeline.py"],
        ],
    }


def _sesacre_case_spec(case_id: str) -> dict[str, Any]:
    cnpj = case_id.split(":")[-1]
    case_dir = ENTREGA_DIR / "sesacre_sancoes_respostas" / f"cnpj_{cnpj}"
    return {
        "case_id": case_id,
        "index_csv": case_dir / "sesacre_sancoes_respostas_index.csv",
        "upload_dir": case_dir / "anexos",
        "base_dir": ENTREGA_DIR,
        "seed_rows": _sesacre_case_rows(case_id),
        "readme_path": case_dir / "README.md",
        "readme_title": f"SESACRE - Caixa do fornecedor {cnpj}",
        "readme_body": [
            "Pasta de respostas oficiais do caso sancionatório estadual.",
            "",
            "- Use o índice CSV para marcar o que foi recebido.",
            "- Anexos recebidos devem ficar em `anexos/`.",
            "- O workflow deste caso atualiza inbox, registry e timeline.",
        ],
        "workflow_commands": [
            [".venv/bin/python", "scripts/sync_ops_inbox.py", "--case-id", case_id],
            [".venv/bin/python", "scripts/sync_ops_case_registry.py"],
            [".venv/bin/python", "scripts/sync_ops_timeline.py"],
        ],
    }


def _seed_case_inbox(spec: dict[str, Any]) -> None:
    index_csv = spec["index_csv"]
    if not index_csv.exists() and spec.get("seed_rows"):
        _write_index_csv(index_csv, spec["seed_rows"])
    readme_path = spec.get("readme_path")
    if readme_path:
        _write_case_readme(readme_path, spec.get("readme_title", "Caixa Operacional"), spec.get("readme_body", []))


def get_case_inbox_spec(case_id: str) -> dict[str, Any] | None:
    if case_id == CEDIMP_CASE_ID:
        return _cedimp_spec()
    if case_id.startswith("rb:contrato:"):
        return _rb_case_spec(case_id)
    if case_id.startswith("sesacre:sancao:"):
        return _sesacre_case_spec(case_id)
    return None


def inbox_specs(con: duckdb.DuckDBPyConnection | None = None) -> dict[str, dict[str, Any]]:
    case_ids = {CEDIMP_CASE_ID}
    if con is not None:
        try:
            tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
            if "ops_case_registry" in tables:
                case_ids.update(row[0] for row in con.execute("SELECT case_id FROM ops_case_registry").fetchall())
        except duckdb.Error:
            pass
    specs: dict[str, dict[str, Any]] = {}
    for case_id in sorted(case_ids):
        spec = get_case_inbox_spec(case_id)
        if spec:
            specs[case_id] = spec
    return specs


def load_case_inbox_index(case_id: str) -> list[dict[str, Any]]:
    spec = get_case_inbox_spec(case_id)
    if not spec:
        return []
    index_csv = spec["index_csv"]
    _seed_case_inbox(spec)
    if not index_csv.exists():
        return []
    with index_csv.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def sync_ops_inbox(con: duckdb.DuckDBPyConnection, case_id: str | None = None) -> dict[str, Any]:
    ensure_ops_runtime(con)
    ensure_ops_inbox(con)
    specs = inbox_specs(con)
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

    search_stats = sync_ops_search_index(con)
    return {"case_id": case_id, "rows_written": rows_written, "indexed_docs": int(search_stats.get("indexed_docs", 0))}


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
