from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[2]
OPS_EXPORT_DIR = (
    ROOT
    / "docs"
    / "Claude-march"
    / "patch_claude"
    / "claude_update"
    / "patch"
    / "entrega_denuncia_atual"
    / "ops_exports"
)


EXPORT_GATE_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_export_gate (
    gate_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    export_mode VARCHAR NOT NULL,
    allowed BOOLEAN NOT NULL,
    blocking_reason VARCHAR,
    rationale VARCHAR,
    disclaimer VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

EXPORT_GATE_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_export_gate AS
SELECT *
FROM ops_case_export_gate
ORDER BY case_id, export_mode
"""

GENERATED_EXPORT_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_generated_export (
    export_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    export_mode VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    sha256 VARCHAR NOT NULL,
    size_bytes BIGINT NOT NULL,
    label VARCHAR NOT NULL,
    actor VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

GENERATED_EXPORT_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_generated_export AS
SELECT *
FROM ops_case_generated_export
ORDER BY created_at DESC, case_id, export_mode
"""


def ensure_ops_export_gate(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(EXPORT_GATE_DDL)
    con.execute(EXPORT_GATE_VIEW)
    con.execute(GENERATED_EXPORT_DDL)
    con.execute(GENERATED_EXPORT_VIEW)


def _safe_disclaimer() -> str:
    return (
        "Este texto relata fatos e contradicoes documentais para apuracao. "
        "Nao imputa culpa, dolo, fraude consumada ou crime, e deve ser usado com as fontes indicadas."
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _case_slug(case_id: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "_", case_id.lower())


def _case_export_dir(case_id: str) -> Path:
    return OPS_EXPORT_DIR / _case_slug(case_id)


def build_generated_export_artifacts(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    ensure_ops_export_gate(con)
    rows = con.execute(
        """
        SELECT export_id, case_id, export_mode, path, sha256, size_bytes, actor, created_at
        FROM v_ops_case_generated_export
        ORDER BY created_at DESC, export_mode
        """
    ).fetchall()
    artifacts: list[dict[str, Any]] = []
    for export_id, case_id, export_mode, path, sha256, size_bytes, actor, created_at in rows:
        relpath = Path(path)
        exists = (ROOT / relpath).exists() if not relpath.is_absolute() else relpath.exists()
        artifacts.append(
            {
                "artifact_id": f"{case_id}:generated_export:{export_id}".replace(" ", "_").replace("/", "_"),
                "case_id": case_id,
                "label": f"saida_controlada_{export_mode.lower()}_{str(created_at).replace(' ', '_')}",
                "kind": "generated_export",
                "path": str(relpath),
                "exists": exists,
                "sha256": sha256,
                "size_bytes": size_bytes,
                "metadata_json": json.dumps(
                    {
                        "export_id": export_id,
                        "export_mode": export_mode,
                        "actor": actor,
                        "created_at": str(created_at),
                    },
                    ensure_ascii=False,
                ),
            }
        )
    return artifacts


def sync_ops_export_gate(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_export_gate(con)
    con.execute("DELETE FROM ops_case_export_gate")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_case_registry" not in tables:
        return {"rows_written": 0, "cases": 0}

    cases = con.execute(
        """
        SELECT case_id, family, estagio_operacional
        FROM ops_case_registry
        ORDER BY case_id
        """
    ).fetchall()

    written = 0
    for case_id, family, estagio in cases:
        guard_count = 0
        if "ops_case_language_guard" in tables:
            guard_count = int(
                con.execute("SELECT COUNT(*) FROM ops_case_language_guard WHERE case_id = ?", [case_id]).fetchone()[0] or 0
            )
        contradiction_count = 0
        if "ops_case_contradiction" in tables:
            contradiction_count = int(
                con.execute("SELECT COUNT(*) FROM ops_case_contradiction WHERE case_id = ?", [case_id]).fetchone()[0] or 0
            )
        documental_count = 0
        if "ops_case_burden_item" in tables:
            documental_count = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM ops_case_burden_item
                    WHERE case_id = ? AND status = 'COMPROVADO_DOCUMENTAL'
                    """,
                    [case_id],
                ).fetchone()[0]
                or 0
            )
        pending_docs = 0
        if "ops_case_burden_item" in tables:
            pending_docs = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM ops_case_burden_item
                    WHERE case_id = ? AND status = 'PENDENTE_DOCUMENTO'
                    """,
                    [case_id],
                ).fetchone()[0]
                or 0
            )

        modes = ["NOTA_INTERNA", "PEDIDO_DOCUMENTAL", "NOTICIA_FATO"]
        for mode in modes:
            allowed = True
            blocking_reason = None
            rationale = "Modo liberado com base no estagio operacional e no lastro documental atual."

            if guard_count > 0:
                allowed = False
                blocking_reason = "Ha flags de linguagem externa no caso; saneie a saida antes de exportar."
                rationale = "O export gate bloqueia qualquer saida externa com linguagem potencialmente acusatoria."
            elif mode == "NOTICIA_FATO":
                if family == "saude_societario":
                    allowed = False
                    blocking_reason = "Caso ainda depende de documentos funcionais e enquadramento humano."
                    rationale = "Casos societario-funcionais nao sobem automaticamente para noticia de fato."
                elif documental_count == 0:
                    allowed = False
                    blocking_reason = "Nao ha itens comprovados documentalmente em quantidade suficiente."
                    rationale = "Noticia de fato exige pelo menos um nucleo documental robusto."
                elif family == "rb_sus_contrato" and contradiction_count == 0 and estagio != "APTO_REPRESENTACAO_PRELIMINAR":
                    allowed = False
                    blocking_reason = "Caso municipal sem contradicao materializada para noticia de fato."
                    rationale = "Para contrato municipal, noticia de fato exige contradicao objetiva ou cruzamento forte."
            elif mode == "PEDIDO_DOCUMENTAL":
                if pending_docs == 0 and family != "saude_societario":
                    rationale = "Modo liberado, mas o caso ja tem pouca pendencia documental."
            elif mode == "NOTA_INTERNA":
                rationale = "Nota interna sempre permitida, desde que o gate de linguagem esteja limpo."

            con.execute(
                """
                INSERT INTO ops_case_export_gate (
                    gate_id, case_id, family, export_mode, allowed, blocking_reason,
                    rationale, disclaimer, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    f"{case_id}:{mode}",
                    case_id,
                    family,
                    mode,
                    allowed,
                    blocking_reason,
                    rationale,
                    _safe_disclaimer(),
                ],
            )
            written += 1

    return {"rows_written": written, "cases": len(cases)}


def _list_from_json(payload: str | None) -> list[Any]:
    try:
        return json.loads(payload or "[]")
    except Exception:
        return []


def build_case_external_text(
    con: duckdb.DuckDBPyConnection,
    *,
    case_id: str,
    export_mode: str,
) -> dict[str, Any]:
    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_case_export_gate" not in tables:
        raise ValueError("Gate de exportacao ainda nao materializado neste banco.")
    gate = con.execute(
        """
        SELECT allowed, blocking_reason, rationale, disclaimer
        FROM ops_case_export_gate
        WHERE case_id = ? AND export_mode = ?
        """,
        [case_id, export_mode],
    ).fetchone()
    if not gate:
        raise ValueError("Gate de exportacao nao materializado para este caso/modo.")
    if not gate[0]:
        raise ValueError(gate[1] or "Modo de exportacao bloqueado para este caso.")

    case = con.execute(
        """
        SELECT
            case_id, family, title, subtitle, subject_name, subject_doc, orgao,
            municipio, uf, classe_achado, estagio_operacional, uso_externo,
            prioridade, valor_referencia_brl, resumo_curto, proximo_passo
        FROM ops_case_registry
        WHERE case_id = ?
        """,
        [case_id],
    ).fetchone()
    if not case:
        raise ValueError("Caso nao encontrado no registry operacional.")

    contradictions = con.execute(
        """
        SELECT title, rationale, next_action
        FROM ops_case_contradiction
        WHERE case_id = ?
        ORDER BY severity DESC, title
        """,
        [case_id],
    ).fetchall()
    burden = con.execute(
        """
        SELECT item_label, status, next_action, source_refs_json
        FROM ops_case_burden_item
        WHERE case_id = ?
        ORDER BY status_order, item_key
        """,
        [case_id],
    ).fetchall()

    assunto_prefix = {
        "PEDIDO_DOCUMENTAL": "ASSUNTO: pedido documental para apuracao preliminar",
        "NOTICIA_FATO": "ASSUNTO: noticia de fato para apuracao preliminar",
        "NOTA_INTERNA": "ASSUNTO: nota interna de triagem probatoria",
    }[export_mode]

    lines = [
        f"{assunto_prefix} :: {case[2]}",
        "",
        "FATO OBJETIVO",
        f"1. Caso: {case[2]}",
        f"2. Sujeito: {case[4] or 'N/D'}" + (f" / documento {case[5]}" if case[5] else ""),
        f"3. Orgao: {case[6] or 'N/D'} / {case[7] or 'N/D'}-{case[8] or 'N/D'}",
        f"4. Classe do achado: {case[9] or 'N/D'}",
        f"5. Resumo: {case[14] or 'N/D'}",
    ]
    if case[13] is not None:
        value_fmt = f"{float(case[13]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        lines.append(f"6. Valor de referencia: R$ {value_fmt}")

    if contradictions:
        lines.extend(["", "CONTRADICOES OBJETIVAS"])
        for idx, item in enumerate(contradictions, start=1):
            lines.append(f"{idx}. {item[0]} — {item[1]}")

    proved = [row for row in burden if row[1] == "COMPROVADO_DOCUMENTAL"]
    pending = [row for row in burden if row[1] == "PENDENTE_DOCUMENTO"]
    if proved:
        lines.extend(["", "BASE DOCUMENTAL MINIMA"])
        for idx, row in enumerate(proved[:5], start=1):
            lines.append(f"{idx}. {row[0]}")
    if export_mode == "PEDIDO_DOCUMENTAL" and pending:
        lines.extend(["", "DOCUMENTOS E DILIGENCIAS RECOMENDADOS"])
        for idx, row in enumerate(pending[:8], start=1):
            lines.append(f"{idx}. {row[0]} — {row[2]}")

    lines.extend(
        [
            "",
            "LIMITE DA CONCLUSAO",
            gate[3],
            "",
            "USO SUGERIDO",
            gate[2],
            "",
            "PROXIMO PASSO",
            case[15] or "Prosseguir com diligencia documental e analise humana.",
        ]
    )

    return {
        "case_id": case_id,
        "export_mode": export_mode,
        "text": "\n".join(lines).strip() + "\n",
    }


def freeze_case_external_text(
    con: duckdb.DuckDBPyConnection,
    *,
    case_id: str,
    export_mode: str,
    actor: str = "app",
) -> dict[str, Any]:
    ensure_ops_export_gate(con)
    payload = build_case_external_text(con, case_id=case_id, export_mode=export_mode)
    content = payload["text"]
    content_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()

    existing = con.execute(
        """
        SELECT export_id, path, sha256, size_bytes, created_at
        FROM ops_case_generated_export
        WHERE case_id = ? AND export_mode = ? AND sha256 = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [case_id, export_mode, content_sha256],
    ).fetchone()
    if existing:
        return {
            "rows_written": 0,
            "export_id": existing[0],
            "case_id": case_id,
            "export_mode": export_mode,
            "path": existing[1],
            "sha256": existing[2],
            "size_bytes": int(existing[3] or 0),
            "created_at": existing[4],
            "reused": True,
        }

    export_dir = _case_export_dir(case_id)
    export_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    filename = f"{stamp}__{export_mode.lower()}.txt"
    file_path = export_dir / filename
    file_path.write_text(content, encoding="utf-8")
    sha256 = _sha256_file(file_path)
    relpath = file_path.relative_to(ROOT)
    export_id = f"{case_id}:{export_mode}:{stamp}"
    label = f"saida_controlada_{export_mode.lower()}_{stamp}"

    con.execute(
        """
        INSERT INTO ops_case_generated_export (
            export_id, case_id, export_mode, path, sha256, size_bytes, label, actor, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            export_id,
            case_id,
            export_mode,
            str(relpath),
            sha256,
            file_path.stat().st_size,
            label,
            actor,
        ],
    )
    return {
        "rows_written": 1,
        "export_id": export_id,
        "case_id": case_id,
        "export_mode": export_mode,
        "path": str(relpath),
        "sha256": sha256,
        "size_bytes": file_path.stat().st_size,
        "created_at": datetime.now().isoformat(),
        "reused": False,
    }
