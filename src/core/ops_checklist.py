from __future__ import annotations

import json
from typing import Any

import duckdb


CHECKLIST_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_checklist (
    checklist_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    step_group VARCHAR NOT NULL,
    step_label VARCHAR NOT NULL,
    step_status VARCHAR NOT NULL,
    blocking BOOLEAN DEFAULT FALSE,
    rationale VARCHAR,
    source_refs_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CHECKLIST_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_checklist AS
SELECT *
FROM ops_case_checklist
ORDER BY case_id, blocking DESC, step_group, step_label
"""


def ensure_ops_checklist(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(CHECKLIST_DDL)
    con.execute(CHECKLIST_VIEW)


def _row(
    *,
    case_id: str,
    family: str,
    step_group: str,
    step_label: str,
    step_status: str,
    blocking: bool,
    rationale: str,
    source_refs: list[str],
) -> dict[str, Any]:
    return {
        "checklist_id": f"{case_id}:{step_group}:{step_label}".replace(" ", "_"),
        "case_id": case_id,
        "family": family,
        "step_group": step_group,
        "step_label": step_label,
        "step_status": step_status,
        "blocking": blocking,
        "rationale": rationale,
        "source_refs_json": json.dumps(source_refs, ensure_ascii=False),
    }


def sync_ops_checklist(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_checklist(con)
    con.execute("DELETE FROM ops_case_checklist")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_case_burden_item" not in tables:
        return {"rows_written": 0, "cases": 0}

    burden_rows = con.execute(
        """
        SELECT
            case_id,
            family,
            item_label,
            status,
            next_action,
            source_refs_json
        FROM ops_case_burden_item
        ORDER BY case_id, status_order, item_key
        """
    ).fetchall()

    written = 0
    case_ids: set[str] = set()
    for case_id, family, item_label, status, next_action, source_refs_json in burden_rows:
        case_ids.add(case_id)
        step_group = {
            "COMPROVADO_DOCUMENTAL": "prova",
            "PENDENTE_DOCUMENTO": "diligencia",
            "PENDENTE_ENQUADRAMENTO": "juridico",
            "SEM_BASE_ATUAL": "bloqueio",
        }.get(status, "prova")
        step_status = {
            "COMPROVADO_DOCUMENTAL": "CONCLUIDO",
            "PENDENTE_DOCUMENTO": "PENDENTE",
            "PENDENTE_ENQUADRAMENTO": "REVISAO_HUMANA",
            "SEM_BASE_ATUAL": "BLOQUEADO",
        }.get(status, "PENDENTE")
        blocking = status in {"PENDENTE_DOCUMENTO", "PENDENTE_ENQUADRAMENTO", "SEM_BASE_ATUAL"}
        rationale = next_action or "Sem acao adicional registrada."
        con.execute(
            """
            INSERT INTO ops_case_checklist (
                checklist_id, case_id, family, step_group, step_label,
                step_status, blocking, rationale, source_refs_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                f"{case_id}:{step_group}:{written}",
                case_id,
                family,
                step_group,
                item_label,
                step_status,
                blocking,
                rationale,
                source_refs_json,
            ],
        )
        written += 1

    return {"rows_written": written, "cases": len(case_ids)}
