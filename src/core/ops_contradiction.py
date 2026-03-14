from __future__ import annotations

import json
from typing import Any

import duckdb


CONTRADICTION_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_contradiction (
    contradiction_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    comparator VARCHAR,
    rationale VARCHAR,
    next_action VARCHAR,
    source_refs_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CONTRADICTION_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_contradiction AS
SELECT *
FROM ops_case_contradiction
ORDER BY case_id, severity DESC, title
"""


def ensure_ops_contradiction(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(CONTRADICTION_DDL)
    con.execute(CONTRADICTION_VIEW)


def sync_ops_contradiction(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_contradiction(con)
    con.execute("DELETE FROM ops_case_contradiction")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_case_semantic_issue" not in tables:
        return {"rows_written": 0, "cases": 0}

    rows = con.execute(
        """
        SELECT
            case_id,
            comparator,
            field_key,
            severity,
            status,
            left_value,
            center_value,
            right_value,
            rationale,
            source_refs_json
        FROM ops_case_semantic_issue
        WHERE status = 'DIVERGENTE'
        ORDER BY case_id, severity DESC, comparator, field_key
        """
    ).fetchall()

    written = 0
    case_ids: set[str] = set()
    for case_id, comparator, field_key, severity, status, left_value, center_value, right_value, rationale, source_refs_json in rows:
        case_ids.add(case_id)
        title = f"{comparator} :: {field_key}"
        next_action = "Preservar a contradicao em noticia de fato e, se preciso, solicitar memoria comparativa e processo integral."
        con.execute(
            """
            INSERT INTO ops_case_contradiction (
                contradiction_id, case_id, title, severity, status, comparator,
                rationale, next_action, source_refs_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                f"{case_id}:{comparator}:{field_key}",
                case_id,
                title,
                severity,
                status,
                comparator,
                rationale,
                next_action,
                source_refs_json,
            ],
        )
        written += 1

    return {"rows_written": written, "cases": len(case_ids)}
