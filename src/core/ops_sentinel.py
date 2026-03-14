from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[2]

SENTINEL_CASE_DDL = """
CREATE TABLE IF NOT EXISTS ops_rule_sentinel_case (
    sentinel_id VARCHAR PRIMARY KEY,
    rule_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    expected_json JSON NOT NULL,
    note VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

SENTINEL_CASE_VIEW = """
CREATE OR REPLACE VIEW v_ops_rule_sentinel_case AS
SELECT *
FROM ops_rule_sentinel_case
ORDER BY rule_id, family, sentinel_id
"""

SENTINEL_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS ops_rule_sentinel_result (
    sentinel_result_id VARCHAR PRIMARY KEY,
    sentinel_id VARCHAR NOT NULL,
    rule_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    finding VARCHAR NOT NULL,
    details_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

SENTINEL_RESULT_VIEW = """
CREATE OR REPLACE VIEW v_ops_rule_sentinel_result AS
SELECT *
FROM ops_rule_sentinel_result
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
    rule_id,
    family,
    sentinel_id
"""

SENTINEL_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW v_ops_rule_sentinel_summary AS
SELECT
    rule_id,
    status,
    COUNT(*) AS total
FROM ops_rule_sentinel_result
GROUP BY 1, 2
ORDER BY rule_id, status
"""

SENTINELS: list[dict[str, Any]] = [
    {
        "sentinel_id": "RB_TEMPORAL_FALSE_POSITIVE",
        "rule_id": "FAMILY_CONFIDENCE_GUARD",
        "family": "rb_sus_contrato",
        "title": "Contrato 3895 nao reaparece como cruzamento sancionatorio ativo",
        "expected": {
            "type": "absent_case",
            "case_id": "rb:contrato:3895",
        },
        "note": "Trava o falso positivo temporal conhecido.",
    },
    {
        "sentinel_id": "RB_3898_SEMANTIC_TRIAD",
        "rule_id": "RB_SEMANTIC_TRIAD",
        "family": "rb_sus_contrato",
        "title": "Contrato 3898 mantem triangulacao semantica esperada",
        "expected": {
            "type": "semantic_statuses",
            "case_id": "rb:contrato:3898",
            "required": [
                ["item_x_edital", "item_critico", "DIVERGENTE"],
                ["item_x_propostas", "item_critico", "DIVERGENTE"],
                ["contrato_x_edital", "objeto", "COERENTE"],
                ["contrato_x_licitacao", "numero_processo", "COERENTE"],
            ],
        },
        "note": "Trava a regra semantica central do caso municipal.",
    },
    {
        "sentinel_id": "SAUDE_NO_NEPOTISM_LABEL",
        "rule_id": "SAUDE_SOCIETARIO_CNES",
        "family": "saude_societario",
        "title": "Trilha societaria em saude nao volta a usar rotulo de nepotismo",
        "expected": {
            "type": "query_zero",
            "queries": [
                "SELECT COUNT(*) FROM ops_case_burden_item WHERE family = 'saude_societario' AND LOWER(COALESCE(item_label, '')) LIKE '%nepot%'",
                "SELECT COUNT(*) FROM ops_case_checklist WHERE family = 'saude_societario' AND LOWER(COALESCE(step_label, '')) LIKE '%nepot%'",
            ],
        },
        "note": "Trava regressao de linguagem indevida na familia societaria.",
    },
    {
        "sentinel_id": "SAUDE_NOTICIA_FATO_BLOCKED",
        "rule_id": "SAUDE_SOCIETARIO_CNES",
        "family": "saude_societario",
        "title": "CEDIMP continua bloqueado para noticia de fato",
        "expected": {
            "type": "gate_boolean",
            "case_id": "cedimp:saude_societario:13325100000130",
            "export_mode": "NOTICIA_FATO",
            "allowed": False,
        },
        "note": "Trava sobrepromocao juridica do caso funcional.",
    },
    {
        "sentinel_id": "SESACRE_NO_OVERCLAIM_LANGUAGE",
        "rule_id": "SESACRE_SANCTION_CROSS",
        "family": "sesacre_sancao",
        "title": "Familia SESACRE nao reintroduz linguagem temporal forte",
        "expected": {
            "type": "query_zero",
            "queries": [
                "SELECT COUNT(*) FROM ops_case_registry WHERE family = 'sesacre_sancao' AND (LOWER(COALESCE(title, '')) LIKE '%concomitante%' OR LOWER(COALESCE(resumo_curto, '')) LIKE '%concomitante%')",
                "SELECT COUNT(*) FROM ops_case_burden_item WHERE family = 'sesacre_sancao' AND LOWER(COALESCE(item_label, '')) LIKE '%concomitante%'",
            ],
        },
        "note": "Trava regressao da linguagem estadual para algo mais forte do que a prova comporta.",
    },
]


def ensure_ops_sentinel(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(SENTINEL_CASE_DDL)
    con.execute(SENTINEL_CASE_VIEW)
    con.execute(SENTINEL_RESULT_DDL)
    con.execute(SENTINEL_RESULT_VIEW)
    con.execute(SENTINEL_SUMMARY_VIEW)


def _insert_cases(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DELETE FROM ops_rule_sentinel_case")
    for item in SENTINELS:
        con.execute(
            """
            INSERT INTO ops_rule_sentinel_case (
                sentinel_id, rule_id, family, title, expected_json, note, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                item["sentinel_id"],
                item["rule_id"],
                item["family"],
                item["title"],
                json.dumps(item["expected"], ensure_ascii=False),
                item["note"],
            ],
        )


def _result(sentinel_id: str, rule_id: str, family: str, status: str, finding: str, details: dict[str, Any]) -> dict[str, Any]:
    return {
        "sentinel_result_id": f"{sentinel_id}:{status}",
        "sentinel_id": sentinel_id,
        "rule_id": rule_id,
        "family": family,
        "status": status,
        "finding": finding,
        "details_json": json.dumps(details, ensure_ascii=False),
    }


def _gate_allowed(con: duckdb.DuckDBPyConnection, case_id: str, export_mode: str) -> bool | None:
    row = con.execute(
        """
        SELECT allowed
        FROM ops_case_export_gate
        WHERE case_id = ? AND export_mode = ?
        """,
        [case_id, export_mode],
    ).fetchone()
    return None if row is None else bool(row[0])


def sync_ops_sentinel(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_sentinel(con)
    _insert_cases(con)
    con.execute("DELETE FROM ops_rule_sentinel_result")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    results: list[dict[str, Any]] = []

    for item in SENTINELS:
        sentinel_id = item["sentinel_id"]
        rule_id = item["rule_id"]
        family = item["family"]
        expected = item["expected"]
        kind = str(expected["type"])

        if kind == "absent_case":
            case_id = str(expected["case_id"])
            count = 0
            if "ops_case_registry" in tables:
                count = int(con.execute("SELECT COUNT(*) FROM ops_case_registry WHERE case_id = ?", [case_id]).fetchone()[0] or 0)
            results.append(
                _result(
                    sentinel_id,
                    rule_id,
                    family,
                    "PASS" if count == 0 else "FAIL",
                    "Caso historico permanece fora da fila ativa." if count == 0 else f"Caso historico reapareceu: {case_id}.",
                    {"case_id": case_id, "count": count},
                )
            )
            continue

        if kind == "semantic_statuses":
            case_id = str(expected["case_id"])
            mismatches: list[dict[str, Any]] = []
            for comparator, field_key, status in expected.get("required", []):
                row = con.execute(
                    """
                    SELECT status
                    FROM ops_case_semantic_issue
                    WHERE case_id = ? AND comparator = ? AND field_key = ?
                    """,
                    [case_id, comparator, field_key],
                ).fetchone()
                actual = None if row is None else str(row[0])
                if actual != status:
                    mismatches.append(
                        {
                            "comparator": comparator,
                            "field_key": field_key,
                            "expected": status,
                            "actual": actual,
                        }
                    )
            results.append(
                _result(
                    sentinel_id,
                    rule_id,
                    family,
                    "PASS" if not mismatches else "FAIL",
                    "Tríade semântica do 3898 permaneceu estável." if not mismatches else f"{len(mismatches)} divergência(s) na regra semântica do 3898.",
                    {"case_id": case_id, "mismatches": mismatches},
                )
            )
            continue

        if kind == "gate_boolean":
            case_id = str(expected["case_id"])
            export_mode = str(expected["export_mode"])
            allowed = bool(expected["allowed"])
            actual = _gate_allowed(con, case_id, export_mode) if "ops_case_export_gate" in tables else None
            results.append(
                _result(
                    sentinel_id,
                    rule_id,
                    family,
                    "PASS" if actual == allowed else "FAIL",
                    f"Gate {export_mode} permaneceu em {allowed}." if actual == allowed else f"Gate {export_mode} divergiu: esperado {allowed}, atual {actual}.",
                    {"case_id": case_id, "export_mode": export_mode, "expected": allowed, "actual": actual},
                )
            )
            continue

        if kind == "query_zero":
            queries = [str(q) for q in expected.get("queries", [])]
            failures: list[dict[str, Any]] = []
            for idx, query in enumerate(queries, start=1):
                count = int(con.execute(query).fetchone()[0] or 0)
                if count != 0:
                    failures.append({"query_index": idx, "count": count, "query": query})
            results.append(
                _result(
                    sentinel_id,
                    rule_id,
                    family,
                    "PASS" if not failures else "FAIL",
                    "Consulta(s) sentinela permaneceram zeradas." if not failures else f"{len(failures)} consulta(s) sentinela retornaram valor diferente de zero.",
                    {"failures": failures},
                )
            )
            continue

        results.append(
            _result(
                sentinel_id,
                rule_id,
                family,
                "WARN",
                f"Tipo de sentinela nao reconhecido: {kind}.",
                {"type": kind},
            )
        )

    for row in results:
        con.execute(
            """
            INSERT INTO ops_rule_sentinel_result (
                sentinel_result_id, sentinel_id, rule_id, family, status, finding, details_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                row["sentinel_result_id"],
                row["sentinel_id"],
                row["rule_id"],
                row["family"],
                row["status"],
                row["finding"],
                row["details_json"],
            ],
        )

    fail_rows = sum(1 for row in results if row["status"] == "FAIL")
    warn_rows = sum(1 for row in results if row["status"] == "WARN")
    return {
        "sentinel_rows": len(SENTINELS),
        "result_rows": len(results),
        "fail_rows": fail_rows,
        "warn_rows": warn_rows,
    }
