from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[2]

CALIBRATION_CASE_DDL = """
CREATE TABLE IF NOT EXISTS ops_calibration_case (
    benchmark_id VARCHAR PRIMARY KEY,
    family VARCHAR NOT NULL,
    benchmark_class VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    expectation_type VARCHAR NOT NULL,
    expected_json JSON NOT NULL,
    note VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CALIBRATION_CASE_VIEW = """
CREATE OR REPLACE VIEW v_ops_calibration_case AS
SELECT *
FROM ops_calibration_case
ORDER BY family, benchmark_id
"""

CALIBRATION_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS ops_calibration_result (
    calibration_result_id VARCHAR PRIMARY KEY,
    benchmark_id VARCHAR NOT NULL,
    family VARCHAR NOT NULL,
    benchmark_class VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    finding VARCHAR NOT NULL,
    details_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CALIBRATION_RESULT_VIEW = """
CREATE OR REPLACE VIEW v_ops_calibration_result AS
SELECT *
FROM ops_calibration_result
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
    benchmark_class,
    family,
    benchmark_id
"""

CALIBRATION_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW v_ops_calibration_summary AS
SELECT
    benchmark_class,
    status,
    COUNT(*) AS total
FROM ops_calibration_result
GROUP BY 1, 2
ORDER BY benchmark_class, status
"""

BENCHMARKS: list[dict[str, Any]] = [
    {
        "benchmark_id": "RB_3898_ACTIVE_DOCUMENTAL",
        "family": "rb_sus_contrato",
        "benchmark_class": "confirmado",
        "title": "Contrato 3898 permanece ativo como divergência documental municipal",
        "expectation_type": "case_fields",
        "expected": {
            "case_id": "rb:contrato:3898",
            "family": "rb_sus_contrato",
            "classe_achado": "DIVERGENCIA_DOCUMENTAL",
            "estagio_operacional": "APTO_A_NOTICIA_DE_FATO",
            "uso_externo": "APTO_APURACAO",
            "noticia_fato_allowed": True,
        },
        "note": "Benchmark positivo municipal após limpeza do falso positivo sancionatório.",
    },
    {
        "benchmark_id": "RB_3895_FALSE_POSITIVE_REMOVED",
        "family": "rb_sus_contrato",
        "benchmark_class": "descartado",
        "title": "Contrato 3895 não retorna à fila municipal ativa",
        "expectation_type": "case_absent",
        "expected": {
            "case_id": "rb:contrato:3895",
            "historical_note": "investigations/claude_march/patch_claude/claude_update/patch/entrega_denuncia_atual/nota_historica_3895_sancao_invalidada.txt",
        },
        "note": "Benchmark negativo para travar regressão do falso positivo temporal.",
    },
    {
        "benchmark_id": "CEDIMP_DOCUMENT_REQUEST_ONLY",
        "family": "saude_societario",
        "benchmark_class": "confirmado",
        "title": "CEDIMP fica restrito a pedido documental",
        "expectation_type": "case_fields",
        "expected": {
            "case_id": "cedimp:saude_societario:13325100000130",
            "family": "saude_societario",
            "classe_achado": "HIPOTESE_INVESTIGATIVA",
            "estagio_operacional": "APTO_OFICIO_DOCUMENTAL",
            "uso_externo": "PEDIDO_DOCUMENTAL",
            "noticia_fato_allowed": False,
            "pedido_documental_allowed": True,
        },
        "note": "Benchmark conservador da família societária em saúde.",
    },
    {
        "benchmark_id": "SESACRE_REFERENCE_CASE_ACTIVE",
        "family": "sesacre_sancao",
        "benchmark_class": "confirmado",
        "title": "Caso de referência SESACRE permanece como cruzamento sancionatório conservador",
        "expectation_type": "case_fields",
        "expected": {
            "case_id": "sesacre:sancao:07847837000110",
            "family": "sesacre_sancao",
            "classe_achado": "CRUZAMENTO_SANCIONATORIO",
            "estagio_operacional": "APTO_A_NOTICIA_DE_FATO",
            "noticia_fato_allowed": True,
            "burden_item": "cruzamento_sancao_ativa",
        },
        "note": "Benchmark estadual de referência para garantir que a cerca conservadora não derrube o caso legítimo.",
    },
    {
        "benchmark_id": "GLOBAL_LANGUAGE_GUARD_CLEAN",
        "family": "all",
        "benchmark_class": "confirmado",
        "title": "Nenhuma saída ativa está flagrada pelo guard de linguagem",
        "expectation_type": "global_zero",
        "expected": {
            "table": "ops_case_language_guard",
        },
        "note": "Benchmark transversal de segurança institucional.",
    },
    {
        "benchmark_id": "RB_3898_CORE_DOCS_PENDING",
        "family": "rb_sus_contrato",
        "benchmark_class": "inconclusivo",
        "title": "Contrato 3898 segue com processos integrais pendentes",
        "expectation_type": "multi_burden_status",
        "expected": {
            "case_id": "rb:contrato:3898",
            "status": "PENDENTE_DOCUMENTO",
            "item_keys": [
                "processo_integral_contrato",
                "processo_integral_licitacao",
                "memoria_comparativa_item",
            ],
        },
        "note": "Benchmark de contenção para não tratar a lacuna documental como resolvida.",
    },
    {
        "benchmark_id": "CEDIMP_PARENTESCO_UNPROVEN",
        "family": "saude_societario",
        "benchmark_class": "inconclusivo",
        "title": "CEDIMP continua sem base para parentesco ou designação cruzada",
        "expectation_type": "burden_status",
        "expected": {
            "case_id": "cedimp:saude_societario:13325100000130",
            "item_key": "parentesco_ou_designacao_cruzada",
            "status": "SEM_BASE_ATUAL",
        },
        "note": "Benchmark de contenção para não subir tese pessoal sem prova objetiva.",
    },
    {
        "benchmark_id": "SESACRE_REFERENCE_DUE_DILIGENCE_PENDING",
        "family": "sesacre_sancao",
        "benchmark_class": "inconclusivo",
        "title": "Caso de referência SESACRE ainda depende de processo e due diligence",
        "expectation_type": "multi_burden_status",
        "expected": {
            "case_id": "sesacre:sancao:07847837000110",
            "status": "PENDENTE_DOCUMENTO",
            "item_keys": [
                "processo_integral_contratacao",
                "consulta_integridade_previa",
                "justificativa_manutencao_contratual",
                "lastro_execucao_pagamento",
            ],
        },
        "note": "Benchmark para manter o caso estadual no modo conservador e diligente.",
    },
]


def ensure_ops_calibration(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(CALIBRATION_CASE_DDL)
    con.execute("ALTER TABLE ops_calibration_case ADD COLUMN IF NOT EXISTS benchmark_class VARCHAR")
    con.execute(CALIBRATION_CASE_VIEW)
    con.execute(CALIBRATION_RESULT_DDL)
    con.execute("ALTER TABLE ops_calibration_result ADD COLUMN IF NOT EXISTS benchmark_class VARCHAR")
    con.execute(CALIBRATION_RESULT_VIEW)
    con.execute(CALIBRATION_SUMMARY_VIEW)


def _insert_cases(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DELETE FROM ops_calibration_case")
    for item in BENCHMARKS:
        con.execute(
            """
            INSERT INTO ops_calibration_case (
                benchmark_id, family, benchmark_class, title, expectation_type, expected_json, note, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                item["benchmark_id"],
                item["family"],
                item["benchmark_class"],
                item["title"],
                item["expectation_type"],
                json.dumps(item["expected"], ensure_ascii=False),
                item["note"],
            ],
        )


def _row(
    benchmark_id: str,
    family: str,
    benchmark_class: str,
    status: str,
    finding: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "calibration_result_id": f"{benchmark_id}:{status}",
        "benchmark_id": benchmark_id,
        "family": family,
        "benchmark_class": benchmark_class,
        "status": status,
        "finding": finding,
        "details_json": json.dumps(details, ensure_ascii=False),
    }


def _fetch_case(con: duckdb.DuckDBPyConnection, case_id: str) -> dict[str, Any] | None:
    rows = con.execute(
        """
        SELECT case_id, family, classe_achado, estagio_operacional, uso_externo, title
        FROM ops_case_registry
        WHERE case_id = ?
        """,
        [case_id],
    ).fetchall()
    if not rows:
        return None
    case_id, family, classe_achado, estagio_operacional, uso_externo, title = rows[0]
    return {
        "case_id": case_id,
        "family": family,
        "classe_achado": classe_achado,
        "estagio_operacional": estagio_operacional,
        "uso_externo": uso_externo,
        "title": title,
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


def _has_burden_item(con: duckdb.DuckDBPyConnection, case_id: str, item_key: str) -> bool:
    count = con.execute(
        """
        SELECT COUNT(*)
        FROM ops_case_burden_item
        WHERE case_id = ? AND item_key = ?
        """,
        [case_id, item_key],
    ).fetchone()[0]
    return bool(count)


def _burden_status(con: duckdb.DuckDBPyConnection, case_id: str, item_key: str) -> str | None:
    row = con.execute(
        """
        SELECT status
        FROM ops_case_burden_item
        WHERE case_id = ? AND item_key = ?
        """,
        [case_id, item_key],
    ).fetchone()
    return None if row is None else str(row[0])


def sync_ops_calibration(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_calibration(con)
    _insert_cases(con)
    con.execute("DELETE FROM ops_calibration_result")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    results: list[dict[str, Any]] = []

    for item in BENCHMARKS:
        benchmark_id = item["benchmark_id"]
        family = item["family"]
        benchmark_class = item["benchmark_class"]
        expected = item["expected"]
        expectation_type = item["expectation_type"]

        if expectation_type == "case_fields":
            case_id = str(expected["case_id"])
            case = _fetch_case(con, case_id) if "ops_case_registry" in tables else None
            if case is None:
                results.append(
                    _row(
                        benchmark_id,
                        family,
                        benchmark_class,
                        "FAIL",
                        f"Caso esperado ausente: {case_id}.",
                        {"case_id": case_id},
                    )
                )
                continue

            mismatches: dict[str, Any] = {}
            for key in ("family", "classe_achado", "estagio_operacional", "uso_externo"):
                if key in expected and case.get(key) != expected[key]:
                    mismatches[key] = {"expected": expected[key], "actual": case.get(key)}
            if "noticia_fato_allowed" in expected:
                actual = _gate_allowed(con, case_id, "NOTICIA_FATO")
                if actual != bool(expected["noticia_fato_allowed"]):
                    mismatches["noticia_fato_allowed"] = {
                        "expected": bool(expected["noticia_fato_allowed"]),
                        "actual": actual,
                    }
            if "pedido_documental_allowed" in expected:
                actual = _gate_allowed(con, case_id, "PEDIDO_DOCUMENTAL")
                if actual != bool(expected["pedido_documental_allowed"]):
                    mismatches["pedido_documental_allowed"] = {
                        "expected": bool(expected["pedido_documental_allowed"]),
                        "actual": actual,
                    }
            if "burden_item" in expected and not _has_burden_item(con, case_id, str(expected["burden_item"])):
                mismatches["burden_item"] = {
                    "expected": str(expected["burden_item"]),
                    "actual": "missing",
                }

            if mismatches:
                results.append(
                    _row(
                        benchmark_id,
                        family,
                        benchmark_class,
                        "FAIL",
                        f"Benchmark {benchmark_id} divergiu do esperado.",
                        {"case_id": case_id, "mismatches": mismatches},
                    )
                )
            else:
                results.append(
                    _row(
                        benchmark_id,
                        family,
                        benchmark_class,
                        "PASS",
                        f"Benchmark {benchmark_id} aderente ao comportamento esperado.",
                        {"case_id": case_id},
                    )
                )
            continue

        if expectation_type == "case_absent":
            case_id = str(expected["case_id"])
            case = _fetch_case(con, case_id) if "ops_case_registry" in tables else None
            historical_path = ROOT / str(expected["historical_note"])
            if case is not None:
                results.append(
                    _row(
                        benchmark_id,
                        family,
                        benchmark_class,
                        "FAIL",
                        f"Caso historico reapareceu na fila ativa: {case_id}.",
                        {"case": case},
                    )
                )
            elif not historical_path.exists():
                results.append(
                    _row(
                        benchmark_id,
                        family,
                        benchmark_class,
                        "WARN",
                        "Caso historico ausente da fila ativa, mas nota historica nao foi localizada.",
                        {"path": str(historical_path.relative_to(ROOT))},
                    )
                )
            else:
                results.append(
                    _row(
                        benchmark_id,
                        family,
                        benchmark_class,
                        "PASS",
                        "Caso historico segue fora da fila ativa e com nota de auditoria preservada.",
                        {"path": str(historical_path.relative_to(ROOT))},
                    )
                )
            continue

        if expectation_type == "burden_status":
            case_id = str(expected["case_id"])
            item_key = str(expected["item_key"])
            expected_status = str(expected["status"])
            actual_status = _burden_status(con, case_id, item_key) if "ops_case_burden_item" in tables else None
            status = "PASS" if actual_status == expected_status else "FAIL"
            results.append(
                _row(
                    benchmark_id,
                    family,
                    benchmark_class,
                    status,
                    (
                        f"Burden {item_key} permaneceu em {expected_status}."
                        if status == "PASS"
                        else f"Burden {item_key} divergiu: esperado {expected_status}, atual {actual_status}."
                    ),
                    {
                        "case_id": case_id,
                        "item_key": item_key,
                        "expected_status": expected_status,
                        "actual_status": actual_status,
                    },
                )
            )
            continue

        if expectation_type == "multi_burden_status":
            case_id = str(expected["case_id"])
            expected_status = str(expected["status"])
            item_keys = [str(v) for v in expected.get("item_keys", [])]
            mismatches: dict[str, Any] = {}
            for item_key in item_keys:
                actual_status = _burden_status(con, case_id, item_key) if "ops_case_burden_item" in tables else None
                if actual_status != expected_status:
                    mismatches[item_key] = {
                        "expected_status": expected_status,
                        "actual_status": actual_status,
                    }
            results.append(
                _row(
                    benchmark_id,
                    family,
                    benchmark_class,
                    "PASS" if not mismatches else "FAIL",
                    (
                        f"Todos os burden items permaneceram em {expected_status}."
                        if not mismatches
                        else f"{len(mismatches)} burden item(ns) divergiram do status esperado {expected_status}."
                    ),
                    {
                        "case_id": case_id,
                        "expected_status": expected_status,
                        "item_keys": item_keys,
                        "mismatches": mismatches,
                    },
                )
            )
            continue

        if expectation_type == "global_zero":
            table_name = str(expected["table"])
            count = 0
            if table_name in tables:
                count = int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)
            status = "PASS" if count == 0 else "FAIL"
            results.append(
                _row(
                    benchmark_id,
                    family,
                    benchmark_class,
                    status,
                    "Nenhuma flag ativa no guard de linguagem." if count == 0 else f"{count} flag(s) ativas no guard de linguagem.",
                    {"table": table_name, "count": count},
                )
            )
            continue

        results.append(
            _row(
                benchmark_id,
                family,
                benchmark_class,
                "WARN",
                f"Tipo de expectativa nao reconhecido: {expectation_type}.",
                {"expectation_type": expectation_type},
            )
        )

    for row in results:
        con.execute(
            """
            INSERT INTO ops_calibration_result (
                calibration_result_id, benchmark_id, family, benchmark_class, status, finding, details_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                row["calibration_result_id"],
                row["benchmark_id"],
                row["family"],
                row["benchmark_class"],
                row["status"],
                row["finding"],
                row["details_json"],
            ],
        )

    fail_rows = sum(1 for row in results if row["status"] == "FAIL")
    warn_rows = sum(1 for row in results if row["status"] == "WARN")
    return {
        "benchmark_rows": len(BENCHMARKS),
        "result_rows": len(results),
        "fail_rows": fail_rows,
        "warn_rows": warn_rows,
    }
