from __future__ import annotations

import json
import re
from typing import Any

import duckdb


GUARD_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_language_guard (
    guard_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    source_id VARCHAR NOT NULL,
    label VARCHAR,
    issue_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    snippet VARCHAR,
    rationale VARCHAR,
    suggestion VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

GUARD_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_language_guard AS
SELECT *
FROM ops_case_language_guard
ORDER BY case_id, severity DESC, label, issue_type
"""

RISK_PATTERNS = [
    ("ROTULO_ACUSATORIO", r"\bden[uú]ncia\b", "MEDIO", "Preferir 'noticia de fato', 'relato para apuracao' ou 'pedido documental'."),
    ("ROTULO_ACUSATORIO", r"\brepresenta[cç][aã]o preliminar\b", "MEDIO", "Preferir 'noticia de fato' ou 'pedido de apuracao preliminar'."),
    ("ROTULO_ACUSATORIO", r"\bdenuncia_imediata\b", "ALTO", "Substituir por classificacao operacional neutra."),
    ("AFIRMACAO_PENAL", r"\bfraude consumada\b", "ALTO", "Substituir por 'divergencia documental' ou 'achado a apurar'."),
    ("AFIRMACAO_PENAL", r"\bcrime\b", "ALTO", "Substituir por 'possivel irregularidade' apenas quando houver base documental suficiente."),
    ("LINGUAGEM_IMPROPRIA", r"\bcorrupt[oa]s?\b", "CRITICO", "Remover adjetivo; descrever apenas fato, documento e limite da conclusao."),
    ("LINGUAGEM_IMPROPRIA", r"\bbandid[oa]s?\b", "CRITICO", "Remover adjetivo; descrever apenas fato, documento e limite da conclusao."),
    ("LINGUAGEM_IMPROPRIA", r"\broub[ao]\b", "CRITICO", "Remover imputacao direta; descrever fato documental e trilha de apuracao."),
]

SAFE_CONTEXT = (
    "nao afirma",
    "não afirma",
    "nao prova",
    "não prova",
    "depende de apuracao",
    "depende de apuração",
    "sem concluir",
    "hipotese",
    "hipótese",
    "revisao interna",
    "revisão interna",
    "noticia de fato",
    "notícia de fato",
    "apuracao preliminar",
    "apuração preliminar",
)


def ensure_ops_guard(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(GUARD_DDL)
    con.execute(GUARD_VIEW)


def _has_safe_context(snippet: str) -> bool:
    lowered = snippet.lower()
    return any(token in lowered for token in SAFE_CONTEXT)


def _best_snippet(text: str, pattern: str, radius: int = 120) -> str:
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return text[: radius * 2].replace("\n", " ").strip()
    start = max(0, match.start() - radius)
    end = min(len(text), match.end() + radius)
    return text[start:end].replace("\n", " ").strip()


def sync_ops_language_guard(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_guard(con)
    con.execute("DELETE FROM ops_case_language_guard")

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
    if "ops_artifact_text_index" not in tables:
        return {"rows_written": 0, "sources": 0}

    rows = con.execute(
        """
        SELECT case_id, source_type, source_id, label, kind, content_text
        FROM ops_artifact_text_index
        WHERE source_type = 'artifact'
          AND kind IN ('nota', 'dossie')
        """
    ).fetchall()

    written = 0
    for case_id, source_type, source_id, label, kind, content_text in rows:
        text = str(content_text or "")
        for issue_type, pattern, severity, suggestion in RISK_PATTERNS:
            snippet = _best_snippet(text, pattern)
            if not re.search(pattern, text, re.IGNORECASE):
                continue
            if _has_safe_context(snippet):
                continue
            con.execute(
                """
                INSERT INTO ops_case_language_guard (
                    guard_id, case_id, source_type, source_id, label, issue_type,
                    severity, snippet, rationale, suggestion, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    f"{case_id}:{source_id}:{issue_type}:{written}",
                    case_id,
                    source_type,
                    source_id,
                    label,
                    issue_type,
                    severity,
                    snippet,
                    "A saida externa deve relatar fatos e pedir apuracao, sem imputacao ou adjetivo acusatorio.",
                    suggestion,
                ],
            )
            written += 1

    return {"rows_written": written, "sources": len(rows)}
