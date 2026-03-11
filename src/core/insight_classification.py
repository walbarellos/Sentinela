from __future__ import annotations

import json
import re
import unicodedata
from typing import Any, Mapping

import duckdb


CLASSIFICATION_COLUMNS = [
    ("esfera", "VARCHAR"),
    ("ente", "VARCHAR"),
    ("orgao", "VARCHAR"),
    ("municipio", "VARCHAR"),
    ("uf", "VARCHAR"),
    ("area_tematica", "VARCHAR"),
    ("sus", "BOOLEAN"),
]

MUNICIPAL_ENTE = "Prefeitura de Rio Branco"
STATE_ENTE = "Governo do Estado do Acre"
FEDERAL_ENTE = "Uniao"
RIO_BRANCO = "Rio Branco"
ACRE = "AC"

SUS_KEYWORDS = (
    "SUS",
    "SAUDE",
    "SEMSA",
    "SESACRE",
    "FUNDO MUNICIPAL DE SAUDE",
    "UNIDADE DE SAUDE",
    "UNIDADE BASICA",
    "UBS",
    "UPA",
    "HOSPITAL",
    "MATERNIDADE",
    "MEDICO",
    "ENFERME",
    "ODONTO",
    "FARMAC",
    "AGENTE COMUNITARIO",
    "CLINICO GERAL",
    "PEDIATRA",
    "GINECOLOGIA",
    "OBSTETR",
    "MEDICINA FAM",
)

ORGAO_PATTERNS = (
    ("SEMSA", ("SEMSA", "SECRETARIA MUNICIPAL DE SAUDE", "FUNDO MUNICIPAL DE SAUDE", "UBS", "UNIDADE BASICA")),
    ("SESACRE", ("SESACRE", "SECRETARIA DE ESTADO DE SAUDE")),
    ("SEOP", ("SEOP", "SECRETARIA MUNICIPAL DE OBRAS", "OBRAS PUBLICAS")),
    ("SEINFRA", ("SEINFRA", "INFRAESTRUTURA E MOBILIDADE")),
    ("SEMEL", ("SEMEL", "ESPORTE E LAZER")),
)


def ensure_insight_classification_columns(con: duckdb.DuckDBPyConnection) -> None:
    try:
        existing = {
            row[1]
            for row in con.execute("PRAGMA table_info('insight')").fetchall()
        }
    except duckdb.Error:
        return
    for column, sql_type in CLASSIFICATION_COLUMNS:
        if column in existing:
            continue
        con.execute(f"ALTER TABLE insight ADD COLUMN {column} {sql_type}")


def classification_defaults() -> dict[str, Any]:
    return {
        "esfera": None,
        "ente": None,
        "orgao": None,
        "municipio": None,
        "uf": None,
        "area_tematica": None,
        "sus": False,
    }


def classify_insight_record(
    record: Mapping[str, Any],
    *,
    extra_text: str = "",
) -> dict[str, Any]:
    text = _compose_text(record, extra_text=extra_text)
    result = classification_defaults()

    municipal_hit = _contains_any(
        text,
        (
            "PREFEITURA DE RIO BRANCO",
            "RIO BRANCO",
            "PORTAL DA TRANSPARENCIA RIO BRANCO",
            "PORTAL DA TRANSPARENCIA DIARIAS",
            "PORTAL DA TRANSPARENCIA OBRAS",
            "SEMSA",
            "SEOP",
            "SEINFRA",
            "SEMEL",
        ),
    )
    state_hit = _contains_any(
        text,
        (
            "TRANSPARENCIA AC",
            "GOVERNO DO ESTADO",
            "ERARIO ESTADUAL",
            "ESTADO DO ACRE",
            "GOVERNO_ESTADO_ACRE",
            "SESACRE",
        ),
    )
    federal_hit = _contains_any(
        text,
        (
            "CEIS",
            "CNEP",
            "CGU",
            "PORTAL DA TRANSPARENCIA FEDERAL",
            "SERVIDORES FEDERAIS",
            "TCU",
            "CNJ",
            "UNIAO",
        ),
    )

    if municipal_hit:
        result["esfera"] = "municipal"
        result["ente"] = MUNICIPAL_ENTE
        result["municipio"] = RIO_BRANCO
        result["uf"] = ACRE

    if state_hit and not municipal_hit:
        result["esfera"] = "estadual"
        result["ente"] = STATE_ENTE
        result["uf"] = ACRE

    if federal_hit and not municipal_hit and not state_hit:
        result["esfera"] = "federal"
        result["ente"] = FEDERAL_ENTE
        result["uf"] = "BR"

    for orgao, patterns in ORGAO_PATTERNS:
        if _contains_any(text, patterns):
            result["orgao"] = orgao
            break

    sus = _contains_any(text, SUS_KEYWORDS)
    if result["orgao"] in {"SEMSA", "SESACRE"}:
        sus = True
    result["sus"] = sus

    if sus:
        result["area_tematica"] = "saude"
        if result["esfera"] == "municipal" and not result["orgao"]:
            result["orgao"] = "SEMSA"
        if result["esfera"] == "estadual" and not result["orgao"]:
            result["orgao"] = "SESACRE"
    elif result["orgao"] in {"SEOP", "SEINFRA"}:
        result["area_tematica"] = "infraestrutura"
    elif result["esfera"] == "municipal":
        result["area_tematica"] = "gestao_municipal"
    elif result["esfera"] == "estadual":
        result["area_tematica"] = "gestao_estadual"
    elif result["esfera"] == "federal":
        result["area_tematica"] = "controle_externo"

    if result["ente"] == MUNICIPAL_ENTE and not result["orgao"]:
        result["orgao"] = MUNICIPAL_ENTE
    if result["ente"] == STATE_ENTE and not result["orgao"]:
        result["orgao"] = STATE_ENTE
    if result["ente"] == FEDERAL_ENTE and not result["orgao"]:
        result["orgao"] = FEDERAL_ENTE
    if result["esfera"] == "municipal" and result["orgao"] == "SESACRE":
        result["orgao"] = "SEMSA"
    if result["esfera"] == "estadual" and result["orgao"] == "SEMSA":
        result["orgao"] = "SESACRE"

    return result


def build_insight_extra_text(
    con: duckdb.DuckDBPyConnection,
    insight_ids: list[str],
) -> dict[str, str]:
    if not insight_ids:
        return {}

    placeholders = ",".join(["?"] * len(insight_ids))
    extra: dict[str, list[str]] = {insight_id: [] for insight_id in insight_ids}

    evidence_rows = con.execute(
        f"""
        SELECT el.insight_id, e.source, e.excerpt
        FROM evidence_link el
        JOIN evidence e ON e.id = el.evidence_id
        WHERE el.insight_id IN ({placeholders})
        """,
        insight_ids,
    ).fetchall()
    for insight_id, source, excerpt in evidence_rows:
        extra.setdefault(insight_id, []).append(_json_to_text(source))
        extra[insight_id].append(_json_to_text(excerpt))

    event_rows = con.execute(
        f"""
        SELECT il.insight_id, ev.type, ev.title, ev.attributes
        FROM insight_link il
        JOIN event ev ON ev.id = il.event_id
        WHERE il.insight_id IN ({placeholders})
        """,
        insight_ids,
    ).fetchall()
    for insight_id, event_type, title, attributes in event_rows:
        extra.setdefault(insight_id, []).append(_json_to_text(event_type))
        extra[insight_id].append(_json_to_text(title))
        extra[insight_id].append(_json_to_text(attributes))

    return {
        insight_id: " ".join(part for part in parts if part).strip()
        for insight_id, parts in extra.items()
    }


def _compose_text(record: Mapping[str, Any], *, extra_text: str = "") -> str:
    chunks = [
        _json_to_text(record.get("title")),
        _json_to_text(record.get("description_md")),
        _json_to_text(record.get("pattern")),
        _json_to_text(record.get("sources")),
        _json_to_text(record.get("tags")),
        _json_to_text(record.get("ente")),
        _json_to_text(record.get("orgao")),
        _json_to_text(record.get("municipio")),
        extra_text,
    ]
    return _normalize(" ".join(part for part in chunks if part))


def _json_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        stripped = _fix_mojibake(value).strip()
        if not stripped:
            return ""
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return _json_to_text(json.loads(stripped))
            except json.JSONDecodeError:
                return stripped
        return stripped
    if isinstance(value, Mapping):
        return " ".join(_json_to_text(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_json_to_text(v) for v in value)
    return str(value)


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern in text for pattern in patterns)


def _normalize(text: str) -> str:
    text = _fix_mojibake(text)
    text = text.upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fix_mojibake(text: str) -> str:
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError, AttributeError):
        return text
