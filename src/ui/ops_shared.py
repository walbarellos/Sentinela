from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

USO_EXTERNO_LABELS = {
    "APTO_REPRESENTACAO": "APTO_A_NOTICIA_DE_FATO",
    "APTO_APURACAO": "APTO_A_APURACAO",
    "REVISAO_INTERNA": "REVISAO_INTERNA",
    "PEDIDO_DOCUMENTAL": "PEDIDO_DOCUMENTAL",
}

ESTAGIO_LABELS = {
    "APTO_REPRESENTACAO_PRELIMINAR": "APTO_A_NOTICIA_DE_FATO",
    "APTO_OFICIO_DOCUMENTAL": "APTO_OFICIO_DOCUMENTAL",
    "APTO_ANALISE_JURIDICO_FUNCIONAL": "APTO_ANALISE_JURIDICO_FUNCIONAL",
    "TRIAGEM_INTERNA": "TRIAGEM_INTERNA",
}


def get_rw_db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def format_brl(value: float | None) -> str:
    if value is None:
        return "R$ 0,00"
    return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_case_label(row: pd.Series) -> str:
    subject = row.get("subject_name") or row.get("title") or row.get("case_id")
    stage = present_stage_label(row.get("estagio_operacional"))
    return f"{subject} [{stage}]"


def present_external_usage(value: str | None) -> str:
    if not value:
        return "N/D"
    return USO_EXTERNO_LABELS.get(str(value), str(value))


def present_stage_label(value: str | None) -> str:
    if not value:
        return "SEM_ESTAGIO"
    return ESTAGIO_LABELS.get(str(value), str(value))


def resolve_artifact_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path
