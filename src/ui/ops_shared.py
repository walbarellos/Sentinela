from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def get_rw_db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def format_brl(value: float | None) -> str:
    if value is None:
        return "R$ 0,00"
    return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_case_label(row: pd.Series) -> str:
    subject = row.get("subject_name") or row.get("title") or row.get("case_id")
    stage = row.get("estagio_operacional") or "SEM_ESTAGIO"
    return f"{subject} [{stage}]"


def resolve_artifact_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path
