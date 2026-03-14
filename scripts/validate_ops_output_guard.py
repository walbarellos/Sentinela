from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_case_language_guard" not in tables:
            print("guard_table_missing")
            return 1
        guard_rows = int(con.execute("SELECT COUNT(*) FROM ops_case_language_guard").fetchone()[0] or 0)
        export_rows = int(con.execute("SELECT COUNT(*) FROM ops_case_export_gate").fetchone()[0] or 0) if "ops_case_export_gate" in tables else 0
        frozen_rows = int(con.execute("SELECT COUNT(*) FROM ops_case_generated_export").fetchone()[0] or 0) if "ops_case_generated_export" in tables else 0
        print(f"language_guard_rows={guard_rows}")
        print(f"export_gate_rows={export_rows}")
        print(f"generated_export_rows={frozen_rows}")
        if guard_rows != 0:
            return 2
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
