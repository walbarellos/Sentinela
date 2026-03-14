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
        if "ops_rule_validation" not in tables:
            print("rule_validation_table_missing")
            return 1
        total_rows = int(con.execute("SELECT COUNT(*) FROM ops_rule_validation").fetchone()[0] or 0)
        fail_rows = int(con.execute("SELECT COUNT(*) FROM ops_rule_validation WHERE status = 'FAIL'").fetchone()[0] or 0)
        warn_rows = int(con.execute("SELECT COUNT(*) FROM ops_rule_validation WHERE status = 'WARN'").fetchone()[0] or 0)
        print(f"rule_validation_rows={total_rows}")
        print(f"rule_validation_fail_rows={fail_rows}")
        print(f"rule_validation_warn_rows={warn_rows}")
        if fail_rows:
            preview = con.execute(
                """
                SELECT rule_id, title, finding
                FROM v_ops_rule_validation
                WHERE status = 'FAIL'
                ORDER BY severity DESC, validation_id
                LIMIT 10
                """
            ).fetchdf()
            print(preview.to_string(index=False))
            return 2
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
