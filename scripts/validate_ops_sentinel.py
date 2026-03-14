from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        if "ops_rule_sentinel_result" not in tables:
            print("sentinel_table_missing")
            return 1
        total_rows = int(con.execute("SELECT COUNT(*) FROM ops_rule_sentinel_result").fetchone()[0] or 0)
        fail_rows = int(con.execute("SELECT COUNT(*) FROM ops_rule_sentinel_result WHERE status = 'FAIL'").fetchone()[0] or 0)
        warn_rows = int(con.execute("SELECT COUNT(*) FROM ops_rule_sentinel_result WHERE status = 'WARN'").fetchone()[0] or 0)
        print(f"sentinel_result_rows={total_rows}")
        print(f"sentinel_fail_rows={fail_rows}")
        print(f"sentinel_warn_rows={warn_rows}")
        if "v_ops_rule_sentinel_summary" in tables:
            summary = con.execute(
                """
                SELECT rule_id, status, total
                FROM v_ops_rule_sentinel_summary
                ORDER BY rule_id, status
                """
            ).fetchdf()
            if not summary.empty:
                print(summary.to_string(index=False))
        if fail_rows:
            preview = con.execute(
                """
                SELECT sentinel_id, rule_id, family, finding
                FROM v_ops_rule_sentinel_result
                WHERE status = 'FAIL'
                ORDER BY rule_id, family, sentinel_id
                LIMIT 20
                """
            ).fetchdf()
            print(preview.to_string(index=False))
            return 2
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
