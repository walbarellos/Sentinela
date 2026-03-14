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
        if "ops_calibration_result" not in tables:
            print("calibration_table_missing")
            return 1
        total_rows = int(con.execute("SELECT COUNT(*) FROM ops_calibration_result").fetchone()[0] or 0)
        fail_rows = int(con.execute("SELECT COUNT(*) FROM ops_calibration_result WHERE status = 'FAIL'").fetchone()[0] or 0)
        warn_rows = int(con.execute("SELECT COUNT(*) FROM ops_calibration_result WHERE status = 'WARN'").fetchone()[0] or 0)
        print(f"calibration_result_rows={total_rows}")
        print(f"calibration_fail_rows={fail_rows}")
        print(f"calibration_warn_rows={warn_rows}")
        if "v_ops_calibration_summary" in tables:
            summary = con.execute(
                """
                SELECT benchmark_class, status, total
                FROM v_ops_calibration_summary
                ORDER BY benchmark_class, status
                """
            ).fetchdf()
            if not summary.empty:
                print(summary.to_string(index=False))
        if fail_rows:
            preview = con.execute(
                """
                SELECT benchmark_id, benchmark_class, family, finding
                FROM v_ops_calibration_result
                WHERE status = 'FAIL'
                ORDER BY benchmark_class, family, benchmark_id
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
