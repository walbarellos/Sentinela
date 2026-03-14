from __future__ import annotations

from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_calibration import sync_ops_calibration


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    try:
        stats = sync_ops_calibration(con)
        print(f"calibration_benchmark_rows={stats.get('benchmark_rows', 0)}")
        print(f"calibration_result_rows={stats.get('result_rows', 0)}")
        print(f"calibration_fail_rows={stats.get('fail_rows', 0)}")
        print(f"calibration_warn_rows={stats.get('warn_rows', 0)}")
        return 0 if int(stats.get("fail_rows", 0) or 0) == 0 else 2
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
