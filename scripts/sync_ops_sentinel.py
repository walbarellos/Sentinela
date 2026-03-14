from __future__ import annotations

from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_sentinel import sync_ops_sentinel


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    try:
        stats = sync_ops_sentinel(con)
        print(f"sentinel_rows={stats.get('sentinel_rows', 0)}")
        print(f"sentinel_result_rows={stats.get('result_rows', 0)}")
        print(f"sentinel_fail_rows={stats.get('fail_rows', 0)}")
        print(f"sentinel_warn_rows={stats.get('warn_rows', 0)}")
        return 0 if int(stats.get("fail_rows", 0) or 0) == 0 else 2
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
