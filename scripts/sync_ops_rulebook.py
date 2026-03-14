from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_rulebook import sync_ops_rulebook


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    try:
        stats = sync_ops_rulebook(con)
        print(f"rule_rows={stats.get('rules_written', 0)}")
        print(f"validation_rows={stats.get('validation_rows', 0)}")
        print(f"fail_rows={stats.get('fail_rows', 0)}")
        print(f"warn_rows={stats.get('warn_rows', 0)}")
        return 0 if int(stats.get("fail_rows", 0)) == 0 else 2
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
