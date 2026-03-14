from __future__ import annotations

import argparse
from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_runbook import ensure_ops_runbook, sync_ops_runbook


def main() -> None:
    parser = argparse.ArgumentParser(description="Materializa runbook operacional por caso.")
    parser.add_argument("--db", default=str(ROOT / "data" / "sentinela_analytics.duckdb"))
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    try:
        ensure_ops_runbook(con)
        stats = sync_ops_runbook(con)
    finally:
        con.close()

    print(f"rows_written={stats.get('rows_written', 0)}")
    print(f"steps_written={stats.get('steps_written', 0)}")
    print(f"cases={stats.get('cases', 0)}")


if __name__ == "__main__":
    main()
