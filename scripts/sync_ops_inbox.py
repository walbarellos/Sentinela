from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_inbox import sync_ops_inbox


def main() -> None:
    parser = argparse.ArgumentParser(description="Sincroniza a caixa operacional de respostas no DuckDB.")
    parser.add_argument("--case-id", help="Sincroniza apenas um caso configurado.")
    args = parser.parse_args()

    con = duckdb.connect("data/sentinela_analytics.duckdb")
    try:
        stats = sync_ops_inbox(con, case_id=args.case_id)
        print(stats)
    finally:
        con.close()


if __name__ == "__main__":
    main()
