from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_guard import sync_ops_language_guard


def main() -> None:
    parser = argparse.ArgumentParser(description="Materializa o gate de linguagem externa nao-acusatoria.")
    parser.parse_args()

    con = duckdb.connect("data/sentinela_analytics.duckdb")
    try:
        stats = sync_ops_language_guard(con)
        print(stats)
    finally:
        con.close()


if __name__ == "__main__":
    main()
