from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_timeline import ensure_ops_timeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Materializa a view operacional de timeline por caso.")
    parser.parse_args()

    con = duckdb.connect("data/sentinela_analytics.duckdb")
    try:
        ensure_ops_timeline(con)
        count = con.execute("select count(*) from v_ops_case_timeline_event").fetchone()[0]
        print({"timeline_events": count})
    finally:
        con.close()


if __name__ == "__main__":
    main()
