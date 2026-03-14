from __future__ import annotations

from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_registry import sync_ops_case_registry
from src.core.ops_runtime import begin_pipeline_run, ensure_ops_runtime, finish_pipeline_run


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    ensure_ops_runtime(con)
    run_id = begin_pipeline_run(
        con,
        "sync_ops_case_registry",
        trigger_mode="manual",
        actor="script",
    )
    try:
        stats = sync_ops_case_registry(con)
        finish_pipeline_run(
            con,
            run_id,
            status="success",
            rows_written=int(stats["cases"]),
            artifacts_written=int(stats["artifacts"]),
            details=stats,
        )
        print(f"cases={stats['cases']}")
        print(f"artifacts={stats['artifacts']}")
        return 0
    except Exception as exc:
        finish_pipeline_run(
            con,
            run_id,
            status="failed",
            error_text=str(exc),
            details={"pipeline": "sync_ops_case_registry"},
        )
        raise
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
