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
        print(f"burden_rows={stats.get('burden_rows', 0)}")
        print(f"semantic_rows={stats.get('semantic_rows', 0)}")
        print(f"contradiction_rows={stats.get('contradiction_rows', 0)}")
        print(f"checklist_rows={stats.get('checklist_rows', 0)}")
        print(f"language_guard_rows={stats.get('language_guard_rows', 0)}")
        print(f"export_gate_rows={stats.get('export_gate_rows', 0)}")
        print(f"generated_export_rows={stats.get('generated_export_rows', 0)}")
        print(f"indexed_docs={stats.get('indexed_docs', 0)}")
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
