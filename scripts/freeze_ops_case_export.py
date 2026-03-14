from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.ops_export import freeze_case_external_text
from src.core.ops_registry import sync_ops_case_registry
from src.core.ops_runtime import begin_pipeline_run, ensure_ops_runtime, finish_pipeline_run


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def main() -> int:
    parser = argparse.ArgumentParser(description="Congela uma exportacao segura como artefato controlado do caso.")
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--mode", required=True, choices=["NOTA_INTERNA", "PEDIDO_DOCUMENTAL", "NOTICIA_FATO"])
    parser.add_argument("--actor", default="script")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))
    ensure_ops_runtime(con)
    run_id = begin_pipeline_run(
        con,
        f"freeze_ops_case_export:{args.case_id}:{args.mode.lower()}",
        trigger_mode="manual",
        actor=args.actor,
    )
    try:
        frozen = freeze_case_external_text(con, case_id=args.case_id, export_mode=args.mode, actor=args.actor)
        registry_stats = sync_ops_case_registry(con)
        finish_pipeline_run(
            con,
            run_id,
            status="success",
            rows_written=int(frozen.get("rows_written", 0)),
            artifacts_written=1,
            details={
                "case_id": args.case_id,
                "mode": args.mode,
                "path": frozen.get("path"),
                "sha256": frozen.get("sha256"),
                "reused": bool(frozen.get("reused")),
                "registry_cases": int(registry_stats.get("cases", 0)),
                "registry_artifacts": int(registry_stats.get("artifacts", 0)),
            },
        )
        print(f"case_id={args.case_id}")
        print(f"export_mode={args.mode}")
        print(f"path={frozen.get('path')}")
        print(f"sha256={frozen.get('sha256')}")
        print(f"reused={str(bool(frozen.get('reused'))).lower()}")
        return 0
    except Exception as exc:
        finish_pipeline_run(
            con,
            run_id,
            status="failed",
            error_text=str(exc),
            details={"case_id": args.case_id, "mode": args.mode},
        )
        raise
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
