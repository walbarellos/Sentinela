from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = (
    ROOT
    / "docs"
    / "Claude-march"
    / "patch_claude"
    / "claude_update"
    / "patch"
    / "entrega_denuncia_atual"
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict]:
    df = con.execute(query).fetchdf()
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def build_dossier(rows: list[dict]) -> str:
    lines: list[str] = []
    lines.append("# Trace Vinculo Societario em Saude - Maturidade Probatoria")
    lines.append("")
    lines.append("Matriz de resposta para dizer com clareza o que esta provado, o que ainda depende de documento e o que nao tem base atual.")
    lines.append("")
    lines.append("## Regra")
    lines.append("")
    lines.append("- `COMPROVADO_DOCUMENTAL`: fato sustentado por fonte primaria ou base publica objetiva.")
    lines.append("- `PENDENTE_DOCUMENTO` / `PENDENTE_ENQUADRAMENTO`: exige documento ou analise juridica adicional.")
    lines.append("- `SEM_BASE_ATUAL` / `SEM_CONCLUSAO_AUTOMATICA`: o sistema nao deve afirmar isso hoje.")
    lines.append("")
    lines.append(f"## Linhas: `{len(rows)}`")
    lines.append("")
    if not rows:
        lines.append("Nenhuma linha materializada.")
        return "\n".join(lines)
    for row in rows:
        lines.append(f"### {row['eixo']}")
        lines.append("")
        lines.append(f"- status: `{row['status_probatorio']}`")
        lines.append(f"- uso externo: `{row['uso_externo']}`")
        lines.append(f"- evidencia: {row['evidencia_resumo']}")
        lines.append(f"- proximo documento/ato: {row['proximo_documento']}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_societario_saude_maturidade
        ORDER BY eixo
        """,
    )
    con.close()

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_maturidade.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_DOSSIE.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_MANIFEST.json"

    write_csv(csv_path, rows)
    dossier_path.write_text(build_dossier(rows), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "rows": len(rows),
        "files": {
            csv_path.name: sha256_file(csv_path),
            dossier_path.name: sha256_file(dossier_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"rows={len(rows)}")
    print(f"dossier={dossier_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
