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
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def build_dossier(rows: list[dict], coverage: list[dict]) -> str:
    total = len(rows)
    received = sum(1 for row in rows if row["status_documento"] in {"RECEBIDO", "ANALISADO", "VALIDADO"})
    pending = sum(1 for row in rows if row["status_documento"] == "PENDENTE")
    missing = sum(1 for row in rows if row["status_documento"] == "ARQUIVO_NAO_LOCALIZADO")

    lines: list[str] = []
    lines.append("# Trace Vinculo Societario em Saude - Respostas Oficiais")
    lines.append("")
    lines.append("Camada de recepcao, hash e cobertura dos documentos oficiais esperados para o caso `CEDIMP`.")
    lines.append("")
    lines.append("## Resumo")
    lines.append("")
    lines.append(f"- documentos esperados: `{total}`")
    lines.append(f"- documentos recebidos/localizados: `{received}`")
    lines.append(f"- documentos ainda pendentes: `{pending}`")
    lines.append(f"- referencias quebradas: `{missing}`")
    lines.append("")
    lines.append("## Cobertura por eixo")
    lines.append("")
    if not coverage:
        lines.append("Nenhuma cobertura materializada.")
        return "\n".join(lines)
    for row in coverage:
        lines.append(f"### {row['eixo']}")
        lines.append("")
        lines.append(f"- recebidos/localizados: `{row['docs_localizados']}` de `{row['docs_esperados']}`")
        lines.append(f"- pendencias: {row.get('documentos_pendentes') or 'nenhuma'}")
        lines.append(
            f"- ficha funcional: `{bool(row['has_ficha_funcional'])}` / declaracao acumulacao: `{bool(row['has_declaracao_acumulacao'])}` / escala-ponto: `{bool(row['has_escala_ponto'])}`"
        )
        lines.append(
            f"- parecer compatibilidade: `{bool(row['has_parecer_compatibilidade'])}` / autorizacao gerencia: `{bool(row['has_autorizacao_gerencia'])}`"
        )
        lines.append(
            f"- processo contrato: `{bool(row['has_processo_contrato'])}` / execucao-medicao: `{bool(row['has_execucao_medicao'])}` / profissionais execucao: `{bool(row['has_relacao_profissionais_execucao'])}`"
        )
        lines.append("")
    lines.append("## Regra")
    lines.append("")
    lines.append("- Esta camada nao conclui ilegalidade.")
    lines.append("- Ela apenas registra se a prova complementar chegou, com hash e trilha local.")
    lines.append("- A maturidade do caso so pode subir por analise humana e rerun dos syncs.")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_societario_saude_respostas
        ORDER BY destino, eixo, documento_chave
        """,
    )
    coverage = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_societario_saude_respostas_cobertura
        ORDER BY eixo
        """,
    )
    con.close()

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_respostas.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_DOSSIE.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_MANIFEST.json"

    write_csv(csv_path, rows)
    dossier_path.write_text(build_dossier(rows, coverage), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "rows": len(rows),
        "coverage_rows": len(coverage),
        "files": {
            csv_path.name: sha256_file(csv_path),
            dossier_path.name: sha256_file(dossier_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"rows={len(rows)}")
    print(f"coverage_rows={len(coverage)}")
    print(f"dossier={dossier_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
