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


def fmt_brl(value: object) -> str:
    number = float(value or 0)
    text = f"{number:,.2f}"
    return "R$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")


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


def build_dossier(rows: list[dict]) -> str:
    lines: list[str] = []
    lines.append("# Trace Vinculo Societario em Saude - Gate Operacional")
    lines.append("")
    lines.append("Camada de decisao conservadora para dizer se o caso esta pronto para ofício documental, analise interna ou representacao preliminar.")
    lines.append("")
    lines.append("## Regra")
    lines.append("")
    lines.append("- O gate nao decide ilegalidade.")
    lines.append("- O gate so decide o proximo uso operacional permitido pelo estado atual da prova.")
    lines.append("- Se os bloqueios remanescentes forem funcionais/juridicos, o caso nao sobe para representacao.")
    lines.append("")
    lines.append(f"## Casos: `{len(rows)}`")
    lines.append("")
    if not rows:
        lines.append("Nenhum caso materializado.")
        return "\n".join(lines)
    for row in rows:
        requisitos = json.loads(row.get("requisitos_cumpridos_json") or "[]")
        bloqueios = json.loads(row.get("bloqueios_json") or "[]")
        recomendacoes = json.loads(row.get("recomendacoes_json") or "[]")
        lines.append(f"### {row['razao_social']}")
        lines.append("")
        lines.append(f"- CNPJ: `{row['cnpj']}`")
        lines.append(f"- contrato: `{row['contrato_numero']}` / `{row['contrato_orgao']}` / `{fmt_brl(row['contrato_valor_brl'])}`")
        lines.append(f"- score funcional: `{row['score_triagem']}`")
        lines.append(f"- estagio operacional: `{row['estagio_operacional']}`")
        lines.append(f"- uso recomendado: `{row['uso_recomendado']}` / externo `{bool(row['pode_uso_externo'])}`")
        lines.append(f"- resumo: {row['resumo_decisao']}")
        lines.append("- requisitos cumpridos:")
        for item in requisitos:
            lines.append(f"  - {item}")
        lines.append("- bloqueios remanescentes:")
        for item in bloqueios:
            lines.append(f"  - {item}")
        lines.append("- recomendacoes:")
        for item in recomendacoes:
            lines.append(f"  - {item}")
        lines.append(f"- limite: {row['limite_decisao']}")
        lines.append("")
    return "\n".join(lines)


def build_note(row: dict) -> str:
    requisitos = json.loads(row.get("requisitos_cumpridos_json") or "[]")
    bloqueios = json.loads(row.get("bloqueios_json") or "[]")
    recomendacoes = json.loads(row.get("recomendacoes_json") or "[]")
    lines = [
        f"Assunto: nota operacional conservadora - {row['razao_social']} ({row['cnpj']})",
        "",
        f"Estagio atual: {row['estagio_operacional']}",
        f"Uso recomendado: {row['uso_recomendado']}",
        "",
        "Requisitos cumpridos:",
    ]
    for idx, item in enumerate(requisitos, start=1):
        lines.append(f"{idx}. {item}")
    lines.extend(["", "Bloqueios remanescentes:"])
    for idx, item in enumerate(bloqueios, start=1):
        lines.append(f"{idx}. {item}")
    lines.extend(["", "Proximo passo recomendado:"])
    for idx, item in enumerate(recomendacoes, start=1):
        lines.append(f"{idx}. {item}")
    lines.extend(["", f"Limite: {row['limite_decisao']}"])
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_societario_saude_gate
        ORDER BY score_triagem DESC, contrato_valor_brl DESC, razao_social
        """,
    )
    con.close()

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_gate.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_DOSSIE.md"
    note_path = OUT_DIR / "nota_operacional_cedimp.txt"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_MANIFEST.json"

    write_csv(csv_path, rows)
    dossier_path.write_text(build_dossier(rows), encoding="utf-8")
    if rows:
        note_path.write_text(build_note(rows[0]), encoding="utf-8")

    files = {
        csv_path.name: sha256_file(csv_path),
        dossier_path.name: sha256_file(dossier_path),
    }
    if rows:
        files[note_path.name] = sha256_file(note_path)
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "rows": len(rows),
        "files": files,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"rows={len(rows)}")
    print(f"dossier={dossier_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
