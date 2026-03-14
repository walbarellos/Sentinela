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
    lines.append("# Trace Vinculo Societario em Saude - Apuracao Funcional")
    lines.append("")
    lines.append("Camada interna de priorizacao funcional. Nao conclui ilegalidade; organiza diligencia.")
    lines.append("")
    lines.append("## Regra")
    lines.append("")
    lines.append("- Usa apenas fatos ja documentados em `CNES`, `QSA`, base local e contrato.")
    lines.append("- O score e a prioridade sao instrumentos internos de triagem.")
    lines.append("- A saida permanece em `REVISAO_INTERNA`.")
    lines.append("")
    lines.append(f"## Casos: `{len(rows)}`")
    lines.append("")

    if not rows:
        lines.append("Nenhum caso elegivel foi materializado.")
        lines.append("")
        return "\n".join(lines)

    for row in rows:
        flags = json.loads(row.get("flags_json") or "[]")
        resumo = json.loads(row.get("resumo_profissionais_json") or "[]")
        diligencias = json.loads(row.get("diligencias_json") or "[]")
        lines.append(f"### {row['razao_social']}")
        lines.append("")
        lines.append(f"- CNPJ: `{row['cnpj']}`")
        lines.append(f"- contrato: `{row['contrato_numero']}` / `{row['contrato_orgao']}` / `{fmt_brl(row['contrato_valor_brl'])}`")
        lines.append(f"- score interno: `{row['score_triagem']}` / prioridade `{row['prioridade']}`")
        if flags:
            lines.append(f"- flags de triagem: `{', '.join(flags)}`")
        lines.append("- resumo por profissional:")
        for item in resumo:
            lines.append(
                f"  - `{item['nome']}` / local `{item['ch_publica_local']}h` / CNES publico max `{item['max_ch_publica_cnes']}h` / "
                f"empresa max `{item['max_ch_empresa_cnes']}h` / pico `{item['max_ch_total_concomitante']}h` / "
                f"delta publico `{item['delta_publico']}h` / flags `{', '.join(item['flags']) or 'nenhuma'}`"
            )
        lines.append("- diligencias sugeridas:")
        for item in diligencias:
            lines.append(f"  - {item}")
        lines.append(f"- conclusao operacional: {row['conclusao_operacional']}")
        lines.append(f"- limite: {row['limite_conclusao']}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_societario_saude_apuracao_funcional
        ORDER BY score_triagem DESC, contrato_valor_brl DESC, razao_social
        """,
    )
    insights = load_rows(
        con,
        """
        SELECT id, kind, classe_achado, grau_probatorio, uso_externo
        FROM insight
        WHERE kind = 'APURACAO_FUNCIONAL_SAUDE_PRIORITARIA'
        ORDER BY id
        """,
    )
    con.close()

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_apuracao_funcional.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_DOSSIE.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_MANIFEST.json"

    write_csv(csv_path, rows)
    dossier_path.write_text(build_dossier(rows), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "rows": len(rows),
        "insights": len(insights),
        "files": {
            csv_path.name: sha256_file(csv_path),
            dossier_path.name: sha256_file(dossier_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"rows={len(rows)}")
    print(f"insights={len(insights)}")
    print(f"dossier={dossier_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
