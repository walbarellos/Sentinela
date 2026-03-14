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
    lines.append("# Trace Vinculo Societario em Saude - Matriz Juridico-Funcional")
    lines.append("")
    lines.append("Matriz conservadora para controle externo. Organiza fatos, normas e perguntas de apuracao sem declarar ilegalidade automatica.")
    lines.append("")
    lines.append("## Regra")
    lines.append("")
    lines.append("- So entra aqui caso com fonte primaria de saude, contrato publico e carga concomitante documentada.")
    lines.append("- Os marcos `>=60h` e `>=80h` sao criterios tecnicos de triagem, nao conclusao juridica.")
    lines.append("- A matriz aponta o que precisa ser confrontado com regime funcional e norma aplicavel.")
    lines.append("")
    lines.append(f"## Casos: `{len(rows)}`")
    lines.append("")

    if not rows:
        lines.append("Nenhum caso elegivel foi materializado.")
        lines.append("")
        return "\n".join(lines)

    for row in rows:
        normas = json.loads(row.get("normas_json") or "[]")
        perguntas = json.loads(row.get("perguntas_apuracao_json") or "[]")
        achados = json.loads(row.get("achados_objetivos_json") or "[]")
        socio_admin = json.loads(row.get("socios_administradores_publicos_json") or "[]")
        lines.append(f"### {row['razao_social']}")
        lines.append("")
        lines.append(f"- CNPJ: `{row['cnpj']}`")
        lines.append(f"- contrato: `{row['contrato_numero']}` / `{row['contrato_orgao']}` / `{fmt_brl(row['contrato_valor_brl'])}`")
        lines.append(
            f"- metricas: `{row['n_profissionais_concomitancia']}` profissional(is) / "
            f"`{row['n_competencias_concomitantes_total']}` competencias / "
            f"`>=60h` `{row['n_competencias_ge_60h']}` / `>=80h` `{row['n_competencias_ge_80h']}` / "
            f"pico `{row['max_ch_total_concomitante']}h`"
        )
        if socio_admin:
            lines.append("- socios-administradores com coincidencia exata em base publica:")
            for item in socio_admin:
                lines.append(
                    f"  - `{item['socio_nome']}` / `{item['qualificacao_qsa']}` / `{item['cargo_publico']}` / "
                    f"`{item['secretaria']}` / `{item['ch_publica_local']}h`"
                )
        lines.append("- achados objetivos:")
        for item in achados:
            lines.append(f"  - {item}")
        lines.append("- perguntas de apuracao:")
        for item in perguntas:
            normas_ids = ", ".join(item.get("normas", []))
            lines.append(f"  - `{item['codigo']}` {item['pergunta']} [{normas_ids}]")
        lines.append("- normas primarias para confronto:")
        for item in normas:
            lines.append(f"  - `{item['artigo']}` / `{item['fonte']}` / {item['url']}")
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
        FROM v_vinculo_societario_saude_juridico
        ORDER BY contrato_valor_brl DESC, razao_social
        """,
    )
    con.close()

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_juridico.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_DOSSIE.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_MANIFEST.json"

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
