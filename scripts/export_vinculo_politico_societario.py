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


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_rows(con: duckdb.DuckDBPyConnection, query: str, params: list | None = None) -> list[dict]:
    df = con.execute(query, params or []).fetchdf()
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def build_dossier(
    resumo_rows: list[dict],
    match_rows: list[dict],
    contract_rows: list[dict],
    insight_rows: list[dict],
) -> str:
    lines: list[str] = []
    lines.append("# Trace Vinculo Politico/Societario")
    lines.append("")
    lines.append("Camada conservadora para sobreposicoes exatas entre quadro societario, contratos publicos e bases publicas.")
    lines.append("")
    lines.append("## Resumo")
    lines.append("")
    lines.append(f"- empresas-alvo com QSA e contratos: `{len(resumo_rows)}`")
    lines.append(f"- empresas com match objetivo: `{sum(1 for row in resumo_rows if int(row['n_matches_objetivos'] or 0) > 0)}`")
    lines.append(f"- matches brutos materializados: `{len(match_rows)}`")
    lines.append(f"- contratos relacionados exportados: `{len(contract_rows)}`")
    lines.append(f"- insights gerados: `{len(insight_rows)}`")
    lines.append("")
    lines.append("## Leitura de rigor")
    lines.append("")
    lines.append("- Esta camada nao prova nepotismo, conflito ilegal ou favorecimento por si so.")
    lines.append("- Ela prova apenas coincidencia exata entre nome societario e base publica carregada.")
    lines.append("- Quando o QSA mascara CPF, a coincidencia por nome permanece objetiva, mas exige confirmacao documental externa para elevar o achado.")
    lines.append("")
    lines.append("## Casos")
    lines.append("")

    matches_by_cnpj: dict[str, list[dict]] = {}
    for row in match_rows:
        matches_by_cnpj.setdefault(str(row["cnpj"]), []).append(row)

    contracts_by_cnpj: dict[str, list[dict]] = {}
    for row in contract_rows:
        contracts_by_cnpj.setdefault(str(row["cnpj"]), []).append(row)

    insights_by_cnpj: dict[str, list[dict]] = {}
    for row in insight_rows:
        cnpj = str(row["id"]).split(":", 1)[-1]
        insights_by_cnpj.setdefault(cnpj, []).append(row)

    positive_rows = [row for row in resumo_rows if int(row["n_matches_objetivos"] or 0) > 0]
    if not positive_rows:
        lines.append("Nenhum caso com match objetivo foi materializado.")
        lines.append("")
        return "\n".join(lines)

    for row in positive_rows:
        cnpj = str(row["cnpj"])
        lines.append(f"### {row['razao_social']}")
        lines.append("")
        lines.append(f"- CNPJ: `{cnpj}`")
        lines.append(f"- exposicao publica mapeada: `{fmt_brl(row['exposure_brl'])}`")
        lines.append(f"- socios no QSA: `{int(row['n_socios'] or 0)}`")
        lines.append(f"- socios com match objetivo: `{int(row['n_socios_com_match'] or 0)}`")
        lines.append(f"- pessoas distintas com match: `{int(row['n_pessoas_distintas'] or 0)}`")
        lines.append(f"- bases objetivas atingidas: `{int(row['n_bases_objetivas'] or 0)}`")
        risco = json.loads(row.get("risco_json") or "[]")
        orgaos = json.loads(row.get("orgaos_json") or "[]")
        case_contracts = contracts_by_cnpj.get(cnpj, [])
        lines.append(f"- contratos relacionados exportados: `{len(case_contracts)}`")
        if risco:
            lines.append(f"- flags conservadoras: `{', '.join(risco)}`")
        if orgaos:
            lines.append("- orgaos/contratos mapeados:")
            for orgao in orgaos:
                lines.append(
                    f"  - `{orgao['fonte']}` / `{orgao['orgao']}` / `{orgao['n_contratos']}` contrato(s) / `{fmt_brl(orgao['valor_brl'])}`"
                )
        case_matches = matches_by_cnpj.get(cnpj, [])
        if case_matches:
            lines.append("- matches objetivos:")
            for match in case_matches:
                matched_orgao = match.get("matched_orgao") or "-"
                matched_vinculo = match.get("matched_vinculo") or "-"
                matched_cargo = match.get("matched_cargo") or "-"
                lines.append(
                    f"  - socio `{match.get('socio_nome') or '-'}` -> `{match['source_table']}` -> "
                    f"`{match.get('matched_nome') or '-'}` / `{matched_cargo}` / `{matched_orgao}` / `{matched_vinculo}`"
                )
        if case_contracts:
            lines.append("- contratos relacionados:")
            for contract in case_contracts:
                lines.append(
                    f"  - `{contract['fonte']}` / `{contract['ano']}` / `{contract['numero']}` / "
                    f"`{contract['orgao']}` / `{fmt_brl(contract['valor_brl'])}`"
                )
        case_insights = insights_by_cnpj.get(cnpj, [])
        if case_insights:
            lines.append("- limites do achado:")
            for ins in case_insights:
                lines.append(f"  - inferencia permitida: {ins['inferencia_permitida']}")
                lines.append(f"  - limite: {ins['limite_conclusao']}")
        lines.append("")

    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)

    resumo_rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_politico_societario_resumo
        ORDER BY n_socios_com_match DESC, n_pessoas_distintas DESC, exposure_brl DESC, razao_social
        """,
    )
    match_rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_politico_societario_matches
        ORDER BY cnpj, socio_nome, match_kind, source_table
        """,
    )
    contract_rows = load_rows(
        con,
        """
        WITH cnpjs AS (
            SELECT DISTINCT cnpj
            FROM v_vinculo_politico_societario_resumo
            WHERE n_matches_objetivos > 0
        )
        SELECT
            'estado_ac_contratos' AS fonte,
            regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') AS cnpj,
            ano,
            numero,
            coalesce(unidade_gestora, orgao) AS orgao,
            valor AS valor_brl,
            objeto
        FROM estado_ac_contratos
        WHERE regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') IN (SELECT cnpj FROM cnpjs)
        UNION ALL
        SELECT
            'rb_contratos' AS fonte,
            regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') AS cnpj,
            ano,
            numero_contrato AS numero,
            secretaria AS orgao,
            valor_brl,
            objeto
        FROM rb_contratos
        WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') IN (SELECT cnpj FROM cnpjs)
        ORDER BY cnpj, fonte, ano, numero
        """,
    )
    insight_rows = load_rows(
        con,
        """
        SELECT id, kind, severity, confidence, classe_achado, grau_probatorio, uso_externo,
               inferencia_permitida, limite_conclusao, title
        FROM insight
        WHERE kind LIKE 'RISCO_VINCULO_SOCIETARIO_%'
        ORDER BY id
        """,
    )
    con.close()

    resumo_path = OUT_DIR / "trace_vinculo_societario_resumo.csv"
    matches_path = OUT_DIR / "trace_vinculo_societario_matches.csv"
    contracts_path = OUT_DIR / "trace_vinculo_societario_contratos.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_DOSSIE.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_MANIFEST.json"

    write_csv(resumo_path, resumo_rows)
    write_csv(matches_path, match_rows)
    write_csv(contracts_path, contract_rows)
    dossier_path.write_text(
        build_dossier(resumo_rows, match_rows, contract_rows, insight_rows),
        encoding="utf-8",
    )

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "resumo_rows": len(resumo_rows),
        "positive_rows": sum(1 for row in resumo_rows if int(row["n_matches_objetivos"] or 0) > 0),
        "match_rows": len(match_rows),
        "contract_rows": len(contract_rows),
        "insight_rows": len(insight_rows),
        "files": {
            resumo_path.name: sha256_file(resumo_path),
            matches_path.name: sha256_file(matches_path),
            contracts_path.name: sha256_file(contracts_path),
            dossier_path.name: sha256_file(dossier_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"resumo_rows={len(resumo_rows)}")
    print(f"positive_rows={manifest['positive_rows']}")
    print(f"match_rows={len(match_rows)}")
    print(f"contract_rows={len(contract_rows)}")
    print(f"insight_rows={len(insight_rows)}")
    print(f"dossier={dossier_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
