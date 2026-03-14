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
    lines.append("# Trace Vinculo Societario em Saude")
    lines.append("")
    lines.append("Follow-up conservador com `CNES` oficial, contrato publico e perfil funcional local.")
    lines.append("")
    lines.append("## Regra")
    lines.append("")
    lines.append("- O caso so entra aqui quando existe match exato em base publica, contrato de saude e estabelecimento oficial no CNES.")
    lines.append("- A saida continua em revisao interna. Nao afirma nepotismo, impedimento legal ou conflito ilicito.")
    lines.append("")
    lines.append(f"## Casos: `{len(rows)}`")
    lines.append("")

    if not rows:
        lines.append("Nenhum caso elegivel foi materializado.")
        lines.append("")
        return "\n".join(lines)

    for row in rows:
        socios = json.loads(row.get("socios_publicos_json") or "[]")
        flags = json.loads(row.get("overlap_flags_json") or "[]")
        horario = json.loads(row.get("cnes_horario_json") or "[]")
        classificacoes = json.loads(row.get("cnes_servicos_classificacao_json") or "[]")
        profissionais = json.loads(row.get("cnes_profissionais_match_json") or "[]")
        historico = json.loads(row.get("cnes_profissionais_historico_json") or "[]")
        metricas = json.loads(row.get("cnes_historico_metricas_json") or "{}")
        lines.append(f"### {row['razao_social']}")
        lines.append("")
        lines.append(f"- CNPJ: `{row['cnpj']}`")
        lines.append(f"- contrato estadual: `{row['contrato_numero']}` / `{row['contrato_orgao']}` / `{fmt_brl(row['contrato_valor_brl'])}`")
        lines.append(f"- CNAE: `{row.get('cnae_principal') or '-'}` / `{row.get('cnae_descricao') or '-'}`")
        lines.append(f"- CNES: `{row.get('cnes_code') or '-'}` / `{row.get('cnes_nome') or '-'}`")
        lines.append(f"- ficha oficial: `{row.get('cnes_ficha_url') or '-'}`")
        lines.append(
            f"- tipo CNES: `{row.get('cnes_tipo_estabelecimento') or '-'}` / "
            f"`{row.get('cnes_subtipo_estabelecimento') or '-'}` / gestao `{row.get('cnes_gestao') or '-'}` / dependencia `{row.get('cnes_dependencia') or '-'}`"
        )
        lines.append(
            f"- cadastro CNES: `{row.get('cnes_cadastrado_em') or '-'}` / ultima atualizacao `{row.get('cnes_ultima_atualizacao') or '-'}` / local `{row.get('cnes_atualizacao_local') or '-'}`"
        )
        lines.append(
            f"- endereco CNES: `{row.get('cnes_endereco') or '-'} / {row.get('cnes_bairro') or '-'} / {row.get('cnes_municipio') or '-'} / {row.get('cnes_uf') or '-'}`"
        )
        if horario:
            horario_text = "; ".join(f"{item['dia_semana']}: {item['horario']}" for item in horario[:3])
            lines.append(f"- horario amostra: `{horario_text}`")
        if classificacoes:
            lines.append(
                "- classificacoes CNES de diagnostico: `"
                + ", ".join(item["classificacao"] for item in classificacoes)
                + "`"
            )
        if flags:
            lines.append(f"- flags objetivas: `{', '.join(flags)}`")
        if profissionais:
            lines.append("- profissionais coincidentes no CNES:")
            for prof in profissionais:
                lines.append(
                    f"  - `{prof['nome']}` / `{prof['cbo']}` / amb `{prof['ch_amb']}` / total `{prof['total']}` / "
                    f"`{prof['vinculacao']}` / `{prof['tipo']}` / situacao `{prof['situacao']}`"
                )
        if historico:
            lines.append("- historico oficial do CNES:")
            for prof in historico:
                comp_list = [item.get("comp", "") for item in prof.get("concomitancias", []) if item.get("comp")]
                comp_sample = ", ".join(comp_list[:6]) if comp_list else "-"
                if len(comp_list) > 6:
                    comp_sample += f" ... (+{len(comp_list) - 6})"
                publico = prof.get("publico_rows", [])
                empresa = prof.get("empresa_rows", [])
                publico_txt = (
                    f"{publico[0].get('estabelecimento','-')} / {publico[0].get('tipo','-')} / {publico[0].get('subtipo','-')}"
                    if publico
                    else "-"
                )
                empresa_txt = (
                    f"{empresa[0].get('estabelecimento','-')} / {empresa[0].get('tipo','-')} / {empresa[0].get('subtipo','-')}"
                    if empresa
                    else "-"
                )
                lines.append(
                    f"  - `{prof['nome']}` / CNS `{prof.get('cns') or '-'}` / competencias concomitantes `{comp_sample}` / "
                    f"publico `{publico_txt}` / empresa `{empresa_txt}`"
                )
        if metricas:
            lines.append(
                f"- metricas documentais de carga: `{metricas.get('n_competencias_concomitantes_total', 0)}` competencias / "
                f"`>=60h` `{metricas.get('n_competencias_ge_60h', 0)}` / `>=80h` `{metricas.get('n_competencias_ge_80h', 0)}` / "
                f"pico `{metricas.get('max_ch_total_concomitante', 0)}h`"
            )
            for item in metricas.get("profissionais", []):
                lines.append(
                    f"  - `{item['nome']}` / pico `{item.get('competencia_pico') or '-'}` / "
                    f"publico `{item.get('max_ch_total_publico', 0)}h` / empresa `{item.get('max_ch_total_empresa', 0)}h` / "
                    f"total `{item.get('max_ch_total_concomitante', 0)}h` / `>=60h` `{item.get('n_competencias_ge_60h', 0)}` / "
                    f"`>=80h` `{item.get('n_competencias_ge_80h', 0)}`"
                )
        lines.append("- socios publicos locais:")
        for socio in socios:
            lines.append(
                f"  - `{socio['socio_nome']}` / `{socio['cargo']}` / `{socio['secretaria'] or socio['lotacao']}` / "
                f"`{socio['ch']}h` / admissao `{socio['admissao']}` / faixa liquida `{fmt_brl(socio['salario_liquido_min'])}` a `{fmt_brl(socio['salario_liquido_max'])}`"
            )
        lines.append("- limite do achado:")
        lines.append("  - confirma sobreposicao objetiva entre quadro societario, CNES oficial, ficha oficial de profissionais, contrato estadual e cargo publico municipal.")
        lines.append("  - nao confirma, sozinho, impedimento legal, conflito de interesses vedado, nepotismo ou acumulo ilicito.")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = load_rows(
        con,
        """
        SELECT *
        FROM v_vinculo_societario_saude_followup
        ORDER BY contrato_valor_brl DESC, razao_social
        """,
    )
    insights = load_rows(
        con,
        """
        SELECT id, kind, classe_achado, grau_probatorio, uso_externo, inferencia_permitida, limite_conclusao
        FROM insight
        WHERE kind IN (
            'QSA_VINCULO_SOCIETARIO_SAUDE_EXATO',
            'VINCULO_EXATO_CNES_PROFISSIONAL_SAUDE',
            'VINCULO_EXATO_CNES_HISTORICO_PUBLICO_PRIVADO_SAUDE',
            'VINCULO_EXATO_CNES_CARGA_CONCOMITANTE_SAUDE'
        )
        ORDER BY id
        """,
    )
    con.close()

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_followup.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_MANIFEST.json"

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
