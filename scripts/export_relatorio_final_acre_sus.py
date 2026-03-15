from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
PATCH_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch"
OUT_MD = PATCH_DIR / "relatorio_final_acre_rio_branco_sus.md"
OUT_CSV = PATCH_DIR / "painel_prioridades_acre_rio_branco_sus.csv"
OUT_JSON = PATCH_DIR / "manifest_relatorio_final_acre_rio_branco_sus.json"
RB_BUNDLE = PATCH_DIR / "rb_sus_prioritarios_bundle_20260313.tar.gz"
SESACRE_BUNDLE = PATCH_DIR / "sesacre_prioritarios_bundle_20260313.tar.gz"
TOP_SESACRE = 10


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bundle_meta(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"path": str(path.relative_to(ROOT)), "exists": False}
    return {
        "path": str(path.relative_to(ROOT)),
        "exists": True,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def fetch_snapshot(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    snapshot: dict[str, object] = {}
    scalar_queries = {
        "rb_lotacao_total": "SELECT COUNT(*) FROM rb_servidores_lotacao",
        "rb_lotacao_sus": "SELECT COUNT(*) FROM rb_servidores_lotacao WHERE sus = TRUE",
        "rb_sus_unidades": """
            SELECT COUNT(DISTINCT COALESCE(NULLIF(lotacao,''), NULLIF(secretaria,''), NULLIF(unidade,'')))
            FROM rb_servidores_lotacao
            WHERE sus = TRUE
        """,
        "rb_contratos_sus": "SELECT COUNT(*) FROM rb_contratos WHERE sus = TRUE",
        "rb_contratos_sus_cnpj": "SELECT COUNT(*) FROM rb_contratos WHERE sus = TRUE AND cnpj IS NOT NULL AND cnpj <> ''",
        "rb_prioritarios": "SELECT COUNT(*) FROM v_rb_contratos_prioritarios",
        "rb_insight_sancionado": "SELECT COUNT(*) FROM insight WHERE kind = 'RB_CONTRATO_SANCIONADO'",
        "rb_insight_inconsistente": "SELECT COUNT(*) FROM insight WHERE kind = 'RB_CONTRATO_LICITACAO_INCONSISTENTE'",
        "sesacre_ativas": "SELECT COUNT(*) FROM v_sancoes_ativas WHERE orgao_ac = 'SESACRE'",
        "sesacre_valor": "SELECT COALESCE(SUM(valor_contratado_ac), 0) FROM v_sancoes_ativas WHERE orgao_ac = 'SESACRE'",
        "sesacre_cross_rows": "SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes WHERE orgao = 'SESACRE'",
        "sesacre_insights": "SELECT COUNT(*) FROM insight WHERE kind = 'SESACRE_SANCAO_ATIVA' AND orgao = 'SESACRE'",
    }
    for key, query in scalar_queries.items():
        snapshot[key] = con.execute(query).fetchone()[0]

    snapshot["rb_despesas_sus"] = [
        {
            "ano": row[0],
            "unidade_relatorio": row[1],
            "atualizado_brl": float(row[2] or 0),
            "empenhado_brl": float(row[3] or 0),
            "liquidado_brl": float(row[4] or 0),
            "pago_brl": float(row[5] or 0),
        }
        for row in con.execute(
            """
            SELECT ano, unidade_relatorio, atualizado_brl, empenhado_brl, liquidado_brl, pago_brl
            FROM rb_despesas_unidade
            WHERE sus = TRUE
            ORDER BY ano, unidade_relatorio
            """
        ).fetchall()
    ]

    snapshot["rb_prioritarios_rows"] = [
        {
            "numero_contrato": str(row[0] or ""),
            "numero_processo": str(row[1] or ""),
            "secretaria": str(row[2] or ""),
            "fornecedor": str(row[3] or ""),
            "cnpj": str(row[4] or ""),
            "valor_referencia_brl": float(row[5] or 0),
            "fila": str(row[6] or ""),
            "prioridade": int(row[7] or 0),
        }
        for row in con.execute(
            """
            SELECT
                numero_contrato,
                numero_processo,
                secretaria,
                fornecedor,
                cnpj,
                valor_referencia_brl,
                fila_investigacao_final,
                prioridade_final
            FROM v_rb_contratos_prioritarios
            ORDER BY prioridade_final DESC, numero_contrato
            """
        ).fetchall()
    ]

    snapshot["sesacre_top10"] = [
        {
            "cnpj": str(row[0] or ""),
            "nome": str(row[1] or ""),
            "n_sancoes_ativas": int(row[2] or 0),
            "valor_contratado_ac": float(row[3] or 0),
            "n_contratos_ac": int(row[4] or 0),
            "tipos_sancao": str(row[5] or ""),
        }
        for row in con.execute(
            """
            SELECT
                cnpj_cpf,
                nome_sancionado,
                n_sancoes_ativas,
                valor_contratado_ac,
                n_contratos_ac,
                tipos_sancao
            FROM v_sancoes_ativas
            WHERE orgao_ac = 'SESACRE'
            ORDER BY valor_contratado_ac DESC
            LIMIT ?
            """,
            [TOP_SESACRE],
        ).fetchall()
    ]
    snapshot["sesacre_top10_qsa_covered"] = con.execute(
        """
        WITH top AS (
            SELECT cnpj_cpf
            FROM v_sancoes_ativas
            WHERE orgao_ac = 'SESACRE'
            ORDER BY valor_contratado_ac DESC
            LIMIT ?
        )
        SELECT COUNT(*)
        FROM top
        WHERE EXISTS (
            SELECT 1
            FROM estado_ac_fornecedor_qsa q
            WHERE q.orgao = 'SESACRE' AND q.cnpj = top.cnpj_cpf
        )
        """,
        [TOP_SESACRE],
    ).fetchone()[0]
    snapshot["sesacre_top10_socios_covered"] = con.execute(
        """
        WITH top AS (
            SELECT cnpj_cpf
            FROM v_sancoes_ativas
            WHERE orgao_ac = 'SESACRE'
            ORDER BY valor_contratado_ac DESC
            LIMIT ?
        )
        SELECT COUNT(*)
        FROM top
        WHERE EXISTS (
            SELECT 1
            FROM empresa_socios s
            WHERE s.cnpj = top.cnpj_cpf
        )
        """,
        [TOP_SESACRE],
    ).fetchone()[0]
    snapshot["sesacre_top10_detalhes_covered"] = con.execute(
        """
        WITH top AS (
            SELECT cnpj_cpf
            FROM v_sancoes_ativas
            WHERE orgao_ac = 'SESACRE'
            ORDER BY valor_contratado_ac DESC
            LIMIT ?
        )
        SELECT COUNT(*)
        FROM top
        WHERE EXISTS (
            SELECT 1
            FROM estado_ac_fornecedor_detalhes d
            WHERE d.orgao = 'SESACRE' AND d.cnpjcpf = top.cnpj_cpf
        )
        """,
        [TOP_SESACRE],
    ).fetchone()[0]
    snapshot["sesacre_top10_detail_rows"] = con.execute(
        """
        SELECT COUNT(*)
        FROM estado_ac_fornecedor_detalhes
        WHERE orgao = 'SESACRE'
          AND cnpjcpf IN (
            SELECT cnpj_cpf
            FROM v_sancoes_ativas
            WHERE orgao_ac = 'SESACRE'
            ORDER BY valor_contratado_ac DESC
            LIMIT ?
          )
        """,
        [TOP_SESACRE],
    ).fetchone()[0]
    snapshot["sesacre_qsa_insights"] = con.execute(
        "SELECT COUNT(*) FROM insight WHERE kind LIKE 'SESACRE_QSA_%' AND orgao = 'SESACRE'"
    ).fetchone()[0]
    return snapshot


def build_panel(snapshot: dict[str, object]) -> list[dict[str, object]]:
    panel: list[dict[str, object]] = []
    for row in snapshot["rb_prioritarios_rows"]:
        tipo_achado = row["fila"]
        if tipo_achado == "auditoria_documental_licitacao":
            tipo_achado = "divergencia_documental_licitacao"
        panel.append(
            {
                "esfera": "municipal",
                "orgao": "SEMSA",
                "tipo_achado": tipo_achado,
                "identificador": f"contrato_{row['numero_contrato']}",
                "processo_ou_cnpj": row["numero_processo"],
                "nome_ou_fornecedor": row["fornecedor"] or "(nao resolvido)",
                "documento": row["cnpj"],
                "valor_brl": row["valor_referencia_brl"],
                "prioridade": row["prioridade"],
            }
        )
    prioridade_base = 90
    for idx, row in enumerate(snapshot["sesacre_top10"], start=1):
        panel.append(
            {
                "esfera": "estadual",
                "orgao": "SESACRE",
                "tipo_achado": "fornecedor_sancionado_ativo",
                "identificador": f"sesacre_top_{idx}",
                "processo_ou_cnpj": row["cnpj"],
                "nome_ou_fornecedor": row["nome"],
                "documento": row["cnpj"],
                "valor_brl": row["valor_contratado_ac"],
                "prioridade": prioridade_base - idx,
            }
        )
    panel.sort(key=lambda item: (-int(item["prioridade"]), -float(item["valor_brl"])))
    return panel


def write_panel_csv(rows: list[dict[str, object]], path: Path) -> dict[str, object]:
    headers = [
        "esfera",
        "orgao",
        "tipo_achado",
        "identificador",
        "processo_ou_cnpj",
        "nome_ou_fornecedor",
        "documento",
        "valor_brl",
        "prioridade",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    return {
        "path": str(path.relative_to(ROOT)),
        "rows": len(rows),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def build_markdown(snapshot: dict[str, object], panel: list[dict[str, object]], bundles: dict[str, dict[str, object]]) -> str:
    lines = [
        "# Relatorio Final - Acre / Rio Branco / SUS",
        "",
        f"Gerado em `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`.",
        "",
        "Este relatorio consolida o estado atual do sistema para o recorte `Acre / Rio Branco / SUS`, distinguindo caso municipal ativo, historico invalidado e eixo estadual da SESACRE.",
        "",
        "## Resumo executivo",
        "",
        f"- Rio Branco: `{snapshot['rb_lotacao_total']}` servidores com lotacao materializada, sendo `{snapshot['rb_lotacao_sus']}` classificados como SUS.",
        f"- Rio Branco: `{snapshot['rb_contratos_sus']}` contratos SUS, com `{snapshot['rb_contratos_sus_cnpj']}` ja resolvidos com CNPJ.",
        f"- Rio Branco: `{snapshot['rb_prioritarios']}` caso(s) municipal(is) ativo(s) para uso externo.",
        "- Rio Branco: o contrato `3895` foi rebaixado para nota historica apos validacao temporal do cruzamento sancionatorio.",
        f"- SESACRE: `{snapshot['sesacre_ativas']}` fornecedores com sancao ativa, somando `{brl(snapshot['sesacre_valor'])}`.",
        f"- SESACRE: `{snapshot['sesacre_cross_rows']}` linhas de cruzamento bruto e `{snapshot['sesacre_insights']}` insights `SESACRE_SANCAO_ATIVA`.",
        f"- SESACRE top 10: `{snapshot['sesacre_top10_qsa_covered']}/10` com QSA, `{snapshot['sesacre_top10_socios_covered']}/10` com socios e `{snapshot['sesacre_top10_detalhes_covered']}/10` com detalhe financeiro (`{snapshot['sesacre_top10_detail_rows']}` linha(s)).",
        "",
        "## Despesas SUS municipais",
        "",
    ]
    for row in snapshot["rb_despesas_sus"]:
        lines.append(
            f"- `{row['ano']}` / `{row['unidade_relatorio']}`: atualizado `{brl(row['atualizado_brl'])}`, empenhado `{brl(row['empenhado_brl'])}`, liquidado `{brl(row['liquidado_brl'])}`, pago `{brl(row['pago_brl'])}`."
        )

    lines.extend(["", "## Fila priorizada consolidada", ""])
    for row in panel:
        ident = row["identificador"]
        lines.append(
            f"- prioridade `{row['prioridade']}` | `{row['esfera']}` | `{row['orgao']}` | `{ident}` | `{row['nome_ou_fornecedor']}` | `{brl(row['valor_brl'])}` | `{row['tipo_achado']}`"
        )

    lines.extend(["", "## Casos municipais finais", ""])
    for row in snapshot["rb_prioritarios_rows"]:
        lines.append(
            f"- contrato `{row['numero_contrato']}` / processo `{row['numero_processo']}` / valor `{brl(row['valor_referencia_brl'])}` / fila `{row['fila']}` / fornecedor `{row['fornecedor'] or '(nao resolvido)'}` / CNPJ `{row['cnpj'] or ''}`"
        )

    lines.extend(
        [
            "",
            "## Caso municipal historico rebaixado",
            "",
            "- contrato `3895` / processo `3044` / cruzamento sancionatorio invalidado por filtro temporal",
            "- leitura correta: historico de validacao de falso positivo, sem uso sancionatorio externo",
        ]
    )

    lines.extend(["", "## Top 10 SESACRE por valor contratado sob sancao ativa", ""])
    for idx, row in enumerate(snapshot["sesacre_top10"], start=1):
        lines.append(
            f"- `{idx}`. `{row['nome']}` | CNPJ `{row['cnpj']}` | `{row['n_sancoes_ativas']}` sancao(oes) ativa(s) | `{row['n_contratos_ac']}` contratos | `{brl(row['valor_contratado_ac'])}`"
        )

    lines.extend(
        [
            "",
            "## Artefatos prontos",
            "",
            f"- Bundle municipal: `{bundles['rb']['path']}` | sha256 `{bundles['rb'].get('sha256', '')}`",
            f"- Bundle SESACRE: `{bundles['sesacre']['path']}` | sha256 `{bundles['sesacre'].get('sha256', '')}`",
            "- Dossie municipal: `investigations/claude_march/patch_claude/claude_update/patch/dossie_rb_sus_prioritarios.md`",
            "- Relato municipal: `investigations/claude_march/patch_claude/claude_update/patch/relato_apuracao_3898.txt`",
            "- Nota historica 3895: `investigations/claude_march/patch_claude/claude_update/patch/entrega_denuncia_atual/nota_historica_3895_sancao_invalidada.txt`",
            "- Dossie SESACRE: `investigations/claude_march/patch_claude/claude_update/patch/sesacre_prioritarios/dossie_sesacre_sancoes_prioritarias.md`",
            "- Relato SESACRE: `investigations/claude_march/patch_claude/claude_update/patch/sesacre_prioritarios/relato_apuracao_sesacre_top10.txt`",
            "- Indice geral: `investigations/claude_march/patch_claude/claude_update/patch/INDEX_PRIORITARIOS.md`",
            "",
            "## Pendencias remanescentes",
            "",
            "- Municipal: o contrato `3898` segue sem fornecedor/CNPJ por fonte aberta, mas com anomalia documental forte ja materializada.",
            "- Municipal: a lotacao SUS ainda esta colapsada em 1 unidade materializada; falta granularidade mais fina por UBS/CAPS/UPA quando o portal expuser essa lotacao.",
            f"- Estadual: o top 10 da SESACRE ficou com `{snapshot['sesacre_top10_qsa_covered']}/10` QSA resolvido e `{snapshot['sesacre_top10_socios_covered']}/10` com socios; o gargalo remanescente e detalhe financeiro, hoje em `{snapshot['sesacre_top10_detalhes_covered']}/10` fornecedores.",
            f"- Estadual: existem `{snapshot['sesacre_qsa_insights']}` insights societarios `SESACRE_QSA_%`, mas a cobertura ainda nao e uniforme em todos os sancionados priorizados.",
            "- Federal: `PNCP` continua sem papel relevante no caso municipal ja resolvido; segue como fonte complementar eventual, nao como eixo principal.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_text(path: Path, content: str) -> dict[str, object]:
    path.write_text(content, encoding="utf-8")
    return {
        "path": str(path.relative_to(ROOT)),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    snapshot = fetch_snapshot(con)
    con.close()

    panel = build_panel(snapshot)
    bundles = {
        "rb": bundle_meta(RB_BUNDLE),
        "sesacre": bundle_meta(SESACRE_BUNDLE),
    }

    md_meta = write_text(OUT_MD, build_markdown(snapshot, panel, bundles))
    csv_meta = write_panel_csv(panel, OUT_CSV)
    json_meta = write_text(
        OUT_JSON,
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(),
                "database": str(DB_PATH.relative_to(ROOT)),
                "snapshot": snapshot,
                "panel_rows": len(panel),
                "artifacts": {
                    "markdown": md_meta,
                    "csv": csv_meta,
                    "bundles": bundles,
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
    )

    print(md_meta["path"])
    print(csv_meta["path"])
    print(json_meta["path"])


if __name__ == "__main__":
    main()
