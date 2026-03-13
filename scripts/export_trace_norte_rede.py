from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
OUT_MD = OUT_DIR / "TRACE_NORTE_REDE_DOSSIE.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_REDE_MANIFEST.json"
OUT_CSV = OUT_DIR / "trace_norte_rede_empresas.csv"
OUT_CONTRATOS_CSV = OUT_DIR / "trace_norte_rede_contratos.csv"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def fetch_payload(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    empresas = con.execute("SELECT * FROM v_trace_norte_rede_resumo").fetchall()
    cols = [row[1] for row in con.execute("PRAGMA table_info('v_trace_norte_rede_resumo')").fetchall()]
    socios = con.execute(
        """
        SELECT cnpj, socio_nome, qualificacao, shared_with_focal, match_servidores, match_rb_lotacao, match_cross_candidato_servidor
        FROM trace_norte_rede_socios
        ORDER BY cnpj, socio_nome
        """
    ).fetchall()
    contratos = con.execute(
        """
        SELECT cnpj, fornecedor_nome, orgao, unidade_gestora, ano, numero_contrato, valor_brl, terceirizacao_pessoal, objeto
        FROM trace_norte_rede_contratos
        ORDER BY valor_brl DESC, orgao, numero_contrato
        """
    ).fetchall()
    return {
        "empresa_cols": cols,
        "empresas": empresas,
        "socios": socios,
        "contratos": contratos,
    }


def write_csv(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM v_trace_norte_rede_resumo
        ) TO '{OUT_CSV.as_posix()}' (HEADER, DELIMITER ',')
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM trace_norte_rede_contratos
            ORDER BY valor_brl DESC, orgao, numero_contrato
        ) TO '{OUT_CONTRATOS_CSV.as_posix()}' (HEADER, DELIMITER ',')
        """
    )


def render_md(payload: dict[str, object]) -> str:
    cols = payload["empresa_cols"]
    empresas = [dict(zip(cols, row)) for row in payload["empresas"]]
    socios = payload["socios"]
    contratos = payload["contratos"]
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    parts = [
        "# Trace NORTE - Rede de Leads",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este dossie cobre apenas a camada de expansao da rede por nome semelhante em contratos de terceirizacao.",
        "",
        "## Resumo",
        "",
        f"- Empresas-lead enriquecidas: `{len(empresas)}`",
        f"- Socios materializados nos leads: `{len(socios)}`",
        f"- Contratos exportados para auditoria: `{len(contratos)}`",
        f"- Shared socio exato com a NORTE focal: `{sum(1 for row in empresas if row['shared_socio_with_focal'])}` empresa(s)",
        f"- Match nominal local em socios dos leads: `{sum(1 for row in empresas if row['socio_match_local'])}` empresa(s)",
        "",
        "## Empresas-lead",
        "",
    ]

    for row in empresas:
        parts.extend(
            [
                f"- `{row['nome_referencia']}` (`{row['cnpj']}`) | terceirizacao `{brl(row['valor_terceirizacao_brl'])}` | contratos `{int(row['contratos_terceirizacao'] or 0)}`",
                f"  razao social `{row['razao_social']}` | municipio/UF `{row['municipio']}/{row['uf']}` | socios `{int(row['qtd_socios'] or 0)}`",
                f"  shared_with_focal `{bool(row['shared_socio_with_focal'])}` | socio_match_local `{bool(row['socio_match_local'])}` | flags `{row['flags_json']}`",
            ]
        )

    parts.extend(["", "## Socios dos leads", ""])
    for cnpj, socio_nome, qualificacao, shared_with_focal, m1, m2, m3 in socios:
        parts.append(
            f"- `{cnpj}` | `{socio_nome}` | qual `{qualificacao}` | shared_with_focal `{bool(shared_with_focal)}` | matches locais `{int(m1 or 0) + int(m2 or 0) + int(m3 or 0)}`"
        )

    parts.extend(["", "## Contratos para auditoria", ""])
    for cnpj, fornecedor_nome, orgao, unidade_gestora, ano, numero_contrato, valor_brl, terceirizacao_pessoal, objeto in contratos:
        parts.extend(
            [
                f"- `{fornecedor_nome}` (`{cnpj}`) | orgao `{orgao}` | contrato `{numero_contrato}` | ano `{ano}` | valor `{brl(valor_brl)}` | terceirizacao `{bool(terceirizacao_pessoal)}`",
                f"  unidade gestora `{unidade_gestora}`",
                f"  objeto: {objeto}",
            ]
        )

    parts.extend(
        [
            "",
            "## Leitura tecnica",
            "",
            "- Esta camada nao prova grupo economico; ela apenas eleva a qualidade da triagem por nome semelhante.",
            "- O objetivo aqui e descobrir se o relato de terceirizacao de pessoal aponta para outro CNPJ da rede, e nao para o CNPJ focal de fornecimento de bens.",
        ]
    )
    return "\n".join(parts) + "\n"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    payload = fetch_payload(con)
    write_csv(con)
    OUT_MD.write_text(render_md(payload), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inputs": [
            "trace_norte_leads",
            "trace_norte_rede_empresas",
            "trace_norte_rede_socios",
            "v_trace_norte_rede_resumo",
        ],
        "outputs": [
            str(OUT_MD.relative_to(ROOT)),
            str(OUT_JSON.relative_to(ROOT)),
            str(OUT_CSV.relative_to(ROOT)),
            str(OUT_CONTRATOS_CSV.relative_to(ROOT)),
        ],
        "empresas": len(payload["empresas"]),
        "socios": len(payload["socios"]),
        "contratos": len(payload["contratos"]),
    }
    OUT_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
