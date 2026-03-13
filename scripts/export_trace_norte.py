from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
PATCH_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch"
OUT_DIR = PATCH_DIR / "entrega_denuncia_atual"
OUT_MD = OUT_DIR / "TRACE_NORTE_DOSSIE.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_MANIFEST.json"
OUT_CSV = OUT_DIR / "trace_norte_contratos.csv"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def fetch_payload(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    resumo_row = con.execute("SELECT * FROM v_trace_norte_resumo").fetchone()
    if not resumo_row:
        raise RuntimeError("v_trace_norte_resumo vazio. Rode sync_trace_norte.py antes.")

    resumo_cols = [row[1] for row in con.execute("PRAGMA table_info('v_trace_norte_resumo')").fetchall()]
    resumo = dict(zip(resumo_cols, resumo_row))

    empresa = con.execute(
        "SELECT cnpj, razao_social, situacao, capital_social, data_abertura, porte, cnae_principal, municipio, uf FROM empresas_cnpj WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = '37306014000148' LIMIT 1"
    ).fetchone()
    socios = con.execute(
        """
        SELECT socio_nome, socio_cpf_cnpj, qualificacao, data_entrada, match_servidores, match_rb_lotacao, match_cross_candidato_servidor
        FROM trace_norte_socios
        ORDER BY socio_nome
        """
    ).fetchall()
    contratos = con.execute(
        """
        SELECT
            esfera, ente, orgao, unidade_gestora, ano, numero_contrato, numero_processo,
            valor_brl, tipo_objeto, terceirizacao_pessoal, sinal_sancao_ativa,
            n_sancoes_ativas, fornecedor_nome, cnpj, objeto, detail_url
        FROM trace_norte_contratos
        ORDER BY esfera, valor_brl DESC, ano DESC, numero_contrato
        """
    ).fetchall()
    sancoes = con.execute(
        """
        SELECT
            fonte, tipo_sancao, data_inicio_sancao, data_fim_sancao, orgao_sancionador,
            processo_sancao, ativa, abrangencia_sancao
        FROM trace_norte_sancoes
        ORDER BY ativa DESC, data_inicio_sancao DESC, orgao_sancionador
        """
    ).fetchall()
    insight = con.execute(
        """
        SELECT severity, confidence, title, description_md
        FROM insight
        WHERE kind = 'TRACE_NORTE_EXPOSICAO'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    leads = con.execute(
        """
        SELECT
            lead_kind, esfera, orgao, unidade_gestora, ano, numero_contrato,
            valor_brl, fornecedor_nome, cnpj, objeto
        FROM trace_norte_leads
        ORDER BY valor_brl DESC, orgao, numero_contrato
        """
    ).fetchall()
    return {
        "resumo": resumo,
        "empresa": empresa,
        "socios": socios,
        "contratos": contratos,
        "sancoes": sancoes,
        "insight": insight,
        "leads": leads,
    }


def write_csv(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        COPY (
            SELECT
                esfera, ente, orgao, unidade_gestora, ano, numero_contrato, numero_processo,
                valor_brl, tipo_objeto, terceirizacao_pessoal, sinal_sancao_ativa,
                n_sancoes_ativas, fornecedor_nome, cnpj, objeto, detail_url
            FROM trace_norte_contratos
            ORDER BY esfera, valor_brl DESC, ano DESC, numero_contrato
        ) TO '{OUT_CSV.as_posix()}' (HEADER, DELIMITER ',')
        """
    )


def render_md(payload: dict[str, object]) -> str:
    resumo = payload["resumo"]
    empresa = payload["empresa"]
    socios = payload["socios"]
    contratos = payload["contratos"]
    sancoes = payload["sancoes"]
    insight = payload["insight"]
    leads = payload["leads"]
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    parts = [
        "# Trace NORTE",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este dossie consolida apenas o que o software provou de forma objetiva sobre o CNPJ `37.306.014/0001-48`.",
        "",
        "## Resumo executivo",
        "",
        f"- CNPJ: `{resumo['cnpj']}`",
        f"- Razao social de referencia: `{resumo['razao_social_ref']}`",
        f"- Contratos mapeados: `{int(resumo['total_contratos'] or 0)}`",
        f"- Valor total mapeado: `{brl(resumo['valor_total'])}`",
        f"- Municipal: `{int(resumo['contratos_municipais'] or 0)}` contrato(s) / `{brl(resumo['valor_municipal'])}`",
        f"- Estadual: `{int(resumo['contratos_estaduais'] or 0)}` contrato(s) / `{brl(resumo['valor_estadual'])}`",
        f"- Sancoes CEIS ativas: `{int(resumo['sancoes_ativas'] or 0)}`",
        f"- Contratos classificados como fornecimento de bens: `{int(resumo['bens_hits'] or 0)}`",
        f"- Contratos com sinal textual direto de terceirizacao de pessoal: `{int(resumo['terceirizacao_hits'] or 0)}`",
        f"- Coincidencias nominais exatas de socios em bases locais: `{int(resumo['socios_com_match'] or 0)}`",
        f"- Leads por nome semelhante em contratos de terceirizacao: `{int(resumo['leads_total'] or 0)}` / `{brl(resumo['leads_valor_total'])}`",
        "",
        "## Empresa",
        "",
    ]

    if empresa:
        parts.extend(
            [
                f"- Razao social: `{empresa[1]}`",
                f"- Situacao: `{empresa[2]}`",
                f"- Capital social: `{brl(empresa[3])}`",
                f"- Abertura: `{empresa[4]}`",
                f"- Porte: `{empresa[5]}`",
                f"- CNAE principal: `{empresa[6]}`",
                f"- Municipio/UF: `{empresa[7]}/{empresa[8]}`",
            ]
        )
    else:
        parts.append("- Cadastro empresarial nao materializado no banco.")

    parts.extend(["", "## Contratos mapeados", ""])
    for row in contratos:
        parts.extend(
            [
                f"- `{row[0]}` | orgao `{row[2]}` | contrato `{row[5]}` | ano `{row[4]}` | valor `{brl(row[7])}` | tipo `{row[8]}` | sancao_ativa `{bool(row[10])}`",
                f"  fornecedor `{row[12]}` | processo `{row[6] or ''}`",
                f"  objeto: {row[14]}",
            ]
        )
        if row[15]:
            parts.append(f"  detalhe: {row[15]}")

    parts.extend(["", "## Sancoes brutas CEIS", ""])
    for row in sancoes:
        parts.append(
            f"- `{row[0]}` | {row[1]} | inicio `{row[2]}` | fim `{row[3]}` | orgao `{row[4]}` | processo `{row[5]}` | ativa `{bool(row[6])}` | abrangencia `{row[7]}`"
        )

    parts.extend(["", "## QSA e checagem nominal", ""])
    if not socios:
        parts.append("- Nenhum socio materializado em `trace_norte_socios`.")
    for row in socios:
        parts.append(
            f"- `{row[0]}` | qual `{row[2]}` | entrada `{row[3]}` | matches servidores `{row[4]}` | lotacao `{row[5]}` | candidato-servidor `{row[6]}`"
        )

    parts.extend(
        [
            "",
            "## Leitura tecnica",
            "",
            "- O banco atual comprova colisao entre contratos publicos e sancoes ativas para o mesmo CNPJ.",
            "- O banco atual nao encontrou coincidencia nominal exata da socia cadastrada com as bases locais de servidor/candidato carregadas.",
            "- A classificacao dos objetos mapeados para este CNPJ ficou concentrada em `fornecimento_bens`; nao apareceu, neste recorte, contrato com sinal textual direto de terceirizacao de pessoal.",
            "- Isso nao elimina a hipotese de grupo economico, empresa relacionada ou outro CNPJ operando mao de obra; apenas delimita o que este CNPJ especifico provou ate aqui.",
        ]
    )

    parts.extend(["", "## Leads por nome semelhante", ""])
    if not leads:
        parts.append("- Nenhum lead de nome semelhante com terceirizacao foi materializado.")
    else:
        parts.append(
            "- Esta secao e apenas triagem por similaridade nominal (`NORTE`) mais objeto textual de terceirizacao. Nao prova vinculacao societaria com o CNPJ focal."
        )
        for row in leads:
            parts.extend(
                [
                    f"- `{row[7]}` (`{row[8]}`) | orgao `{row[2]}` | contrato `{row[5]}` | ano `{row[4]}` | valor `{brl(row[6])}`",
                    f"  objeto: {row[9]}",
                ]
            )

    if insight:
        parts.extend(
            [
                "",
                "## Insight focal",
                "",
                f"- Severidade: `{insight[0]}`",
                f"- Confianca: `{insight[1]}`",
                f"- Titulo: {insight[2]}",
                f"- Descricao: {insight[3]}",
            ]
        )

    parts.extend(
        [
            "",
            "## Proximo passo",
            "",
            "Expandir a trilha para empresas relacionadas, começando por:",
            "- outras pessoas juridicas que compartilhem a mesma socia/administradora;",
            "- variantes nominais com `NORTE` no Acre e em Rondônia;",
            "- contratos de servicos/terceirizacao nas bases estadual e municipal para detectar o CNPJ realmente usado na substituicao de servidores, se for outro.",
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
            "trace_norte_contratos",
            "trace_norte_sancoes",
            "trace_norte_socios",
            "v_trace_norte_resumo",
            "insight(kind=TRACE_NORTE_EXPOSICAO)",
        ],
        "outputs": [
            str(OUT_MD.relative_to(ROOT)),
            str(OUT_JSON.relative_to(ROOT)),
            str(OUT_CSV.relative_to(ROOT)),
        ],
        "summary": payload["resumo"],
    }
    OUT_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
