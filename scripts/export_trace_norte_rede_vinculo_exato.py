from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
OUT_MD = OUT_DIR / "TRACE_NORTE_REDE_VINCULO_EXATO_DOSSIE.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_REDE_VINCULO_EXATO_MANIFEST.json"
OUT_CSV_LINKS = OUT_DIR / "trace_norte_rede_vinculo_exato.csv"
OUT_CSV_DIVERG = OUT_DIR / "trace_norte_rede_vinculo_divergencias.csv"
OUT_RAW_DIR = OUT_DIR / "trace_norte_rede_vinculo_exato_raw"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def dump_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_RAW_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM v_trace_norte_rede_vinculo_exato
            ORDER BY valor_brl DESC, fornecedor_nome, numero_contrato
        ) TO '{OUT_CSV_LINKS.as_posix()}' (HEADER, DELIMITER ',')
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM v_trace_norte_rede_vinculo_divergencias
            ORDER BY valor_brl DESC, cnpj, numero_contrato
        ) TO '{OUT_CSV_DIVERG.as_posix()}' (HEADER, DELIMITER ',')
        """
    )

    resumo = con.execute(
        """
        SELECT
            COUNT(*) AS n_links,
            COUNT(DISTINCT cnpj) AS n_empresas,
            SUM(valor_brl) AS total_valor
        FROM trace_norte_rede_vinculo_exato
        """
    ).fetchone()
    diverg = con.execute(
        """
        SELECT COUNT(*), SUM(valor_brl)
        FROM v_trace_norte_rede_vinculo_divergencias
        """
    ).fetchone()
    central = con.execute(
        """
        SELECT
            fornecedor_nome, cnpj, orgao, unidade_gestora, lic_ano, lic_numero, lic_processo_adm,
            lic_modalidade, lic_abertura, lic_status, COUNT(*) AS n_contratos, SUM(valor_brl) AS total_valor,
            MIN(vigencia_inicial) AS min_ini, MAX(vigencia_inicial) AS max_ini
        FROM trace_norte_rede_vinculo_exato
        WHERE cnpj = '36990588000115'
          AND orgao = 'SEPLANH'
          AND lic_numero = '053'
        GROUP BY ALL
        """
    ).fetchone()
    central_div = con.execute(
        """
        SELECT
            numero_contrato, valor_brl, heuristic_processo, heuristic_modalidade,
            exact_licitacao_ano, exact_licitacao_numero, exact_processo_adm, exact_modalidade, motivo
        FROM v_trace_norte_rede_vinculo_divergencias
        WHERE cnpj = '36990588000115'
          AND orgao = 'SEPLANH'
        ORDER BY valor_brl DESC, numero_contrato
        """
    ).fetchall()
    raw_contracts = con.execute(
        """
        SELECT numero_contrato, raw_contrato_json, lic_publicacoes_json, evidence_json
        FROM trace_norte_rede_vinculo_exato
        WHERE cnpj = '36990588000115'
          AND orgao = 'SEPLANH'
          AND lic_numero = '053'
        ORDER BY valor_brl DESC, numero_contrato
        """
    ).fetchall()
    con.close()

    for numero_contrato, raw_contrato_json, lic_publicacoes_json, evidence_json in raw_contracts:
        stem = f"central_norte_seplan_{str(numero_contrato).replace('/', '_')}"
        dump_json(OUT_RAW_DIR / f"{stem}_contrato.json", json.loads(str(raw_contrato_json)))
        dump_json(OUT_RAW_DIR / f"{stem}_licitacao_publicacoes.json", json.loads(str(lic_publicacoes_json)))
        dump_json(OUT_RAW_DIR / f"{stem}_evidence.json", json.loads(str(evidence_json)))

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Trace NORTE - Vinculo Exato Contrato x Licitacao",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este arquivo substitui a leitura heuristica quando o proprio portal de contratos entrega `id_licitacao` resolvido.",
        "",
        f"- Vinculos exatos materializados: `{int(resumo[0] or 0)}`",
        f"- Empresas com vinculo exato: `{int(resumo[1] or 0)}`",
        f"- Valor agregado com vinculo exato: `{brl(resumo[2])}`",
        f"- Divergencias entre heuristica e vinculo exato: `{int(diverg[0] or 0)}`",
        f"- Valor agregado dessas divergencias: `{brl(diverg[1])}`",
        "",
    ]

    if central:
        (
            fornecedor_nome,
            cnpj,
            orgao,
            unidade_gestora,
            lic_ano,
            lic_numero,
            lic_processo_adm,
            lic_modalidade,
            lic_abertura,
            lic_status,
            n_contratos,
            total_valor,
            min_ini,
            max_ini,
        ) = central
        parts.extend(
            [
                "## Bloco confirmado - CENTRAL NORTE / SEPLAN",
                "",
                f"- Fornecedor: `{fornecedor_nome}` (`{cnpj}`)",
                f"- Orgão: `{orgao}` / `{unidade_gestora}`",
                f"- Licitação exata no portal: `{lic_numero}/{lic_ano}`",
                f"- Processo administrativo: `{lic_processo_adm}`",
                f"- Modalidade: `{lic_modalidade}`",
                f"- Abertura: `{lic_abertura}`",
                f"- Situação: `{lic_status}`",
                f"- Contratos ligados: `{int(n_contratos)}`",
                f"- Valor total dos contratos ligados: `{brl(total_valor)}`",
                f"- Vigências iniciais observadas: `{min_ini}` até `{max_ini}`",
                "",
                "O portal de contratos devolve `id_licitacao = 27462` para os 5 contratos da `CENTRAL NORTE` em `SEPLAN`, e o portal de licitações resolve esse id como `Pregão Presencial 053/2023`.",
                "",
            ]
        )

    if central_div:
        parts.extend(
            [
                "## Divergencia com a heuristica anterior",
                "",
                "O bloco `CENTRAL NORTE x SEPLAN` tinha sido aproximado heurísticamente do `processo 282/2024`. Esse vínculo deve ser descartado no recorte atual, porque o próprio portal resolve os contratos para outro processo.",
                "",
            ]
        )
        for row in central_div:
            parts.extend(
                [
                    f"- contrato `{row[0]}` | valor `{brl(row[1])}`",
                    f"  heuristica: processo `{row[2]}` / modalidade `{row[3]}`",
                    f"  portal exato: licitacao `{row[5]}/{row[4]}` / processo `{row[6]}` / modalidade `{row[7]}`",
                    f"  motivo: {row[8]}",
                ]
            )
        parts.append("")

    parts.extend(
        [
            "## Arquivos gerados",
            "",
            f"- `{OUT_CSV_LINKS.name}`",
            f"- `{OUT_CSV_DIVERG.name}`",
            f"- diretório `{OUT_RAW_DIR.name}` com JSON bruto do portal para o bloco `CENTRAL NORTE / SEPLAN`",
            "",
        ]
    )

    OUT_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "inputs": [
                    "trace_norte_rede_contratos",
                    "trace_norte_rede_vinculo_exato",
                    "trace_norte_rede_vinculo_audit",
                    "v_trace_norte_rede_vinculo_divergencias",
                ],
                "outputs": [
                    str(OUT_MD.relative_to(ROOT)),
                    str(OUT_JSON.relative_to(ROOT)),
                    str(OUT_CSV_LINKS.relative_to(ROOT)),
                    str(OUT_CSV_DIVERG.relative_to(ROOT)),
                    str(OUT_RAW_DIR.relative_to(ROOT)),
                ],
                "summary": {
                    "links": int(resumo[0] or 0),
                    "empresas": int(resumo[1] or 0),
                    "valor_total_brl": float(resumo[2] or 0),
                    "divergencias": int(diverg[0] or 0),
                    "divergencias_valor_brl": float(diverg[1] or 0),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
