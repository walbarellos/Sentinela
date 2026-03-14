from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
CEIS_ZIP = ROOT / "data" / "federal" / "ceis_20260312.zip"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "sesacre_prioritarios"
CURATED_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
EVIDENCE_DIR = OUT_DIR / "evidencias"
TOP_N = 10


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


def slug(value: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or "caso"


def write_csv(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object],
    target_path: Path,
) -> dict[str, object]:
    cur = con.execute(query, params)
    headers = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    with target_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)
    return {
        "path": str(target_path.relative_to(ROOT)),
        "rows": len(rows),
        "sha256": sha256_file(target_path),
        "size_bytes": target_path.stat().st_size,
    }


def write_text(path: Path, content: str) -> dict[str, object]:
    path.write_text(content, encoding="utf-8")
    return {
        "path": str(path.relative_to(ROOT)),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def write_text_mirrored(path: Path, content: str, mirror_name: str | None = None) -> dict[str, object]:
    meta = write_text(path, content)
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    write_text(CURATED_DIR / (mirror_name or path.name), content)
    return meta


def fetch_top_cases(con: duckdb.DuckDBPyConnection, top_n: int) -> list[dict[str, object]]:
    rows = con.execute(
        """
        SELECT
            cnpj_cpf,
            nome_sancionado,
            fonte,
            orgao_ac,
            n_sancoes_ativas,
            n_sancoes_total,
            tipos_sancao,
            data_inicio_mais_antiga,
            data_fim_mais_recente,
            valor_contratado_ac,
            n_contratos_ac
        FROM v_sancoes_ativas
        WHERE orgao_ac = 'SESACRE'
        ORDER BY valor_contratado_ac DESC, nome_sancionado
        LIMIT ?
        """,
        [top_n],
    ).fetchall()
    cases: list[dict[str, object]] = []
    for rank, row in enumerate(rows, start=1):
        cases.append(
            {
                "rank": rank,
                "cnpj": str(row[0] or ""),
                "nome": str(row[1] or ""),
                "fonte": str(row[2] or ""),
                "orgao": str(row[3] or ""),
                "n_sancoes_ativas": int(row[4] or 0),
                "n_sancoes_total": int(row[5] or 0),
                "tipos_sancao": str(row[6] or ""),
                "data_inicio_mais_antiga": str(row[7] or ""),
                "data_fim_mais_recente": str(row[8] or ""),
                "valor_contratado_ac": float(row[9] or 0),
                "n_contratos_ac": int(row[10] or 0),
            }
        )
    if not cases:
        raise SystemExit("Nenhum fornecedor com sancao ativa encontrado para SESACRE.")
    return cases


def export_case_evidence(con: duckdb.DuckDBPyConnection, case: dict[str, object]) -> dict[str, object]:
    case_slug = f"{int(case['rank']):02d}_{case['cnpj']}_{slug(str(case['nome']))[:50]}"
    case_dir = EVIDENCE_DIR / case_slug
    case_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "case_dir": str(case_dir.relative_to(ROOT)),
        "collapsed_csv": write_csv(
            con,
            """
            SELECT
                cnpj_cpf,
                nome_sancionado,
                fonte,
                orgao_ac,
                n_sancoes_ativas,
                n_sancoes_total,
                tipos_sancao,
                data_inicio_mais_antiga,
                data_fim_mais_recente,
                valor_contratado_ac,
                n_contratos_ac
            FROM v_sancoes_ativas
            WHERE orgao_ac = 'SESACRE' AND cnpj_cpf = ?
            """,
            [case["cnpj"]],
            case_dir / "sancoes_collapsed.csv",
        ),
        "raw_sancoes_csv": write_csv(
            con,
            """
            SELECT
                ano,
                fornecedor_nome,
                fornecedor_cnpj,
                total_pago,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                status_sancao,
                orgao_sancionador,
                fundamentacao_legal,
                n_pagamentos
            FROM estado_ac_fornecedor_sancoes
            WHERE orgao = 'SESACRE' AND fornecedor_cnpj = ?
            ORDER BY total_pago DESC, data_inicio_sancao
            """,
            [case["cnpj"]],
            case_dir / "estado_ac_fornecedor_sancoes.csv",
        ),
        "contratos_csv": write_csv(
            con,
            """
            SELECT
                ano,
                numero,
                tipo,
                data_inicio_vigencia,
                data_fim_vigencia,
                valor,
                credor,
                cnpjcpf,
                objeto
            FROM estado_ac_contratos
            WHERE orgao = 'SESACRE' AND cnpjcpf = ?
            ORDER BY valor DESC, numero
            """,
            [case["cnpj"]],
            case_dir / "estado_ac_contratos.csv",
        ),
        "detalhes_csv": write_csv(
            con,
            """
            SELECT
                ano,
                entidade,
                orgao,
                razao_social,
                cnpjcpf,
                numero_empenho,
                ano_empenho,
                data_empenho,
                total_empenho,
                numero_liquidacao,
                data_liquidacao,
                valor_liquidacao,
                numero_pagamento,
                data_pagamento,
                valor_pagamento,
                historico,
                despesa_orcamentaria,
                funcao,
                subfuncao,
                fonte_recurso
            FROM estado_ac_fornecedor_detalhes
            WHERE orgao = 'SESACRE' AND cnpjcpf = ?
            ORDER BY valor_pagamento DESC, total_empenho DESC, numero_empenho
            """,
            [case["cnpj"]],
            case_dir / "estado_ac_fornecedor_detalhes.csv",
        ),
        "federal_ceis_csv": write_csv(
            con,
            """
            SELECT
                cnpj,
                nome,
                tipo_sancao,
                data_inicio_sancao,
                data_fim_sancao,
                orgao_sancionador,
                fundamentacao_legal,
                numero_processo,
                categoria_sancao,
                data_publicacao,
                publicacao,
                abrangencia_sancao,
                uf_orgao_sancionador,
                esfera_orgao_sancionador,
                origem_informacoes
            FROM federal_ceis
            WHERE cnpj = ?
            ORDER BY data_inicio_sancao, orgao_sancionador
            """,
            [case["cnpj"]],
            case_dir / "federal_ceis.csv",
        ),
        "qsa_csv": write_csv(
            con,
            """
            SELECT
                ano,
                orgao,
                cnpj,
                fornecedor_nome,
                total_pago,
                razao_social_receita,
                nome_fantasia,
                situacao_cadastral,
                capital_social,
                data_abertura,
                municipio,
                uf,
                cnae_descricao,
                qtd_socios,
                socios_json,
                flags_json
            FROM estado_ac_fornecedor_qsa
            WHERE orgao = 'SESACRE' AND cnpj = ?
            ORDER BY ano DESC
            """,
            [case["cnpj"]],
            case_dir / "estado_ac_fornecedor_qsa.csv",
        ),
        "socios_csv": write_csv(
            con,
            """
            SELECT
                cnpj,
                socio_nome,
                socio_cpf_cnpj,
                qualificacao,
                data_entrada,
                capturado_em
            FROM empresa_socios
            WHERE cnpj = ?
            ORDER BY socio_nome
            """,
            [case["cnpj"]],
            case_dir / "empresa_socios.csv",
        ),
    }
    case_summary = write_text(
        case_dir / "case_summary.json",
        json.dumps(case, ensure_ascii=False, indent=2) + "\n",
    )
    outputs["case_summary_json"] = case_summary
    return outputs


def build_markdown(cases: list[dict[str, object]], evidence: dict[str, dict[str, object]]) -> str:
    lines = [
        "# Dossie Prioritario SESACRE",
        "",
        f"Gerado em: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        "",
        "Este dossie consolida os maiores fornecedores com sancao ativa contratados pela SESACRE, com base nas tabelas locais do projeto e na carga aberta do CEIS.",
        "",
        "## Resumo executivo",
        "",
    ]
    total_valor = sum(float(case["valor_contratado_ac"]) for case in cases)
    total_contratos = sum(int(case["n_contratos_ac"]) for case in cases)
    total_sancoes = sum(int(case["n_sancoes_ativas"]) for case in cases)
    lines.extend(
        [
            f"- Casos priorizados: `{len(cases)}`",
            f"- Valor agregado: `{brl(total_valor)}`",
            f"- Contratos agregados: `{total_contratos}`",
            f"- Sancoes ativas agregadas: `{total_sancoes}`",
            "",
        ]
    )

    for case in cases:
        outputs = evidence[case["cnpj"]]
        lines.extend(
            [
                f"## Caso {int(case['rank'])} — {case['nome']}",
                "",
                f"- CNPJ: `{case['cnpj']}`",
                f"- Fonte sancionatoria: `{case['fonte']}`",
                f"- Orgao contratante: `{case['orgao']}`",
                f"- Sancoes ativas: `{case['n_sancoes_ativas']}` de `{case['n_sancoes_total']}`",
                f"- Tipos: {case['tipos_sancao']}",
                f"- Inicio mais antigo: `{case['data_inicio_mais_antiga']}`",
                f"- Fim mais recente: `{case['data_fim_mais_recente']}`",
                f"- Valor contratado com SESACRE: `{brl(case['valor_contratado_ac'])}`",
                f"- Numero de contratos: `{case['n_contratos_ac']}`",
                "",
                "### Evidencias locais",
                "",
                f"- Linha colapsada: `{outputs['collapsed_csv']['path']}`",
                f"- Sancoes cruas: `{outputs['raw_sancoes_csv']['path']}`",
                f"- Contratos: `{outputs['contratos_csv']['path']}`",
                f"- Detalhamento fornecedor: `{outputs['detalhes_csv']['path']}`",
                f"- CEIS bruto: `{outputs['federal_ceis_csv']['path']}`",
                f"- QSA: `{outputs['qsa_csv']['path']}`",
                f"- Socios: `{outputs['socios_csv']['path']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Leitura operacional",
            "",
            "- O eixo estadual ja esta em nivel de representacao: fornecedor sancionado ativo, valor contratado, contratos e detalhamento financeiro por empenho/pagamento.",
            "- O pacote abaixo nao depende de o portal permanecer online para a prova minima, porque os extratos CSV e o zip do CEIS foram preservados localmente.",
            "- Onde QSA/socios vieram vazios, isso indica ausencia de cobertura local atual para aquele CNPJ, nao ausencia de risco.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_plaintext(cases: list[dict[str, object]]) -> str:
    lines = [
        "ASSUNTO: noticia de fato sobre fornecedores com sancao ativa contratados pela SESACRE",
        "",
        "FATOS OBJETIVOS",
    ]
    for index, case in enumerate(cases, start=1):
        lines.append(
            f"{index}. {case['nome']} (CNPJ {case['cnpj']}) aparece com {case['n_sancoes_ativas']} sancao(oes) ativa(s) e {brl(case['valor_contratado_ac'])} em contratos com a SESACRE, distribuidos em {case['n_contratos_ac']} contrato(s)."
        )
    lines.extend(
        [
            "",
            "REQUERIMENTO INICIAL",
            "Solicita-se a apuracao da regularidade da manutencao desses contratos pela SESACRE, com verificacao da compatibilidade entre as sancoes vigentes, a abrangencia das restricoes e a permanencia das contratacoes no periodo auditado.",
            "",
            "FONTES MINIMAS",
            f"- Base aberta CEIS preservada localmente: {str((OUT_DIR / 'evidencias' / 'fontes_federais' / CEIS_ZIP.name).relative_to(ROOT))}",
            "- Extratos CSV por fornecedor em sesacre_prioritarios/evidencias/",
            "- Insight local: kind SESACRE_SANCAO_ATIVA",
            "",
            "OBSERVACAO",
            "O texto acima descreve fatos objetivos do banco local e nao afirma, por si so, fraude consumada ou dolo, cuja caracterizacao depende de apuracao pelos orgaos competentes.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_manifest(cases: list[dict[str, object]], evidence: dict[str, dict[str, object]], copied_files: list[dict[str, object]]) -> str:
    payload = {
        "generated_at": datetime.now().isoformat(),
        "database": str(DB_PATH.relative_to(ROOT)),
        "cases": cases,
        "evidence": evidence,
        "copied_files": copied_files,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    cases = fetch_top_cases(con, TOP_N)
    evidence: dict[str, dict[str, object]] = {}
    for case in cases:
        evidence[str(case["cnpj"])] = export_case_evidence(con, case)
    con.close()

    copied_files: list[dict[str, object]] = []
    federal_dir = EVIDENCE_DIR / "fontes_federais"
    federal_dir.mkdir(parents=True, exist_ok=True)
    if CEIS_ZIP.exists():
        target = federal_dir / CEIS_ZIP.name
        shutil.copy2(CEIS_ZIP, target)
        copied_files.append(
            {
                "path": str(target.relative_to(ROOT)),
                "sha256": sha256_file(target),
                "size_bytes": target.stat().st_size,
                "source_path": str(CEIS_ZIP.relative_to(ROOT)),
            }
        )

    dossier_path = OUT_DIR / "dossie_sesacre_sancoes_prioritarias.md"
    dossier_meta = write_text(dossier_path, build_markdown(cases, evidence))
    texto_path = OUT_DIR / "relato_apuracao_sesacre_top10.txt"
    texto_meta = write_text_mirrored(texto_path, build_plaintext(cases), "relato_apuracao_sesacre_top10.txt")
    manifest_path = OUT_DIR / "manifest_sesacre_prioritarios.json"
    manifest_meta = write_text(manifest_path, build_manifest(cases, evidence, copied_files))

    print(dossier_meta["path"])
    print(texto_meta["path"])
    print(manifest_meta["path"])
    for item in copied_files:
        print(item["path"])


if __name__ == "__main__":
    main()
