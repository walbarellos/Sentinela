from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def render_sources(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except Exception:
        return [str(raw)]
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def fetch_cases(con: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    cases: dict[str, dict] = {}

    row_3895 = con.execute(
        """
        SELECT
            t.numero_contrato,
            t.numero_processo,
            t.ano,
            c.numero_termo,
            c.detail_url,
            t.secretaria,
            t.objeto,
            t.valor_referencia_brl,
            t.fornecedor,
            t.cnpj,
            t.n_sancoes_ativas,
            t.sancao_fontes,
            t.fila_investigacao_final,
            t.prioridade_final
        FROM v_rb_contratos_triagem_final t
        JOIN rb_contratos c ON c.row_id = t.row_id
        WHERE t.numero_contrato = '3895'
        """
    ).fetchone()
    if row_3895:
        sancoes = con.execute(
            """
            SELECT sancao_fonte, sancao_tipo, sancao_inicio, sancao_fim, orgao_sancao, ativa
            FROM v_rb_contrato_ceis
            WHERE numero_contrato = '3895'
            ORDER BY orgao_sancao, sancao_inicio
            """
        ).fetchall()
        insight = con.execute(
            """
            SELECT severity, confidence, title, description_md, sources
            FROM insight
            WHERE kind = 'RB_CONTRATO_SANCIONADO'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        cases["3895"] = {
            "numero_contrato": row_3895[0],
            "numero_processo": row_3895[1],
            "ano": row_3895[2],
            "numero_termo": row_3895[3],
            "detail_url": row_3895[4],
            "secretaria": row_3895[5],
            "objeto": row_3895[6],
            "valor_referencia_brl": row_3895[7],
            "fornecedor": row_3895[8],
            "cnpj": row_3895[9],
            "n_sancoes_ativas": row_3895[10],
            "sancao_fontes": row_3895[11],
            "fila": row_3895[12],
            "prioridade": row_3895[13],
            "sancoes": sancoes,
            "insight": insight,
        }

    row_3898 = con.execute(
        """
        SELECT
            t.numero_contrato,
            t.numero_processo,
            t.ano,
            c.numero_termo,
            c.detail_url,
            t.secretaria,
            t.objeto,
            t.valor_referencia_brl,
            t.inconsistencias_licitacao,
            t.fila_investigacao_final,
            t.prioridade_final
        FROM v_rb_contratos_triagem_final t
        JOIN rb_contratos c ON c.row_id = t.row_id
        WHERE t.numero_contrato = '3898'
        """
    ).fetchone()
    if row_3898:
        audit_rows = con.execute(
            """
            SELECT
                item_ordem,
                item_descricao,
                quantidade,
                valor_unitario,
                valor_total,
                found_in_proposals,
                found_in_edital,
                candidate_count,
                anomaly_kind,
                severity,
                evidence_json
            FROM rb_contratos_item_audit
            WHERE numero_contrato = '3898'
            ORDER BY item_ordem
            """
        ).fetchall()
        insight = con.execute(
            """
            SELECT severity, confidence, title, description_md, sources
            FROM insight
            WHERE kind = 'RB_CONTRATO_LICITACAO_INCONSISTENTE'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        cases["3898"] = {
            "numero_contrato": row_3898[0],
            "numero_processo": row_3898[1],
            "ano": row_3898[2],
            "numero_termo": row_3898[3],
            "detail_url": row_3898[4],
            "secretaria": row_3898[5],
            "objeto": row_3898[6],
            "valor_referencia_brl": row_3898[7],
            "inconsistencias_licitacao": row_3898[8],
            "fila": row_3898[9],
            "prioridade": row_3898[10],
            "audit_rows": audit_rows,
            "insight": insight,
        }

    return cases


def build_markdown(cases: dict[str, dict]) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Dossie Prioritario SUS Rio Branco",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este dossie consolida os dois casos municipais prioritarios do recorte SUS em Rio Branco com base no banco local do projeto e nas fontes publicas ja integradas.",
        "",
        "## Caso 1 — Contrato 3895 / fornecedor sancionado",
        "",
    ]

    case = cases["3895"]
    parts.extend(
        [
            f"- Contrato: `{case['numero_contrato']}`",
            f"- Processo: `{case['numero_processo']}`",
            f"- Termo: `{case['numero_termo']}`",
            f"- Ano: `{case['ano']}`",
            f"- Secretaria: `{case['secretaria']}`",
            f"- Objeto: {case['objeto']}",
            f"- Valor de referencia: {brl(case['valor_referencia_brl'])}",
            f"- Fornecedor: **{case['fornecedor']}**",
            f"- CNPJ: `{case['cnpj']}`",
            f"- Fila final: `{case['fila']}`",
            f"- Prioridade final: `{case['prioridade']}`",
            f"- Link do contrato: {case['detail_url']}",
            "",
            "### Fatos objetivos",
            "",
            f"- O fornecedor aparece no banco com `{case['n_sancoes_ativas']}` sancao(oes) ativa(s).",
            f"- Fontes de sancao materializadas: `{case['sancao_fontes']}`.",
            "- O contrato esta vinculado a `SEMSA` e ja consta como caso de denuncia imediata na triagem final.",
            "",
            "### Sancoes materializadas",
            "",
        ]
    )
    for fonte, tipo, inicio, fim, orgao, ativa in case["sancoes"]:
        status = "ativa" if ativa else "inativa"
        parts.append(
            f"- `{fonte}` | {tipo} | inicio `{inicio}` | fim `{fim}` | orgao sancionador `{orgao}` | status `{status}`"
        )

    insight = case["insight"]
    if insight:
        parts.extend(
            [
                "",
                "### Insight consolidado",
                "",
                f"- Severidade: `{insight[0]}`",
                f"- Confianca: `{insight[1]}`",
                f"- Titulo: {insight[2]}",
                f"- Fontes: {', '.join(render_sources(insight[4]))}",
            ]
        )

    case = cases["3898"]
    parts.extend(
        [
            "",
            "## Caso 2 — Contrato 3898 / anomalia documental de licitacao",
            "",
            f"- Contrato: `{case['numero_contrato']}`",
            f"- Processo: `{case['numero_processo']}`",
            f"- Termo: `{case['numero_termo']}`",
            f"- Ano: `{case['ano']}`",
            f"- Secretaria: `{case['secretaria']}`",
            f"- Objeto: {case['objeto']}",
            f"- Valor de referencia: {brl(case['valor_referencia_brl'])}",
            f"- Fila final: `{case['fila']}`",
            f"- Prioridade final: `{case['prioridade']}`",
            f"- Link do contrato: {case['detail_url']}",
            "",
            "### Fatos objetivos",
            "",
            "- A licitacao mae do processo `3006` ja foi confirmada no portal municipal: `2274334`.",
            "- O edital e a retificacao oficiais da CPL usados no confronto foram as publicacoes `1554` e `1640`.",
            "- O contrato permanece sem fornecedor/CNPJ confirmado por fonte aberta.",
            "",
            "### Itens auditados",
            "",
        ]
    )
    for (
        item_ordem,
        item_descricao,
        quantidade,
        valor_unitario,
        valor_total,
        found_in_proposals,
        found_in_edital,
        candidate_count,
        anomaly_kind,
        severity,
        evidence_json,
    ) in case["audit_rows"]:
        evidence = json.loads(evidence_json) if evidence_json else {}
        line = (
            f"- Item `{item_ordem}`: {item_descricao} | qtd `{quantidade}` | "
            f"unit `{brl(valor_unitario)}` | total `{brl(valor_total)}` | "
            f"propostas `{found_in_proposals}` | edital `{found_in_edital}` | candidatos `{candidate_count}`"
        )
        if anomaly_kind:
            line += f" | anomalia `{anomaly_kind}` | severidade `{severity}`"
        parts.append(line)
        if evidence.get("terms"):
            parts.append(f"  termos auditados: {', '.join(evidence['terms'])}")

    insight = case["insight"]
    if insight:
        parts.extend(
            [
                "",
                "### Insight consolidado",
                "",
                f"- Severidade: `{insight[0]}`",
                f"- Confianca: `{insight[1]}`",
                f"- Titulo: {insight[2]}",
                f"- Fontes: {', '.join(render_sources(insight[4]))}",
            ]
        )

    parts.extend(
        [
            "",
            "## Leitura operacional",
            "",
            "- `3895` ja esta pronto para representacao como contratacao municipal com fornecedor sancionado.",
            "- `3898` nao fecha fornecedor por fonte aberta, mas fecha uma inconsistencia documental forte: item fora do edital e fora das propostas publicas da licitacao mae.",
            "- Os dois casos permanecem juntos na fila prioritaria municipal, com prioridades `100` e `95`.",
            "",
            "## Proximos atos uteis",
            "",
            "- Protocolo de representacao com anexacao dos links e evidencias acima.",
            "- Se necessario, diligencia complementar manual em ambiente autenticado do `licitacoes-e` apenas para tentar identificar o lote/fornecedor do `3898`.",
            "- Preservacao de captura PDF/HTML das telas do contrato, da licitacao mae e das publicacoes da CPL.",
        ]
    )
    return "\n".join(parts) + "\n"


def build_plaintext_3895(case: dict) -> str:
    lines = [
        "ASSUNTO: representacao preliminar sobre contratacao SUS municipal com fornecedor sancionado",
        "",
        "FATOS OBJETIVOS",
        f"1. No contrato {case['numero_contrato']}, processo {case['numero_processo']}, vinculado a {case['secretaria']}, consta o fornecedor {case['fornecedor']}, CNPJ {case['cnpj']}.",
        f"2. O valor de referencia do contrato no portal municipal e {brl(case['valor_referencia_brl'])}.",
        f"3. O banco local materializou {case['n_sancoes_ativas']} sancao(oes) ativa(s) associadas a esse CNPJ, com origem {case['sancao_fontes']}.",
        "4. A triagem final do projeto classificou o caso como denuncia_imediata.",
        "",
        "REQUERIMENTO INICIAL",
        "Solicita-se a apuracao da regularidade da contratacao, com verificacao da compatibilidade entre a situacao sancionatoria do fornecedor e a sua contratacao pela Secretaria Municipal de Saude de Rio Branco.",
        "",
        "FONTES MINIMAS",
        f"- Contrato municipal: {case['detail_url']}",
        "- Cruzamento sancionatorio: tabela v_rb_contrato_ceis / insight RB_CONTRATO_SANCIONADO",
        "",
        "OBSERVACAO",
        "O texto acima descreve fatos objetivos do banco e nao afirma, por si so, a existencia de dolo ou fraude, que dependem de apuracao pelos orgaos competentes.",
    ]
    return "\n".join(lines) + "\n"


def build_plaintext_3898(case: dict) -> str:
    lines = [
        "ASSUNTO: representacao preliminar sobre inconsistencia documental entre contrato SUS municipal e licitacao mae",
        "",
        "FATOS OBJETIVOS",
        f"1. No contrato {case['numero_contrato']}, processo {case['numero_processo']}, vinculado a {case['secretaria']}, consta o objeto {case['objeto']}.",
        f"2. O valor de referencia do contrato no portal municipal e {brl(case['valor_referencia_brl'])}.",
        "3. A licitacao mae do processo foi confirmada no portal municipal como a licitacao 2274334.",
        "4. As publicacoes oficiais da CPL usadas no confronto foram 1554 e 1640.",
        "5. Na auditoria documental do contrato, o item 'Coletor de Material Perfurocortante' apareceu no contrato, mas nao apareceu nas propostas do portal nem no edital/retificacao oficiais da CPL para o PE SRP 141/2023.",
        "6. O caso foi classificado na fila final como auditoria_documental_licitacao, prioridade 95.",
        "",
        "REQUERIMENTO INICIAL",
        "Solicita-se a apuracao da compatibilidade entre o contrato municipal e a licitacao mae, com verificacao da origem do item divergente, do fornecedor efetivamente contratado e do lastro documental do item registrado no contrato.",
        "",
        "FONTES MINIMAS",
        f"- Contrato municipal: {case['detail_url']}",
        "- Licitacao mae: https://transparencia.riobranco.ac.gov.br/licitacao/ver/2274334/",
        "- CPL: https://cpl.riobranco.ac.gov.br/publicacao/1554",
        "- CPL: https://cpl.riobranco.ac.gov.br/publicacao/1640",
        "- Auditoria: tabela rb_contratos_item_audit / insight RB_CONTRATO_LICITACAO_INCONSISTENTE",
        "",
        "OBSERVACAO",
        "O texto acima descreve uma divergencia documental objetiva e nao afirma, por si so, fraude consumada, o que depende de apuracao e contraditorio.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    cases = fetch_cases(con)
    con.close()
    if "3895" not in cases or "3898" not in cases:
        raise SystemExit("Casos prioritarios 3895/3898 nao encontrados no banco.")

    dossier_path = OUT_DIR / "dossie_rb_sus_prioritarios.md"
    dossier_path.write_text(build_markdown(cases), encoding="utf-8")

    txt_3895 = OUT_DIR / "denuncia_preliminar_3895.txt"
    txt_3895.write_text(build_plaintext_3895(cases["3895"]), encoding="utf-8")

    txt_3898 = OUT_DIR / "denuncia_preliminar_3898.txt"
    txt_3898.write_text(build_plaintext_3898(cases["3898"]), encoding="utf-8")

    print(dossier_path)
    print(txt_3895)
    print(txt_3898)


if __name__ == "__main__":
    main()
