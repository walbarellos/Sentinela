from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch"
CURATED_DIR = OUT_DIR / "entrega_denuncia_atual"


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


def write_text_pair(filename: str, content: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / filename).write_text(content, encoding="utf-8")
    (CURATED_DIR / filename).write_text(content, encoding="utf-8")


def fetch_active_case_3898(con: duckdb.DuckDBPyConnection) -> dict:
    row = con.execute(
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
    if not row:
        raise SystemExit("Contrato 3898 nao encontrado no banco atual.")

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
    return {
        "numero_contrato": row[0],
        "numero_processo": row[1],
        "ano": row[2],
        "numero_termo": row[3],
        "detail_url": row[4],
        "secretaria": row[5],
        "objeto": row[6],
        "valor_referencia_brl": row[7],
        "inconsistencias_licitacao": row[8],
        "fila": row[9],
        "prioridade": row[10],
        "audit_rows": audit_rows,
        "insight": insight,
    }


def fetch_historical_3895(con: duckdb.DuckDBPyConnection) -> dict | None:
    row = con.execute(
        """
        SELECT
            numero_contrato,
            numero_processo,
            ano,
            secretaria,
            objeto,
            valor_referencia_brl,
            fornecedor,
            cnpj,
            data_contrato_referencia,
            COUNT(*) AS n_ocorrencias,
            MIN(sancao_inicio_date) AS sancao_inicio_min,
            MAX(sancao_fim_date) AS sancao_fim_max,
            STRING_AGG(DISTINCT motivo_exclusao, ', ') AS motivos
        FROM v_rb_contrato_ceis_invalida
        WHERE numero_contrato = '3895'
        GROUP BY
            numero_contrato,
            numero_processo,
            ano,
            secretaria,
            objeto,
            valor_referencia_brl,
            fornecedor,
            cnpj,
            data_contrato_referencia
        """
    ).fetchone()
    if not row:
        return None
    return {
        "numero_contrato": row[0],
        "numero_processo": row[1],
        "ano": row[2],
        "secretaria": row[3],
        "objeto": row[4],
        "valor_referencia_brl": row[5],
        "fornecedor": row[6],
        "cnpj": row[7],
        "data_contrato_referencia": str(row[8] or ""),
        "n_ocorrencias": int(row[9] or 0),
        "sancao_inicio_min": str(row[10] or ""),
        "sancao_fim_max": str(row[11] or ""),
        "motivos": str(row[12] or ""),
    }


def build_markdown(active_case: dict, historical_3895: dict | None) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Dossie Prioritario SUS Rio Branco",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este dossie consolida o estado municipal atual do recorte SUS em Rio Branco.",
        "",
        "## Resumo operacional",
        "",
        "- Caso municipal ativo: `3898`.",
        "- Natureza do caso ativo: `DIVERGENCIA_DOCUMENTAL`.",
        "- Uso externo recomendado: `noticia de fato` ou `pedido de apuracao preliminar`.",
    ]
    if historical_3895:
        parts.append("- Caso `3895` rebaixado para nota historica apos validacao temporal do cruzamento sancionatorio.")
    parts.extend(
        [
            "",
            "## Caso ativo - Contrato 3898 / anomalia documental de licitacao",
            "",
            f"- Contrato: `{active_case['numero_contrato']}`",
            f"- Processo: `{active_case['numero_processo']}`",
            f"- Termo: `{active_case['numero_termo']}`",
            f"- Ano: `{active_case['ano']}`",
            f"- Secretaria: `{active_case['secretaria']}`",
            f"- Objeto: {active_case['objeto']}",
            f"- Valor de referencia: {brl(active_case['valor_referencia_brl'])}",
            f"- Fila final: `{active_case['fila']}`",
            f"- Prioridade final: `{active_case['prioridade']}`",
            f"- Link do contrato: {active_case['detail_url']}",
            "",
            "### Fatos objetivos",
            "",
            "- A licitacao-mae do processo `3006` foi confirmada no portal municipal: `2274334`.",
            "- O edital e a retificacao oficiais usados no confronto foram as publicacoes `1554` e `1640` da CPL.",
            "- O contrato segue sem fornecedor/CNPJ confirmado por fonte aberta, mas a divergencia documental do item foi preservada por auditoria.",
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
    ) in active_case["audit_rows"]:
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

    insight = active_case["insight"]
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

    if historical_3895:
        parts.extend(
            [
                "",
                "## Nota historica - Contrato 3895 / cruzamento sancionatorio invalidado",
                "",
                f"- Contrato: `{historical_3895['numero_contrato']}`",
                f"- Processo: `{historical_3895['numero_processo']}`",
                f"- Secretaria: `{historical_3895['secretaria']}`",
                f"- Fornecedor: `{historical_3895['fornecedor']}`",
                f"- CNPJ: `{historical_3895['cnpj']}`",
                f"- Valor de referencia: `{brl(historical_3895['valor_referencia_brl'])}`",
                f"- Data de referencia do contrato: `{historical_3895['data_contrato_referencia']}`",
                f"- Primeira data de sancao encontrada: `{historical_3895['sancao_inicio_min']}`",
                f"- Linhas invalidadas: `{historical_3895['n_ocorrencias']}`",
                f"- Motivo de exclusao: `{historical_3895['motivos']}`",
                "",
                "Leitura correta: o contrato foi preservado apenas como trilha historica de auditoria. Ele nao integra mais a fila municipal ativa nem sustenta insight sancionatorio valido.",
            ]
        )

    parts.extend(
        [
            "",
            "## Proximos atos uteis",
            "",
            "- Protocolar noticia de fato ou pedido de apuracao com foco na divergencia documental do contrato `3898`.",
            "- Preservar HTML/PDF do contrato, da licitacao-mae e das publicacoes da CPL.",
            "- Manter o `3895` apenas como historico de validacao de falso positivo temporal.",
        ]
    )
    return "\n".join(parts) + "\n"


def build_relato_3898(case: dict) -> str:
    lines = [
        "ASSUNTO: noticia de fato sobre inconsistencia documental entre contrato SUS municipal e licitacao-mae",
        "",
        "FATOS OBJETIVOS",
        f"1. No contrato {case['numero_contrato']}, processo {case['numero_processo']}, vinculado a {case['secretaria']}, consta o objeto {case['objeto']}.",
        f"2. O valor de referencia do contrato no portal municipal e {brl(case['valor_referencia_brl'])}.",
        "3. A licitacao-mae do processo foi confirmada no portal municipal como a licitacao 2274334.",
        "4. As publicacoes oficiais da CPL usadas no confronto foram 1554 e 1640.",
        "5. Na auditoria documental, o item 'Coletor de Material Perfurocortante' apareceu no contrato, mas nao apareceu nas propostas do portal nem no edital/retificacao oficiais da CPL para o PE SRP 141/2023.",
        "6. O caso foi classificado como divergencia documental objetiva, apto a noticia de fato.",
        "",
        "REQUERIMENTO INICIAL",
        "Solicita-se a apuracao da compatibilidade entre o contrato municipal e a licitacao-mae, com verificacao da origem do item divergente, do fornecedor efetivamente contratado e do lastro documental do item registrado no contrato.",
        "",
        "FONTES MINIMAS",
        f"- Contrato municipal: {case['detail_url']}",
        "- Licitacao-mae: https://transparencia.riobranco.ac.gov.br/licitacao/ver/2274334/",
        "- CPL: https://cpl.riobranco.ac.gov.br/publicacao/1554",
        "- CPL: https://cpl.riobranco.ac.gov.br/publicacao/1640",
        "- Auditoria: tabela rb_contratos_item_audit / insight RB_CONTRATO_LICITACAO_INCONSISTENTE",
        "",
        "OBSERVACAO",
        "O texto acima descreve divergencia documental objetiva e nao afirma, por si so, fraude consumada, favorecimento ou dolo, o que depende de apuracao e contraditorio.",
    ]
    return "\n".join(lines) + "\n"


def build_historical_note_3895(case: dict) -> str:
    lines = [
        "ASSUNTO: nota historica de validacao sobre o contrato 3895",
        "",
        "SITUACAO ATUAL",
        f"1. O contrato {case['numero_contrato']}, processo {case['numero_processo']}, antes aparecia em cruzamento sancionatorio bruto.",
        f"2. A data de referencia do contrato ficou em {case['data_contrato_referencia']}.",
        f"3. A primeira data de sancao encontrada ficou em {case['sancao_inicio_min']}.",
        f"4. Foram invalidadas {case['n_ocorrencias']} linha(s) do cruzamento pelo motivo {case['motivos']}.",
        "5. O contrato foi removido da fila municipal ativa e nao sustenta mais o insight RB_CONTRATO_SANCIONADO.",
        "",
        "LEITURA CORRETA",
        "O caso deve permanecer apenas como trilha de auditoria sobre falso positivo temporal, sem uso como noticia de fato sancionatoria.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    active_case = fetch_active_case_3898(con)
    historical_3895 = fetch_historical_3895(con)
    con.close()

    write_text_pair("dossie_rb_sus_prioritarios.md", build_markdown(active_case, historical_3895))
    write_text_pair("relato_apuracao_3898.txt", build_relato_3898(active_case))
    if historical_3895:
        write_text_pair(
            "nota_historica_3895_sancao_invalidada.txt",
            build_historical_note_3895(historical_3895),
        )

    print(OUT_DIR / "dossie_rb_sus_prioritarios.md")
    print(OUT_DIR / "relato_apuracao_3898.txt")
    if historical_3895:
        print(OUT_DIR / "nota_historica_3895_sancao_invalidada.txt")


if __name__ == "__main__":
    main()
