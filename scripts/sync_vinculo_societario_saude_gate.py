from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

DDL = """
CREATE TABLE IF NOT EXISTS vinculo_societario_saude_gate (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    contrato_numero VARCHAR,
    contrato_orgao VARCHAR,
    contrato_valor_brl DOUBLE,
    score_triagem INTEGER,
    estagio_operacional VARCHAR,
    uso_recomendado VARCHAR,
    pode_uso_externo BOOLEAN,
    requisitos_cumpridos_json JSON,
    bloqueios_json JSON,
    recomendacoes_json JSON,
    resumo_decisao VARCHAR,
    limite_decisao VARCHAR,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def fmt_brl(value: object) -> str:
    number = float(value or 0)
    text = f"{number:,.2f}"
    return "R$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")


def load_one(con: duckdb.DuckDBPyConnection, query: str) -> dict:
    df = con.execute(query).fetchdf()
    if df.empty:
        return {}
    return json.loads(df.to_json(orient="records", force_ascii=False))[0]


def load_rows(con: duckdb.DuckDBPyConnection, query: str, params: list | None = None) -> list[dict]:
    df = con.execute(query, params or []).fetchdf()
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def build_gate(case: dict, maturidade_rows: list[dict], respostas_rows: list[dict], cobertura_rows: list[dict]) -> dict:
    maturity = {row["eixo"]: row for row in maturidade_rows}
    coverage = {row["eixo"]: row for row in cobertura_rows}

    base_axes = [
        "contrato_estadual",
        "qsa_socios",
        "socio_administrador",
        "cnes_profissional_ativo",
        "cnes_historico_concomitante",
        "carga_concomitante_extrema",
    ]
    base_ok = all(maturity.get(axis, {}).get("status_probatorio") == "COMPROVADO_DOCUMENTAL" for axis in base_axes)

    docs_localizados = sum(int(row.get("docs_localizados") or 0) for row in cobertura_rows)
    docs_recebidos = sum(int(row.get("docs_recebidos") or 0) for row in cobertura_rows)
    score_triagem = int(case.get("score_triagem") or 0)

    compat_status = maturity.get("compatibilidade_horarios", {}).get("status_probatorio")
    vedacao_status = maturity.get("vedacao_art_107_x", {}).get("status_probatorio")
    acum_status = maturity.get("acumulacao_ilegal", {}).get("status_probatorio")

    requisitos: list[str] = []
    bloqueios: list[str] = []
    recomendacoes: list[str] = []

    if base_ok:
        requisitos.append("base_documental_minima_fechada")
    else:
        bloqueios.append("base_documental_minima_incompleta")

    if score_triagem >= 70:
        requisitos.append("triagem_funcional_alta")
    else:
        bloqueios.append("triagem_funcional_abaixo_do_patamar_alto")

    if docs_localizados > 0:
        requisitos.append("ha_documentos_oficiais_recebidos")
    else:
        bloqueios.append("nenhum_documento_oficial_recebido")

    if compat_status == "COMPROVADO_DOCUMENTAL":
        requisitos.append("compatibilidade_horarios_analisada")
    else:
        bloqueios.append("compatibilidade_horarios_nao_fechada")

    if vedacao_status == "COMPROVADO_DOCUMENTAL":
        requisitos.append("enquadramento_art_107_x_fechado")
    else:
        bloqueios.append("enquadramento_art_107_x_pendente")

    if acum_status == "COMPROVADO_DOCUMENTAL":
        requisitos.append("acumulacao_ilegal_analisada")
    else:
        bloqueios.append("acumulacao_ilegal_sem_conclusao")

    if not base_ok:
        estagio = "TRIAGEM_INTERNA"
        uso = "REVISAO_INTERNA"
        pode_uso_externo = False
        recomendacoes.append("Concluir a base documental minima antes de qualquer passo externo.")
    elif docs_localizados == 0:
        estagio = "APTO_OFICIO_DOCUMENTAL"
        uso = "PEDIDO_DOCUMENTAL"
        pode_uso_externo = True
        recomendacoes.extend(
            [
                "Protocolar pedidos objetivos para SEMSA/RH e SESACRE.",
                "Anexar respostas oficiais na caixa local do caso com hash.",
                "Nao escalar para representacao preliminar sem documento funcional recebido.",
            ]
        )
    elif compat_status in {"DOCUMENTO_RECEBIDO_PENDENTE_ANALISE", "PENDENTE_DOCUMENTO"} or vedacao_status in {"DOCUMENTO_RECEBIDO_PENDENTE_ANALISE", "PENDENTE_ENQUADRAMENTO"} or acum_status in {"DOCUMENTO_RECEBIDO_PENDENTE_ANALISE", "SEM_CONCLUSAO_AUTOMATICA"}:
        estagio = "APTO_ANALISE_JURIDICO_FUNCIONAL"
        uso = "APURACAO_INTERNA"
        pode_uso_externo = False
        recomendacoes.extend(
            [
                "Ler a documentacao funcional recebida por competencia e por profissional.",
                "Fechar compatibilidade de horarios e enquadramento do art. 107, X, antes de qualquer representacao.",
            ]
        )
    else:
        estagio = "APTO_A_NOTICIA_DE_FATO"
        uso = "REPRESENTACAO_PRELIMINAR"
        pode_uso_externo = True
        recomendacoes.extend(
            [
                "Consolidar a cadeia de prova e redigir representacao preliminar restrita aos fatos documentados.",
                "Manter nepotismo e fraude penal fora do texto, salvo nova prova objetiva.",
            ]
        )

    resumo = (
        f"O caso {case['razao_social']} ({case['cnpj']}) esta em estagio `{estagio}`. "
        f"Contrato {case['contrato_numero']} da {case['contrato_orgao']} no valor de {fmt_brl(case['contrato_valor_brl'])}. "
        f"Base documental minima={'sim' if base_ok else 'nao'}; documentos oficiais recebidos={docs_recebidos}; "
        f"documentos localizados={docs_localizados}; score funcional={score_triagem}."
    )
    limite = (
        "Esta camada decide fluxo operacional, nao ilegalidade. "
        "Ela nao autoriza afirmar nepotismo, fraude penal ou acumulacao ilicita sem fechamento juridico-funcional."
    )

    return {
        "row_id": f"vps_gate:{case['cnpj']}",
        "cnpj": case["cnpj"],
        "razao_social": case["razao_social"],
        "contrato_numero": case["contrato_numero"],
        "contrato_orgao": case["contrato_orgao"],
        "contrato_valor_brl": float(case["contrato_valor_brl"] or 0),
        "score_triagem": score_triagem,
        "estagio_operacional": estagio,
        "uso_recomendado": uso,
        "pode_uso_externo": pode_uso_externo,
        "requisitos_cumpridos_json": json.dumps(requisitos, ensure_ascii=False),
        "bloqueios_json": json.dumps(bloqueios, ensure_ascii=False),
        "recomendacoes_json": json.dumps(recomendacoes, ensure_ascii=False),
        "resumo_decisao": resumo,
        "limite_decisao": limite,
        "evidence_json": json.dumps(
            {
                "case": case,
                "maturidade": maturidade_rows,
                "respostas": respostas_rows,
                "cobertura": cobertura_rows,
                "computed_at": datetime.now().isoformat(),
            },
            ensure_ascii=False,
        ),
    }


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    con.execute("DELETE FROM vinculo_societario_saude_gate")

    case = load_one(con, "SELECT * FROM v_vinculo_societario_saude_apuracao_funcional ORDER BY score_triagem DESC, contrato_valor_brl DESC LIMIT 1")
    if not case:
        con.execute(
            """
            CREATE OR REPLACE VIEW v_vinculo_societario_saude_gate AS
            SELECT * FROM vinculo_societario_saude_gate
            """
        )
        con.close()
        print("rows=0")
        return 0

    maturidade_rows = load_rows(con, "SELECT * FROM v_vinculo_societario_saude_maturidade WHERE cnpj = ? ORDER BY eixo", [case["cnpj"]])
    respostas_rows = load_rows(con, "SELECT * FROM v_vinculo_societario_saude_respostas WHERE cnpj = ? ORDER BY destino, eixo, documento_chave", [case["cnpj"]])
    cobertura_rows = load_rows(con, "SELECT * FROM v_vinculo_societario_saude_respostas_cobertura WHERE cnpj = ? ORDER BY eixo", [case["cnpj"]])

    row = build_gate(case, maturidade_rows, respostas_rows, cobertura_rows)
    con.execute(
        """
        INSERT OR REPLACE INTO vinculo_societario_saude_gate (
            row_id, cnpj, razao_social, contrato_numero, contrato_orgao, contrato_valor_brl,
            score_triagem, estagio_operacional, uso_recomendado, pode_uso_externo,
            requisitos_cumpridos_json, bloqueios_json, recomendacoes_json,
            resumo_decisao, limite_decisao, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row["row_id"],
            row["cnpj"],
            row["razao_social"],
            row["contrato_numero"],
            row["contrato_orgao"],
            row["contrato_valor_brl"],
            row["score_triagem"],
            row["estagio_operacional"],
            row["uso_recomendado"],
            row["pode_uso_externo"],
            row["requisitos_cumpridos_json"],
            row["bloqueios_json"],
            row["recomendacoes_json"],
            row["resumo_decisao"],
            row["limite_decisao"],
            row["evidence_json"],
        ],
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_gate AS
        SELECT *
        FROM vinculo_societario_saude_gate
        ORDER BY score_triagem DESC, contrato_valor_brl DESC, razao_social
        """
    )
    con.close()
    print("rows=1")
    print(f"stage={row['estagio_operacional']}")
    print(f"uso={row['uso_recomendado']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
