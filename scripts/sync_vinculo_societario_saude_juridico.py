from __future__ import annotations

import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

RIO_BRANCO_STATUTE_URL = (
    "https://portalcgm.riobranco.ac.gov.br/lai/wp-content/uploads/2012/05/"
    "LEI-N%C2%BA-1.794-de-30.12.2009-Regime-Juridico-Estatutario-da-PMRB.pdf"
)
CF_URL = "https://www.planalto.gov.br/ccivil_03/constituicao/constituicaocompilado.htm"

DDL = """
CREATE TABLE IF NOT EXISTS vinculo_societario_saude_juridico (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    contrato_numero VARCHAR,
    contrato_orgao VARCHAR,
    contrato_valor_brl DOUBLE,
    n_profissionais_concomitancia INTEGER,
    n_competencias_concomitantes_total INTEGER,
    n_competencias_ge_60h INTEGER,
    n_competencias_ge_80h INTEGER,
    max_ch_total_publico INTEGER,
    max_ch_total_empresa INTEGER,
    max_ch_total_concomitante INTEGER,
    socios_publicos_json JSON,
    socios_qsa_json JSON,
    socios_administradores_publicos_json JSON,
    normas_json JSON,
    perguntas_apuracao_json JSON,
    achados_objetivos_json JSON,
    conclusao_operacional VARCHAR,
    limite_conclusao VARCHAR,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def fmt_brl(value: object) -> str:
    number = float(value or 0)
    text = f"{number:,.2f}"
    return "R$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")


def normalize_name(value: object) -> str:
    return " ".join(str(value or "").upper().split())


def load_followup_cases(con: duckdb.DuckDBPyConnection) -> list[dict]:
    df = con.execute(
        """
        SELECT *
        FROM vinculo_societario_saude_followup
        ORDER BY contrato_valor_brl DESC, razao_social
        """
    ).fetchdf()
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def load_socios_qsa(con: duckdb.DuckDBPyConnection, cnpj: str) -> list[dict]:
    df = con.execute(
        """
        SELECT cnpj, socio_nome, socio_cpf_cnpj, qualificacao, data_entrada
        FROM empresa_socios
        WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?
        ORDER BY socio_nome
        """,
        [cnpj],
    ).fetchdf()
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def build_norms() -> list[dict]:
    return [
        {
            "id": "CF_ART_37_XVI_C",
            "fonte": "Constituicao Federal",
            "artigo": "art. 37, XVI, c",
            "url": CF_URL,
            "nota": "Admite acumulacao de dois cargos ou empregos privativos de profissionais de saude, com profissoes regulamentadas.",
        },
        {
            "id": "CF_ART_37_XVII",
            "fonte": "Constituicao Federal",
            "artigo": "art. 37, XVII",
            "url": CF_URL,
            "nota": "A proibicao de acumular alcanca cargos, empregos e funcoes nas diversas esferas e entidades publicas.",
        },
        {
            "id": "RB_LEI_1794_ART_107_X",
            "fonte": "Lei Municipal 1.794/2009",
            "artigo": "art. 107, X",
            "url": RIO_BRANCO_STATUTE_URL,
            "nota": "Veda participar de gerencia ou administracao de sociedade privada, ressalvadas as hipoteses do paragrafo unico.",
        },
        {
            "id": "RB_LEI_1794_ART_108_2",
            "fonte": "Lei Municipal 1.794/2009",
            "artigo": "art. 108, §2",
            "url": RIO_BRANCO_STATUTE_URL,
            "nota": "Mesmo a acumulacao licita depende de comprovacao de compatibilidade de horarios.",
        },
        {
            "id": "RB_LEI_1794_ART_123_124",
            "fonte": "Lei Municipal 1.794/2009",
            "artigo": "arts. 123 e 124",
            "url": RIO_BRANCO_STATUTE_URL,
            "nota": "Prevem demissao e procedimento especifico quando detectada acumulacao ilegal.",
        },
    ]


def build_case(case: dict, socios_qsa: list[dict]) -> dict:
    socios_publicos = json.loads(case.get("socios_publicos_json") or "[]")
    metricas = json.loads(case.get("cnes_historico_metricas_json") or "{}")
    public_by_name = {normalize_name(item.get("socio_nome")): item for item in socios_publicos}
    public_local_ch_by_name = {
        normalize_name(item.get("socio_nome")): int(item.get("ch") or 0)
        for item in socios_publicos
    }

    socios_administradores_publicos: list[dict] = []
    for socio in socios_qsa:
        qual = str(socio.get("qualificacao") or "")
        if "ADMINISTRADOR" not in qual.upper():
            continue
        match = public_by_name.get(normalize_name(socio.get("socio_nome")))
        if not match:
            continue
        socios_administradores_publicos.append(
            {
                "socio_nome": socio.get("socio_nome", ""),
                "qualificacao_qsa": qual,
                "cargo_publico": match.get("cargo", ""),
                "secretaria": match.get("secretaria", "") or match.get("lotacao", ""),
                "ch_publica_local": match.get("ch", 0),
            }
        )

    achados_objetivos = [
        f"Contrato estadual de saude {case.get('contrato_numero') or '-'} com {case.get('contrato_orgao') or '-'} no valor de {fmt_brl(case.get('contrato_valor_brl') or 0)}.",
        f"{int(case.get('n_profissionais_concomitancia') or 0)} profissional(is) com concomitancia documental no historico oficial do CNES.",
        f"{int(case.get('n_competencias_concomitantes_total') or 0)} competencias concomitantes no total.",
        f"{int(case.get('n_competencias_ge_60h') or 0)} competencias com carga concomitante documentada >= 60h.",
        f"{int(case.get('n_competencias_ge_80h') or 0)} competencias com carga concomitante documentada >= 80h.",
        f"Pico documental de carga concomitante: {int(case.get('max_ch_total_concomitante') or 0)}h.",
    ]
    if socios_administradores_publicos:
        achados_objetivos.append(
            f"{len(socios_administradores_publicos)} socio(s)-administrador(es) do QSA com coincidencia exata em base publica municipal."
        )

    comparativo_publico_local_cnes: list[dict] = []
    for item in metricas.get("profissionais", []):
        nome_norm = normalize_name(item.get("nome"))
        local_ch = int(public_local_ch_by_name.get(nome_norm) or 0)
        cnes_publico = int(item.get("max_ch_total_publico") or 0)
        if cnes_publico <= 0:
            continue
        comparativo_publico_local_cnes.append(
            {
                "nome": item.get("nome", ""),
                "ch_publica_local": local_ch,
                "max_ch_publica_cnes": cnes_publico,
                "delta_publico": cnes_publico - local_ch,
            }
        )
        if cnes_publico > local_ch:
            achados_objetivos.append(
                f"{item.get('nome', '')}: base local carregada mostra {local_ch}h, mas o historico publico do CNES chega a {cnes_publico}h."
            )

    perguntas = [
        {
            "codigo": "Q1",
            "pergunta": "Os vinculos publicos municipais e privados documentados no CNES possuem compatibilidade formal de horarios por competencia?",
            "normas": ["CF_ART_37_XVI_C", "RB_LEI_1794_ART_108_2"],
        },
        {
            "codigo": "Q2",
            "pergunta": "A participacao societaria identificada no QSA envolve gerencia ou administracao vedada ao servidor municipal no caso concreto?",
            "normas": ["RB_LEI_1794_ART_107_X"],
        },
        {
            "codigo": "Q3",
            "pergunta": "Se houver incompatibilidade ou acumulacao indevida, o caso exige apuracao disciplinar nos moldes dos arts. 123 e 124 do estatuto municipal?",
            "normas": ["RB_LEI_1794_ART_123_124"],
        },
        {
            "codigo": "Q4",
            "pergunta": "A diferenca entre a carga publica da base local e a carga publica maxima documentada no CNES indica vinculos publicos adicionais, cadastro incompleto ou necessidade de saneamento funcional?",
            "normas": ["RB_LEI_1794_ART_108_2"],
        },
    ]

    conclusao = (
        "Caso apto a apuracao juridico-funcional: ha contrato estadual de saude, sobreposicao societaria exata, "
        "profissionais listados no CNES da empresa e historico oficial com carga concomitante documentada. "
        "O caso exige verificacao de compatibilidade de horarios, alcance do art. 107, X, da Lei 1.794/2009 "
        "e eventual incidencia do procedimento disciplinar por acumulacao ilegal."
    )
    limite = (
        "A matriz nao conclui ilegalidade, nepotismo ou conflito vedado por si so. "
        "Ela organiza fatos primarios e perguntas juridicas para revisao humana."
    )

    return {
        "row_id": f"vps_juridico:{case['cnpj']}",
        "cnpj": case["cnpj"],
        "razao_social": case["razao_social"],
        "contrato_numero": case.get("contrato_numero"),
        "contrato_orgao": case.get("contrato_orgao"),
        "contrato_valor_brl": float(case.get("contrato_valor_brl") or 0),
        "n_profissionais_concomitancia": int(case.get("n_profissionais_concomitancia") or 0),
        "n_competencias_concomitantes_total": int(case.get("n_competencias_concomitantes_total") or 0),
        "n_competencias_ge_60h": int(case.get("n_competencias_ge_60h") or 0),
        "n_competencias_ge_80h": int(case.get("n_competencias_ge_80h") or 0),
        "max_ch_total_publico": int(case.get("max_ch_total_publico") or 0),
        "max_ch_total_empresa": int(case.get("max_ch_total_empresa") or 0),
        "max_ch_total_concomitante": int(case.get("max_ch_total_concomitante") or 0),
        "socios_publicos_json": json.dumps(socios_publicos, ensure_ascii=False),
        "socios_qsa_json": json.dumps(socios_qsa, ensure_ascii=False),
        "socios_administradores_publicos_json": json.dumps(socios_administradores_publicos, ensure_ascii=False),
        "normas_json": json.dumps(build_norms(), ensure_ascii=False),
        "perguntas_apuracao_json": json.dumps(perguntas, ensure_ascii=False),
        "achados_objetivos_json": json.dumps(achados_objetivos, ensure_ascii=False),
        "conclusao_operacional": conclusao,
        "limite_conclusao": limite,
        "evidence_json": json.dumps(
            {
                "metricas": metricas,
                "case_row": case,
                "socios_administradores_publicos": socios_administradores_publicos,
                "comparativo_publico_local_cnes": comparativo_publico_local_cnes,
            },
            ensure_ascii=False,
        ),
    }


def insert_case(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO vinculo_societario_saude_juridico (
            row_id, cnpj, razao_social, contrato_numero, contrato_orgao, contrato_valor_brl,
            n_profissionais_concomitancia, n_competencias_concomitantes_total, n_competencias_ge_60h,
            n_competencias_ge_80h, max_ch_total_publico, max_ch_total_empresa, max_ch_total_concomitante,
            socios_publicos_json, socios_qsa_json, socios_administradores_publicos_json,
            normas_json, perguntas_apuracao_json, achados_objetivos_json,
            conclusao_operacional, limite_conclusao, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row["row_id"],
            row["cnpj"],
            row["razao_social"],
            row["contrato_numero"],
            row["contrato_orgao"],
            row["contrato_valor_brl"],
            row["n_profissionais_concomitancia"],
            row["n_competencias_concomitantes_total"],
            row["n_competencias_ge_60h"],
            row["n_competencias_ge_80h"],
            row["max_ch_total_publico"],
            row["max_ch_total_empresa"],
            row["max_ch_total_concomitante"],
            row["socios_publicos_json"],
            row["socios_qsa_json"],
            row["socios_administradores_publicos_json"],
            row["normas_json"],
            row["perguntas_apuracao_json"],
            row["achados_objetivos_json"],
            row["conclusao_operacional"],
            row["limite_conclusao"],
            row["evidence_json"],
        ],
    )


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    con.execute("DELETE FROM vinculo_societario_saude_juridico")
    rows = load_followup_cases(con)
    inserted = 0
    for case in rows:
        if int(case.get("n_competencias_concomitantes_total") or 0) <= 0:
            continue
        socios_qsa = load_socios_qsa(con, case["cnpj"])
        juridico = build_case(case, socios_qsa)
        insert_case(con, juridico)
        inserted += 1
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_juridico AS
        SELECT *
        FROM vinculo_societario_saude_juridico
        ORDER BY contrato_valor_brl DESC, razao_social
        """
    )
    con.close()
    print(f"cases={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
