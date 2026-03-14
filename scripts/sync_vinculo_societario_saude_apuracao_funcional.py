from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

DDL = """
CREATE TABLE IF NOT EXISTS vinculo_societario_saude_apuracao_funcional (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    contrato_numero VARCHAR,
    contrato_orgao VARCHAR,
    contrato_valor_brl DOUBLE,
    score_triagem INTEGER,
    prioridade VARCHAR,
    flags_json JSON,
    resumo_profissionais_json JSON,
    diligencias_json JSON,
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


def load_cases(con: duckdb.DuckDBPyConnection) -> list[dict]:
    df = con.execute(
        """
        SELECT
            j.*,
            f.socios_publicos_json,
            f.cnes_profissionais_historico_json,
            f.cnes_historico_metricas_json
        FROM vinculo_societario_saude_juridico j
        JOIN vinculo_societario_saude_followup f
          ON f.cnpj = j.cnpj
        ORDER BY j.contrato_valor_brl DESC, j.razao_social
        """
    ).fetchdf()
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def build_case(case: dict) -> dict:
    socios_publicos = json.loads(case.get("socios_publicos_json") or "[]")
    historico = json.loads(case.get("cnes_profissionais_historico_json") or "[]")
    metricas = json.loads(case.get("cnes_historico_metricas_json") or "{}")
    socio_admin = json.loads(case.get("socios_administradores_publicos_json") or "[]")

    local_by_name = {normalize_name(item.get("socio_nome")): item for item in socios_publicos}
    historico_by_name = {normalize_name(item.get("nome")): item for item in historico}
    metric_by_name = {normalize_name(item.get("nome")): item for item in metricas.get("profissionais", [])}

    resumo_profissionais: list[dict] = []
    flags: list[str] = []
    score = 0

    if socio_admin:
        flags.append("socio_administrador_em_base_publica")
        score += 30

    total_ge_80 = int(case.get("n_competencias_ge_80h") or 0)
    total_ge_60 = int(case.get("n_competencias_ge_60h") or 0)
    if total_ge_80 > 0:
        flags.append("carga_documentada_ge_80h")
        score += 20
    elif total_ge_60 > 0:
        flags.append("carga_documentada_ge_60h")
        score += 10

    for name_norm, metric in metric_by_name.items():
        local = local_by_name.get(name_norm, {})
        hist = historico_by_name.get(name_norm, {})
        public_rows = hist.get("publico_rows", [])
        company_rows = hist.get("empresa_rows", [])
        public_estabs = sorted({row.get("estabelecimento", "") for row in public_rows if row.get("estabelecimento")})
        company_estabs = sorted({row.get("estabelecimento", "") for row in company_rows if row.get("estabelecimento")})
        local_ch = int(local.get("ch") or 0)
        max_public = int(metric.get("max_ch_total_publico") or 0)
        delta_public = max_public - local_ch

        prof_flags: list[str] = []
        if delta_public >= 20:
            prof_flags.append("delta_publico_cnes_local_ge_20h")
        if len(public_estabs) > 1:
            prof_flags.append("multiplos_estabelecimentos_publicos_cnes")
        if len(company_estabs) > 1:
            prof_flags.append("multiplos_estabelecimentos_empresa_cnes")

        if "delta_publico_cnes_local_ge_20h" in prof_flags and "delta_publico_cnes_local_ge_20h" not in flags:
            flags.append("delta_publico_cnes_local_ge_20h")
            score += 20
        if "multiplos_estabelecimentos_publicos_cnes" in prof_flags and "multiplos_estabelecimentos_publicos_cnes" not in flags:
            flags.append("multiplos_estabelecimentos_publicos_cnes")
            score += 10
        if "multiplos_estabelecimentos_empresa_cnes" in prof_flags and "multiplos_estabelecimentos_empresa_cnes" not in flags:
            flags.append("multiplos_estabelecimentos_empresa_cnes")
            score += 10

        resumo_profissionais.append(
            {
                "nome": metric.get("nome", ""),
                "cns": metric.get("cns", ""),
                "cargo_publico_local": local.get("cargo", ""),
                "secretaria_local": local.get("secretaria", "") or local.get("lotacao", ""),
                "ch_publica_local": local_ch,
                "max_ch_publica_cnes": max_public,
                "max_ch_empresa_cnes": int(metric.get("max_ch_total_empresa") or 0),
                "max_ch_total_concomitante": int(metric.get("max_ch_total_concomitante") or 0),
                "n_competencias_ge_60h": int(metric.get("n_competencias_ge_60h") or 0),
                "n_competencias_ge_80h": int(metric.get("n_competencias_ge_80h") or 0),
                "competencia_pico": metric.get("competencia_pico", ""),
                "delta_publico": delta_public,
                "public_estabs": public_estabs,
                "company_estabs": company_estabs,
                "flags": prof_flags,
            }
        )

    score = min(score, 100)
    if score >= 70:
        prioridade = "ALTA"
    elif score >= 40:
        prioridade = "MEDIA"
    else:
        prioridade = "BAIXA"

    diligencias = [
        "Obter ficha funcional completa e eventuais acumulacoes formalmente declaradas pelo(s) servidor(es).",
        "Confrontar o historico do CNES com escalas, jornada e compatibilidade de horarios por competencia.",
        "Verificar se a condicao de socio-administrador se enquadra nas restricoes do art. 107, X, da Lei 1.794/2009.",
        "Esclarecer a diferenca entre a carga publica da base local carregada e a carga publica maxima documentada no CNES.",
        "Se necessario, abrir apuracao funcional/disciplinar nos termos do estatuto municipal.",
    ]
    conclusao = (
        f"Triagem funcional {prioridade.lower()} para apuracao: o caso combina sobreposicao societaria, socio-administrador "
        "em base publica municipal, historico oficial do CNES com concomitancia documentada e diferenca entre carga publica "
        "localmente carregada e carga publica maxima no CNES."
    )
    limite = (
        "O score e a prioridade sao instrumentos internos de triagem. Nao representam conclusao juridica automatica "
        "nem substituem a verificacao de regime, autorizacoes e compatibilidade de horarios."
    )

    return {
        "row_id": f"vps_func:{case['cnpj']}",
        "cnpj": case["cnpj"],
        "razao_social": case["razao_social"],
        "contrato_numero": case["contrato_numero"],
        "contrato_orgao": case["contrato_orgao"],
        "contrato_valor_brl": float(case["contrato_valor_brl"] or 0),
        "score_triagem": score,
        "prioridade": prioridade,
        "flags_json": json.dumps(flags, ensure_ascii=False),
        "resumo_profissionais_json": json.dumps(resumo_profissionais, ensure_ascii=False),
        "diligencias_json": json.dumps(diligencias, ensure_ascii=False),
        "conclusao_operacional": conclusao,
        "limite_conclusao": limite,
        "evidence_json": json.dumps(
            {
                "case": case,
                "resumo_profissionais": resumo_profissionais,
                "flags": flags,
                "score_triagem": score,
                "prioridade": prioridade,
            },
            ensure_ascii=False,
        ),
    }


def insert_case(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO vinculo_societario_saude_apuracao_funcional (
            row_id, cnpj, razao_social, contrato_numero, contrato_orgao, contrato_valor_brl,
            score_triagem, prioridade, flags_json, resumo_profissionais_json, diligencias_json,
            conclusao_operacional, limite_conclusao, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row["row_id"],
            row["cnpj"],
            row["razao_social"],
            row["contrato_numero"],
            row["contrato_orgao"],
            row["contrato_valor_brl"],
            row["score_triagem"],
            row["prioridade"],
            row["flags_json"],
            row["resumo_profissionais_json"],
            row["diligencias_json"],
            row["conclusao_operacional"],
            row["limite_conclusao"],
            row["evidence_json"],
        ],
    )


def upsert_insight(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    flags = json.loads(row["flags_json"])
    resumo = json.loads(row["resumo_profissionais_json"])
    diligencias = json.loads(row["diligencias_json"])

    prof_lines = []
    for item in resumo:
        prof_lines.append(
            f"- {item['nome']} / cargo local `{item['cargo_publico_local']}` / local `{item['ch_publica_local']}h` / "
            f"CNES publico max `{item['max_ch_publica_cnes']}h` / empresa max `{item['max_ch_empresa_cnes']}h` / "
            f"pico `{item['max_ch_total_concomitante']}h` / flags `{', '.join(item['flags']) or 'nenhuma'}`"
        )

    description = (
        f"A triagem funcional do caso **{row['razao_social']}** (`{row['cnpj']}`) resultou em score interno **{row['score_triagem']}** "
        f"e prioridade **{row['prioridade']}**, com base em fatos ja documentados no `CNES`, no `QSA`, na base municipal local e no contrato "
        f"`{row['contrato_numero']}` da `{row['contrato_orgao']}` no valor de **{fmt_brl(row['contrato_valor_brl'])}**.\n\n"
        "Sintese por profissional:\n"
        + "\n".join(prof_lines)
        + "\n\n"
        + "Diligencias sugeridas:\n"
        + "\n".join(f"- {item}" for item in diligencias)
        + "\n\n"
        + "Este insight e de revisao interna. Ele aponta prioridade de apuracao funcional e nao declara ilegalidade, nepotismo ou conflito vedado."
    )

    con.execute(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl, title, description_md,
            pattern, sources, tags, sample_n, unit_total, created_at,
            esfera, ente, orgao, municipio, uf, area_tematica, sus,
            valor_referencia, ano_referencia, fonte,
            classe_achado, grau_probatorio, fonte_primaria, uso_externo,
            inferencia_permitida, limite_conclusao
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            f"vps_func:{row['cnpj']}",
            "APURACAO_FUNCIONAL_SAUDE_PRIORITARIA",
            "MEDIO",
            92,
            float(row["contrato_valor_brl"] or 0),
            f"Apuracao funcional prioritaria para {row['razao_social']}",
            description,
            "qsa -> socio_administrador -> cnes_historico -> carga_documentada -> apuracao_funcional",
            json.dumps(["CNES", "DATASUS", "empresa_socios", "rb_servidores_mass", "rb_servidores_lotacao"], ensure_ascii=False),
            json.dumps(["apuracao_funcional", "saude", row["cnpj"], *flags], ensure_ascii=False),
            len(resumo),
            float(row["contrato_valor_brl"] or 0),
            datetime.now(),
            "municipal",
            "Prefeitura de Rio Branco",
            "SEMSA",
            "Rio Branco",
            "AC",
            "saude",
            True,
            float(row["contrato_valor_brl"] or 0),
            2023,
            "vinculo_societario_saude_apuracao_funcional",
            "HIPOTESE_INVESTIGATIVA",
            "INDICIARIO",
            "DATASUS_CNES",
            "REVISAO_INTERNA",
            "Ha fatos documentados suficientes para diligencia funcional dirigida sobre compatibilidade, gestao societaria e carga concomitante.",
            "Nao afirma ilegalidade, acumulacao vedada, nepotismo ou conflito ilicito sem confrontacao juridica e funcional adicional.",
        ],
    )


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    con.execute("DELETE FROM vinculo_societario_saude_apuracao_funcional")
    con.execute("DELETE FROM insight WHERE kind = 'APURACAO_FUNCIONAL_SAUDE_PRIORITARIA'")
    cases = load_cases(con)
    inserted = 0
    for case in cases:
        row = build_case(case)
        insert_case(con, row)
        upsert_insight(con, row)
        inserted += 1
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_apuracao_funcional AS
        SELECT *
        FROM vinculo_societario_saude_apuracao_funcional
        ORDER BY score_triagem DESC, contrato_valor_brl DESC, razao_social
        """
    )
    con.close()
    print(f"cases={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
