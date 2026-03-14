from __future__ import annotations

import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

DDL = """
CREATE TABLE IF NOT EXISTS vinculo_societario_saude_maturidade (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    eixo VARCHAR,
    status_probatorio VARCHAR,
    uso_externo VARCHAR,
    evidencia_resumo VARCHAR,
    proximo_documento VARCHAR,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def fmt_brl(value: object) -> str:
    number = float(value or 0)
    text = f"{number:,.2f}"
    return "R$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")


def load_case(con: duckdb.DuckDBPyConnection) -> dict:
    query = """
    SELECT
        f.*,
        j.normas_json,
        j.perguntas_apuracao_json,
        j.socios_administradores_publicos_json,
        a.score_triagem,
        a.prioridade,
        a.flags_json,
        a.resumo_profissionais_json
    FROM v_vinculo_societario_saude_followup f
    LEFT JOIN v_vinculo_societario_saude_juridico j ON j.cnpj = f.cnpj
    LEFT JOIN v_vinculo_societario_saude_apuracao_funcional a ON a.cnpj = f.cnpj
    ORDER BY f.contrato_valor_brl DESC
    LIMIT 1
    """
    df = con.execute(query).fetchdf()
    if df.empty:
        return {}
    return json.loads(df.to_json(orient="records", force_ascii=False))[0]


def load_response_coverage(con: duckdb.DuckDBPyConnection, cnpj: str) -> dict[str, dict]:
    try:
        df = con.execute(
            """
            SELECT *
            FROM v_vinculo_societario_saude_respostas_cobertura
            WHERE cnpj = ?
            """,
            [cnpj],
        ).fetchdf()
    except duckdb.Error:
        return {}
    if df.empty:
        return {}
    rows = json.loads(df.to_json(orient="records", force_ascii=False))
    return {row["eixo"]: row for row in rows}


def build_rows(case: dict, response_coverage: dict[str, dict]) -> list[dict]:
    historico = json.loads(case.get("cnes_profissionais_historico_json") or "[]")
    socio_publicos = json.loads(case.get("socios_publicos_json") or "[]")
    profiss = json.loads(case.get("cnes_profissionais_match_json") or "[]")

    n_concom = int(case.get("n_competencias_concomitantes_total") or 0)
    n_ge80 = int(case.get("n_competencias_ge_80h") or 0)
    has_admin = any("administrador" in str(s.get("qualificacao_qsa", "")).lower() for s in json.loads(case.get("socios_administradores_publicos_json") or "[]"))
    coverage_compat = response_coverage.get("compatibilidade_horarios", {})
    coverage_vedacao = response_coverage.get("vedacao_art_107_x", {})
    coverage_acum = response_coverage.get("acumulacao_ilegal", {})

    def row(eixo: str, status: str, uso: str, evidencia: str, proximo: str) -> dict:
        return {
            "row_id": f"vps_maturidade:{case['cnpj']}:{eixo}",
            "cnpj": case["cnpj"],
            "razao_social": case["razao_social"],
            "eixo": eixo,
            "status_probatorio": status,
            "uso_externo": uso,
            "evidencia_resumo": evidencia,
            "proximo_documento": proximo,
        }

    rows: list[dict] = []
    rows.append(
        row(
            "contrato_estadual",
            "COMPROVADO_DOCUMENTAL",
            "APTO_APURACAO",
            f"Contrato {case.get('contrato_numero')} da {case.get('contrato_orgao')} identificado no valor de {fmt_brl(case.get('contrato_valor_brl') or 0)}.",
            "Processo integral do contrato e anexos de execucao.",
        )
    )
    rows.append(
        row(
            "qsa_socios",
            "COMPROVADO_DOCUMENTAL",
            "APTO_APURACAO",
            f"{len(socio_publicos)} coincidencia(s) nominal(is) exata(s) entre QSA e base municipal local.",
            "Confirmacao funcional completa e declaracoes de acumulacao.",
        )
    )
    rows.append(
        row(
            "socio_administrador",
            "COMPROVADO_DOCUMENTAL" if has_admin else "SEM_BASE_ATUAL",
            "APTO_APURACAO" if has_admin else "REVISAO_INTERNA",
            "Ha socio-administrador com coincidencia exata em base publica municipal." if has_admin else "Nao apareceu socio-administrador com coincidencia exata no recorte atual.",
            "Ficha funcional e eventual autorizacao para gerencia/administracao societaria." if has_admin else "Nenhum.",
        )
    )
    rows.append(
        row(
            "cnes_profissional_ativo",
            "COMPROVADO_DOCUMENTAL" if profiss else "SEM_BASE_ATUAL",
            "APTO_APURACAO" if profiss else "REVISAO_INTERNA",
            f"{len(profiss)} profissional(is) coincidente(s) listado(s) no modulo oficial de profissionais do CNES." if profiss else "Nenhum profissional coincidente materializado.",
            "Fichas individuais dos profissionais no CNES.",
        )
    )
    rows.append(
        row(
            "cnes_historico_concomitante",
            "COMPROVADO_DOCUMENTAL" if historico else "SEM_BASE_ATUAL",
            "APTO_APURACAO" if historico else "REVISAO_INTERNA",
            f"{n_concom} competencias concomitantes documentadas no historico oficial do CNES." if historico else "Sem historico oficial materializado.",
            "Escalas, ponto e compatibilidade por competencia.",
        )
    )
    rows.append(
        row(
            "carga_concomitante_extrema",
            "COMPROVADO_DOCUMENTAL" if n_ge80 > 0 else "INDICIO_DOCUMENTAL",
            "APTO_APURACAO" if n_ge80 > 0 else "REVISAO_INTERNA",
            f"{n_ge80} competencias com >=80h documentadas; pico de {int(case.get('max_ch_total_concomitante') or 0)}h." if n_ge80 > 0 else f"Pico documental inferior a 80h; maximo {int(case.get('max_ch_total_concomitante') or 0)}h.",
            "Confrontar jornadas com norma funcional e autorizacoes.",
        )
    )

    if coverage_compat.get("has_ficha_funcional") and coverage_compat.get("has_escala_ponto"):
        compat_status = "DOCUMENTO_RECEBIDO_PENDENTE_ANALISE"
        compat_evid = (
            f"Ja existem {coverage_compat.get('docs_localizados', 0)} documento(s) localizados para compatibilidade, "
            "incluindo ficha funcional e escala/ponto. Ainda depende de leitura juridico-funcional."
        )
        compat_next = "Analisar compatibilidade por competencia e confrontar com pareceres/autorizacoes."
    else:
        compat_status = "PENDENTE_DOCUMENTO"
        compat_evid = "A prova atual documenta carga e concomitancia, mas nao fecha compatibilidade formal de horarios."
        compat_next = "Escalas, ponto, declaracoes e pareceres de compatibilidade."
    rows.append(
        row(
            "compatibilidade_horarios",
            compat_status,
            "REVISAO_INTERNA",
            compat_evid,
            compat_next,
        )
    )

    if coverage_vedacao.get("has_ficha_funcional") and coverage_vedacao.get("has_autorizacao_gerencia"):
        vedacao_status = "DOCUMENTO_RECEBIDO_PENDENTE_ANALISE"
        vedacao_evid = (
            f"Ja existem {coverage_vedacao.get('docs_localizados', 0)} documento(s) localizados no eixo do art. 107, X, "
            "incluindo ficha funcional e documento ligado a gerencia/administracao societaria."
        )
        vedacao_next = "Enquadramento juridico do art. 107, X, sobre a situacao funcional concreta."
    else:
        vedacao_status = "PENDENTE_ENQUADRAMENTO"
        vedacao_evid = "Ha dado objetivo de socio-administrador, mas a incidência juridica do art. 107, X, ainda depende de enquadramento funcional e excecoes aplicaveis."
        vedacao_next = "Analise juridica do estatuto municipal e da situacao funcional concreta."
    rows.append(
        row(
            "vedacao_art_107_x",
            vedacao_status,
            "REVISAO_INTERNA",
            vedacao_evid,
            vedacao_next,
        )
    )

    if coverage_acum.get("has_ficha_funcional") and coverage_acum.get("has_declaracao_acumulacao") and coverage_compat.get("has_escala_ponto"):
        acum_status = "DOCUMENTO_RECEBIDO_PENDENTE_ANALISE"
        acum_evid = (
            f"Ja existem {coverage_acum.get('docs_localizados', 0)} documento(s) localizados para acumulacao, "
            "com declaracao e ficha funcional, alem de escala/ponto no eixo de compatibilidade."
        )
        acum_next = "Confrontar regime, declaracoes e horarios antes de qualquer conclusao juridica."
    else:
        acum_status = "SEM_CONCLUSAO_AUTOMATICA"
        acum_evid = "A prova atual nao basta para concluir acumulacao ilegal sem confrontar regime, autorizacoes e horarios."
        acum_next = "Processo funcional, declaracoes e validacao juridica."
    rows.append(
        row(
            "acumulacao_ilegal",
            acum_status,
            "REVISAO_INTERNA",
            acum_evid,
            acum_next,
        )
    )
    rows.append(
        row(
            "nepotismo",
            "SEM_BASE_ATUAL",
            "NAO_USAR_EXTERNAMENTE",
            "Nao ha base objetiva atual para afirmar nepotismo neste caso.",
            "Somente se surgirem documentos especificos de parentesco ou nomeacao cruzada.",
        )
    )
    rows.append(
        row(
            "fraude_penal",
            "SEM_BASE_ATUAL",
            "NAO_USAR_EXTERNAMENTE",
            "Nao ha base automatizada atual para afirmar fraude penal consumada.",
            "Depende de apuracao humana, contraditorio e prova adicional.",
        )
    )
    return rows


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    con.execute("DELETE FROM vinculo_societario_saude_maturidade")
    case = load_case(con)
    inserted = 0
    if case:
        response_coverage = load_response_coverage(con, case["cnpj"])
        for row in build_rows(case, response_coverage):
            con.execute(
                """
                INSERT OR REPLACE INTO vinculo_societario_saude_maturidade (
                    row_id, cnpj, razao_social, eixo, status_probatorio, uso_externo,
                    evidencia_resumo, proximo_documento
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["row_id"],
                    row["cnpj"],
                    row["razao_social"],
                    row["eixo"],
                    row["status_probatorio"],
                    row["uso_externo"],
                    row["evidencia_resumo"],
                    row["proximo_documento"],
                ],
            )
            inserted += 1
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_maturidade AS
        SELECT *
        FROM vinculo_societario_saude_maturidade
        ORDER BY cnpj, eixo
        """
    )
    con.close()
    print(f"rows={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
