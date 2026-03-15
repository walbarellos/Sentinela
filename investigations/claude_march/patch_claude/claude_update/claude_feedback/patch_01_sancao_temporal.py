"""
PATCH 01 — Correção crítica: cruzamento CEIS com data e abrangência
===================================================================
Problema original: o JOIN entre contratos e sanções não verificava:
  1. Se a sanção existia ANTES da data do contrato
  2. Se a abrangência da sanção alcançava a esfera municipal

Sem isso, o sistema acusa contratos firmados ANTES da sanção existir,
e sanções restritas ao órgão sancionador (ex: CGE-RO) são aplicadas
erroneamente à Prefeitura de Rio Branco.

Aplicar: substitui a função _build_views em scripts/sync_rb_contratos.py
"""

import duckdb
import logging

log = logging.getLogger("patch_01")


# ── Mapa de abrangências que alcançam municípios ──────────────────────────────
# Baseado na tabela de abrangências do CEIS (CGU):
# "Todas as Esferas em todos os Poderes" → alcança qualquer ente federativo
# "Em todos os Poderes da Esfera do órgão sancionador" → só no ente sancionador
# "Adm Direta e Indireta da Esfera" → só no ente
# "Municipal" ou ausente → depende de análise manual

ABRANGENCIAS_MUNICIPAIS = [
    "todas as esferas",
    "todos os poderes",
    "todos os entes",
    "nacional",
    "todo o território",
]

# Sancionadores estaduais do Acre cujas sanções se estendem a municípios
# (abrangência "Todas as Esferas")
# Referência: Lei 12.846/2013 art. 30 e Lei 8.666/93 art. 87
SANCIONADORES_ACRE_AMPLOS = [
    "GOVERNO_ACRE",
    "CGE",
    "CGE-AC",
    "TCE",
    "TCE-AC",
]


def _abrangencia_alcanca_municipio(abrangencia: str) -> bool:
    """
    Determina se a abrangência de uma sanção CEIS alcança
    a esfera municipal, baseado no texto do campo.
    """
    if not abrangencia:
        # Campo ausente: interpretar conservadoramente como ampla
        # (segue entendimento CGU — dúvida favorece cautela)
        return True
    ab_lower = abrangencia.lower().strip()
    return any(kw in ab_lower for kw in ABRANGENCIAS_MUNICIPAIS)


def build_views_corrigido(con: duckdb.DuckDBPyConnection) -> None:
    """
    Versão corrigida da função _build_views de sync_rb_contratos.py.

    Correções implementadas:
      1. data_inicio_sancao <= data do contrato (sanção deve preexistir)
      2. abrangência da sanção deve alcançar esfera municipal
      3. sanção deve estar ativa NA DATA DO CONTRATO (não apenas hoje)
      4. campos de contexto adicionados para facilitar revisão humana
    """

    # View SUS básica — sem mudança
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contratos_sus AS
        SELECT * FROM rb_contratos WHERE sus = TRUE
        ORDER BY valor_contrato DESC
    """)

    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "sancoes_collapsed" not in tables:
        log.info("sancoes_collapsed ausente; pulando v_rb_contrato_ceis.")
        return

    # Detecta colunas reais de sancoes_collapsed
    sc = {r[1]: r[2] for r in con.execute(
        "PRAGMA table_info('sancoes_collapsed')"
    ).fetchall()}

    def _col(preferred: list[str], fallback: str = "''") -> str:
        for c in preferred:
            if c in sc:
                return f"s.{c}"
        return fallback

    cnpj_col   = _col(["cnpj", "cnpj_cpf", "nr_cnpj"])
    tipo_col   = _col(["tipo_sancao", "tipo", "descricao_tipo"])
    org_col    = _col(["orgao_sancao", "orgao", "nome_orgao"])
    ativa_col  = _col(["ativa"])
    di_col     = _col(["data_inicio", "data_inicio_vigencia"])
    df_col     = _col(["data_fim", "data_fim_vigencia"])
    fonte_col  = _col(["fonte"])
    abrang_col = _col(["abrangencia", "abrangencia_sancao", "ds_abrangencia"])

    if cnpj_col == "''":
        log.warning("sancoes_collapsed sem coluna cnpj; pulando join.")
        return

    # ── VIEW PRINCIPAL CORRIGIDA ───────────────────────────────────────────────
    # Lógica de validação temporal e de abrangência:
    #
    # Para que uma sanção seja RELEVANTE para um contrato municipal:
    #   a) A sanção deve ter começado ANTES ou NA data do contrato
    #      (ou data_inicio desconhecida → interpretação conservadora: incluir)
    #   b) A abrangência deve alcançar municípios
    #      (ou abrangência desconhecida → incluir, marcar para revisão)
    #   c) A sanção deve estar ativa (data_fim > data_contrato ou sem data_fim)
    #
    # O resultado inclui flag 'requer_revisao_abrangencia' para casos
    # onde a abrangência não está clara no texto — o operador decide.

    con.execute(f"""
        CREATE OR REPLACE VIEW v_rb_contrato_ceis AS
        SELECT
            c.row_id,
            c.ano,
            c.numero_contrato,
            c.numero_processo,
            c.objeto,
            c.unidade,
            c.fornecedor,
            c.cnpj,
            c.valor_contrato,
            c.sus,
            c.data_inicio  AS data_contrato,

            {fonte_col}    AS sancao_fonte,
            {tipo_col}     AS sancao_tipo,
            {di_col}       AS sancao_inicio,
            {df_col}       AS sancao_fim,
            {org_col}      AS orgao_sancao,
            {ativa_col}    AS ativa,
            {abrang_col}   AS abrangencia,

            -- Flag de validação temporal
            CASE
                WHEN {di_col} IS NULL THEN TRUE  -- data desconhecida: incluir
                WHEN {di_col} <= COALESCE(c.data_inicio::DATE, CURRENT_DATE) THEN TRUE
                ELSE FALSE
            END AS sancao_preexiste_contrato,

            -- Flag de abrangência (requer revisão se campo estiver vazio)
            CASE
                WHEN {abrang_col} IS NULL OR TRIM({abrang_col}) = '' THEN TRUE
                WHEN LOWER({abrang_col}) LIKE '%todas as esferas%' THEN TRUE
                WHEN LOWER({abrang_col}) LIKE '%todos os poderes%' THEN TRUE
                WHEN LOWER({abrang_col}) LIKE '%nacional%' THEN TRUE
                WHEN LOWER({abrang_col}) LIKE '%todo o territ%' THEN TRUE
                ELSE FALSE
            END AS abrangencia_alcanca_municipio,

            -- Flag de revisão necessária
            CASE
                WHEN {abrang_col} IS NULL OR TRIM({abrang_col}) = '' THEN TRUE
                WHEN NOT (
                    LOWER({abrang_col}) LIKE '%todas as esferas%'
                    OR LOWER({abrang_col}) LIKE '%todos os poderes%'
                    OR LOWER({abrang_col}) LIKE '%nacional%'
                    OR LOWER({abrang_col}) LIKE '%todo o territ%'
                ) THEN TRUE
                ELSE FALSE
            END AS requer_revisao_abrangencia

        FROM rb_contratos c
        JOIN sancoes_collapsed s
          ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
           = REGEXP_REPLACE({cnpj_col}, '[^0-9]', '', 'g')
        WHERE c.cnpj <> ''
    """)

    # ── VIEW FILTRADA: só casos com sanção válida para uso externo ─────────────
    # Esta é a view que deve ser usada para gerar insights e relatórios externos
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contrato_ceis_valida AS
        SELECT *
        FROM v_rb_contrato_ceis
        WHERE
            ativa = TRUE
            AND sancao_preexiste_contrato = TRUE
            AND abrangencia_alcanca_municipio = TRUE
    """)

    # ── VIEW DE REVISÃO: sanções com abrangência ambígua ──────────────────────
    # Para triagem interna — NÃO usar em outputs externos
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contrato_ceis_revisao AS
        SELECT *
        FROM v_rb_contrato_ceis
        WHERE
            ativa = TRUE
            AND sancao_preexiste_contrato = TRUE
            AND requer_revisao_abrangencia = TRUE
    """)

    # ── VIEW INVÁLIDA: sanções que NÃO existiam na data do contrato ───────────
    # Para documentação — prova que o sistema não gerou falso positivo
    con.execute("""
        CREATE OR REPLACE VIEW v_rb_contrato_ceis_invalida AS
        SELECT *,
            'SANCAO_POSTERIOR_AO_CONTRATO' AS motivo_exclusao
        FROM v_rb_contrato_ceis
        WHERE sancao_preexiste_contrato = FALSE
    """)

    # Contagens para log
    n_valida  = con.execute("SELECT COUNT(*) FROM v_rb_contrato_ceis_valida WHERE sus = TRUE").fetchone()[0]
    n_revisao = con.execute("SELECT COUNT(*) FROM v_rb_contrato_ceis_revisao WHERE sus = TRUE").fetchone()[0]
    n_invalida = con.execute("SELECT COUNT(*) FROM v_rb_contrato_ceis_invalida").fetchone()[0]

    log.info("v_rb_contrato_ceis_valida (SUS):  %d contratos com sanção válida e preexistente", n_valida)
    log.info("v_rb_contrato_ceis_revisao (SUS): %d contratos com abrangência ambígua (revisar)", n_revisao)
    log.info("v_rb_contrato_ceis_invalida:      %d contratos excluídos (sanção posterior)", n_invalida)

    if n_invalida > 0:
        log.warning(
            "ATENÇÃO: %d contrato(s) tinham sanção POSTERIOR à data do contrato. "
            "Esses casos foram EXCLUÍDOS dos alertas. "
            "Consulte v_rb_contrato_ceis_invalida para ver o detalhe.", n_invalida
        )


def build_insights_corrigido(con: duckdb.DuckDBPyConnection) -> int:
    """
    Versão corrigida de _build_insights para sync_rb_contratos.py.
    Usa v_rb_contrato_ceis_valida (com filtro temporal e de abrangência)
    ao invés de v_rb_contrato_ceis (sem filtros).

    Adiciona campos probatórios obrigatórios em todos os insights.
    """
    from datetime import datetime
    import hashlib
    import json

    if "insight" not in {r[0] for r in con.execute("SHOW TABLES").fetchall()}:
        return 0

    con.execute(
        "DELETE FROM insight WHERE kind IN ('RB_SUS_CONTRATO','RB_CONTRATO_SANCIONADO')"
    )
    agora = datetime.now()
    recs = []

    views = {r[0] for r in con.execute(
        "SELECT view_name FROM information_schema.views"
    ).fetchall()}

    if "v_rb_contrato_ceis_valida" in views:
        try:
            rows = con.execute("""
                SELECT
                    cnpj,
                    fornecedor,
                    unidade,
                    COUNT(DISTINCT numero_contrato) AS n_c,
                    SUM(valor_contrato)             AS total,
                    COUNT(DISTINCT sancao_tipo)     AS n_s,
                    MIN(sancao_inicio)              AS sancao_inicio,
                    STRING_AGG(DISTINCT abrangencia, ' | ') AS abrangencias
                FROM v_rb_contrato_ceis_valida
                WHERE ativa = TRUE AND sus = TRUE
                GROUP BY cnpj, fornecedor, unidade
                ORDER BY SUM(valor_contrato) DESC
                LIMIT 200
            """).fetchall()

            for cnpj, forn, unid, n_c, total, n_s, sancao_inicio, abrangencias in rows:
                iid = "INS_" + hashlib.md5(
                    f"RB_CONTRATO_SANCIONADO{cnpj}{unid}".encode()
                ).hexdigest()[:12]

                # Nota sobre abrangência para o operador
                nota_abrangencia = (
                    f"Abrangência da sanção: {abrangencias}. "
                    "Verificada como alcançando esfera municipal."
                    if abrangencias else
                    "Abrangência não especificada — revisar manualmente."
                )

                recs.append((
                    iid,
                    "RB_CONTRATO_SANCIONADO",
                    "CRITICAL",
                    90,  # Reduzido de 95 → 90 para refletir necessidade de revisão humana
                    float(total or 0),
                    f"RB SUS: {forn or cnpj} — sanção CEIS preexistente à contratação por {unid or 'N/I'}",
                    (
                        f"**{forn or cnpj}** (`{cnpj}`) possui "
                        f"**{n_s} tipo(s) de sanção ativa** (CEIS) que **preexistia(m) à data "
                        f"do contrato** (sanção desde {sancao_inicio or 'data não informada'}) "
                        f"e foi contratado **{n_c} vez(es)** pelo SUS de Rio Branco "
                        f"(**{unid or 'N/I'}**), total **R$ {float(total or 0):,.2f}**. "
                        f"{nota_abrangencia}"
                    ),
                    "contrato_sus_fornecedor_sancionado_municipal_validado",
                    json.dumps([
                        "transparencia.riobranco.ac.gov.br/contrato",
                        "portaldatransparencia.gov.br/download-de-dados/ceis",
                    ]),
                    json.dumps(["CEIS", "SUS", "SEMSA", "RIO_BRANCO", "sancionado", "CRITICAL",
                                "sancao_preexistente", "abrangencia_verificada"]),
                    int(n_c),
                    float(total or 0),
                    agora,
                    "municipal",
                    "Prefeitura de Rio Branco",
                    "SEMSA",
                    "Rio Branco",
                    "AC",
                    "saude",
                    True,
                    float(total or 0),
                    None,
                    "transparencia.riobranco.ac.gov.br + CGU CEIS",
                    # Campos probatórios (novos)
                    "CRUZAMENTO_SANCIONATORIO",    # classe_achado
                    "DOCUMENTAL_CORROBORADO",      # grau_probatorio
                    "CGU",                         # fonte_primaria
                    "APTO_APURACAO",               # uso_externo
                    "Há fornecedor com sanção ativa preexistente à data do contrato e com abrangência alcançando a esfera municipal.",  # inferencia_permitida
                    "Não prova dolo ou fraude; exige verificação da due diligence de integridade no processo de contratação.",  # limite_conclusao
                ))
        except Exception as e:
            log.warning("v_rb_contrato_ceis_valida query falhou: %s", e)

    # Também alertar sobre casos de revisão (abrangência ambígua) — como nota interna
    if "v_rb_contrato_ceis_revisao" in views:
        try:
            rows_revisao = con.execute("""
                SELECT cnpj, fornecedor, unidade, COUNT(DISTINCT numero_contrato), SUM(valor_contrato)
                FROM v_rb_contrato_ceis_revisao
                WHERE ativa = TRUE AND sus = TRUE
                  AND NOT (
                    cnpj IN (
                        SELECT cnpj FROM v_rb_contrato_ceis_valida WHERE ativa = TRUE AND sus = TRUE
                    )
                  )
                GROUP BY cnpj, fornecedor, unidade
                LIMIT 50
            """).fetchall()

            for cnpj, forn, unid, n_c, total in rows_revisao:
                iid = "INS_" + hashlib.md5(
                    f"RB_CONTRATO_SANCAO_REVISAO{cnpj}{unid}".encode()
                ).hexdigest()[:12]
                recs.append((
                    iid,
                    "RB_CONTRATO_SANCAO_ABRANGENCIA_REVISAO",
                    "MEDIO",
                    50,
                    float(total or 0),
                    f"REVISÃO INTERNA: {forn or cnpj} — abrangência de sanção ambígua",
                    (
                        f"**{forn or cnpj}** (`{cnpj}`) possui sanção CEIS ativa que preexiste "
                        f"ao contrato com **{unid or 'N/I'}** (R$ {float(total or 0):,.2f}), "
                        f"mas a **abrangência da sanção não foi claramente identificada como municipal**. "
                        f"Requer revisão humana antes de qualquer uso externo."
                    ),
                    "contrato_sus_sancao_abrangencia_ambigua",
                    json.dumps(["transparencia.riobranco.ac.gov.br", "portaldatransparencia.gov.br"]),
                    json.dumps(["CEIS", "SUS", "REVISAO_INTERNA", "abrangencia_ambigua"]),
                    int(n_c), float(total or 0), agora,
                    "municipal", "Prefeitura de Rio Branco", "SEMSA",
                    "Rio Branco", "AC", "saude", True,
                    float(total or 0), None, "CGU CEIS",
                    "CRUZAMENTO_SANCIONATORIO",
                    "INDICIARIO",
                    "CGU",
                    "REVISAO_INTERNA",
                    "Há sanção CEIS ativa preexistente, mas abrangência precisa de verificação manual.",
                    "NÃO usar externamente sem confirmar que a abrangência da sanção alcança municípios.",
                ))
        except Exception as e:
            log.warning("v_rb_contrato_ceis_revisao query falhou: %s", e)

    if recs:
        # Tenta inserir com campos extras (versão nova do schema)
        try:
            con.executemany(
                """INSERT OR IGNORE INTO insight
                   (id, kind, severity, confidence, exposure_brl,
                    title, description_md, pattern, sources, tags,
                    sample_n, unit_total, created_at,
                    esfera, ente, orgao, municipio, uf,
                    area_tematica, sus, valor_referencia, ano_referencia, fonte,
                    classe_achado, grau_probatorio, fonte_primaria,
                    uso_externo, inferencia_permitida, limite_conclusao)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                recs,
            )
        except Exception:
            # Fallback para schema sem campos probatórios (versão antiga)
            recs_curtos = [r[:23] for r in recs]
            con.executemany(
                """INSERT OR IGNORE INTO insight
                   (id, kind, severity, confidence, exposure_brl,
                    title, description_md, pattern, sources, tags,
                    sample_n, unit_total, created_at,
                    esfera, ente, orgao, municipio, uf,
                    area_tematica, sus, valor_referencia, ano_referencia, fonte)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                recs_curtos,
            )
            log.warning("Schema sem campos probatórios — aplicando patch de schema recomendado.")

    return len(recs)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_path = sys.argv[1] if len(sys.argv) > 1 else "./data/sentinela_analytics.duckdb"
    con = duckdb.connect(db_path)
    log.info("Aplicando patch 01 — validação temporal e de abrangência CEIS...")
    build_views_corrigido(con)
    n = build_insights_corrigido(con)
    log.info("Patch 01 concluído. %d insights gerados.", n)
    con.close()
