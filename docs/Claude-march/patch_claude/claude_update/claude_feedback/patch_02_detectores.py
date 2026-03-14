"""
PATCH 02 — Correções no cross_reference_engine.py
==================================================
Problemas corrigidos:

  1. detect_nepotismo_sobrenome → rebaixado para triagem interna pura,
     NUNCA gera alerta externo, exige pré-filtro de sobrenomes comuns.

  2. detect_fracionamento → threshold corrigido para R$ 50.000 (Art. 75
     Lei 14.133/2021). Referência legal corrigida.

  3. Todos os detectores agora incluem campos probatórios:
     classe_achado, grau_probatorio, fonte_primaria, uso_externo,
     inferencia_permitida, limite_conclusao.

  4. detect_outlier_salarial → exige n >= 30, separação conceitual de
     adicionais legais.

  5. detect_empresa_suspensa → verifica data da sanção vs data do contrato.

Aplicar: substituir as funções correspondentes em src/core/cross_reference_engine.py
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

import duckdb
import pandas as pd

# ── SOBRENOMES COMUNS NO ACRE (falso positivo garantido) ─────────────────────
# Fonte: IBGE + dados eleitorais TSE/AC 2024
# Sobrenomes com >= 1000 portadores no Acre são excluídos do cruzamento de nepotismo
SOBRENOMES_COMUNS_ACRE = frozenset([
    "SILVA", "SOUZA", "SANTOS", "OLIVEIRA", "LIMA", "PEREIRA", "COSTA",
    "FERREIRA", "ALVES", "RODRIGUES", "NASCIMENTO", "GOMES", "ARAÚJO",
    "ARAUJO", "ROCHA", "MARTINS", "CARVALHO", "MELO", "RIBEIRO", "MOURA",
    "BATISTA", "NUNES", "CAVALCANTE", "PINTO", "LOPES", "MOREIRA", "DIAS",
    "MONTEIRO", "CASTRO", "CAMPOS", "MENDES", "TEIXEIRA", "FREITAS",
    "MACEDO", "CUNHA", "ANDRADE", "RAMOS", "SOARES", "CARDOSO", "FARIAS",
    "MEDEIROS", "NETO", "FILHO", "JUNIOR", "SALES", "VALE", "QUEIROZ",
    "BEZERRA", "BARBOSA", "CORREIA", "AZEVEDO", "FIGUEIREDO", "NOGUEIRA",
    # Sobrenomes de origem indígena frequentes no Acre
    "KAXINAWÁ", "KAXINAWA", "PANO", "YAWANAWÁ", "YAWANAWA", "ASHANINKA",
    "SHARANAWA", "APURINÃ", "APURINA", "MANCHINERI", "JAMINAWA",
])

# Comprimento mínimo de sobrenome para evitar falsos positivos
MIN_SOBRENOME_LEN = 6

# ── LIMITES DE DISPENSA — Lei 14.133/2021 art. 75 ───────────────────────────
# Valores vigentes desde a publicação da lei (1° abril 2021)
# Não existe decreto de correção pelo IPCA até a data desta análise (mar/2026)
LIMITE_DISPENSA_BENS_SERVICOS = 50_000.00   # Art. 75, I
LIMITE_DISPENSA_OBRAS_ENGENHARIA = 100_000.00  # Art. 75, I (obras)
LIMITE_CONTRATACAO_DIRETA = 15_000.00  # Art. 75, II (pesquisa de preço)

# ── DATACLASS DE ALERTA EXPANDIDO ────────────────────────────────────────────

@dataclass
class AlertaExpanded:
    detector_id: str
    severity: str
    entity_type: str
    entity_name: str
    description: str
    exposure_brl: float
    base_legal: str
    # Campos probatórios obrigatórios (alinhados com protocolo ops_registry)
    classe_achado: str = "HIPOTESE_INVESTIGATIVA"
    grau_probatorio: str = "EXPLORATORIO"
    fonte_primaria: str = ""
    uso_externo: str = "REVISAO_INTERNA"
    inferencia_permitida: str = ""
    limite_conclusao: str = "Não usar externamente sem revisão humana."
    evidence: list = field(default_factory=list)
    dossie_id: str = ""

    def __post_init__(self):
        if not self.dossie_id:
            prefix = self.detector_id[:4].upper()
            self.dossie_id = f"{prefix}_{hash(self.entity_name) % 999999}"


# ── DETECTORES CORRIGIDOS ────────────────────────────────────────────────────

def detect_fracionamento_corrigido(conn: duckdb.DuckDBPyConnection) -> list[AlertaExpanded]:
    """
    Detecta fracionamento de despesas para burlar licitação.

    Correção: threshold R$ 50.000 (não R$ 57.278,16 sem base legal).
    Base legal: Lei 14.133/2021, Art. 75, I + Art. 22, §4° (vedação ao
    fracionamento que enquadre parcelas abaixo do limite de dispensa quando
    a contratação global exigiria licitação).

    Nota: o fracionamento exige dolo demonstrável — o alerta é
    HIPOTESE_INVESTIGATIVA, nunca FATO_DOCUMENTAL por si só.
    """
    base_legal = (
        "Lei 14.133/2021, art. 75, I (limite de dispensa R$ 50.000 para bens/serviços) "
        "c/c art. 22, §4° (vedação ao fracionamento com intenção de enquadrar "
        "em modalidade de licitação menos rigorosa). "
        "Ver também: Acórdão TCU 1.094/2013-Plenário."
    )

    try:
        df = conn.execute(f"""
            SELECT
                empresa_nome,
                secretaria,
                COUNT(*)                    AS num_contratos,
                SUM(CAST(valor_total AS DOUBLE)) AS valor_agregado,
                MAX(CAST(valor_total AS DOUBLE)) AS maior_contrato,
                MIN(capturado_em)           AS primeiro,
                MAX(capturado_em)           AS ultimo
            FROM (
                SELECT empresa_nome, secretaria, valor_total, capturado_em FROM obras
                UNION ALL
                SELECT empresa_nome, secretaria, valor_total, capturado_em FROM licitacoes
            )
            WHERE CAST(valor_total AS DOUBLE) < {LIMITE_DISPENSA_BENS_SERVICOS}
              AND CAST(valor_total AS DOUBLE) > 0
              AND empresa_nome IS NOT NULL
              AND secretaria IS NOT NULL
            GROUP BY empresa_nome, secretaria
            HAVING num_contratos >= 3
              AND valor_agregado > {LIMITE_DISPENSA_BENS_SERVICOS}
            ORDER BY valor_agregado DESC
            LIMIT 100
        """).fetchdf()
    except Exception as e:
        return []

    alertas = []
    for _, row in df.iterrows():
        # Grau probatório depende do número de contratos e padrão temporal
        # 3 contratos → indiciário; 5+ → documental primário (padrão mais claro)
        n = int(row["num_contratos"])
        grau = "DOCUMENTAL_PRIMARIO" if n >= 5 else "INDICIARIO"
        uso = "APTO_APURACAO" if n >= 5 else "REVISAO_INTERNA"

        alertas.append(AlertaExpanded(
            detector_id="FRAC",
            severity="CRÍTICO" if n >= 5 else "ALTO",
            entity_type="empresa",
            entity_name=row["empresa_nome"],
            description=(
                f"{n} contratos de '{row['empresa_nome']}' com '{row['secretaria']}', "
                f"todos abaixo do limite de dispensa (R$ {LIMITE_DISPENSA_BENS_SERVICOS:,.0f}). "
                f"Valor agregado: R$ {row['valor_agregado']:,.2f}. "
                f"Período: {row['primeiro']} a {row['ultimo']}. "
                f"Maior contrato individual: R$ {row['maior_contrato']:,.2f}."
            ),
            exposure_brl=float(row["valor_agregado"]),
            base_legal=base_legal,
            classe_achado="RASTRO_CONTRATUAL",
            grau_probatorio=grau,
            fonte_primaria="PORTAL_RIO_BRANCO",
            uso_externo=uso,
            inferencia_permitida=(
                "Há padrão de contratações abaixo do limite de dispensa com o mesmo fornecedor "
                "e secretaria, cujo valor agregado exigiria licitação formal. "
                "O padrão é compatível com fracionamento vedado."
            ),
            limite_conclusao=(
                "Fracionamento exige demonstração de dolo — o padrão contratual é necessário "
                "mas não suficiente. Verificar: modalidade licitatória utilizada, "
                "objeto de cada contrato (se são serviços distintos ou o mesmo bem/serviço), "
                "e cronologia (contratações simultâneas ou sequenciais próximas)."
            ),
        ))
    return alertas


def detect_outlier_salarial_corrigido(conn: duckdb.DuckDBPyConnection) -> list[AlertaExpanded]:
    """
    Detecta outliers salariais com Z-score.

    Correções:
      - n mínimo: 30 servidores por cargo (antes era 5 — estatisticamente inválido)
      - compara salario_liquido apenas com o referencial do mesmo cargo
      - adiciona nota sobre adicionais legítimos (periculosidade, insalubridade, etc.)
      - grau rebaixado: sempre INDICIARIO (sem acesso ao histórico de adicionais)

    Base legal corrigida: CF art. 37, X (remuneração fixada por lei específica)
    — não art. 37, XI (teto) pois o teto se aplica ao bruto, não ao líquido.
    """
    base_legal = (
        "CF art. 37, X — a remuneração dos servidores públicos somente poderá ser fixada "
        "ou alterada por lei específica. Valores discrepantes sem base legal formal "
        "podem configurar improbidade administrativa (Lei 8.429/1992, art. 9°, I). "
        "ATENÇÃO: adicionais de periculosidade, insalubridade, função gratificada, "
        "adicional noturno e tempo de serviço são legítimos e podem elevar significativamente "
        "o salário líquido — verificar antes de qualquer conclusão."
    )

    try:
        df = conn.execute("""
            WITH stats AS (
                SELECT
                    cargo,
                    AVG(CAST(salario_liquido AS DOUBLE))    AS media,
                    STDDEV(CAST(salario_liquido AS DOUBLE)) AS desvio,
                    COUNT(*)                                AS n
                FROM servidores
                WHERE salario_liquido IS NOT NULL
                  AND CAST(salario_liquido AS DOUBLE) > 0
                GROUP BY cargo
                HAVING COUNT(*) >= 30  -- mínimo estatístico
                   AND STDDEV(CAST(salario_liquido AS DOUBLE)) > 0
            )
            SELECT
                split_part(servidor, ' - ', 2) AS nome,
                s.cargo,
                s.secretaria,
                CAST(s.salario_liquido AS DOUBLE) AS salario,
                st.media,
                st.desvio,
                st.n AS n_cargo,
                (CAST(s.salario_liquido AS DOUBLE) - st.media) / st.desvio AS zscore
            FROM servidores s
            JOIN stats st ON s.cargo = st.cargo
            WHERE (CAST(s.salario_liquido AS DOUBLE) - st.media) / st.desvio > 3.5
            ORDER BY zscore DESC
            LIMIT 100
        """).fetchdf()
    except Exception as e:
        return []

    alertas = []
    for _, row in df.iterrows():
        alertas.append(AlertaExpanded(
            detector_id="SAL",
            severity="ALTO" if row["zscore"] > 5 else "MÉDIO",
            entity_type="servidor",
            entity_name=row.get("nome", ""),
            description=(
                f"Salário líquido R$ {row['salario']:,.2f} representa {row['zscore']:.1f}σ "
                f"acima da média do cargo '{row['cargo']}' "
                f"(média: R$ {row['media']:,.2f}, DP: R$ {row['desvio']:,.2f}, "
                f"n={int(row['n_cargo'])} servidores). "
                f"NOTA: adicionais legítimos (periculosidade, insalubridade, função gratificada) "
                f"devem ser verificados antes de qualquer conclusão."
            ),
            exposure_brl=max(0, float(row["salario"]) - float(row["media"])),
            base_legal=base_legal,
            classe_achado="HIPOTESE_INVESTIGATIVA",
            grau_probatorio="INDICIARIO",
            fonte_primaria="PORTAL_RIO_BRANCO",
            uso_externo="REVISAO_INTERNA",  # NUNCA externo sem checar adicionais
            inferencia_permitida=(
                "Há salário líquido estatisticamente discrepante para o cargo. "
                "O padrão pode indicar acúmulo ilegal, gratificação indevida ou erro de lançamento."
            ),
            limite_conclusao=(
                "NÃO usar externamente. Verificar obrigatoriamente: "
                "(a) adicionais legítimos previstos em lei (periculosidade, insalubridade, "
                "função gratificada, horas extras, adicional noturno, progressão); "
                "(b) se o servidor acumula cargo legalmente autorizado; "
                "(c) se há erro de lançamento na folha. "
                "O Z-score é triagem estatística, não prova de irregularidade."
            ),
        ))
    return alertas


def detect_nepotismo_triagem_interna(conn: duckdb.DuckDBPyConnection) -> list[AlertaExpanded]:
    """
    Triagem INTERNA de possível nepotismo por coincidência de sobrenome.

    ATENÇÃO: Esta função NÃO deve gerar alertas externos nem notícias de fato.
    É exclusivamente uma lista de casos para investigação humana posterior.

    Correções:
      - Exclui sobrenomes comuns no Acre (falso positivo garantido)
      - Exige sobrenome com pelo menos 6 caracteres
      - uso_externo = REVISAO_INTERNA (bloqueado para saída)
      - Título deixa claro que é triagem por sobrenome, não prova

    Base legal: Súmula Vinculante 13 STF exige PROVA de parentesco.
    Sobrenome idêntico NÃO é prova de parentesco.
    """
    base_legal = (
        "Súmula Vinculante n° 13 STF — nepotismo exige prova de parentesco civil ou biológico. "
        "Este detector produz apenas triagem por sobrenome, que NÃO constitui prova. "
        "Usar exclusivamente para direcionar investigação documental humana."
    )

    # Monta lista de sobrenomes a excluir como parâmetros SQL
    sobrenomes_excluir = "', '".join(sorted(SOBRENOMES_COMUNS_ACRE))

    try:
        df = conn.execute(f"""
            WITH servidor_sobrenomes AS (
                SELECT
                    split_part(servidor, ' - ', 2) AS nome_servidor,
                    secretaria,
                    cargo,
                    SPLIT_PART(TRIM(UPPER(split_part(servidor, ' - ', 2))), ' ', -1) AS sobrenome
                FROM servidores
                WHERE LENGTH(TRIM(split_part(servidor, ' - ', 2))) > 5
                  AND LENGTH(SPLIT_PART(TRIM(UPPER(split_part(servidor, ' - ', 2))), ' ', -1)) >= {MIN_SOBRENOME_LEN}
                  AND SPLIT_PART(TRIM(UPPER(split_part(servidor, ' - ', 2))), ' ', -1)
                      NOT IN ('{sobrenomes_excluir}')
            ),
            socios_sobrenomes AS (
                SELECT
                    s.socio_nome,
                    e.razao_social AS empresa,
                    e.cnpj,
                    SPLIT_PART(TRIM(UPPER(s.socio_nome)), ' ', -1) AS sobrenome
                FROM empresa_socios s
                JOIN empresas_cnpj e ON s.cnpj = e.cnpj
                WHERE LENGTH(TRIM(s.socio_nome)) > 5
                  AND LENGTH(SPLIT_PART(TRIM(UPPER(s.socio_nome)), ' ', -1)) >= {MIN_SOBRENOME_LEN}
                  AND SPLIT_PART(TRIM(UPPER(s.socio_nome)), ' ', -1)
                      NOT IN ('{sobrenomes_excluir}')
            )
            SELECT
                sv.nome_servidor,
                sv.secretaria,
                sv.cargo,
                sc.socio_nome,
                sc.empresa,
                sc.cnpj,
                sv.sobrenome AS sobrenome_coincidente
            FROM servidor_sobrenomes sv
            JOIN socios_sobrenomes sc ON sv.sobrenome = sc.sobrenome
            ORDER BY sv.sobrenome, sv.secretaria
            LIMIT 200
        """).fetchdf()
    except Exception as e:
        return []

    alertas = []
    for _, row in df.iterrows():
        alertas.append(AlertaExpanded(
            detector_id="NEP",
            severity="MÉDIO",  # Nunca CRÍTICO ou ALTO sem prova de parentesco
            entity_type="servidor",
            entity_name=row["nome_servidor"],
            description=(
                f"[TRIAGEM INTERNA — NÃO É PROVA] "
                f"Servidor '{row['nome_servidor']}' ({row['cargo']} / {row['secretaria']}) "
                f"compartilha o sobrenome '{row['sobrenome_coincidente']}' com o sócio "
                f"'{row['socio_nome']}' da empresa '{row['empresa']}' (CNPJ: {row['cnpj']}). "
                f"Sobrenome idêntico não é prova de parentesco. Requer investigação documental."
            ),
            exposure_brl=0,
            base_legal=base_legal,
            classe_achado="HIPOTESE_INVESTIGATIVA",
            grau_probatorio="EXPLORATORIO",
            fonte_primaria="PORTAL_RIO_BRANCO",
            uso_externo="REVISAO_INTERNA",  # BLOQUEADO — nunca externo
            inferencia_permitida=(
                "Há coincidência de sobrenome entre servidor público e sócio de empresa "
                "contratada pela mesma entidade. Pode indicar relação familiar, "
                "mas também pode ser coincidência casual."
            ),
            limite_conclusao=(
                "BLOQUEADO PARA USO EXTERNO. Nepotismo exige prova documental de parentesco "
                "(certidão de nascimento, casamento ou declaração equivalente). "
                "Este alerta é apenas direcionador para investigação, "
                "nunca fundamento de notícia de fato ou representação."
            ),
        ))
    return alertas


def detect_empresa_suspensa_corrigido(conn: duckdb.DuckDBPyConnection) -> list[AlertaExpanded]:
    """
    Detecta contratos com empresas sancionadas no CEIS/CNEP.

    Correção: verifica se a sanção existia ANTES do contrato
    (mesmo filtro temporal do patch 01 para rb_contratos).
    """
    base_legal = (
        "Lei 14.133/2021, art. 156, §1° — sanção de impedimento de licitar impede "
        "contratação enquanto vigente. Art. 7° da Lei 10.520/2002 (ainda aplicável para "
        "pregões anteriores à 14.133/21). CEIS/CNEP: consulta obrigatória antes de "
        "qualquer contratação (Decreto 11.129/2022, art. 7°)."
    )

    try:
        df = conn.execute("""
            SELECT
                o.empresa_nome,
                o.secretaria,
                SUM(CAST(o.valor_total AS DOUBLE)) AS total_contratos,
                c.motivo_sancao,
                c.data_inicio_sancao,
                c.data_fim_sancao,
                c.abrangencia
            FROM obras o
            JOIN cgu_ceis c ON (
                o.empresa_nome ILIKE '%' || c.nome_sancionado || '%'
                OR c.cnpj_cpf_sancionado = o.empresa_id::VARCHAR
            )
            WHERE
                -- Sanção deve preexistir ao contrato
                (c.data_inicio_sancao IS NULL OR c.data_inicio_sancao <= o.data_contrato)
            GROUP BY o.empresa_nome, o.secretaria, c.motivo_sancao,
                     c.data_inicio_sancao, c.data_fim_sancao, c.abrangencia
        """).fetchdf()
    except Exception as e:
        return []

    alertas = []
    for _, row in df.iterrows():
        alertas.append(AlertaExpanded(
            detector_id="CEIS",
            severity="CRÍTICO",
            entity_type="empresa",
            entity_name=row["empresa_nome"],
            description=(
                f"'{row['empresa_nome']}' consta no CEIS/CNEP com sanção "
                f"'{row['motivo_sancao']}' desde {row['data_inicio_sancao']} "
                f"(sanção preexistente ao contrato). "
                f"Abrangência: {row.get('abrangencia', 'não informada')}. "
                f"Valor contratado com '{row['secretaria']}': R$ {row['total_contratos']:,.2f}."
            ),
            exposure_brl=float(row["total_contratos"]),
            base_legal=base_legal,
            classe_achado="CRUZAMENTO_SANCIONATORIO",
            grau_probatorio="DOCUMENTAL_CORROBORADO",
            fonte_primaria="CGU",
            uso_externo="APTO_APURACAO",
            inferencia_permitida=(
                "Há empresa contratada com sanção ativa preexistente à data do contrato. "
                "A contratação pode ser irregular, dependendo da vigência e abrangência da sanção."
            ),
            limite_conclusao=(
                "Verificar: (a) se a sanção estava vigente na data de assinatura do contrato; "
                "(b) se a abrangência da sanção alcança a esfera do contratante; "
                "(c) se houve consulta formal ao CEIS antes da contratação; "
                "(d) se há justificativa no processo para a manutenção do contrato."
            ),
        ))
    return alertas


# ── REGISTRO ATUALIZADO ───────────────────────────────────────────────────────

DETECTORS_CORRIGIDOS: dict[str, Callable] = {
    "fracionamento":       detect_fracionamento_corrigido,
    "outlier_salarial":    detect_outlier_salarial_corrigido,
    "empresa_suspensa":    detect_empresa_suspensa_corrigido,
    # nepotismo: apenas triagem interna, não externalizado
    "nepotismo_triagem":   detect_nepotismo_triagem_interna,
}


def save_alerts_com_campos_probatorios(
    conn: duckdb.DuckDBPyConnection,
    alertas: list[AlertaExpanded],
) -> None:
    """Salva alertas com todos os campos probatórios."""
    if not alertas:
        return

    import hashlib, json
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            dossie_id           VARCHAR,
            detector_id         VARCHAR,
            severity            VARCHAR,
            entity_type         VARCHAR,
            entity_name         VARCHAR,
            description         VARCHAR,
            exposure_brl        DOUBLE,
            base_legal          VARCHAR,
            classe_achado       VARCHAR,
            grau_probatorio     VARCHAR,
            fonte_primaria      VARCHAR,
            uso_externo         VARCHAR,
            inferencia_permitida VARCHAR,
            limite_conclusao    VARCHAR,
            evidence            VARCHAR,
            detected_at         TIMESTAMP,
            status              VARCHAR DEFAULT 'DETECTADO'
        )
    """)

    try:
        existing = set(
            conn.execute("SELECT dossie_id FROM alerts").fetchdf()["dossie_id"]
        )
    except Exception:
        existing = set()

    rows = []
    for a in alertas:
        if a.dossie_id in existing:
            continue
        rows.append((
            a.dossie_id, a.detector_id, a.severity, a.entity_type,
            a.entity_name, a.description, a.exposure_brl, a.base_legal,
            a.classe_achado, a.grau_probatorio, a.fonte_primaria,
            a.uso_externo, a.inferencia_permitida, a.limite_conclusao,
            json.dumps(a.evidence, ensure_ascii=False),
            datetime.utcnow().isoformat(),
            "DETECTADO",
        ))

    if rows:
        conn.executemany(
            """INSERT INTO alerts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        print(f"{len(rows)} alertas novos salvos com campos probatórios.")
