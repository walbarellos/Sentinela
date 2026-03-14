from __future__ import annotations

import json
import re
import unicodedata
from typing import Any, Mapping

import duckdb


CLASSIFICATION_COLUMNS = [
    ("esfera", "VARCHAR"),
    ("ente", "VARCHAR"),
    ("orgao", "VARCHAR"),
    ("municipio", "VARCHAR"),
    ("uf", "VARCHAR"),
    ("area_tematica", "VARCHAR"),
    ("sus", "BOOLEAN"),
]

PROBATIVE_COLUMNS = [
    ("classe_achado", "VARCHAR"),
    ("grau_probatorio", "VARCHAR"),
    ("fonte_primaria", "VARCHAR"),
    ("uso_externo", "VARCHAR"),
    ("inferencia_permitida", "VARCHAR"),
    ("limite_conclusao", "VARCHAR"),
]

MUNICIPAL_ENTE = "Prefeitura de Rio Branco"
STATE_ENTE = "Governo do Estado do Acre"
FEDERAL_ENTE = "Uniao"
RIO_BRANCO = "Rio Branco"
ACRE = "AC"

SUS_KEYWORDS = (
    "SUS",
    "SAUDE",
    "SEMSA",
    "SESACRE",
    "FUNDO MUNICIPAL DE SAUDE",
    "UNIDADE DE SAUDE",
    "UNIDADE BASICA",
    "UBS",
    "UPA",
    "HOSPITAL",
    "MATERNIDADE",
    "MEDICO",
    "ENFERME",
    "ODONTO",
    "FARMAC",
    "AGENTE COMUNITARIO",
    "CLINICO GERAL",
    "PEDIATRA",
    "GINECOLOGIA",
    "OBSTETR",
    "MEDICINA FAM",
)

ORGAO_PATTERNS = (
    ("SEMSA", ("SEMSA", "SECRETARIA MUNICIPAL DE SAUDE", "FUNDO MUNICIPAL DE SAUDE", "UBS", "UNIDADE BASICA")),
    ("SESACRE", ("SESACRE", "SECRETARIA DE ESTADO DE SAUDE")),
    ("SEOP", ("SEOP", "SECRETARIA MUNICIPAL DE OBRAS", "OBRAS PUBLICAS")),
    ("SEINFRA", ("SEINFRA", "INFRAESTRUTURA E MOBILIDADE")),
    ("SEMEL", ("SEMEL", "ESPORTE E LAZER")),
)


def ensure_insight_classification_columns(con: duckdb.DuckDBPyConnection) -> None:
    try:
        existing = {
            row[1]
            for row in con.execute("PRAGMA table_info('insight')").fetchall()
        }
    except duckdb.Error:
        return
    for column, sql_type in CLASSIFICATION_COLUMNS + PROBATIVE_COLUMNS:
        if column in existing:
            continue
        con.execute(f"ALTER TABLE insight ADD COLUMN {column} {sql_type}")


def classification_defaults() -> dict[str, Any]:
    return {
        "esfera": None,
        "ente": None,
        "orgao": None,
        "municipio": None,
        "uf": None,
        "area_tematica": None,
        "sus": False,
    }


def probative_defaults() -> dict[str, Any]:
    return {
        "classe_achado": None,
        "grau_probatorio": None,
        "fonte_primaria": None,
        "uso_externo": None,
        "inferencia_permitida": None,
        "limite_conclusao": None,
    }


def classify_insight_record(
    record: Mapping[str, Any],
    *,
    extra_text: str = "",
) -> dict[str, Any]:
    text = _compose_text(record, extra_text=extra_text)
    result = classification_defaults()

    municipal_hit = _contains_any(
        text,
        (
            "PREFEITURA DE RIO BRANCO",
            "RIO BRANCO",
            "PORTAL DA TRANSPARENCIA RIO BRANCO",
            "PORTAL DA TRANSPARENCIA DIARIAS",
            "PORTAL DA TRANSPARENCIA OBRAS",
            "SEMSA",
            "SEOP",
            "SEINFRA",
            "SEMEL",
        ),
    )
    state_hit = _contains_any(
        text,
        (
            "TRANSPARENCIA AC",
            "GOVERNO DO ESTADO",
            "ERARIO ESTADUAL",
            "ESTADO DO ACRE",
            "GOVERNO_ESTADO_ACRE",
            "SESACRE",
        ),
    )
    federal_hit = _contains_any(
        text,
        (
            "CEIS",
            "CNEP",
            "CGU",
            "PORTAL DA TRANSPARENCIA FEDERAL",
            "SERVIDORES FEDERAIS",
            "TCU",
            "CNJ",
            "UNIAO",
        ),
    )

    if municipal_hit:
        result["esfera"] = "municipal"
        result["ente"] = MUNICIPAL_ENTE
        result["municipio"] = RIO_BRANCO
        result["uf"] = ACRE

    if state_hit and not municipal_hit:
        result["esfera"] = "estadual"
        result["ente"] = STATE_ENTE
        result["uf"] = ACRE

    if federal_hit and not municipal_hit and not state_hit:
        result["esfera"] = "federal"
        result["ente"] = FEDERAL_ENTE
        result["uf"] = "BR"

    for orgao, patterns in ORGAO_PATTERNS:
        if _contains_any(text, patterns):
            result["orgao"] = orgao
            break

    sus = _contains_any(text, SUS_KEYWORDS)
    if result["orgao"] in {"SEMSA", "SESACRE"}:
        sus = True
    result["sus"] = sus

    if sus:
        result["area_tematica"] = "saude"
        if result["esfera"] == "municipal" and not result["orgao"]:
            result["orgao"] = "SEMSA"
        if result["esfera"] == "estadual" and not result["orgao"]:
            result["orgao"] = "SESACRE"
    elif result["orgao"] in {"SEOP", "SEINFRA"}:
        result["area_tematica"] = "infraestrutura"
    elif result["esfera"] == "municipal":
        result["area_tematica"] = "gestao_municipal"
    elif result["esfera"] == "estadual":
        result["area_tematica"] = "gestao_estadual"
    elif result["esfera"] == "federal":
        result["area_tematica"] = "controle_externo"

    if result["ente"] == MUNICIPAL_ENTE and not result["orgao"]:
        result["orgao"] = MUNICIPAL_ENTE
    if result["ente"] == STATE_ENTE and not result["orgao"]:
        result["orgao"] = STATE_ENTE
    if result["ente"] == FEDERAL_ENTE and not result["orgao"]:
        result["orgao"] = FEDERAL_ENTE
    if result["esfera"] == "municipal" and result["orgao"] == "SESACRE":
        result["orgao"] = "SEMSA"
    if result["esfera"] == "estadual" and result["orgao"] == "SEMSA":
        result["orgao"] = "SESACRE"

    return result


def classify_probative_record(
    record: Mapping[str, Any],
    *,
    extra_text: str = "",
) -> dict[str, Any]:
    text = f" {_compose_text(record, extra_text=extra_text)} "
    kind = f" {_normalize(_json_to_text(record.get('kind')))} "
    source_groups = _detect_source_groups(record, text)
    fonte_primaria = _detect_primary_source(text, source_groups)
    primary_doc_hit = _has_primary_document_hit(text, fonte_primaria)
    corroborated_hit = len(source_groups) >= 2

    if _contains_any(kind, ("INCONSIST", "DIVERG", "VENCEDOR DIVERGENTE", "OBJETO DIVERGENTE")):
        classe = "DIVERGENCIA_DOCUMENTAL"
        grau = "DOCUMENTAL_CORROBORADO" if primary_doc_hit and corroborated_hit else "DOCUMENTAL_PRIMARIO" if primary_doc_hit else "INDICIARIO"
        uso = "APTO_REPRESENTACAO" if grau == "DOCUMENTAL_CORROBORADO" else "APTO_APURACAO"
        inferencia = "Ha divergencia documental objetiva entre ato formal, portal publico ou publicacoes oficiais."
        limite = "Nao prova dolo, fraude penal ou direcionamento por si so; exige analise juridica e contexto administrativo."
    elif _contains_any(kind, ("SANCAO", "CEIS", "CNEP", "CEPIM", "CEAF")) or _contains_any(text, (" CEIS ", " CNEP ", " CEPIM ", " CEAF ", " SANCAO ")):
        classe = "CRUZAMENTO_SANCIONATORIO"
        grau = "DOCUMENTAL_CORROBORADO" if corroborated_hit else "DOCUMENTAL_PRIMARIO"
        uso = "APTO_APURACAO"
        inferencia = "Ha fornecedor, entidade ou CNPJ com registro sancionatorio ou impeditivo a ser confrontado com a contratacao."
        limite = "Nao basta para afirmar irregularidade sem verificar vigencia, alcance juridico, fundamento da sancao e aderencia ao caso concreto."
    elif _contains_any(kind, ("CONTRATO EXATO", "ORIGEM ADESAO", "ORIGEM FORMAL", "VINCULO EXATO")) or _contains_any(text, (" CONTRATO EXATO ", " TERMO DE ADESAO ", " EXTRATO DO CONTRATO ", " ORIGEM FORMAL ", " HOMOLOGACAO ", " PORTARIA ")):
        classe = "FATO_DOCUMENTAL"
        grau = "DOCUMENTAL_CORROBORADO" if corroborated_hit else "DOCUMENTAL_PRIMARIO"
        uso = "APTO_APURACAO"
        inferencia = "Ha vinculo formal documentado entre contrato, processo, licitacao, adesao ou publicacao oficial."
        limite = "O achado nao comprova favorecimento, superfaturamento, nepotismo ou fraude por si so."
    elif _contains_any(kind, ("CADEIA", "RASTRO", "SEM ID LICITACAO", "COMPATIVEL", "PORTAL CIAP")):
        classe = "RASTRO_CONTRATUAL"
        grau = "INDICIARIO" if primary_doc_hit or corroborated_hit else "EXPLORATORIO"
        uso = "APTO_APURACAO" if grau == "INDICIARIO" else "REVISAO_INTERNA"
        inferencia = "Ha rastro contratual ou compatibilidade material relevante que orienta apuracao dirigida."
        limite = "O rastro nao fecha sozinho a origem juridica nem prova ilicitude; ainda pode haver explicacao administrativa valida."
    elif _contains_any(kind, ("QSA", "REDE", "LEAD", "MATCH", "EXPOSICAO", "PENDENCIA")):
        classe = "HIPOTESE_INVESTIGATIVA"
        grau = "INDICIARIO" if corroborated_hit else "EXPLORATORIO"
        uso = "REVISAO_INTERNA"
        inferencia = "Ha pista societaria, relacional ou contratual que merece checagem manual."
        limite = "Nao pode ser usada isoladamente para afirmar nepotismo, vinculacao politica, fraude ou beneficio indevido."
    else:
        classe = "FATO_DOCUMENTAL" if primary_doc_hit else "HIPOTESE_INVESTIGATIVA"
        grau = "DOCUMENTAL_PRIMARIO" if primary_doc_hit else "EXPLORATORIO"
        uso = "APTO_APURACAO" if primary_doc_hit else "REVISAO_INTERNA"
        inferencia = "Ha fato objetivo documentado." if primary_doc_hit else "Ha achado preliminar para triagem."
        limite = "Sem corroboracao adicional, o sistema nao deve elevar este achado para conclusao acusatoria."

    return {
        "classe_achado": classe,
        "grau_probatorio": grau,
        "fonte_primaria": fonte_primaria,
        "uso_externo": uso,
        "inferencia_permitida": inferencia,
        "limite_conclusao": limite,
    }


def build_insight_extra_text(
    con: duckdb.DuckDBPyConnection,
    insight_ids: list[str],
) -> dict[str, str]:
    if not insight_ids:
        return {}

    placeholders = ",".join(["?"] * len(insight_ids))
    extra: dict[str, list[str]] = {insight_id: [] for insight_id in insight_ids}

    evidence_rows = con.execute(
        f"""
        SELECT el.insight_id, e.source, e.excerpt
        FROM evidence_link el
        JOIN evidence e ON e.id = el.evidence_id
        WHERE el.insight_id IN ({placeholders})
        """,
        insight_ids,
    ).fetchall()
    for insight_id, source, excerpt in evidence_rows:
        extra.setdefault(insight_id, []).append(_json_to_text(source))
        extra[insight_id].append(_json_to_text(excerpt))

    event_rows = con.execute(
        f"""
        SELECT il.insight_id, ev.type, ev.title, ev.attributes
        FROM insight_link il
        JOIN event ev ON ev.id = il.event_id
        WHERE il.insight_id IN ({placeholders})
        """,
        insight_ids,
    ).fetchall()
    for insight_id, event_type, title, attributes in event_rows:
        extra.setdefault(insight_id, []).append(_json_to_text(event_type))
        extra[insight_id].append(_json_to_text(title))
        extra[insight_id].append(_json_to_text(attributes))

    return {
        insight_id: " ".join(part for part in parts if part).strip()
        for insight_id, parts in extra.items()
    }


def has_probative_classification(record: Mapping[str, Any]) -> bool:
    return bool(record.get("classe_achado") and record.get("grau_probatorio") and record.get("uso_externo"))


def _compose_text(record: Mapping[str, Any], *, extra_text: str = "") -> str:
    chunks = [
        _json_to_text(record.get("title")),
        _json_to_text(record.get("description_md")),
        _json_to_text(record.get("pattern")),
        _json_to_text(record.get("sources")),
        _json_to_text(record.get("tags")),
        _json_to_text(record.get("ente")),
        _json_to_text(record.get("orgao")),
        _json_to_text(record.get("municipio")),
        extra_text,
    ]
    return _normalize(" ".join(part for part in chunks if part))


def _json_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        stripped = _fix_mojibake(value).strip()
        if not stripped:
            return ""
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return _json_to_text(json.loads(stripped))
            except json.JSONDecodeError:
                return stripped
        return stripped
    if isinstance(value, Mapping):
        return " ".join(_json_to_text(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_json_to_text(v) for v in value)
    return str(value)


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern in text for pattern in patterns)


def _has_primary_document_hit(text: str, fonte_primaria: str | None) -> bool:
    if fonte_primaria in {"DOE_AC", "DJE_TJAC", "PNCP", "CGU", "TSE"}:
        return True
    return _contains_any(
        text,
        (
            " DIARIO OFICIAL ",
            " DOE ",
            " DJE ",
            " TJAC ",
            " EXTRATO DO CONTRATO ",
            " TERMO DE ADESAO ",
            " PORTARIA ",
            " HOMOLOGACAO ",
            " EDITAL ",
            " RETIFICACAO ",
            " PNCP ",
            " CEIS ",
            " CNEP ",
        ),
    )


def _detect_primary_source(text: str, source_groups: set[str]) -> str | None:
    priority = [
        "DJE_TJAC",
        "DOE_AC",
        "CGU",
        "PNCP",
        "TSE",
        "PORTAL_RIO_BRANCO",
        "PORTAL_ACRE",
        "CNPJ_QSA",
        "DATASUS_CNES",
    ]
    for group in priority:
        if group in source_groups:
            return group
    if " TJAC " in text or " DJE " in text:
        return "DJE_TJAC"
    if " DIARIO OFICIAL " in text or " DOE " in text or " DIARIO.AC.GOV.BR " in text:
        return "DOE_AC"
    if " CEIS " in text or " CNEP " in text or " CGU " in text:
        return "CGU"
    if " PNCP " in text:
        return "PNCP"
    if " RIO BRANCO " in text or " CPL " in text:
        return "PORTAL_RIO_BRANCO"
    if " ESTADO DO ACRE " in text or " TRANSPARENCIA AC " in text:
        return "PORTAL_ACRE"
    return None


def _detect_source_groups(record: Mapping[str, Any], text: str) -> set[str]:
    groups: set[str] = set()
    source_text = _normalize(_json_to_text(record.get("sources")))
    tag_text = _normalize(_json_to_text(record.get("tags")))
    combined = f"{text} {source_text} {tag_text}"

    if _contains_any(combined, (" TJAC ", " DJE ", " TJAC JUS BR ")):
        groups.add("DJE_TJAC")
    if _contains_any(combined, (" DIARIO OFICIAL ", " DOE ", " DIARIO AC GOV BR ", " CPL_PUBLICACAO ", " PUBLICACAO CPL ")):
        groups.add("DOE_AC")
    if _contains_any(combined, (" PORTAL TRANSPARENCIA RIO BRANCO ", " RIO BRANCO ", " CPL ", " RB_CONTRATO ", " RB_SUS ")):
        groups.add("PORTAL_RIO_BRANCO")
    if _contains_any(combined, (" PORTAL TRANSPARENCIA ACRE ", " PORTAL_TRANSPARENCIA_ACRE ", " ESTADO_AC_", " GOVERNO DO ESTADO DO ACRE ", " TRANSPARENCIA AC ")):
        groups.add("PORTAL_ACRE")
    if _contains_any(combined, (" CEIS ", " CNEP ", " CEPIM ", " CEAF ", " CGU ")):
        groups.add("CGU")
    if _contains_any(combined, (" PNCP ", " COMPRASNET ")):
        groups.add("PNCP")
    if _contains_any(combined, (" QSA ", " CNPJ ", " BRASILAPI ", " RECEITA ")):
        groups.add("CNPJ_QSA")
    if _contains_any(combined, (" TSE ", " CANDIDATURA ", " DOACAO ELEITORAL ")):
        groups.add("TSE")
    if _contains_any(combined, (" CNES ", " DATASUS ", " SIH ", " SIM ", " SINAN ")):
        groups.add("DATASUS_CNES")
    return groups


def _normalize(text: str) -> str:
    text = _fix_mojibake(text)
    text = text.upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fix_mojibake(text: str) -> str:
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError, AttributeError):
        return text
