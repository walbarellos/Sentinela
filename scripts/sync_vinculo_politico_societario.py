from __future__ import annotations

import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.insight_classification import (
    classify_insight_record,
    classify_probative_record,
    ensure_insight_classification_columns,
)


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

DDL_TARGETS = """
CREATE TABLE IF NOT EXISTS vinculo_politico_societario_targets (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    fontes_json JSON,
    orgaos_json JSON,
    n_contratos INTEGER,
    exposure_brl DOUBLE,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_MATCHES = """
CREATE TABLE IF NOT EXISTS vinculo_politico_societario_matches (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    socio_nome VARCHAR,
    socio_doc VARCHAR,
    qualificacao VARCHAR,
    data_entrada VARCHAR,
    match_kind VARCHAR,
    match_strength VARCHAR,
    source_table VARCHAR,
    matched_nome VARCHAR,
    matched_doc VARCHAR,
    matched_cargo VARCHAR,
    matched_orgao VARCHAR,
    matched_vinculo VARCHAR,
    matched_servidor VARCHAR,
    matched_partido VARCHAR,
    matched_ano_eleicao INTEGER,
    matched_receita VARCHAR,
    matched_valor_brl DOUBLE,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_RESUMO = """
CREATE TABLE IF NOT EXISTS vinculo_politico_societario_resumo (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    n_socios INTEGER,
    n_matches_objetivos INTEGER,
    n_socios_com_match INTEGER,
    n_pessoas_distintas INTEGER,
    n_bases_objetivas INTEGER,
    n_match_servidor INTEGER,
    n_match_candidato INTEGER,
    n_match_doacao INTEGER,
    exposure_brl DOUBLE,
    orgaos_json JSON,
    risco_json JSON,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def fix_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def normalize_text(value: object) -> str:
    text = fix_text(value).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_targets(con: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = con.execute(
        """
        WITH rb AS (
            SELECT
                regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') AS cnpj,
                max(fornecedor) AS razao_social,
                count(*) AS n_contratos,
                coalesce(sum(valor_brl), 0) AS exposure_brl,
                list(distinct 'rb_contratos') AS fontes,
                list(distinct secretaria) AS orgaos
            FROM rb_contratos
            WHERE length(regexp_replace(coalesce(cnpj,''), '\\D', '', 'g')) = 14
            GROUP BY 1
        ),
        ac AS (
            SELECT
                regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') AS cnpj,
                max(credor) AS razao_social,
                count(*) AS n_contratos,
                coalesce(sum(valor), 0) AS exposure_brl,
                list(distinct 'estado_ac_contratos') AS fontes,
                list(distinct coalesce(unidade_gestora, orgao)) AS orgaos
            FROM estado_ac_contratos
            WHERE length(regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g')) = 14
            GROUP BY 1
        ),
        unioned AS (
            SELECT * FROM rb
            UNION ALL
            SELECT * FROM ac
        ),
        socios_cnpj AS (
            SELECT DISTINCT regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') AS cnpj
            FROM empresa_socios
        )
        SELECT
            u.cnpj,
            coalesce(max(e.razao_social), max(u.razao_social)) AS razao_social,
            sum(u.n_contratos) AS n_contratos,
            sum(u.exposure_brl) AS exposure_brl
        FROM unioned u
        JOIN socios_cnpj s ON s.cnpj = u.cnpj
        LEFT JOIN empresas_cnpj e ON regexp_replace(coalesce(e.cnpj,''), '\\D', '', 'g') = u.cnpj
        GROUP BY 1
        ORDER BY exposure_brl DESC, u.cnpj
        """
    ).fetchall()

    targets: list[dict] = []
    for cnpj, razao_social, n_contratos, exposure_brl in rows:
        source_rows = con.execute(
            """
            SELECT 'rb_contratos' AS fonte, secretaria AS orgao, count(*) AS n, coalesce(sum(valor_brl), 0) AS valor
            FROM rb_contratos
            WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?
            GROUP BY 1, 2
            UNION ALL
            SELECT 'estado_ac_contratos' AS fonte, coalesce(unidade_gestora, orgao) AS orgao, count(*) AS n, coalesce(sum(valor), 0) AS valor
            FROM estado_ac_contratos
            WHERE regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') = ?
            GROUP BY 1, 2
            ORDER BY valor DESC, orgao
            """,
            [cnpj, cnpj],
        ).fetchall()
        targets.append(
            {
                "cnpj": cnpj,
                "razao_social": fix_text(razao_social),
                "n_contratos": int(n_contratos or 0),
                "exposure_brl": float(exposure_brl or 0),
                "fontes": sorted({fix_text(row[0]) for row in source_rows if row[0]}),
                "orgaos": [
                    {
                        "fonte": fix_text(row[0]),
                        "orgao": fix_text(row[1]),
                        "n_contratos": int(row[2] or 0),
                        "valor_brl": float(row[3] or 0),
                    }
                    for row in source_rows
                ],
            }
        )
    return targets


def load_socios(con: duckdb.DuckDBPyConnection, cnpj: str) -> list[dict]:
    rows = con.execute(
        """
        SELECT socio_nome, socio_cpf_cnpj, qualificacao, data_entrada
        FROM empresa_socios
        WHERE regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') = ?
        ORDER BY socio_nome
        """,
        [cnpj],
    ).fetchall()
    return [
        {
            "nome": fix_text(row[0]),
            "doc": clean_doc(row[1]),
            "qualificacao": fix_text(row[2]),
            "data_entrada": fix_text(row[3]),
        }
        for row in rows
    ]


def build_indexes(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, list[dict]]]:
    servidores: dict[str, list[dict]] = {}
    for nome, cargo, secretaria, vinculo in con.execute(
        "SELECT DISTINCT servidor_nome, cargo, secretaria, vinculo FROM servidores"
    ).fetchall():
        servidores.setdefault(normalize_text(nome), []).append(
            {
                "nome": fix_text(nome),
                "cargo": fix_text(cargo),
                "orgao": fix_text(secretaria),
                "vinculo": fix_text(vinculo),
            }
        )

    lotacao: dict[str, list[dict]] = {}
    for nome, cargo, lotacao_nome, secretaria, vinculo in con.execute(
        "SELECT DISTINCT nome, cargo, lotacao, secretaria, vinculo FROM rb_servidores_lotacao"
    ).fetchall():
        lotacao.setdefault(normalize_text(nome), []).append(
            {
                "nome": fix_text(nome),
                "cargo": fix_text(cargo),
                "orgao": fix_text(secretaria or lotacao_nome),
                "vinculo": fix_text(vinculo),
            }
        )

    candidatos_nome: dict[str, list[dict]] = {}
    cand_rows = con.execute(
        """
        SELECT DISTINCT nm_candidato, nm_urna_candidato, nm_social_candidato,
               nr_cpf_candidato, sg_uf, ano_eleicao, ds_cargo, sg_partido, ds_situacao_candidatura
        FROM tse_candidatos
        """
    ).fetchall()
    for nm_candidato, nm_urna, nm_social, cpf, sg_uf, ano_eleicao, ds_cargo, sg_partido, situacao in cand_rows:
        payload = {
            "nome": fix_text(nm_candidato),
            "cpf": clean_doc(cpf),
            "uf": fix_text(sg_uf),
            "ano_eleicao": int(ano_eleicao or 0) if str(ano_eleicao or "").isdigit() else None,
            "cargo": fix_text(ds_cargo),
            "partido": fix_text(sg_partido),
            "situacao": fix_text(situacao),
        }
        for raw_name in [nm_candidato, nm_urna, nm_social]:
            norm = normalize_text(raw_name)
            if norm and norm != "NULO":
                candidatos_nome.setdefault(norm, []).append(payload)

    candidatos_doc: dict[str, list[dict]] = {}
    for nm_candidato, nm_urna, nm_social, cpf, sg_uf, ano_eleicao, ds_cargo, sg_partido, situacao in cand_rows:
        cpf_clean = clean_doc(cpf)
        if len(cpf_clean) != 11:
            continue
        candidatos_doc.setdefault(cpf_clean, []).append(
            {
                "nome": fix_text(nm_candidato),
                "cpf": cpf_clean,
                "uf": fix_text(sg_uf),
                "ano_eleicao": int(ano_eleicao or 0) if str(ano_eleicao or "").isdigit() else None,
                "cargo": fix_text(ds_cargo),
                "partido": fix_text(sg_partido),
                "situacao": fix_text(situacao),
            }
        )

    doacoes_doc: dict[str, list[dict]] = {}
    for doc, nome, nome_rfb, ano, uf, receita, valor in con.execute(
        """
        SELECT DISTINCT nr_cpf_cnpj_doador_originario, nm_doador_originario,
               nm_doador_originario_rfb, aa_eleicao, sg_uf, ds_receita, vr_receita
        FROM tse_doacoes
        """
    ).fetchall():
        clean = clean_doc(doc)
        if len(clean) not in (11, 14):
            continue
        valor_num = None
        raw = str(valor or "").strip()
        if raw:
            raw = raw.replace(".", "").replace(",", ".")
            raw = re.sub(r"[^\d.]", "", raw)
            valor_num = float(raw) if raw else None
        doacoes_doc.setdefault(clean, []).append(
            {
                "nome": fix_text(nome_rfb or nome),
                "ano_eleicao": int(ano or 0) if str(ano or "").isdigit() else None,
                "uf": fix_text(uf),
                "receita": fix_text(receita),
                "valor_brl": valor_num,
            }
        )

    cross_nome: dict[str, list[dict]] = {}
    for candidato, servidor, cargo, salario in con.execute(
        "SELECT DISTINCT nm_candidato, servidor, cargo, salario_liquido FROM cross_candidato_servidor"
    ).fetchall():
        cross_nome.setdefault(normalize_text(candidato), []).append(
            {
                "nome": fix_text(candidato),
                "servidor": fix_text(servidor),
                "cargo": fix_text(cargo),
                "valor_brl": float(salario or 0),
            }
        )

    return {
        "servidores": servidores,
        "lotacao": lotacao,
        "candidatos_nome": candidatos_nome,
        "candidatos_doc": candidatos_doc,
        "doacoes_doc": doacoes_doc,
        "cross_nome": cross_nome,
    }


def build_matches(
    con: duckdb.DuckDBPyConnection, targets: list[dict]
) -> tuple[list[tuple], list[tuple], list[tuple], int]:
    indexes = build_indexes(con)
    target_rows: list[tuple] = []
    match_rows: list[tuple] = []
    resumo_rows: list[tuple] = []
    insight_count = 0

    con.execute(
        """
        DELETE FROM insight
        WHERE kind IN (
            'RISCO_VINCULO_SOCIETARIO_SERVIDOR_EXATO',
            'RISCO_VINCULO_SOCIETARIO_CANDIDATO_CPF_EXATO',
            'RISCO_VINCULO_SOCIETARIO_DOADOR_EXATO'
        )
        """
    )

    for target in targets:
        cnpj = target["cnpj"]
        razao = target["razao_social"]
        socios = load_socios(con, cnpj)
        target_rows.append(
            (
                row_hash("vps_target", cnpj),
                cnpj,
                razao,
                json.dumps(target["fontes"], ensure_ascii=False),
                json.dumps(target["orgaos"], ensure_ascii=False),
                target["n_contratos"],
                target["exposure_brl"],
            )
        )

        n_match_servidor = 0
        n_match_candidato = 0
        n_match_doacao = 0
        company_matches: list[dict] = []
        socios_com_match: set[str] = set()
        pessoas_distintas: set[str] = set()
        bases_objetivas: set[str] = set()

        for socio in socios:
            norm = normalize_text(socio["nome"])
            socio_doc = socio["doc"]
            socio_key = socio_doc if len(socio_doc) in (11, 14) else norm

            for hit in indexes["servidores"].get(norm, []):
                evidence = {
                    "target": {"cnpj": cnpj, "razao_social": razao, "exposure_brl": target["exposure_brl"]},
                    "socio": socio,
                    "match": hit,
                    "source_table": "servidores",
                }
                match_rows.append(
                    (
                        row_hash("vps_match", cnpj, socio["nome"], "SOCIO_SERVIDOR_EXATO", hit["nome"], hit["orgao"]),
                        cnpj,
                        razao,
                        socio["nome"],
                        socio_doc,
                        socio["qualificacao"],
                        socio["data_entrada"],
                        "SOCIO_SERVIDOR_EXATO",
                        "NOME_EXATO",
                        "servidores",
                        hit["nome"],
                        None,
                        hit["cargo"],
                        hit["orgao"],
                        hit["vinculo"],
                        None,
                        None,
                        None,
                        None,
                        None,
                        json.dumps(evidence, ensure_ascii=False),
                    )
                )
                n_match_servidor += 1
                company_matches.append(evidence)
                socios_com_match.add(socio_key)
                pessoas_distintas.add(normalize_text(hit["nome"]))
                bases_objetivas.add("servidores")

            for hit in indexes["lotacao"].get(norm, []):
                evidence = {
                    "target": {"cnpj": cnpj, "razao_social": razao, "exposure_brl": target["exposure_brl"]},
                    "socio": socio,
                    "match": hit,
                    "source_table": "rb_servidores_lotacao",
                }
                match_rows.append(
                    (
                        row_hash("vps_match", cnpj, socio["nome"], "SOCIO_SERVIDOR_LOTACAO_EXATO", hit["nome"], hit["orgao"]),
                        cnpj,
                        razao,
                        socio["nome"],
                        socio_doc,
                        socio["qualificacao"],
                        socio["data_entrada"],
                        "SOCIO_SERVIDOR_LOTACAO_EXATO",
                        "NOME_EXATO",
                        "rb_servidores_lotacao",
                        hit["nome"],
                        None,
                        hit["cargo"],
                        hit["orgao"],
                        hit["vinculo"],
                        None,
                        None,
                        None,
                        None,
                        None,
                        json.dumps(evidence, ensure_ascii=False),
                    )
                )
                n_match_servidor += 1
                company_matches.append(evidence)
                socios_com_match.add(socio_key)
                pessoas_distintas.add(normalize_text(hit["nome"]))
                bases_objetivas.add("rb_servidores_lotacao")

            for hit in indexes["cross_nome"].get(norm, []):
                evidence = {
                    "target": {"cnpj": cnpj, "razao_social": razao, "exposure_brl": target["exposure_brl"]},
                    "socio": socio,
                    "match": hit,
                    "source_table": "cross_candidato_servidor",
                }
                match_rows.append(
                    (
                        row_hash("vps_match", cnpj, socio["nome"], "SOCIO_CANDIDATO_SERVIDOR_EXATO", hit["nome"], hit["servidor"]),
                        cnpj,
                        razao,
                        socio["nome"],
                        socio_doc,
                        socio["qualificacao"],
                        socio["data_entrada"],
                        "SOCIO_CANDIDATO_SERVIDOR_EXATO",
                        "NOME_EXATO",
                        "cross_candidato_servidor",
                        hit["nome"],
                        None,
                        hit["cargo"],
                        None,
                        None,
                        hit["servidor"],
                        None,
                        None,
                        None,
                        hit["valor_brl"],
                        json.dumps(evidence, ensure_ascii=False),
                    )
                )
                n_match_candidato += 1
                company_matches.append(evidence)
                socios_com_match.add(socio_key)
                pessoas_distintas.add(normalize_text(hit["nome"]))
                bases_objetivas.add("cross_candidato_servidor")

            for hit in indexes["candidatos_doc"].get(socio_doc, []):
                evidence = {
                    "target": {"cnpj": cnpj, "razao_social": razao, "exposure_brl": target["exposure_brl"]},
                    "socio": socio,
                    "match": hit,
                    "source_table": "tse_candidatos",
                }
                match_rows.append(
                    (
                        row_hash("vps_match", cnpj, socio_doc, "SOCIO_CANDIDATO_CPF_EXATO", hit["nome"], hit["ano_eleicao"]),
                        cnpj,
                        razao,
                        socio["nome"],
                        socio_doc,
                        socio["qualificacao"],
                        socio["data_entrada"],
                        "SOCIO_CANDIDATO_CPF_EXATO",
                        "DOC_EXATO",
                        "tse_candidatos",
                        hit["nome"],
                        hit["cpf"],
                        hit["cargo"],
                        hit["uf"],
                        None,
                        None,
                        hit["partido"],
                        hit["ano_eleicao"],
                        None,
                        None,
                        json.dumps(evidence, ensure_ascii=False),
                    )
                )
                n_match_candidato += 1
                company_matches.append(evidence)
                socios_com_match.add(socio_key)
                pessoas_distintas.add(hit["cpf"] or normalize_text(hit["nome"]))
                bases_objetivas.add("tse_candidatos")

            for hit in indexes["doacoes_doc"].get(socio_doc, []):
                evidence = {
                    "target": {"cnpj": cnpj, "razao_social": razao, "exposure_brl": target["exposure_brl"]},
                    "socio": socio,
                    "match": hit,
                    "source_table": "tse_doacoes",
                }
                match_rows.append(
                    (
                        row_hash("vps_match", cnpj, socio_doc, "SOCIO_DOADOR_EXATO", hit["nome"], hit["ano_eleicao"]),
                        cnpj,
                        razao,
                        socio["nome"],
                        socio_doc,
                        socio["qualificacao"],
                        socio["data_entrada"],
                        "SOCIO_DOADOR_EXATO",
                        "DOC_EXATO",
                        "tse_doacoes",
                        hit["nome"],
                        socio_doc,
                        None,
                        hit["uf"],
                        None,
                        None,
                        None,
                        hit["ano_eleicao"],
                        hit["receita"],
                        hit["valor_brl"],
                        json.dumps(evidence, ensure_ascii=False),
                    )
                )
                n_match_doacao += 1
                company_matches.append(evidence)
                socios_com_match.add(socio_key)
                pessoas_distintas.add(socio_key)
                bases_objetivas.add("tse_doacoes")

        for hit in indexes["doacoes_doc"].get(cnpj, []):
            evidence = {
                "target": {"cnpj": cnpj, "razao_social": razao, "exposure_brl": target["exposure_brl"]},
                "match": hit,
                "source_table": "tse_doacoes",
            }
            match_rows.append(
                (
                    row_hash("vps_match", cnpj, cnpj, "EMPRESA_DOADORA_CNPJ_EXATO", hit["nome"], hit["ano_eleicao"]),
                    cnpj,
                    razao,
                    None,
                    None,
                    None,
                    None,
                    "EMPRESA_DOADORA_CNPJ_EXATO",
                    "DOC_EXATO",
                    "tse_doacoes",
                    hit["nome"],
                    cnpj,
                    None,
                    hit["uf"],
                    None,
                    None,
                    None,
                    hit["ano_eleicao"],
                    hit["receita"],
                    hit["valor_brl"],
                    json.dumps(evidence, ensure_ascii=False),
                )
            )
            n_match_doacao += 1
            company_matches.append(evidence)
            pessoas_distintas.add(f"empresa_doacao:{cnpj}")
            bases_objetivas.add("tse_doacoes")

        risco_flags = []
        if n_match_servidor > 0:
            risco_flags.append("sobreposicao_socio_servidor")
        if n_match_candidato > 0:
            risco_flags.append("sobreposicao_socio_candidatura")
        if n_match_doacao > 0:
            risco_flags.append("documento_exato_tse")

        resumo_rows.append(
            (
                row_hash("vps_resumo", cnpj),
                cnpj,
                razao,
                len(socios),
                len(company_matches),
                len(socios_com_match),
                len(pessoas_distintas),
                len(bases_objetivas),
                n_match_servidor,
                n_match_candidato,
                n_match_doacao,
                target["exposure_brl"],
                json.dumps(target["orgaos"], ensure_ascii=False),
                json.dumps(risco_flags, ensure_ascii=False),
                json.dumps(
                    {
                        "fontes": target["fontes"],
                        "orgaos": target["orgaos"],
                        "n_socios_com_match": len(socios_com_match),
                        "n_pessoas_distintas": len(pessoas_distintas),
                        "n_bases_objetivas": len(bases_objetivas),
                        "amostra_matches": company_matches[:5],
                    },
                    ensure_ascii=False,
                ),
            )
        )

        if not company_matches:
            continue

        title = f"Sobreposição societária exata com base pública para {razao}"
        description = (
            f"A empresa **{razao}** (`{cnpj}`) soma **R$ {target['exposure_brl']:,.2f}** "
            f"em **{target['n_contratos']}** contrato(s) público(s) mapeado(s) no Acre. "
            f"O QSA materializa **{len(socios)}** sócio(s), dos quais **{len(socios_com_match)}** aparecem "
            f"em **{len(bases_objetivas)}** base(s) pública(s) objetiva(s), cobrindo **{len(pessoas_distintas)}** pessoa(s) distinta(s). "
            f"No detalhe bruto, ha **{n_match_servidor}** ocorrência(s) em base local de servidor, "
            f"**{n_match_candidato}** ocorrência(s) em base eleitoral/candidato e **{n_match_doacao}** ocorrência(s) "
            "em base de doações eleitorais. "
            "Isto prova apenas sobreposição documental entre quadro societário e base pública; "
            "não prova impedimento legal, nepotismo ou favorecimento sem checagem de CPF completo, "
            "regime jurídico, contemporaneidade do vínculo societário e compatibilidade normativa."
        )
        sources = [
            "empresa_socios",
            "empresas_cnpj",
            *target["fontes"],
        ]
        if n_match_servidor:
            sources.extend(["servidores", "rb_servidores_lotacao"])
        if n_match_candidato:
            sources.extend(["tse_candidatos", "cross_candidato_servidor"])
        if n_match_doacao:
            sources.append("tse_doacoes")

        ins = {
            "id": f"vps:{cnpj}",
            "kind": "RISCO_VINCULO_SOCIETARIO_SERVIDOR_EXATO" if n_match_servidor else "RISCO_VINCULO_SOCIETARIO_DOADOR_EXATO" if n_match_doacao else "RISCO_VINCULO_SOCIETARIO_CANDIDATO_CPF_EXATO",
            "severity": "MEDIO" if n_match_servidor else "BAIXO",
            "confidence": 89 if n_match_servidor else 92 if (n_match_candidato or n_match_doacao) else 75,
            "exposure_brl": target["exposure_brl"],
            "title": title,
            "description_md": description,
            "pattern": "empresa -> socios -> base_publica_exata",
            "sources": json.dumps(sorted(set(sources)), ensure_ascii=False),
            "tags": json.dumps(["vinculo_societario", "triagem_conservadora", cnpj], ensure_ascii=False),
            "sample_n": len(company_matches),
            "unit_total": target["exposure_brl"],
            "valor_referencia": target["exposure_brl"],
            "ano_referencia": None,
            "fonte": "vinculo_politico_societario",
        }
        institutional = classify_insight_record(
            {
                "kind": ins["kind"],
                "title": ins["title"],
                "description_md": ins["description_md"],
                "pattern": ins["pattern"],
                "sources": json.loads(ins["sources"]),
                "tags": json.loads(ins["tags"]),
            }
        )
        probative = classify_probative_record(
            {
                "kind": ins["kind"],
                "title": ins["title"],
                "description_md": ins["description_md"],
                "pattern": ins["pattern"],
                "sources": json.loads(ins["sources"]),
                "tags": json.loads(ins["tags"]),
            }
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
                ins["id"],
                ins["kind"],
                ins["severity"],
                ins["confidence"],
                ins["exposure_brl"],
                ins["title"],
                ins["description_md"],
                ins["pattern"],
                ins["sources"],
                ins["tags"],
                ins["sample_n"],
                ins["unit_total"],
                datetime.now(),
                institutional["esfera"],
                institutional["ente"],
                institutional["orgao"],
                institutional["municipio"],
                institutional["uf"],
                institutional["area_tematica"],
                institutional["sus"],
                ins["valor_referencia"],
                ins["ano_referencia"],
                ins["fonte"],
                probative["classe_achado"],
                probative["grau_probatorio"],
                probative["fonte_primaria"],
                probative["uso_externo"],
                probative["inferencia_permitida"],
                probative["limite_conclusao"],
            ],
        )
        insight_count += 1

    return target_rows, match_rows, resumo_rows, insight_count


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    ensure_insight_classification_columns(con)
    con.execute("DROP VIEW IF EXISTS v_vinculo_politico_societario_matches")
    con.execute("DROP VIEW IF EXISTS v_vinculo_politico_societario_resumo")
    con.execute("DROP TABLE IF EXISTS vinculo_politico_societario_targets")
    con.execute("DROP TABLE IF EXISTS vinculo_politico_societario_matches")
    con.execute("DROP TABLE IF EXISTS vinculo_politico_societario_resumo")
    con.execute(DDL_TARGETS)
    con.execute(DDL_MATCHES)
    con.execute(DDL_RESUMO)

    targets = build_targets(con)
    target_rows, match_rows, resumo_rows, insight_count = build_matches(con, targets)

    con.execute("DELETE FROM vinculo_politico_societario_targets")
    if target_rows:
        con.executemany(
            """
            INSERT INTO vinculo_politico_societario_targets (
                row_id, cnpj, razao_social, fontes_json, orgaos_json, n_contratos, exposure_brl
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            target_rows,
        )

    con.execute("DELETE FROM vinculo_politico_societario_matches")
    if match_rows:
        con.executemany(
            """
            INSERT INTO vinculo_politico_societario_matches (
                row_id, cnpj, razao_social, socio_nome, socio_doc, qualificacao, data_entrada,
                match_kind, match_strength, source_table, matched_nome, matched_doc, matched_cargo,
                matched_orgao, matched_vinculo, matched_servidor, matched_partido,
                matched_ano_eleicao, matched_receita, matched_valor_brl, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            match_rows,
        )

    con.execute("DELETE FROM vinculo_politico_societario_resumo")
    if resumo_rows:
        con.executemany(
            """
            INSERT INTO vinculo_politico_societario_resumo (
                row_id, cnpj, razao_social, n_socios, n_matches_objetivos, n_socios_com_match,
                n_pessoas_distintas, n_bases_objetivas, n_match_servidor, n_match_candidato,
                n_match_doacao, exposure_brl, orgaos_json, risco_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            resumo_rows,
        )

    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_politico_societario_matches AS
        SELECT *
        FROM vinculo_politico_societario_matches
        ORDER BY cnpj, socio_nome, match_kind, source_table
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_politico_societario_resumo AS
        SELECT *
        FROM vinculo_politico_societario_resumo
        ORDER BY n_socios_com_match DESC, n_pessoas_distintas DESC, exposure_brl DESC, razao_social
        """
    )
    con.close()

    print(f"targets={len(target_rows)}")
    print(f"matches={len(match_rows)}")
    print(f"resumos={len(resumo_rows)}")
    print(f"insights={insight_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
