from __future__ import annotations

import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.insight_classification import (  # noqa: E402
    classify_insight_record,
    classify_probative_record,
    ensure_insight_classification_columns,
)


DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

CNES_MUNICIPIO_URLS = {
    ("AC", "RIO BRANCO"): "https://cnes2.datasus.gov.br/Lista_Es_Municipio.asp?NomeEstado=ACRE&VCodMunicipio=120040&VEstado=12",
}

DDL = """
CREATE TABLE IF NOT EXISTS vinculo_societario_saude_followup (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    razao_social VARCHAR,
    cnae_principal VARCHAR,
    cnae_descricao VARCHAR,
    contrato_ano INTEGER,
    contrato_numero VARCHAR,
    contrato_orgao VARCHAR,
    contrato_valor_brl DOUBLE,
    contrato_objeto VARCHAR,
    cnes_ficha_url VARCHAR,
    cnes_code VARCHAR,
    cnes_nome VARCHAR,
    cnes_nome_empresarial VARCHAR,
    cnes_cnpj VARCHAR,
    cnes_gestao VARCHAR,
    cnes_tipo_estabelecimento VARCHAR,
    cnes_subtipo_estabelecimento VARCHAR,
    cnes_dependencia VARCHAR,
    cnes_cadastrado_em VARCHAR,
    cnes_ultima_atualizacao VARCHAR,
    cnes_atualizacao_local VARCHAR,
    cnes_endereco VARCHAR,
    cnes_bairro VARCHAR,
    cnes_cep VARCHAR,
    cnes_municipio VARCHAR,
    cnes_uf VARCHAR,
    cnes_telefone VARCHAR,
    cnes_horario_json JSON,
    cnes_turno_atendimento VARCHAR,
    cnes_instalacoes_json JSON,
    cnes_servicos_apoio_json JSON,
    cnes_servicos_classificacao_json JSON,
    cnes_profissionais_match_json JSON,
    n_cnes_profissionais_match INTEGER,
    socios_publicos_json JSON,
    overlap_flags_json JSON,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

EXTRA_COLUMNS = [
    ("cnes_turno_atendimento", "VARCHAR"),
    ("cnes_instalacoes_json", "JSON"),
    ("cnes_servicos_apoio_json", "JSON"),
    ("cnes_servicos_classificacao_json", "JSON"),
    ("cnes_profissionais_match_json", "JSON"),
    ("n_cnes_profissionais_match", "INTEGER"),
]


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


def fetch(url: str) -> str:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def ensure_followup_columns(con: duckdb.DuckDBPyConnection) -> None:
    existing = {row[1] for row in con.execute("PRAGMA table_info('vinculo_societario_saude_followup')").fetchall()}
    for column, sql_type in EXTRA_COLUMNS:
        if column not in existing:
            con.execute(f"ALTER TABLE vinculo_societario_saude_followup ADD COLUMN {column} {sql_type}")


def parse_label_tables(soup: BeautifulSoup) -> dict[str, str]:
    data: dict[str, str] = {}
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [fix_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)
        for idx in range(0, len(rows) - 1):
            headers = rows[idx]
            values = rows[idx + 1]
            if not any(header.endswith(":") for header in headers):
                continue
            if len(values) < len(headers):
                continue
            for pos, header in enumerate(headers):
                key = normalize_text(header.rstrip(":"))
                value = values[pos] if pos < len(values) else ""
                if key and value:
                    data[key] = value
            if (
                normalize_text(" ".join(headers)) == "COMPLEMENTO BAIRRO CEP MUNICIPIO"
                and len(values) == len(headers) + 1
                and values[-1]
            ):
                data["UF"] = values[-1]
    return data


def parse_cnes_schedule(soup: BeautifulSoup) -> list[dict[str, str]]:
    schedules: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        rows = [
            [fix_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            for tr in table.find_all("tr")
        ]
        rows = [row for row in rows if row]
        if not rows:
            continue
        head = " ".join(rows[0]).upper()
        if "DIA SEMANA" not in head or "HOR" not in head:
            continue
        for row in rows[1:]:
            if len(row) >= 2:
                schedules.append({"dia_semana": row[0], "horario": row[1]})
    return schedules


def parse_cnes_dates(text: str) -> dict[str, str]:
    cleaned = " ".join(text.split())
    patterns = {
        "cnes_cadastrado_em": r"CADASTRADO NO CNES EM:\s*([0-9/]+)",
        "cnes_ultima_atualizacao": r"ULTIMA ATUALIZAÇÃO EM:\s*([0-9/]+)",
        "cnes_atualizacao_local": r"DATA DE ATUALIZAÇÃO LOCAL:\s*([0-9/]+)",
    }
    out: dict[str, str] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, cleaned, re.I)
        if match:
            out[key] = match.group(1)
    return out


def find_cnes_entry(cnpj: str, municipio: str, uf: str) -> dict[str, str] | None:
    list_url = CNES_MUNICIPIO_URLS.get((normalize_text(uf), normalize_text(municipio)))
    if not list_url:
        return None
    soup = BeautifulSoup(fetch(list_url), "html.parser")
    for anchor in soup.find_all("a", href=re.compile(r"Exibe_Ficha_Estabelecimento\.asp", re.I)):
        row = anchor.find_parent("tr")
        if not row:
            continue
        cells = row.find_all("td", recursive=False) or row.find_all("td")
        texts = [fix_text(cell.get_text(" ", strip=True)) for cell in cells]
        if len(texts) < 4:
            continue
        row_cnpj = clean_doc(texts[2])
        if row_cnpj != cnpj:
            continue
        href = anchor.get("href") or ""
        ficha_url = requests.compat.urljoin(list_url, href)
        return {
            "cnes_nome": texts[0],
            "cnes_code": clean_doc(texts[1]),
            "cnes_cnpj": row_cnpj,
            "cnes_gestao_sigla": texts[3],
            "cnes_ficha_url": ficha_url,
            "cnes_lista_url": list_url,
        }
    return None


def fetch_cnes_details(entry: dict[str, str]) -> dict[str, Any]:
    html = fetch(entry["cnes_ficha_url"])
    soup = BeautifulSoup(html, "html.parser")
    text = " ".join(soup.stripped_strings)
    labels = parse_label_tables(soup)
    schedule = parse_cnes_schedule(soup)
    details: dict[str, Any] = {
        **entry,
        **parse_cnes_dates(text),
        "cnes_nome_empresarial": labels.get("NOME EMPRESARIAL", ""),
        "cnes_personalidade": labels.get("PERSONALIDADE", ""),
        "cnes_endereco": labels.get("LOGRADOURO", ""),
        "cnes_numero": labels.get("NUMERO", ""),
        "cnes_telefone": labels.get("TELEFONE", ""),
        "cnes_complemento": labels.get("COMPLEMENTO", ""),
        "cnes_bairro": labels.get("BAIRRO", ""),
        "cnes_cep": labels.get("CEP", ""),
        "cnes_municipio": labels.get("MUNICÍPIO", "") or labels.get("MUNICIPIO", ""),
        "cnes_uf": labels.get("UF", ""),
        "cnes_tipo_estabelecimento": labels.get("TIPO ESTABELECIMENTO", ""),
        "cnes_subtipo_estabelecimento": labels.get("SUB TIPO ESTABELECIMENTO", ""),
        "cnes_dependencia": labels.get("DEPENDÊNCIA", "") or labels.get("DEPENDENCIA", ""),
        "cnes_gestao": labels.get("GESTÃO", "") or labels.get("GESTAO", ""),
        "cnes_horario_json": schedule,
    }
    details["cnes_endereco"] = " ".join(
        part for part in [details["cnes_endereco"], details["cnes_numero"]] if part
    ).strip()
    return details


def fetch_cnes_info(entry: dict[str, str]) -> dict[str, Any]:
    match = re.search(r"VCo_Unidade=(\d+)", entry["cnes_ficha_url"])
    if not match:
        return {}
    url = f"https://cnes2.datasus.gov.br/Mod_Conj_Informacoes.asp?VCo_Unidade={match.group(1)}"
    soup = BeautifulSoup(fetch(url), "html.parser")
    text = " ".join(soup.stripped_strings)

    instalacoes: list[dict[str, Any]] = []
    servicos_apoio: list[dict[str, Any]] = []
    servicos_classificacao: list[dict[str, Any]] = []
    turno_atendimento = ""

    for table in soup.find_all("table"):
        rows = [
            [fix_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            for tr in table.find_all("tr")
        ]
        rows = [row for row in rows if row]
        if not rows:
            continue
        head = " ".join(rows[0]).upper()
        if "INSTALAÇÃO" in head and "LEITOS/EQUIPAMENTOS" in head:
            for row in rows[1:]:
                if len(row) >= 3:
                    instalacoes.append(
                        {
                            "instalacao": row[0],
                            "qtde_consultorio": row[1],
                            "leitos_equipamentos": row[2],
                        }
                    )
        elif "SERVIÇO" in head and "CARACTERÍSTICA" in head and len(rows[0]) == 2:
            for row in rows[1:]:
                if len(row) >= 2:
                    servicos_apoio.append(
                        {
                            "servico": row[0],
                            "caracteristica": row[1],
                        }
                    )
        elif "CÓDIGO" in head and "CLASSIFICAÇÃO" in head:
            for row in rows[1:]:
                if len(row) >= 5:
                    servicos_classificacao.append(
                        {
                            "codigo": row[0],
                            "servico": row[1],
                            "classificacao": row[2],
                            "terceiro": row[3],
                            "cnes": row[4],
                        }
                    )
        else:
            for idx, row in enumerate(rows):
                if (
                    len(row) >= 3
                    and normalize_text(" ".join(row)) == "NIVEL DE HIERARQUIA TIPO DE UNIDADE TURNO DE ATENDIMENTO"
                    and idx + 1 < len(rows)
                    and len(rows[idx + 1]) >= 3
                ):
                    turno_atendimento = fix_text(rows[idx + 1][2])
                if len(row) >= 5 and re.match(r"^\d{3}\s*-\s*\d{3}$", row[0]):
                    servicos_classificacao.append(
                        {
                            "codigo": row[0],
                            "servico": row[1],
                            "classificacao": row[2],
                            "terceiro": row[3],
                            "cnes": row[4],
                        }
                    )

    def _dedupe_dict_rows(items: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
        seen: set[tuple[Any, ...]] = set()
        out: list[dict[str, Any]] = []
        for item in items:
            marker = tuple(item.get(key) for key in keys)
            if marker in seen:
                continue
            seen.add(marker)
            out.append(item)
        return out

    instalacoes = _dedupe_dict_rows(instalacoes, ["instalacao", "qtde_consultorio", "leitos_equipamentos"])
    servicos_apoio = _dedupe_dict_rows(servicos_apoio, ["servico", "caracteristica"])
    servicos_classificacao = _dedupe_dict_rows(servicos_classificacao, ["codigo", "servico", "classificacao", "terceiro", "cnes"])

    return {
        "cnes_info_url": url,
        "cnes_turno_atendimento": turno_atendimento,
        "cnes_instalacoes_json": instalacoes,
        "cnes_servicos_apoio_json": servicos_apoio,
        "cnes_servicos_classificacao_json": servicos_classificacao,
    }


def fetch_cnes_profissionais(entry: dict[str, str], socio_names: list[str]) -> list[dict[str, Any]]:
    match = re.search(r"VCo_Unidade=(\d+)", entry["cnes_ficha_url"])
    if not match:
        return []
    url = f"https://cnes2.datasus.gov.br/Mod_Profissional.asp?VCo_Unidade={match.group(1)}"
    soup = BeautifulSoup(fetch(url), "html.parser")
    target_names = {normalize_text(name): fix_text(name) for name in socio_names if normalize_text(name)}
    matches: list[dict[str, Any]] = []

    for table in soup.find_all("table"):
        header_idx = None
        rows = [
            [fix_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            for tr in table.find_all("tr")
        ]
        rows = [row for row in rows if row]
        for idx, row in enumerate(rows):
            if row and row[0] == "Nome" and "CBO" in row:
                header_idx = idx
                break
        if header_idx is None:
            continue
        for row in rows[header_idx + 1 :]:
            if len(row) < 17:
                continue
            norm_name = normalize_text(row[0])
            if norm_name not in target_names:
                continue
            matches.append(
                {
                    "nome": row[0],
                    "dt_entrada": row[1],
                    "cns": row[2],
                    "cns_master": row[3],
                    "dt_atribuicao": row[4],
                    "cbo": row[5],
                    "ch_outros": row[6],
                    "ch_amb": row[7],
                    "ch_hosp": row[8],
                    "total": row[9],
                    "sus": row[10],
                    "vinculacao": row[11],
                    "tipo": row[12],
                    "subtipo": row[13],
                    "comp_desativacao": row[14],
                    "situacao": row[15],
                    "portaria_134": row[16],
                    "cnes_profissionais_url": url,
                }
            )
        break
    return matches


def load_positive_cases(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
            r.cnpj,
            r.razao_social,
            r.n_socios_com_match,
            r.n_pessoas_distintas,
            e.cnae_principal,
            q.cnae_descricao,
            e.municipio,
            e.uf
        FROM vinculo_politico_societario_resumo r
        LEFT JOIN empresas_cnpj e
          ON regexp_replace(coalesce(e.cnpj,''), '\\D', '', 'g') = r.cnpj
        LEFT JOIN (
            SELECT regexp_replace(coalesce(cnpj,''), '\\D', '', 'g') AS cnpj, max(cnae_descricao) AS cnae_descricao
            FROM estado_ac_fornecedor_qsa
            GROUP BY 1
        ) q
          ON q.cnpj = r.cnpj
        WHERE r.n_matches_objetivos > 0
        ORDER BY r.exposure_brl DESC, r.cnpj
        """
    ).fetchall()
    cases: list[dict[str, Any]] = []
    for row in rows:
        cases.append(
            {
                "cnpj": row[0],
                "razao_social": fix_text(row[1]),
                "n_socios_com_match": int(row[2] or 0),
                "n_pessoas_distintas": int(row[3] or 0),
                "cnae_principal": fix_text(row[4]),
                "cnae_descricao": fix_text(row[5]),
                "municipio": fix_text(row[6]),
                "uf": fix_text(row[7]),
            }
        )
    return cases


def load_contracts(con: duckdb.DuckDBPyConnection, cnpj: str) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT ano, numero, coalesce(unidade_gestora, orgao) AS orgao, valor, objeto
        FROM estado_ac_contratos
        WHERE regexp_replace(coalesce(cnpjcpf,''), '\\D', '', 'g') = ?
        ORDER BY valor DESC, ano DESC, numero
        """,
        [cnpj],
    ).fetchall()
    return [
        {
            "ano": int(row[0]) if row[0] is not None else None,
            "numero": fix_text(row[1]),
            "orgao": fix_text(row[2]),
            "valor_brl": float(row[3] or 0),
            "objeto": fix_text(row[4]),
        }
        for row in rows
    ]


def load_public_socios(con: duckdb.DuckDBPyConnection, cnpj: str) -> list[dict[str, Any]]:
    socios = con.execute(
        """
        SELECT DISTINCT socio_nome
        FROM vinculo_politico_societario_matches
        WHERE cnpj = ?
        ORDER BY socio_nome
        """,
        [cnpj],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for (socio_nome,) in socios:
        mass_rows = con.execute(
            """
            SELECT
                split_part(servidor, '-', 1) AS matricula,
                max(admissao) AS admissao,
                max(cargo) AS cargo,
                max(vinculo) AS vinculo,
                max(ch) AS ch,
                min(salario_liquido) AS salario_liquido_min,
                max(salario_liquido) AS salario_liquido_max,
                count(*) AS n_linhas_folha
            FROM rb_servidores_mass
            WHERE upper(split_part(servidor, '-', 2)) = ?
            GROUP BY 1
            ORDER BY matricula
            """,
            [socio_nome.upper()],
        ).fetchall()
        lot_rows = con.execute(
            """
            SELECT DISTINCT matricula_contrato, cargo, lotacao, secretaria, unidade, vinculo
            FROM rb_servidores_lotacao
            WHERE upper(nome) = ?
            ORDER BY matricula_contrato
            """,
            [socio_nome.upper()],
        ).fetchall()
        lot_by_matricula = {
            fix_text(row[0]): {
                "cargo": fix_text(row[1]),
                "lotacao": fix_text(row[2]),
                "secretaria": fix_text(row[3]),
                "unidade": fix_text(row[4]),
                "vinculo": fix_text(row[5]),
            }
            for row in lot_rows
        }
        for row in mass_rows:
            matricula = fix_text(row[0])
            lot = lot_by_matricula.get(matricula, {})
            out.append(
                {
                    "socio_nome": fix_text(socio_nome),
                    "matricula_contrato": matricula,
                    "admissao": fix_text(row[1]),
                    "cargo": fix_text(row[2]) or lot.get("cargo", ""),
                    "vinculo": fix_text(row[3]) or lot.get("vinculo", ""),
                    "ch": int(row[4] or 0),
                    "salario_liquido_min": float(row[5] or 0),
                    "salario_liquido_max": float(row[6] or 0),
                    "n_linhas_folha": int(row[7] or 0),
                    "lotacao": lot.get("lotacao", ""),
                    "secretaria": lot.get("secretaria", ""),
                    "unidade": lot.get("unidade", ""),
                }
            )
    return out


def derive_flags(case: dict[str, Any], contracts: list[dict[str, Any]], cnes: dict[str, Any], socios: list[dict[str, Any]]) -> list[str]:
    flags: list[str] = []
    cnae = normalize_text(case.get("cnae_descricao") or case.get("cnae_principal"))
    contract_text = normalize_text(" ".join(contract["objeto"] for contract in contracts))
    socio_text = normalize_text(
        " ".join(
            " ".join([s["cargo"], s["secretaria"], s["lotacao"], s["unidade"]])
            for s in socios
        )
    )
    cnes_text = normalize_text(
        " ".join(
            [
                cnes.get("cnes_nome", ""),
                cnes.get("cnes_nome_empresarial", ""),
                cnes.get("cnes_tipo_estabelecimento", ""),
                cnes.get("cnes_subtipo_estabelecimento", ""),
                cnes.get("cnes_gestao", ""),
                cnes.get("cnes_turno_atendimento", ""),
                " ".join(item.get("classificacao", "") for item in cnes.get("cnes_servicos_classificacao_json", [])),
            ]
        )
    )
    if cnes.get("cnes_code"):
        flags.append("empresa_com_cnes_oficial")
    if "SADT" in cnes_text or "APOIO DIAGNOSE" in cnes_text:
        flags.append("estabelecimento_sadt")
    if "ESTADUAL" in cnes_text:
        flags.append("gestao_cnes_estadual")
    if "SESACRE" in normalize_text(" ".join(contract["orgao"] for contract in contracts)):
        flags.append("contrato_estadual_sesacre")
    if "MEDICO" in socio_text and "SEMSA" in socio_text:
        flags.append("socios_medicos_semsa")
    if any(token in cnae for token in ("TOMOGRAF", "DIAGNOST", "IMAGEM")):
        flags.append("atividade_cadastral_diagnostico")
    if any(token in contract_text for token in ("TOMOGRAF", "CORONAR", "DIAGNOST", "CONTRASTE", "SEDACAO")):
        flags.append("objeto_contratual_diagnostico")
    if any(token in cnes_text for token in ("RADIOLOGIA", "TOMOGRAFIA", "ULTRASONOGRAFIA", "RESSONANCIA")):
        flags.append("cnes_classificacao_correlata")
    if cnes.get("cnes_profissionais_match_json"):
        flags.append("socio_listado_como_profissional_no_cnes")
    if (
        any(token in socio_text for token in ("RADIOLOG", "ULTRASONOGRAF", "IMAGEM"))
        and any(token in (cnae + " " + contract_text + " " + cnes_text) for token in ("TOMOGRAF", "DIAGNOST", "IMAGEM", "RADIOLOG", "ULTRASONOGRAF"))
    ):
        flags.append("especialidade_publica_correlata_ao_objeto")
    return flags


def upsert_case(con: duckdb.DuckDBPyConnection, case: dict[str, Any], contracts: list[dict[str, Any]], cnes: dict[str, Any], socios: list[dict[str, Any]], flags: list[str]) -> None:
    top_contract = contracts[0]
    evidence = {
        "case": case,
        "contracts": contracts,
        "cnes": cnes,
        "socios_publicos": socios,
        "flags": flags,
    }
    con.execute(
        """
        INSERT OR REPLACE INTO vinculo_societario_saude_followup (
            row_id, cnpj, razao_social, cnae_principal, cnae_descricao,
            contrato_ano, contrato_numero, contrato_orgao, contrato_valor_brl, contrato_objeto,
            cnes_ficha_url, cnes_code, cnes_nome, cnes_nome_empresarial, cnes_cnpj, cnes_gestao,
            cnes_tipo_estabelecimento, cnes_subtipo_estabelecimento, cnes_dependencia,
            cnes_cadastrado_em, cnes_ultima_atualizacao, cnes_atualizacao_local,
            cnes_endereco, cnes_bairro, cnes_cep, cnes_municipio, cnes_uf, cnes_telefone,
            cnes_horario_json, cnes_turno_atendimento, cnes_instalacoes_json, cnes_servicos_apoio_json,
            cnes_servicos_classificacao_json, cnes_profissionais_match_json, n_cnes_profissionais_match,
            socios_publicos_json, overlap_flags_json, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row_hash("vps_saude", case["cnpj"]),
            case["cnpj"],
            case["razao_social"],
            case["cnae_principal"],
            case["cnae_descricao"],
            top_contract["ano"],
            top_contract["numero"],
            top_contract["orgao"],
            top_contract["valor_brl"],
            top_contract["objeto"],
            cnes.get("cnes_ficha_url"),
            cnes.get("cnes_code"),
            cnes.get("cnes_nome"),
            cnes.get("cnes_nome_empresarial"),
            cnes.get("cnes_cnpj"),
            cnes.get("cnes_gestao"),
            cnes.get("cnes_tipo_estabelecimento"),
            cnes.get("cnes_subtipo_estabelecimento"),
            cnes.get("cnes_dependencia"),
            cnes.get("cnes_cadastrado_em"),
            cnes.get("cnes_ultima_atualizacao"),
            cnes.get("cnes_atualizacao_local"),
            cnes.get("cnes_endereco"),
            cnes.get("cnes_bairro"),
            cnes.get("cnes_cep"),
            cnes.get("cnes_municipio"),
            cnes.get("cnes_uf"),
            cnes.get("cnes_telefone"),
            json.dumps(cnes.get("cnes_horario_json", []), ensure_ascii=False),
            cnes.get("cnes_turno_atendimento"),
            json.dumps(cnes.get("cnes_instalacoes_json", []), ensure_ascii=False),
            json.dumps(cnes.get("cnes_servicos_apoio_json", []), ensure_ascii=False),
            json.dumps(cnes.get("cnes_servicos_classificacao_json", []), ensure_ascii=False),
            json.dumps(cnes.get("cnes_profissionais_match_json", []), ensure_ascii=False),
            len(cnes.get("cnes_profissionais_match_json", [])),
            json.dumps(socios, ensure_ascii=False),
            json.dumps(flags, ensure_ascii=False),
            json.dumps(evidence, ensure_ascii=False),
        ],
    )


def upsert_insight(con: duckdb.DuckDBPyConnection, case: dict[str, Any], contracts: list[dict[str, Any]], cnes: dict[str, Any], socios: list[dict[str, Any]], flags: list[str]) -> None:
    top_contract = contracts[0]
    socio_lines = []
    for socio in socios:
        socio_lines.append(
            f"- {socio['socio_nome']} / {socio['cargo']} / {socio['secretaria'] or socio['lotacao']} / {socio['ch']}h"
        )
    description = (
        f"A empresa **{case['razao_social']}** (`{case['cnpj']}`) aparece no **CNES oficial** como "
        f"`{cnes.get('cnes_tipo_estabelecimento','N/I')}`"
        f"{' / ' + cnes.get('cnes_subtipo_estabelecimento') if cnes.get('cnes_subtipo_estabelecimento') else ''}, "
        f"com cadastro em `{cnes.get('cnes_cadastrado_em','N/I')}` e ultima atualizacao em `{cnes.get('cnes_ultima_atualizacao','N/I')}`. "
        f"No banco local, ela tem contrato estadual `{top_contract['numero']}` com `{top_contract['orgao']}` no valor de "
        f"**R$ {top_contract['valor_brl']:,.2f}**, com objeto ligado a diagnostico por imagem. "
        f"O QSA materializa {len(socios)} socio(s) com coincidencia nominal exata em base municipal de servidores, todos lotados na **SEMSA**. "
        "Isto documenta uma sobreposicao objetiva entre estabelecimento de saude privado, contrato publico estadual e funcao publica municipal. "
        "Nao prova nepotismo, impedimento legal ou conflito ilicito sem confirmacao de CPF completo, carga horaria compativel, regime e norma aplicavel.\n\n"
        "Socios publicos identificados:\n"
        + "\n".join(socio_lines)
    )
    payload = {
        "id": f"vps_saude:{case['cnpj']}",
        "kind": "QSA_VINCULO_SOCIETARIO_SAUDE_EXATO",
        "severity": "MEDIO",
        "confidence": 93,
        "exposure_brl": float(top_contract["valor_brl"] or 0),
        "title": f"Sobreposicao societaria exata em saude para {case['razao_social']}",
        "description_md": description,
        "pattern": "empresa_saude -> cnes_oficial -> contrato_estadual -> socio_servidor_municipal",
        "sources": json.dumps(
            [
                "empresa_socios",
                "empresas_cnpj",
                "estado_ac_contratos",
                "servidores",
                "rb_servidores_lotacao",
                "rb_servidores_mass",
                "CNES",
                "DATASUS",
            ],
            ensure_ascii=False,
        ),
        "tags": json.dumps(["vinculo_societario", "saude", "cnes", case["cnpj"], *flags], ensure_ascii=False),
        "sample_n": len(socios),
        "unit_total": float(top_contract["valor_brl"] or 0),
        "valor_referencia": float(top_contract["valor_brl"] or 0),
        "ano_referencia": top_contract["ano"],
        "fonte": "vinculo_societario_saude_followup",
    }
    institutional = classify_insight_record(payload)
    probative = classify_probative_record(payload)
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
            payload["id"],
            payload["kind"],
            payload["severity"],
            payload["confidence"],
            payload["exposure_brl"],
            payload["title"],
            payload["description_md"],
            payload["pattern"],
            payload["sources"],
            payload["tags"],
            payload["sample_n"],
            payload["unit_total"],
            datetime.now(),
            institutional["esfera"],
            institutional["ente"],
            institutional["orgao"],
            institutional["municipio"],
            institutional["uf"],
            institutional["area_tematica"],
            institutional["sus"],
            payload["valor_referencia"],
            payload["ano_referencia"],
            payload["fonte"],
            probative["classe_achado"],
            probative["grau_probatorio"],
            probative["fonte_primaria"],
            probative["uso_externo"],
            probative["inferencia_permitida"],
            probative["limite_conclusao"],
        ],
    )

    prof_matches = cnes.get("cnes_profissionais_match_json", [])
    if not prof_matches:
        return

    prof_lines = []
    for prof in prof_matches:
        prof_lines.append(
            f"- {prof['nome']} / {prof['cbo']} / amb `{prof['ch_amb']}` / total `{prof['total']}` / "
            f"vinculacao `{prof['vinculacao']}` / tipo `{prof['tipo']}` / situacao `{prof['situacao']}`"
        )

    factual_description = (
        f"O `CNES` oficial da empresa **{case['razao_social']}** (`{case['cnpj']}`), unidade `{cnes.get('cnes_code','N/I')}`, "
        f"lista nominalmente **{len(prof_matches)}** socio(s) identificados tambem na base municipal de servidores. "
        f"O mesmo estabelecimento aparece com classificacoes oficiais de diagnostico por imagem e esta ligado ao contrato "
        f"`{top_contract['numero']}` da `{top_contract['orgao']}` no valor de **R$ {top_contract['valor_brl']:,.2f}**.\n\n"
        "Profissionais coincidentes no CNES:\n"
        + "\n".join(prof_lines)
        + "\n\n"
        + "Isto prova coincidencia documental entre ficha oficial de profissionais do estabelecimento privado, "
        + "contrato estadual e base municipal de servidores. Nao prova, sozinho, impedimento legal, conflito vedado ou acumulacao ilicita."
    )
    factual_payload = {
        "id": f"vps_saude_prof:{case['cnpj']}",
        "kind": "VINCULO_EXATO_CNES_PROFISSIONAL_SAUDE",
        "severity": "MEDIO",
        "confidence": 97,
        "exposure_brl": float(top_contract["valor_brl"] or 0),
        "title": f"Vinculo exato CNES-profissional para {case['razao_social']}",
        "description_md": factual_description,
        "pattern": "cnes_profissional -> socio_qsa -> servidor_municipal -> contrato_estadual_saude",
        "sources": json.dumps(
            [
                "CNES",
                "DATASUS",
                "empresa_socios",
                "estado_ac_contratos",
                "rb_servidores_lotacao",
                "rb_servidores_mass",
            ],
            ensure_ascii=False,
        ),
        "tags": json.dumps(
            ["vinculo_societario", "saude", "cnes", "profissional", case["cnpj"], *flags],
            ensure_ascii=False,
        ),
        "sample_n": len(prof_matches),
        "unit_total": float(top_contract["valor_brl"] or 0),
        "valor_referencia": float(top_contract["valor_brl"] or 0),
        "ano_referencia": top_contract["ano"],
        "fonte": "vinculo_societario_saude_followup",
    }
    factual_institutional = classify_insight_record(factual_payload)
    factual_probative = classify_probative_record(factual_payload)
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
            factual_payload["id"],
            factual_payload["kind"],
            factual_payload["severity"],
            factual_payload["confidence"],
            factual_payload["exposure_brl"],
            factual_payload["title"],
            factual_payload["description_md"],
            factual_payload["pattern"],
            factual_payload["sources"],
            factual_payload["tags"],
            factual_payload["sample_n"],
            factual_payload["unit_total"],
            datetime.now(),
            factual_institutional["esfera"],
            factual_institutional["ente"],
            factual_institutional["orgao"],
            factual_institutional["municipio"],
            factual_institutional["uf"],
            factual_institutional["area_tematica"],
            factual_institutional["sus"],
            factual_payload["valor_referencia"],
            factual_payload["ano_referencia"],
            factual_payload["fonte"],
            factual_probative["classe_achado"],
            factual_probative["grau_probatorio"],
            factual_probative["fonte_primaria"],
            factual_probative["uso_externo"],
            factual_probative["inferencia_permitida"],
            factual_probative["limite_conclusao"],
        ],
    )


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    ensure_insight_classification_columns(con)
    con.execute(DDL)
    ensure_followup_columns(con)
    con.execute("DELETE FROM vinculo_societario_saude_followup")
    con.execute(
        """
        DELETE FROM insight
        WHERE kind IN ('QSA_VINCULO_SOCIETARIO_SAUDE_EXATO', 'VINCULO_EXATO_CNES_PROFISSIONAL_SAUDE')
        """
    )

    cases = load_positive_cases(con)
    inserted = 0
    for case in cases:
        contracts = load_contracts(con, case["cnpj"])
        if not contracts:
            continue
        socio_rows = load_public_socios(con, case["cnpj"])
        if not socio_rows:
            continue
        entry = find_cnes_entry(case["cnpj"], case["municipio"], case["uf"])
        if not entry:
            continue
        cnes = fetch_cnes_details(entry)
        cnes.update(fetch_cnes_info(entry))
        cnes["cnes_profissionais_match_json"] = fetch_cnes_profissionais(
            entry, [s["socio_nome"] for s in socio_rows]
        )
        flags = derive_flags(case, contracts, cnes, socio_rows)
        if not {"empresa_com_cnes_oficial", "socios_medicos_semsa"} <= set(flags):
            continue
        upsert_case(con, case, contracts, cnes, socio_rows, flags)
        upsert_insight(con, case, contracts, cnes, socio_rows, flags)
        inserted += 1

    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_followup AS
        SELECT *
        FROM vinculo_societario_saude_followup
        ORDER BY contrato_valor_brl DESC, razao_social
        """
    )
    con.close()

    print(f"cases={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
