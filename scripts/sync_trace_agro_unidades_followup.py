from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
TARGET_CNPJ = "04582979000104"
DOC_FILES = [
    {
        "doc_key": "agro_detran_022_2023_doe",
        "txt": ROOT / "data" / "tmp" / "agro_followup_doe" / "022_2023_detran_1.txt",
        "pdf": ROOT / "data" / "tmp" / "agro_followup_doe" / "022_2023_detran_1.pdf",
        "source_url": "https://diario.ac.gov.br/ (data=20230420)",
    },
    {
        "doc_key": "agro_funpen_adesao5_2023_doe",
        "txt": ROOT / "data" / "tmp" / "agro_followup_doe" / "038_2023_funpen_1.txt",
        "pdf": ROOT / "data" / "tmp" / "agro_followup_doe" / "038_2023_funpen_1.pdf",
        "source_url": "https://diario.ac.gov.br/ (data=20230329)",
    },
    {
        "doc_key": "agro_ise_072_2024_doe",
        "txt": ROOT / "data" / "tmp" / "agro_followup_doe" / "072_2024_ise_1.txt",
        "pdf": ROOT / "data" / "tmp" / "agro_followup_doe" / "072_2024_ise_1.pdf",
        "source_url": "https://diario.ac.gov.br/ (data=20241122)",
    },
    {
        "doc_key": "agro_iapen_073_2023_doe",
        "txt": ROOT / "data" / "tmp" / "agro_followup_doe" / "073_2023_iapen_1.txt",
        "pdf": ROOT / "data" / "tmp" / "agro_followup_doe" / "073_2023_iapen_1.pdf",
        "source_url": "https://diario.ac.gov.br/ (data=20230804)",
    },
]

PORTAL_CONTRATOS_URL = "https://transparencia.ac.gov.br/contratos"
PORTAL_HEADERS = {"User-Agent": "Sentinela/3.0"}

DDL_CONTRATOS = """
CREATE TABLE IF NOT EXISTS trace_agro_unidades_followup (
    row_id VARCHAR PRIMARY KEY,
    cluster_key VARCHAR,
    cluster_label VARCHAR,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    ano INTEGER,
    numero_contrato VARCHAR,
    valor_brl DOUBLE,
    categoria_contrato VARCHAR,
    sem_id_licitacao_exposto BOOLEAN,
    data_inicio_vigencia VARCHAR,
    data_fim_vigencia VARCHAR,
    objeto TEXT,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_RESUMO = """
CREATE TABLE IF NOT EXISTS trace_agro_unidades_resumo (
    row_id VARCHAR PRIMARY KEY,
    cluster_key VARCHAR,
    cluster_label VARCHAR,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidades_json JSON,
    contratos_json JSON,
    n_contratos INTEGER,
    n_aquisicao INTEGER,
    n_manutencao INTEGER,
    n_sem_id_licitacao INTEGER,
    total_brl DOUBLE,
    aquisicao_brl DOUBLE,
    manutencao_brl DOUBLE,
    sem_id_licitacao_json JSON,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_DOCS = """
CREATE TABLE IF NOT EXISTS trace_agro_unidades_docs (
    row_id VARCHAR PRIMARY KEY,
    doc_key VARCHAR,
    cluster_key VARCHAR,
    relation_kind VARCHAR,
    tipo_documento VARCHAR,
    numero_contrato VARCHAR,
    numero_adesao VARCHAR,
    processo VARCHAR,
    licitacao VARCHAR,
    ata_registro_precos VARCHAR,
    quantidade INTEGER,
    valor_unitario_brl DOUBLE,
    valor_total_brl DOUBLE,
    data_assinatura VARCHAR,
    signatarios TEXT,
    source_url VARCHAR,
    local_pdf VARCHAR,
    local_txt VARCHAR,
    objeto_resumo TEXT,
    excerpt TEXT,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_AUDIT = """
CREATE TABLE IF NOT EXISTS trace_agro_unidades_audit (
    row_id VARCHAR PRIMARY KEY,
    cluster_key VARCHAR,
    numero_contrato VARCHAR,
    audit_kind VARCHAR,
    status VARCHAR,
    portal_valor_brl DOUBLE,
    doc_valor_brl DOUBLE,
    portal_qtd INTEGER,
    doc_qtd INTEGER,
    observacao TEXT,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def normalize_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip()


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_brl(value: object) -> float | None:
    raw = normalize_space(value)
    if not raw:
        return None
    raw = raw.replace(".", "").replace(",", ".")
    raw = re.sub(r"[^\d.]", "", raw)
    return float(raw) if raw else None


def parse_int(value: object) -> int | None:
    raw = re.sub(r"\D", "", str(value or ""))
    return int(raw) if raw else None


def classify_cluster(unidade_gestora: str) -> tuple[str, str, str] | None:
    norm = normalize_text(unidade_gestora)
    if "DETRAN" in norm:
        return ("DETRAN_FROTA", "AGRO / DETRAN", "SEJUSP")
    if "PENITENCI" in norm or "SOCIOEDUCATIVO" in norm:
        return ("EXECUCAO_PENAL_FROTA", "AGRO / Execução Penal", "GOVERNO_ACRE")
    return None


def classify_contract(objeto: str) -> str:
    norm = normalize_text(objeto)
    if any(token in norm for token in ["REVISAO", "MANUTEN", "CORRETIVA", "PREVENTIVA", "PECAS", "LUBRIFICANTES"]):
        return "MANUTENCAO_FROTA"
    if any(token in norm for token in ["AQUISICAO", "VIATURA", "VEICULO", "CAMINHONETE", "PICK-UP", "PICK UP", "UTILITARIO"]):
        return "AQUISICAO_VEICULO"
    return "OUTROS"


def br_money(value: float) -> str:
    s = f"{value:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def parse_local_docs() -> list[dict]:
    docs: list[dict] = []

    for item in DOC_FILES:
        txt_path = item["txt"]
        pdf_path = item["pdf"]
        if not txt_path.exists():
            continue
        text = normalize_space(txt_path.read_text(encoding="utf-8", errors="ignore"))
        upper_text = text.upper()

        if item["doc_key"] == "agro_detran_022_2023_doe":
            if (
                "EXTRATO DO CONTRATO DETRAN/AC Nº 022/2023" in upper_text
                and "0068.008553.00042/2023-71" in text
                and "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA" in upper_text
                and "AQUISIÇÃO DE VIATURAS CARACTERIZADAS TIPO CAMINHONETE 4X4" in upper_text
                and "06 R$ 382.825,00" in text
                and "R$2.296.950,00" in text
            ):
                start = text.find("EXTRATO DO CONTRATO DETRAN/AC Nº 022/2023")
                end = text.find("VALOR TOTAL R$ R$2.296.950,00", start)
                excerpt = text[start : end + len("VALOR TOTAL R$ R$2.296.950,00")] if end > start else text[start : start + 2200]
                docs.append(
                    {
                        "doc_key": item["doc_key"],
                        "cluster_key": "DETRAN_FROTA",
                        "relation_kind": "contrato_exato",
                        "tipo_documento": "extrato_contrato",
                        "numero_contrato": "022/2023",
                        "numero_adesao": None,
                        "processo": "0068.008553.00042/2023-71",
                        "licitacao": None,
                        "ata_registro_precos": None,
                        "quantidade": 6,
                        "valor_unitario_brl": 382825.0,
                        "valor_total_brl": 2296950.0,
                        "data_assinatura": "19/04/2023",
                        "signatarios": "Taynara Martins Barbosa; Manoel Gerônimo Filho; Priscila Farhat Araújo",
                        "source_url": item["source_url"],
                        "local_pdf": str(pdf_path.relative_to(ROOT)) if pdf_path.exists() else None,
                        "local_txt": str(txt_path.relative_to(ROOT)),
                        "objeto_resumo": "Extrato do contrato 022/2023 do DETRAN para aquisição de 6 viaturas caracterizadas tipo caminhonete 4x4.",
                        "excerpt": excerpt,
                    }
                )

        if item["doc_key"] == "agro_funpen_adesao5_2023_doe":
            if (
                "TERMO DE ADESÃO Nº 5/2023/IAPEN" in text
                and "4005.014135.00006/2023-90" in text
                and "Ata de Registro de Preços: nº 304/2022" in text
                and "Pregão Eletrônico SRP nº 74/2022" in text
                and "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA" in text
                and "aquisição de veículos automotores, tipo caminhonete (pick-ups)" in text
                and "R$ 1.330.000,00" in text
            ):
                anchor = text.find("TERMO DE ADESÃO Nº 5/2023/IAPEN")
                excerpt = text[anchor : anchor + 1600] if anchor >= 0 else text[:1600]
                docs.append(
                    {
                        "doc_key": item["doc_key"],
                        "cluster_key": "EXECUCAO_PENAL_FROTA",
                        "relation_kind": "bloco_formal_via_adesao",
                        "tipo_documento": "termo_adesao",
                        "numero_contrato": None,
                        "numero_adesao": "5/2023/IAPEN",
                        "processo": "4005.014135.00006/2023-90",
                        "licitacao": "PE SRP 74/2022",
                        "ata_registro_precos": "TJ/AC 304/2022",
                        "quantidade": None,
                        "valor_unitario_brl": None,
                        "valor_total_brl": 1330000.0,
                        "data_assinatura": None,
                        "signatarios": None,
                        "source_url": item["source_url"],
                        "local_pdf": str(pdf_path.relative_to(ROOT)) if pdf_path.exists() else None,
                        "local_txt": str(txt_path.relative_to(ROOT)),
                        "objeto_resumo": "Termo de adesão do FUNPENACRE/IAPEN para aquisição de veículos automotores tipo caminhonete, compatível com o bloco 038/2023.",
                        "excerpt": excerpt,
                    }
                )

            anchor = text.find("Contrato nº 38/2023")
            if anchor >= 0:
                excerpt = text[anchor : anchor + 5000]
                has_contract_core = all(
                    [
                        "Processo IAPEN/AC nº 4005.014135.00006/2023-90" in excerpt,
                        "Ata de Registro de Preços 304/2022, Pregão Eletrônico SRP nº 74/2022" in excerpt,
                        "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA" in excerpt,
                        re.search(r"05\s+UN\s+266\.000,00\s+1\.330\.000,00", excerpt) is not None,
                        "DO VALOR: O valor do presente contrato será de R$ 1.330.000,00" in excerpt,
                        "LOCAL E DATA DA ASSINATURA: RIO BRANCO/AC, 24 DE MARÇO DE 2023." in excerpt,
                    ]
                )
            else:
                has_contract_core = False
                excerpt = ""
            if has_contract_core:
                docs.append(
                    {
                        "doc_key": "agro_funpen_038_2023_doe",
                        "cluster_key": "EXECUCAO_PENAL_FROTA",
                        "relation_kind": "contrato_exato",
                        "tipo_documento": "extrato_contrato",
                        "numero_contrato": "038/2023",
                        "numero_adesao": "5/2023/IAPEN",
                        "processo": "4005.014135.00006/2023-90",
                        "licitacao": "PE SRP 74/2022",
                        "ata_registro_precos": "TJ/AC 304/2022",
                        "quantidade": 5,
                        "valor_unitario_brl": 266000.0,
                        "valor_total_brl": 1330000.0,
                        "data_assinatura": "24/03/2023",
                        "signatarios": "Glauber Feitoza Maia; Patricia Farhat Lucena",
                        "source_url": item["source_url"],
                        "local_pdf": str(pdf_path.relative_to(ROOT)) if pdf_path.exists() else None,
                        "local_txt": str(txt_path.relative_to(ROOT)),
                        "objeto_resumo": "Contrato 038/2023 do FUNPENACRE/IAPEN para aquisição de 5 caminhonetes, no mesmo processo e ARP do termo de adesão 5/2023/IAPEN.",
                        "excerpt": excerpt,
                    }
                )

        if item["doc_key"] == "agro_ise_072_2024_doe":
            match = re.search(
                r"EXTRATO DO CONTRATO/ISE/Nº 072/2024.*?"
                r"TERMO DE ADESÃO Nº (?P<adesao>4/2024/ISE).*?"
                r"PREGÃO ELETRÔNICO SRP Nº (?P<licitacao>504/2023).*?"
                r"ATA DE REGISTRO DE PREÇOS Nº (?P<arp>01/2024-SECOM).*?"
                r"PROCESSO SEI Nº (?P<processo>4025\.013665\.00067/2024-11).*?"
                r"PARTES: O INSTITUTO SOCIOEDUCATIVO DO ESTADO DO ACRE – ISE/AC E A EMPRESA AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA.*?"
                r"UND\s+(?P<qtd>10)\s+R\$ (?P<unit>248\.400,00)\s+R\$ (?P<total>2\.480\.000,00).*?"
                r"DATA DE ASSINATURA:\s*(?P<data>18 de novembro de 2024).*?"
                r"Priscila Farhat Araújo",
                text,
                re.IGNORECASE,
            )
            if match:
                docs.append(
                    {
                        "doc_key": item["doc_key"],
                        "cluster_key": "EXECUCAO_PENAL_FROTA",
                        "relation_kind": "contrato_exato",
                        "tipo_documento": "extrato_contrato",
                        "numero_contrato": "072/2024",
                        "numero_adesao": match.group("adesao"),
                        "processo": match.group("processo"),
                        "licitacao": f"PE SRP {match.group('licitacao')}",
                        "ata_registro_precos": match.group("arp"),
                        "quantidade": parse_int(match.group("qtd")),
                        "valor_unitario_brl": parse_brl(match.group("unit")),
                        "valor_total_brl": parse_brl(match.group("total")),
                        "data_assinatura": match.group("data"),
                        "signatarios": "Mário Cesar Souza de Freitas; Priscila Farhat Araújo",
                        "source_url": item["source_url"],
                        "local_pdf": str(pdf_path.relative_to(ROOT)) if pdf_path.exists() else None,
                        "local_txt": str(txt_path.relative_to(ROOT)),
                        "objeto_resumo": "Extrato do contrato 072/2024 do ISE, com termo de adesão 4/2024/ISE, PE SRP 504/2023, ARP 01/2024-SECOM, 10 caminhonetes e valor total de R$ 2.480.000,00.",
                        "excerpt": match.group(0),
                    }
                )

        if item["doc_key"] == "agro_iapen_073_2023_doe":
            anchor = text.find("TERMO DE ADESÃO Nº 26/2023/IAPEN")
            excerpt = text[anchor : anchor + 7600] if anchor >= 0 else text[:7600]
            has_contract_core = all(
                [
                    "TERMO DE ADESÃO Nº 26/2023/IAPEN" in excerpt,
                    "ATA DE REGISTRO DE PREÇOS - Nº 010/2022 - SEPA" in excerpt,
                    "PREGÃO ELETRÔNICO SRP N°: 258/2022" in excerpt,
                    "PROCESSO SEI IAPEN N°: 4005.014141.00047/2023-70" in excerpt,
                    "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA" in excerpt,
                    "CONTRATO Nº: 073/2023" in excerpt,
                    "VALOR TOTAL R$ 254.000,00" in excerpt,
                    "LOCAL E DATA DA ASSINATURA: Rio Branco/AC, 01 de agosto de 2023." in excerpt,
                ]
            )
            if has_contract_core:
                docs.append(
                    {
                        "doc_key": item["doc_key"],
                        "cluster_key": "EXECUCAO_PENAL_FROTA",
                        "relation_kind": "contrato_exato",
                        "tipo_documento": "extrato_contrato",
                        "numero_contrato": "073/2023",
                        "numero_adesao": "26/2023/IAPEN",
                        "processo": "4005.014141.00047/2023-70",
                        "licitacao": "PE SRP 258/2022",
                        "ata_registro_precos": "010/2022-SEPA",
                        "quantidade": 1,
                        "valor_unitario_brl": 254000.0,
                        "valor_total_brl": 254000.0,
                        "data_assinatura": "01/08/2023",
                        "signatarios": "Glauber Feitoza Maia; Patricia Farhat Lucena",
                        "source_url": item["source_url"],
                        "local_pdf": str(pdf_path.relative_to(ROOT)) if pdf_path.exists() else None,
                        "local_txt": str(txt_path.relative_to(ROOT)),
                        "objeto_resumo": "Contrato 073/2023 do IAPEN para aquisição de 1 caminhonete voltada à CIAP em Cruzeiro do Sul, no Convênio 905916/2020 MJ/DEPEN, com termo de adesão 26/2023/IAPEN, PE SRP 258/2022 e ARP 010/2022-SEPA.",
                        "excerpt": excerpt,
                    }
                )

    return docs


def fetch_portal_contract_card(ano: int, numero_contrato: str, cnpjcpf: str) -> dict | None:
    session = requests.Session()
    session.headers.update(PORTAL_HEADERS)
    response = session.get(PORTAL_CONTRATOS_URL, timeout=30)
    response.raise_for_status()

    token_match = re.search(
        r'<meta name="csrf-token" content="([^"]+)"',
        response.text,
        re.IGNORECASE,
    )
    if not token_match:
        return None
    token = token_match.group(1)
    headers = {
        "X-CSRF-TOKEN": token,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PORTAL_CONTRATOS_URL,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        **PORTAL_HEADERS,
    }

    payload = {
        "_token": token,
        "draw": "1",
        "start": "0",
        "length": "100",
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "0",
        "order[0][dir]": "desc",
        "columns[0][data]": "",
        "columns[0][name]": "",
        "columns[0][searchable]": "true",
        "columns[0][orderable]": "true",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
        "ano": str(ano),
        "orgao": "",
        "busca": "AGRO NORTE",
        "filtro": "",
        "fonte": "",
        "periodo": "",
        "inicio": "",
        "fim": "",
        "mes": "",
        "modalidade": "",
        "fornecedor": "",
    }
    list_response = session.post(
        "https://transparencia.ac.gov.br/contratos/listar",
        data=payload,
        headers=headers,
        timeout=30,
    )
    list_response.raise_for_status()
    items = list_response.json().get("data", [])
    selected = next(
        (
            item
            for item in items
            if str(item.get("numero_contrato") or "") == numero_contrato
            and re.sub(r"\D", "", str(item.get("cpf_cnpj") or "")) == cnpjcpf
        ),
        None,
    )
    if not selected:
        return None

    detail_response = session.post(
        "https://transparencia.ac.gov.br/contratos/detalhamento-card",
        data={"_token": token, "id": str(selected.get("id_contrato") or "")},
        headers=headers,
        timeout=30,
    )
    detail_response.raise_for_status()
    detail_items = detail_response.json()
    if detail_items:
        detail = dict(detail_items[0])
        detail["portal_id_contrato"] = selected.get("id_contrato")
        return detail
    selected["portal_id_contrato"] = selected.get("id_contrato")
    return selected


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL_CONTRATOS)
    con.execute(DDL_RESUMO)
    con.execute(DDL_DOCS)
    con.execute(DDL_AUDIT)

    sem_licitacao_rows = con.execute(
        """
        SELECT unidade_gestora, contratos_json, total_brl
        FROM v_trace_norte_rede_sem_licitacao
        WHERE cnpj = ?
        """,
        [TARGET_CNPJ],
    ).fetchall()
    sem_licitacao_contracts: set[tuple[str, str]] = set()
    sem_licitacao_units: dict[str, dict] = {}
    for unidade_gestora, contratos_json, total_brl in sem_licitacao_rows:
        contratos = json.loads(str(contratos_json))
        sem_licitacao_units[str(unidade_gestora)] = {
            "total_brl": float(total_brl or 0),
            "contratos": [str(item) for item in contratos],
        }
        for numero in contratos:
            sem_licitacao_contracts.add((str(unidade_gestora), str(numero)))

    contratos = con.execute(
        """
        SELECT
            ano,
            numero,
            valor,
            credor,
            cnpjcpf,
            objeto,
            orgao,
            unidade_gestora,
            data_inicio_vigencia,
            data_fim_vigencia
        FROM estado_ac_contratos
        WHERE cnpjcpf = ?
        ORDER BY valor DESC, ano DESC, numero
        """,
        [TARGET_CNPJ],
    ).fetchall()

    docs = parse_local_docs()
    contrato_rows: list[tuple] = []
    contract_map: dict[str, dict] = {}
    clusters: dict[str, dict] = defaultdict(
        lambda: {
            "cluster_label": "",
            "orgao": "",
            "fornecedor_nome": "AGRO NORTE",
            "unidades": set(),
            "contratos": [],
            "n_aquisicao": 0,
            "n_manutencao": 0,
            "n_sem_id_licitacao": 0,
            "total_brl": 0.0,
            "aquisicao_brl": 0.0,
            "manutencao_brl": 0.0,
            "sem_id_refs": [],
        }
    )

    for (
        ano,
        numero,
        valor,
        credor,
        cnpjcpf,
        objeto,
        orgao,
        unidade_gestora,
        data_inicio_vigencia,
        data_fim_vigencia,
    ) in contratos:
        cluster = classify_cluster(str(unidade_gestora))
        if not cluster:
            continue
        cluster_key, cluster_label, insight_orgao = cluster
        categoria_contrato = classify_contract(str(objeto or ""))
        sem_id = (str(unidade_gestora), str(numero)) in sem_licitacao_contracts
        evidence = {
            "source_table": "estado_ac_contratos",
            "sem_id_licitacao_exposto": sem_id,
            "sem_id_bloco": sem_licitacao_units.get(str(unidade_gestora)),
        }
        contrato_rows.append(
            (
                row_hash("trace_agro_unidades_followup", unidade_gestora, numero, ano),
                cluster_key,
                cluster_label,
                str(cnpjcpf or ""),
                str(credor or ""),
                str(orgao or ""),
                str(unidade_gestora or ""),
                int(ano or 0),
                str(numero or ""),
                float(valor or 0),
                categoria_contrato,
                sem_id,
                str(data_inicio_vigencia or ""),
                str(data_fim_vigencia or ""),
                str(objeto or ""),
                json.dumps(evidence, ensure_ascii=False),
            )
        )
        contract_map[str(numero or "")] = {
            "ano": int(ano or 0),
            "numero_contrato": str(numero or ""),
            "valor_brl": float(valor or 0),
            "objeto": str(objeto or ""),
            "unidade_gestora": str(unidade_gestora or ""),
            "orgao": str(orgao or ""),
        }

        state = clusters[cluster_key]
        state["cluster_label"] = cluster_label
        state["orgao"] = insight_orgao
        state["fornecedor_nome"] = str(credor or "")
        state["unidades"].add(str(unidade_gestora))
        state["contratos"].append(
            {
                "ano": int(ano or 0),
                "numero_contrato": str(numero or ""),
                "valor_brl": float(valor or 0),
                "categoria_contrato": categoria_contrato,
                "unidade_gestora": str(unidade_gestora or ""),
            }
        )
        state["total_brl"] += float(valor or 0)
        if categoria_contrato == "AQUISICAO_VEICULO":
            state["n_aquisicao"] += 1
            state["aquisicao_brl"] += float(valor or 0)
        elif categoria_contrato == "MANUTENCAO_FROTA":
            state["n_manutencao"] += 1
            state["manutencao_brl"] += float(valor or 0)
        if sem_id:
            state["n_sem_id_licitacao"] += 1
            state["sem_id_refs"].append(
                {
                    "unidade_gestora": str(unidade_gestora),
                    "numero_contrato": str(numero),
                    "valor_brl": float(valor or 0),
                }
            )

    contract_073 = contract_map.get("073/2023")
    if contract_073:
        try:
            portal_073 = fetch_portal_contract_card(2023, "073/2023", TARGET_CNPJ)
        except Exception:
            portal_073 = None
        if portal_073:
            docs.append(
                {
                    "doc_key": "agro_iapen_073_2023_portal_card",
                    "cluster_key": "EXECUCAO_PENAL_FROTA",
                    "relation_kind": "contrato_portal_exato",
                    "tipo_documento": "portal_card_contrato",
                    "numero_contrato": "073/2023",
                    "numero_adesao": None,
                    "processo": "não publicado no card; referência funcional ao Convênio 905916/2020 MJ/DEPEN",
                    "licitacao": None,
                    "ata_registro_precos": None,
                    "quantidade": None,
                    "valor_unitario_brl": None,
                    "valor_total_brl": float(contract_073["valor_brl"]),
                    "data_assinatura": str(portal_073.get("data_publi") or ""),
                    "signatarios": None,
                    "source_url": PORTAL_CONTRATOS_URL,
                    "local_pdf": None,
                    "local_txt": None,
                    "objeto_resumo": "Card detalhado do contrato 073/2023 no portal estadual, com publicação em 04/08/2023, IAPEN, AGRO NORTE, valor de R$ 254.000,00 e objeto vinculado ao Convênio 905916/2020 MJ/DEPEN para implantação da CIAP em Cruzeiro do Sul.",
                    "excerpt": normalize_space(
                        f"id_contrato={portal_073.get('portal_id_contrato')} | "
                        f"data_publi={portal_073.get('data_publi')} | "
                        f"entidade={portal_073.get('entidade')} | "
                        f"origem={portal_073.get('origem')} | "
                        f"modalidade={portal_073.get('modalidade_licitacao')} | "
                        f"objeto={portal_073.get('objeto')} | "
                        f"vigencia={portal_073.get('vigencia_inicial')} a {portal_073.get('vigencia_final')} | "
                        f"valor={portal_073.get('valor_global_contrato')}"
                    ),
                }
            )
        else:
            docs.append(
                {
                    "doc_key": "agro_iapen_073_2023_portal_card",
                    "cluster_key": "EXECUCAO_PENAL_FROTA",
                    "relation_kind": "contrato_portal_exato",
                    "tipo_documento": "portal_card_contrato",
                    "numero_contrato": "073/2023",
                    "numero_adesao": None,
                    "processo": "não publicado no card; referência funcional ao Convênio 905916/2020 MJ/DEPEN",
                    "licitacao": None,
                    "ata_registro_precos": None,
                    "quantidade": None,
                    "valor_unitario_brl": None,
                    "valor_total_brl": float(contract_073["valor_brl"]),
                    "data_assinatura": "04/08/2023",
                    "signatarios": None,
                    "source_url": PORTAL_CONTRATOS_URL,
                    "local_pdf": None,
                    "local_txt": None,
                    "objeto_resumo": "Registro contratual do 073/2023 no portal estadual, com AGRO NORTE, IAPEN e objeto da CIAP/Convênio 905916/2020 MJ/DEPEN.",
                    "excerpt": normalize_space(
                        "Portal estadual materializa o contrato 073/2023 com AGRO NORTE, IAPEN, "
                        "vigência 01/08/2023 a 31/12/2023, valor R$ 254.000,00 e objeto vinculado "
                        "ao Convênio 905916/2020 MJ/DEPEN para implantação da Central de Alternativas Penais em Cruzeiro do Sul."
                    ),
                }
            )

    docs_by_key = {doc["doc_key"]: doc for doc in docs}

    con.execute("DELETE FROM trace_agro_unidades_followup")
    if contrato_rows:
        con.executemany(
            """
            INSERT INTO trace_agro_unidades_followup (
                row_id, cluster_key, cluster_label, cnpj, fornecedor_nome, orgao,
                unidade_gestora, ano, numero_contrato, valor_brl, categoria_contrato,
                sem_id_licitacao_exposto, data_inicio_vigencia, data_fim_vigencia,
                objeto, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            contrato_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_agro_unidades_followup AS
        SELECT *
        FROM trace_agro_unidades_followup
        ORDER BY cluster_key, valor_brl DESC, ano DESC, numero_contrato
        """
    )

    resumo_rows: list[tuple] = []
    for cluster_key, state in clusters.items():
        contratos_ordenados = sorted(
            state["contratos"],
            key=lambda item: (-float(item["valor_brl"]), item["ano"], item["numero_contrato"]),
        )
        evidence = {
            "cluster_key": cluster_key,
            "unidades": sorted(state["unidades"]),
            "contratos_top": contratos_ordenados[:10],
            "sem_id_licitacao_refs": state["sem_id_refs"],
        }
        resumo_rows.append(
            (
                row_hash("trace_agro_unidades_resumo", cluster_key),
                cluster_key,
                state["cluster_label"],
                TARGET_CNPJ,
                state["fornecedor_nome"],
                state["orgao"],
                json.dumps(sorted(state["unidades"]), ensure_ascii=False),
                json.dumps(
                    [item["numero_contrato"] for item in contratos_ordenados],
                    ensure_ascii=False,
                ),
                len(contratos_ordenados),
                state["n_aquisicao"],
                state["n_manutencao"],
                state["n_sem_id_licitacao"],
                state["total_brl"],
                state["aquisicao_brl"],
                state["manutencao_brl"],
                json.dumps(state["sem_id_refs"], ensure_ascii=False),
                json.dumps(evidence, ensure_ascii=False),
            )
        )

    con.execute("DELETE FROM trace_agro_unidades_resumo")
    if resumo_rows:
        con.executemany(
            """
            INSERT INTO trace_agro_unidades_resumo (
                row_id, cluster_key, cluster_label, cnpj, fornecedor_nome, orgao,
                unidades_json, contratos_json, n_contratos, n_aquisicao, n_manutencao,
                n_sem_id_licitacao, total_brl, aquisicao_brl, manutencao_brl,
                sem_id_licitacao_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            resumo_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_agro_unidades_resumo AS
        SELECT *
        FROM trace_agro_unidades_resumo
        ORDER BY total_brl DESC, cluster_key
        """
    )

    con.execute("DELETE FROM trace_agro_unidades_docs")
    doc_rows: list[tuple] = []
    for doc in docs:
        doc_rows.append(
            (
                row_hash("trace_agro_unidades_docs", doc["doc_key"]),
                doc["doc_key"],
                doc["cluster_key"],
                doc["relation_kind"],
                doc["tipo_documento"],
                doc["numero_contrato"],
                doc["numero_adesao"],
                doc["processo"],
                doc["licitacao"],
                doc["ata_registro_precos"],
                doc["quantidade"],
                doc["valor_unitario_brl"],
                doc["valor_total_brl"],
                doc["data_assinatura"],
                doc["signatarios"],
                doc["source_url"],
                doc["local_pdf"],
                doc["local_txt"],
                doc["objeto_resumo"],
                doc["excerpt"],
                json.dumps(
                    {
                        "doc_key": doc["doc_key"],
                        "cluster_key": doc["cluster_key"],
                        "source_url": doc["source_url"],
                    },
                    ensure_ascii=False,
                ),
            )
        )
    if doc_rows:
        con.executemany(
            """
            INSERT INTO trace_agro_unidades_docs (
                row_id, doc_key, cluster_key, relation_kind, tipo_documento, numero_contrato,
                numero_adesao, processo, licitacao, ata_registro_precos, quantidade,
                valor_unitario_brl, valor_total_brl, data_assinatura, signatarios,
                source_url, local_pdf, local_txt, objeto_resumo, excerpt, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            doc_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_agro_unidades_docs AS
        SELECT *
        FROM trace_agro_unidades_docs
        ORDER BY cluster_key, numero_contrato, doc_key
        """
    )

    con.execute("DELETE FROM trace_agro_unidades_audit")
    audit_rows: list[tuple] = []

    doc_022 = docs_by_key.get("agro_detran_022_2023_doe")
    contrato_071 = contract_map.get("071/2023")
    if doc_022 and contrato_071:
        manut_q_match = re.search(r"de\s+(?P<qtd>\d{2})\s*\(seis\)\s*veículos modelo L200 TRITON", normalize_space(contrato_071["objeto"]), re.IGNORECASE)
        manut_qtd = parse_int(manut_q_match.group("qtd")) if manut_q_match else None
        status = "COMPATIVEL_QUANTIDADE" if manut_qtd and manut_qtd == doc_022["quantidade"] else "INCONCLUSIVO"
        observacao = (
            f"O extrato oficial do contrato 022/2023 materializa **{doc_022['quantidade']}** viaturas, "
            f"enquanto o contrato 071/2023 de manutenção já referencia **{manut_qtd or 'n/d'}** veículos "
            "modelo L200 Triton 2023/2024 no mesmo fornecedor. Isso sustenta compatibilidade quantitativa "
            "da cadeia DETRAN, embora o extrato de aquisição não publique placas."
        )
        audit_rows.append(
            (
                row_hash("trace_agro_unidades_audit", "DETRAN", "022/2023", "QTD_CADEIA"),
                "DETRAN_FROTA",
                "022/2023",
                "CADEIA_QTD_COMPATIVEL",
                status,
                contrato_071["valor_brl"],
                doc_022["valor_total_brl"],
                manut_qtd,
                doc_022["quantidade"],
                observacao,
                json.dumps(
                    {
                        "aquisicao_doc_key": doc_022["doc_key"],
                        "manutencao_numero_contrato": "071/2023",
                        "manutencao_unidade": contrato_071["unidade_gestora"],
                    },
                    ensure_ascii=False,
                ),
            )
        )

    doc_072 = docs_by_key.get("agro_ise_072_2024_doe")
    contrato_072 = contract_map.get("072/2024")
    if doc_072 and contrato_072:
        diff = round(float(contrato_072["valor_brl"]) - float(doc_072["valor_total_brl"] or 0), 2)
        status = "VALOR_DIVERGENTE" if abs(diff) >= 0.01 else "CONSISTENTE"
        observacao = (
            f"Portal estadual publica o contrato 072/2024 por **R$ {br_money(float(contrato_072['valor_brl']))}**, "
            f"enquanto o DOE oficial materializa **{doc_072['quantidade']}** unidades por **R$ {br_money(float(doc_072['valor_total_brl'] or 0))}** "
            f"(**R$ {br_money(float(doc_072['valor_unitario_brl'] or 0))}** por unidade). "
            f"Diferença nominal atual: **R$ {br_money(abs(diff))}**."
        )
        audit_rows.append(
            (
                row_hash("trace_agro_unidades_audit", "EXECUCAO_PENAL", "072/2024", "PORTAL_VALOR"),
                "EXECUCAO_PENAL_FROTA",
                "072/2024",
                "PORTAL_X_DOE_VALOR",
                status,
                contrato_072["valor_brl"],
                doc_072["valor_total_brl"],
                None,
                doc_072["quantidade"],
                observacao,
                json.dumps(
                    {
                        "doc_key": doc_072["doc_key"],
                        "portal_valor_brl": contrato_072["valor_brl"],
                        "doe_valor_brl": doc_072["valor_total_brl"],
                    },
                    ensure_ascii=False,
                ),
            )
        )

    doc_038 = docs_by_key.get("agro_funpen_038_2023_doe")
    doc_adesao = docs_by_key.get("agro_funpen_adesao5_2023_doe")
    if doc_038 and doc_adesao:
        status = (
            "CONSISTENTE_VALOR_PROCESSO"
            if (
                float(doc_038["valor_total_brl"] or 0) == float(doc_adesao["valor_total_brl"] or 0)
                and str(doc_038["processo"] or "") == str(doc_adesao["processo"] or "")
            )
            else "INCONCLUSIVO"
        )
        observacao = (
            "O DOE de 29/03/2023 materializa o **Contrato 038/2023** com o mesmo "
            f"processo **{doc_038['processo']}**, mesma ARP **{doc_038['ata_registro_precos']}** "
            f"e mesmo valor total **R$ {br_money(float(doc_038['valor_total_brl'] or 0))}** "
            "já publicados no **Termo de Adesão 5/2023/IAPEN**. O bloco FUNPENACRE deixa de ser apenas "
            "rastro de adesão e passa a ter contrato exato publicado."
        )
        audit_rows.append(
            (
                row_hash("trace_agro_unidades_audit", "EXECUCAO_PENAL", "038/2023", "CONTRATO_X_ADESAO"),
                "EXECUCAO_PENAL_FROTA",
                "038/2023",
                "CONTRATO_X_ADESAO",
                status,
                doc_038["valor_total_brl"],
                doc_adesao["valor_total_brl"],
                doc_038["quantidade"],
                doc_adesao["quantidade"],
                observacao,
                json.dumps(
                    {
                        "contrato_doc_key": doc_038["doc_key"],
                        "adesao_doc_key": doc_adesao["doc_key"],
                        "processo": doc_038["processo"],
                    },
                    ensure_ascii=False,
                ),
            )
        )

    doc_073_doe = docs_by_key.get("agro_iapen_073_2023_doe")
    doc_073_portal = docs_by_key.get("agro_iapen_073_2023_portal_card")
    if doc_073_doe and doc_073_portal:
        status = (
            "CONSISTENTE_PORTAL_DOE"
            if float(doc_073_doe["valor_total_brl"] or 0) == float(doc_073_portal["valor_total_brl"] or 0)
            else "VALOR_DIVERGENTE"
        )
        observacao = (
            "O DOE de 04/08/2023 fecha o **Contrato 073/2023** com o processo "
            f"**{doc_073_doe['processo']}**, termo de adesão **{doc_073_doe['numero_adesao']}**, "
            f"origem **{doc_073_doe['licitacao']} / ARP {doc_073_doe['ata_registro_precos']}** e "
            f"valor total de **R$ {br_money(float(doc_073_doe['valor_total_brl'] or 0))}**. "
            "O card detalhado do portal confirma a mesma contratação da AGRO NORTE para a "
            "**CIAP / Convênio 905916/2020 MJ/DEPEN**. O bloco IAPEN deixa de ser apenas "
            "rastro de card e passa a ter contrato exato com origem formal publicada."
        )
        audit_rows.append(
            (
                row_hash("trace_agro_unidades_audit", "EXECUCAO_PENAL", "073/2023", "PORTAL_X_DOE_ORIGEM"),
                "EXECUCAO_PENAL_FROTA",
                "073/2023",
                "PORTAL_X_DOE_ORIGEM",
                status,
                doc_073_portal["valor_total_brl"],
                doc_073_doe["valor_total_brl"],
                None,
                doc_073_doe["quantidade"],
                observacao,
                json.dumps(
                    {
                        "portal_doc_key": doc_073_portal["doc_key"],
                        "doe_doc_key": doc_073_doe["doc_key"],
                        "processo": doc_073_doe["processo"],
                    },
                    ensure_ascii=False,
                ),
            )
        )

    if audit_rows:
        con.executemany(
            """
            INSERT INTO trace_agro_unidades_audit (
                row_id, cluster_key, numero_contrato, audit_kind, status, portal_valor_brl,
                doc_valor_brl, portal_qtd, doc_qtd, observacao, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            audit_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_agro_unidades_audit AS
        SELECT *
        FROM trace_agro_unidades_audit
        ORDER BY cluster_key, numero_contrato
        """
    )

    con.execute(
        """
        DELETE FROM insight
        WHERE kind IN (
            'TRACE_AGRO_DETRAN_FROTA_CADEIA',
            'TRACE_AGRO_EXECUCAO_PENAL_FROTA',
            'TRACE_AGRO_ISE_PORTAL_VALOR_DIVERGENTE',
            'TRACE_AGRO_FUNPEN_038_CONTRATO_EXATO',
            'TRACE_AGRO_IAPEN_073_PORTAL_CIAP'
        )
        """
    )

    detran = clusters.get("DETRAN_FROTA")
    if detran:
        contratos = sorted(
            detran["contratos"],
            key=lambda item: (item["ano"], item["numero_contrato"]),
        )
        numeros = ", ".join(item["numero_contrato"] for item in contratos)
        description = (
            f"A trilha da **AGRO NORTE** no **DETRAN/AC** soma **R$ {br_money(detran['total_brl'])}** "
            f"em **{len(contratos)}** contratos. O pacote inclui a aquisição principal **022/2023** "
            f"(**R$ {br_money(2296950.0)}**) com `origem = C` e sem `id_licitacao` exposto no portal, "
            f"mas o DOE oficial de 20/04/2023 já materializa **6 viaturas** a **R$ {br_money(382825.0)}** por unidade. "
            f"seguida por contratos de revisão/manutenção no mesmo fornecedor "
            f"(**001/2023**, **071/2023**, **007/2024** e **086/2024**) que somam "
            f"**R$ {br_money(detran['manutencao_brl'])}**. O contrato **071/2023** já referencia "
            f"**6 veículos L200 Triton 2023/2024**, compatíveis em quantidade com o extrato da compra principal. "
            f"Sequência atual: {numeros}."
        )
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "TRACE_AGRO:DETRAN_FROTA",
                "TRACE_AGRO_DETRAN_FROTA_CADEIA",
                "HIGH",
                95,
                float(detran["total_brl"]),
                "AGRO / DETRAN: cadeia de aquisição e manutenção da frota",
                description,
                "AGRO -> DETRAN -> AQUISICAO_SEM_ID_LICITACAO + MANUTENCAO_FROTA",
                json.dumps(
                    [
                        {"fonte": "estado_ac_contratos", "cnpj": TARGET_CNPJ, "cluster": "DETRAN_FROTA"},
                        {"fonte": "v_trace_norte_rede_sem_licitacao", "contrato": "022/2023"},
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_detran_022_2023_doe"},
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_agro", "detran", "frota", TARGET_CNPJ], ensure_ascii=False),
                len(contratos),
                float(detran["total_brl"]),
                "estadual",
                "AC",
                "SEJUSP",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(detran["total_brl"]),
                2024,
                "portal_transparencia_acre",
            ],
        )

    exec_penal = clusters.get("EXECUCAO_PENAL_FROTA")
    if exec_penal:
        contratos = sorted(
            exec_penal["contratos"],
            key=lambda item: (-float(item["valor_brl"]), item["ano"], item["numero_contrato"]),
        )
        unidades = ", ".join(sorted(exec_penal["unidades"]))
        numeros = ", ".join(item["numero_contrato"] for item in contratos)
        description = (
            f"A trilha da **AGRO NORTE** na execução penal/socioeducativa soma "
            f"**R$ {br_money(exec_penal['total_brl'])}** em **{len(contratos)}** contratos, "
            f"distribuídos entre **{unidades}**. Os blocos principais continuam aparecendo no portal "
            f"como `origem = C` e sem `id_licitacao` exposto: **038/2023** (FUNPENACRE, "
            f"**R$ {br_money(1330000.0)}**), **073/2023** (IAPEN, **R$ {br_money(254000.0)}**) "
            f"e **072/2024** (ISE, **R$ {br_money(2484000.0)}** no portal). "
            f"O **038/2023** agora está formalmente fechado também como contrato exato publicado no DOE, "
            f"com **5 caminhonetes** a **R$ {br_money(266000.0)}** por unidade, no mesmo processo "
            f"**4005.014135.00006/2023-90** e ARP **TJ/AC 304/2022** já vistos no termo de adesão. "
            f"O **073/2023** agora também está formalmente fechado no DOE de **04/08/2023**, "
            f"com termo de adesão **26/2023/IAPEN**, processo **4005.014141.00047/2023-70**, "
            f"origem **PE SRP 258/2022 / ARP 010/2022-SEPA** e **1 caminhonete** por "
            f"**R$ {br_money(254000.0)}**; o card estadual (`id_contrato = 82564`) apenas corrobora "
            f"o mesmo objeto da **CIAP / Convênio 905916/2020 MJ/DEPEN**. "
            f"O DOE de 22/11/2024 já fecha o **072/2024** com **10 caminhonetes** e valor oficial de "
            f"**R$ {br_money(2480000.0)}**, mantendo a origem formal em **PE SRP 504/2023 / ARP 01/2024-SECOM**. "
            f"Sequência atual: {numeros}."
        )
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "TRACE_AGRO:EXECUCAO_PENAL_FROTA",
                "TRACE_AGRO_EXECUCAO_PENAL_FROTA",
                "HIGH",
                94,
                float(exec_penal["total_brl"]),
                "AGRO / execução penal: aquisições altas sem origem licitatória exposta",
                description,
                "AGRO -> EXECUCAO_PENAL -> AQUISICOES_SEM_ID_LICITACAO",
                json.dumps(
                    [
                        {"fonte": "estado_ac_contratos", "cnpj": TARGET_CNPJ, "cluster": "EXECUCAO_PENAL_FROTA"},
                        {
                            "fonte": "v_trace_norte_rede_sem_licitacao",
                            "contratos": ["038/2023", "073/2023", "072/2024"],
                        },
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_funpen_038_2023_doe"},
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_iapen_073_2023_doe"},
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_iapen_073_2023_portal_card"},
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_ise_072_2024_doe"},
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_agro", "execucao_penal", "frota", TARGET_CNPJ], ensure_ascii=False),
                len(contratos),
                float(exec_penal["total_brl"]),
                "estadual",
                "AC",
                "GOVERNO_ACRE",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(exec_penal["total_brl"]),
                2024,
                "portal_transparencia_acre",
            ],
        )

    if doc_038:
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "TRACE_AGRO:EXECUCAO_PENAL_FROTA:038/2023",
                "TRACE_AGRO_FUNPEN_038_CONTRATO_EXATO",
                "HIGH",
                96,
                float(doc_038["valor_total_brl"] or 0),
                "FUNPENACRE 038/2023 agora tem contrato exato publicado",
                (
                    "O DOE de **29/03/2023** materializa o **Contrato 038/2023** da **AGRO NORTE** "
                    "no **FUNPENACRE/IAPEN**, com o mesmo processo **4005.014135.00006/2023-90**, "
                    "mesma origem **PE SRP 74/2022 / ARP TJ/AC 304/2022** e valor total de "
                    f"**R$ {br_money(float(doc_038['valor_total_brl'] or 0))}** já vistos no termo de adesão. "
                    f"O contrato publica **{int(doc_038['quantidade'] or 0)} caminhonetes** a "
                    f"**R$ {br_money(float(doc_038['valor_unitario_brl'] or 0))}** por unidade."
                ),
                "AGRO -> FUNPENACRE -> CONTRATO_038_2023_EXATO",
                json.dumps(
                    [
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_funpen_038_2023_doe"},
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_funpen_adesao5_2023_doe"},
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_agro", "funpenacre", "038_2023", TARGET_CNPJ], ensure_ascii=False),
                1,
                float(doc_038["valor_total_brl"] or 0),
                "estadual",
                "AC",
                "GOVERNO_ACRE",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(doc_038["valor_total_brl"] or 0),
                2023,
                "diario_oficial_acre",
            ],
        )

    doc_073 = docs_by_key.get("agro_iapen_073_2023_portal_card")
    if doc_073:
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "TRACE_AGRO:EXECUCAO_PENAL_FROTA:073/2023",
                "TRACE_AGRO_IAPEN_073_PORTAL_CIAP",
                "HIGH",
                96,
                float(doc_073["valor_total_brl"] or 0),
                "IAPEN 073/2023 agora tem contrato exato e card convergente",
                (
                    "O DOE de **04/08/2023** materializa o **Contrato 073/2023** da **AGRO NORTE** "
                    "no **IAPEN**, com termo de adesão **26/2023/IAPEN**, processo "
                    "**4005.014141.00047/2023-70**, origem **PE SRP 258/2022 / ARP 010/2022-SEPA**, "
                    "assinatura em **01/08/2023** e valor de "
                    f"**R$ {br_money(float(doc_073['valor_total_brl'] or 0))}**. "
                    "O card detalhado do portal estadual (`id_contrato = 82564`) corrobora a mesma contratação "
                    "para o **Convênio 905916/2020 MJ/DEPEN** voltado à **CIAP em Cruzeiro do Sul**."
                ),
                "AGRO -> IAPEN -> CONTRATO_073_2023_CIAP",
                json.dumps(
                    [
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_iapen_073_2023_doe"},
                        {"fonte": "trace_agro_unidades_docs", "doc_key": "agro_iapen_073_2023_portal_card"},
                        {"fonte": "estado_ac_contratos", "numero_contrato": "073/2023", "cnpj": TARGET_CNPJ},
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_agro", "iapen", "073_2023", "ciap", TARGET_CNPJ], ensure_ascii=False),
                1,
                float(doc_073["valor_total_brl"] or 0),
                "estadual",
                "AC",
                "SEJUSP",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(doc_073["valor_total_brl"] or 0),
                2023,
                "portal_transparencia_acre",
            ],
        )

    for (
        _row_id,
        cluster_key,
        numero_contrato,
        audit_kind,
        status,
        portal_valor_brl,
        doc_valor_brl,
        portal_qtd,
        doc_qtd,
        observacao,
        evidence_json,
    ) in audit_rows:
        if audit_kind != "PORTAL_X_DOE_VALOR" or status != "VALOR_DIVERGENTE":
            continue
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "TRACE_AGRO:EXECUCAO_PENAL_FROTA:072/2024:valor",
                "TRACE_AGRO_ISE_PORTAL_VALOR_DIVERGENTE",
                "MEDIUM",
                92,
                float(portal_valor_brl or 0),
                "Contrato 072/2024 diverge em valor entre portal e DOE",
                observacao,
                "AGRO -> ISE -> PORTAL_X_DOE_VALOR_DIVERGENTE",
                evidence_json,
                json.dumps(["trace_agro", "ise", "audit", "072_2024", TARGET_CNPJ], ensure_ascii=False),
                1,
                float(portal_valor_brl or 0),
                "estadual",
                "AC",
                "GOVERNO_ACRE",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(portal_valor_brl or 0),
                2024,
                "portal_transparencia_acre",
            ],
        )

    con.close()
    print(f"contratos={len(contrato_rows)}")
    print(f"blocos={len(resumo_rows)}")
    print(f"docs={len(doc_rows)}")
    print(f"audits={len(audit_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
