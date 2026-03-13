from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
TMP_DIR = ROOT / "data" / "tmp" / "sejusp_ctx"
FORMAL_DIR = ROOT / "data" / "tmp" / "sejusp_formal"
FOLLOWUP_DIR = ROOT / "data" / "tmp" / "sejusp_2023_followup"

DDL_BLOCOS = """
CREATE TABLE IF NOT EXISTS trace_norte_sejusp_blocos (
    row_id VARCHAR PRIMARY KEY,
    bloco_key VARCHAR,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    categoria VARCHAR,
    n_contratos INTEGER,
    total_brl DOUBLE,
    contratos_json JSON,
    ids_contrato_json JSON,
    objetos_json JSON,
    docs_relacionados_json JSON,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_DOCS = """
CREATE TABLE IF NOT EXISTS trace_norte_sejusp_docs (
    row_id VARCHAR PRIMARY KEY,
    doc_key VARCHAR,
    ano INTEGER,
    categoria VARCHAR,
    tipo_documento VARCHAR,
    fornecedor_nome VARCHAR,
    cnpj VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    numero_contrato VARCHAR,
    numero_adesao VARCHAR,
    processo VARCHAR,
    processo_sei VARCHAR,
    lic_numero VARCHAR,
    lic_modalidade VARCHAR,
    ata_registro_precos VARCHAR,
    valor_brl DOUBLE,
    objeto_resumo VARCHAR,
    source_url VARCHAR,
    local_pdf VARCHAR,
    local_txt VARCHAR,
    excerpt TEXT,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_AUDIT = """
CREATE TABLE IF NOT EXISTS trace_norte_sejusp_audit (
    row_id VARCHAR PRIMARY KEY,
    numero_contrato VARCHAR,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    portal_objeto TEXT,
    portal_valor_brl DOUBLE,
    portal_qty INTEGER,
    doc_key VARCHAR,
    doc_tipo VARCHAR,
    doc_objeto_resumo TEXT,
    doc_valor_brl DOUBLE,
    doc_qty INTEGER,
    status VARCHAR,
    observacao TEXT,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DOC_SOURCE_URLS = {
    "2020_norte_centro_sejusp_apoio": "https://agencia.ac.gov.br/wp-content/uploads/2020/11/DO16065240912588.pdf",
    "2023_jwc_sejusp_contrato33_aditivo": "https://agencia.ac.gov.br/wp-content/uploads/2024/01/DO17007103853693.pdf",
    "2025_norte_centro_sejusp_limpeza_aditivo": None,
    "doe_13931_2024-12-26": "https://diario.ac.gov.br/download.php?arquivo=KEQxQHI3IyEpRE8xNzM1MjE5ODIwMjE1NS5wZGY=",
    "doe_13816_2024-07-12": "https://diario.ac.gov.br/download.php?arquivo=KEQxQHI3IyEpRE8xNzIwODM3OTgwNzI4NS5wZGY=",
    "doe_13820_2024-07-17": "https://diario.ac.gov.br/download.php?arquivo=KEQxQHI3IyEpRE8xNzIxMjE3NjQ1MjkwMi5wZGY=",
    "doe_13883_2024-10-15": "https://diario.ac.gov.br/download.php?arquivo=KEQxQHI3IyEpRE8xNzI5MDQ0OTYyNzM3NC5wZGY=",
    "doe_13483_2023-03-01": "https://diario.ac.gov.br/download.php?arquivo=KEQxQHI3IyEpRE8xNjc3Njg4NzkyNDM5LnBkZg==",
    "doe_13638_2023-10-19": "https://diario.ac.gov.br/download.php?arquivo=KEQxQHI3IyEpRE8xNjk3NzE2ODg0NzgxOS5wZGY=",
}


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_space(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def parse_brl(value: object) -> float | None:
    raw = normalize_space(value)
    if not raw:
        return None
    raw = raw.replace(".", "").replace(",", ".")
    raw = re.sub(r"[^\d.]", "", raw)
    return float(raw) if raw else None


def detect_categoria(objetos: list[str], terceirizacao_pessoal: bool) -> str:
    text = normalize_space(" ".join(objetos)).lower()
    if terceirizacao_pessoal or "apoio operacional" in text or "apoio administrativo" in text:
        return "servico_terceirizado"
    if "limpeza" in text or "asseio" in text or "conserva" in text:
        return "servicos_limpeza"
    if "caminhonete" in text or "viatura" in text or "veiculo" in text or "veículo" in text:
        return "viaturas"
    return "outros"


def extract_qty(text: object) -> int | None:
    normalized = normalize_space(text)
    if not normalized:
        return None
    match = re.search(r"\b0*(\d{1,3})\s*\(", normalized)
    if match:
        return int(match.group(1))
    return None


def load_current_blocks(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    return con.execute(
        """
        SELECT
            cnpj,
            fornecedor_nome,
            orgao,
            unidade_gestora,
            COUNT(*) AS n_contratos,
            SUM(valor_brl) AS total_brl,
            LIST(numero_contrato ORDER BY valor_brl DESC, numero_contrato) AS contratos,
            LIST(id_contrato ORDER BY valor_brl DESC, numero_contrato) AS ids_contrato,
            LIST(lic_objeto ORDER BY valor_brl DESC, numero_contrato) AS objetos,
            BOOL_OR(COALESCE(terceirizacao_pessoal, FALSE)) AS terceirizacao_pessoal
        FROM trace_norte_rede_vinculo_exato
        WHERE orgao = 'SEJUSP'
          AND unidade_gestora = 'SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP'
          AND COALESCE(lic_numero, '') = ''
          AND COALESCE(id_licitacao, 0) = 0
          AND (
                fornecedor_nome ILIKE '%NORTE%CENTRO%'
             OR fornecedor_nome ILIKE '%AGRO NORTE%'
          )
        GROUP BY ALL
        ORDER BY total_brl DESC, fornecedor_nome
        """
    ).fetchall()


def parse_local_docs(text_path: Path) -> list[dict]:
    raw_text = text_path.read_text(encoding="utf-8", errors="ignore")
    text = normalize_space(raw_text)
    stem = text_path.stem

    if stem == "2020_norte_centro_sejusp_apoio":
        pattern = re.compile(
            r"EXTRATO DE TERMO DE ADESÃO N° 37/2020.*?EMPRESA: (?P<fornecedor>.+?)\."
            r".*?Pregão Eletrônico SRP n° (?P<lic_numero>023/2019).*?"
            r"Processo Nº (?P<processo>410\.012320\.03365/2020-91).*?"
            r"prestação de serviços de apoio administrativo, atendimento, logística e serviços operacionais.*?"
            r"valor total deste termo é de R\$ (?P<valor>[\d\.,]+)",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if not match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        excerpt = match.group(0)
        return [
            {
                "doc_key": "sejusp_2020_adesao_37_apoio",
                "ano": 2020,
                "categoria": "servico_terceirizado",
                "tipo_documento": "termo_adesao",
                "fornecedor_nome": normalize_space(match.group("fornecedor")).rstrip("."),
                "cnpj": "21813150000194",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "085/2020",
                "numero_adesao": "37/2020",
                "processo": match.group("processo"),
                "processo_sei": None,
                "lic_numero": match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": "004/2020",
                "valor_brl": parse_brl(match.group("valor")),
                "objeto_resumo": "Prestacao de servicos de apoio administrativo, atendimento, logistica e servicos operacionais para a SEJUSP.",
                "excerpt": excerpt,
            }
        ]

    if stem == "2023_jwc_sejusp_contrato33_aditivo":
        pattern = re.compile(
            r"PRIMEIRO TERMO ADITIVO AO CONTRATO Nº 33/2023.*?EMPRESA:\s*(?P<fornecedor>JWC MULTISERVIÇOS LTDA)\."
            r".*?Pregão Eletrônico SRP n° (?P<lic_numero>241/2021).*?"
            r"Processo SEI (?P<processo_sei>0819\.012783\.00026/2022-49).*?"
            r"serviço terceirizado e continuado de apoio operacional e administrativo, com disponibilização de mão de obra em regime de dedicação exclusiva.*?"
            r"valor mensal realinhado é de R\$ (?P<valor>[\d\.,]+)",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if not match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        excerpt = match.group(0)
        return [
            {
                "doc_key": "sejusp_2023_jwc_contrato33_aditivo",
                "ano": 2023,
                "categoria": "servico_terceirizado",
                "tipo_documento": "termo_aditivo",
                "fornecedor_nome": normalize_space(match.group("fornecedor")),
                "cnpj": None,
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "33/2023",
                "numero_adesao": None,
                "processo": None,
                "processo_sei": match.group("processo_sei"),
                "lic_numero": match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": "035/2022 - SESACRE",
                "valor_brl": parse_brl(match.group("valor")),
                "objeto_resumo": "Repactuacao do contrato de apoio operacional e administrativo com mao de obra em regime de dedicacao exclusiva.",
                "excerpt": excerpt,
            }
        ]

    if stem == "2025_norte_centro_sejusp_limpeza_aditivo":
        pattern = re.compile(
            r"NONO TERMO ADITIVO AO CONTRATO Nº 26/2021.*?EMPRESA (?P<fornecedor>NORTE-CENTRO DE DISTRIBUIÇÃO DE MERCADORIAS EM GERAL LTDA)\."
            r".*?Pregão Eletrônico SRP n° (?P<lic_numero>154/2019).*?"
            r"Processo Nº (?P<processo>0019411-7/2019).*?"
            r"PROCESSO SEI: (?P<processo_sei>0819\.012806\.00011/2024-09).*?"
            r"prestação de serviços de limpeza de prédio, mobiliários e equipamentos.*?atender as necessidades da Secretaria de Estado de Justiça e Segurança Pública-SEJUSP, em Rio Branco/AC",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if not match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        excerpt = match.group(0)
        return [
            {
                "doc_key": "sejusp_2025_norte_centro_contrato26_aditivo",
                "ano": 2025,
                "categoria": "servicos_limpeza",
                "tipo_documento": "termo_aditivo",
                "fornecedor_nome": normalize_space(match.group("fornecedor")),
                "cnpj": "21813150000194",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "26/2021",
                "numero_adesao": None,
                "processo": match.group("processo"),
                "processo_sei": match.group("processo_sei"),
                "lic_numero": match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": None,
                "valor_brl": None,
                "objeto_resumo": "Prorrogacao de contrato de limpeza de predio, mobiliarios e equipamentos com disponibilizacao de mao de obra.",
                "excerpt": excerpt,
            }
        ]

    if stem == "doe_13931_2024-12-26":
        start = text.find("PRIMEIRO TERMO ADITIVO AO CONTRATO 76/2024")
        if start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        end = text.find("ESTADO DO ACRE SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA", start)
        if end < 0:
            end = start + 2500
        excerpt = text[start:end]
        arp_match = re.search(r"ATA DE REGISTRO DE PREÇOS Nº (?P<arp>04/2024)", excerpt, re.IGNORECASE)
        lic_match = re.search(
            r"PREGÃO PRESENCIAL PARA REGISTRO DE PREÇOS Nº (?P<lic_numero>053/2023)",
            excerpt,
            re.IGNORECASE,
        )
        processo_match = re.search(r"PROCESSO Nº (?P<processo_sei>0819\.014451\.00277/2024-18)", excerpt, re.IGNORECASE)
        valor_total_match = re.search(r"R\$ (?P<valor_total>1\.388\.780,92)", excerpt, re.IGNORECASE)
        if not (arp_match and lic_match and processo_match and valor_total_match):
            raise RuntimeError(f"Campos esperados nao encontrados em {text_path.name}")
        return [
            {
                "doc_key": "sejusp_2024_norte_centro_contrato76_aditivo",
                "ano": 2024,
                "categoria": "servico_terceirizado",
                "tipo_documento": "termo_aditivo",
                "fornecedor_nome": "NORTE - CENTRO DE DISTRIBUIÇÃO DE MERCADORIAS EM GERAL LTDA",
                "cnpj": "21813150000194",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "076/2024",
                "numero_adesao": None,
                "processo": None,
                "processo_sei": processo_match.group("processo_sei"),
                "lic_numero": lic_match.group("lic_numero"),
                "lic_modalidade": "PREGAO_PRESENCIAL_SRP",
                "ata_registro_precos": arp_match.group("arp"),
                "valor_brl": parse_brl(valor_total_match.group("valor_total")),
                "objeto_resumo": "Primeiro termo aditivo do contrato 076/2024, ligando o bloco de apoio operacional/administrativo a ARP 04/2024 e ao Pregao Presencial 053/2023.",
                "excerpt": excerpt,
            }
        ]

    if stem == "doe_13816_2024-07-12":
        start = text.find("Contrato n° 082/2024")
        if start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        end = text.find("SISTEMA INTEGRADO DE SEGURANÇA PÚBLICA", start)
        if end < 0:
            end = start + 1600
        excerpt = text[start:end + len("SISTEMA INTEGRADO DE SEGURANÇA PÚBLICA")]
        processo_match = re.search(r"Processo SEI nº (?P<processo_sei>0819\.016417\.00053/2024-94)", excerpt, re.IGNORECASE)
        contrato_match = re.search(r"Contrato n° (?P<numero_contrato>082/2024)", excerpt, re.IGNORECASE)
        if not (processo_match and contrato_match and "AGRO NORTE" in excerpt and "caminhonete" in excerpt.lower()):
            raise RuntimeError(f"Campos esperados nao encontrados em {text_path.name}")
        return [
            {
                "doc_key": "sejusp_2024_agro_portaria82",
                "ano": 2024,
                "categoria": "viaturas",
                "tipo_documento": "portaria_gestor_fiscal",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": contrato_match.group("numero_contrato"),
                "numero_adesao": None,
                "processo": None,
                "processo_sei": processo_match.group("processo_sei"),
                "lic_numero": None,
                "lic_modalidade": None,
                "ata_registro_precos": None,
                "valor_brl": None,
                "objeto_resumo": "Portaria de gestao e fiscalizacao do contrato 082/2024 para aquisicao de 3 caminhonetes destinadas ao patrulhamento velado da SEJUSP.",
                "excerpt": excerpt,
            }
        ]

    if stem == "doe_13820_2024-07-17":
        contrato_start = text.find("EXTRATO DE CONTRATO N° 82/2024")
        if contrato_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        contrato_end = text.find("Programa de Trabalho:", contrato_start)
        if contrato_end < 0:
            contrato_end = contrato_start + 1800
        contrato_excerpt = text[contrato_start:contrato_end]
        contrato_match = re.search(r"EXTRATO DE CONTRATO N° (?P<numero_contrato>82/2024)", contrato_excerpt, re.IGNORECASE)
        contrato_valor_match = re.search(r"DO VALOR: O valor total do presente contrato é de R\$ (?P<valor>733\.491,00)", contrato_excerpt, re.IGNORECASE)
        if not (contrato_match and contrato_valor_match and "AGRO NORTE" in contrato_excerpt and "caminhonete" in contrato_excerpt.lower()):
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        adesao_start = text.find("TERMO DE ADESÃO Nº 33/2024")
        if adesao_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        adesao_end = text.find("Programa de Trabalho:", adesao_start)
        if adesao_end < 0:
            adesao_end = adesao_start + 1800
        adesao_excerpt = text[adesao_start:adesao_end]
        adesao_match = re.search(
            r"TERMO DE ADESÃO Nº (?P<numero_adesao>33/2024).*?"
            r"Ata de Registro de Preços nº (?P<arp>49/2023).*?"
            r"Pregão Eletrônico para Registro de Preços nº (?P<lic_numero>206/2023).*?"
            r"Processo nº (?P<processo_sei>0819\.016417\.00053/2024-94).*?"
            r"DO VALOR: O valor total da presente adesão é de R\$ (?P<valor>733\.491,00)",
            adesao_excerpt,
            re.IGNORECASE,
        )
        if not adesao_match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        return [
            {
                "doc_key": "sejusp_2024_agro_contrato82",
                "ano": 2024,
                "categoria": "viaturas",
                "tipo_documento": "extrato_contrato",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": contrato_match.group("numero_contrato"),
                "numero_adesao": None,
                "processo": None,
                "processo_sei": "0819.016417.00053/2024-94",
                "lic_numero": None,
                "lic_modalidade": None,
                "ata_registro_precos": None,
                "valor_brl": parse_brl(contrato_valor_match.group("valor")),
                "objeto_resumo": "Extrato do contrato 82/2024 para aquisicao de 3 caminhonetes destinadas ao patrulhamento velado da SEJUSP.",
                "excerpt": contrato_excerpt,
            },
            {
                "doc_key": "sejusp_2024_agro_adesao33",
                "ano": 2024,
                "categoria": "viaturas",
                "tipo_documento": "termo_adesao",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "082/2024",
                "numero_adesao": adesao_match.group("numero_adesao"),
                "processo": None,
                "processo_sei": adesao_match.group("processo_sei"),
                "lic_numero": adesao_match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": adesao_match.group("arp"),
                "valor_brl": parse_brl(adesao_match.group("valor")),
                "objeto_resumo": "Termo de adesao 33/2024 que amarra o contrato 82/2024 a ARP 49/2023 e ao PE 206/2023.",
                "excerpt": adesao_excerpt,
            },
        ]

    if stem == "doe_13883_2024-10-15":
        contrato147_start = text.find("EXTRATO CONTRATO N° 147/2024")
        if contrato147_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        contrato147_end = text.find("EXTRATO DO CONTRATO N° 144/2024", contrato147_start)
        if contrato147_end < 0:
            contrato147_end = contrato147_start + 2200
        contrato147_excerpt = text[contrato147_start:contrato147_end]
        contrato147_match = re.search(
            r"EXTRATO CONTRATO N° (?P<numero_contrato>147/2024).*?"
            r"ATA DE REGISTRO DE PREÇOS Nº (?P<arp>49/2023).*?"
            r"PREGÃO ELETRÔNICO PARA REGISTRO DE PREÇOS Nº (?P<lic_numero>206/2023).*?"
            r"PROCESSO Nº (?P<processo>0064\.014914\.00006/2024-48).*?"
            r"DO VALOR [–-] O valor deste contrato é de R\$ (?P<valor>977\.988,00)",
            contrato147_excerpt,
            re.IGNORECASE,
        )
        if not contrato147_match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        adesao69_start = text.find("TERMO DE ADESÃO N° 69/2024")
        if adesao69_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        adesao69_end = text.find("SEMA PORTARIA", adesao69_start)
        if adesao69_end < 0:
            adesao69_end = adesao69_start + 1800
        adesao69_excerpt = text[adesao69_start:adesao69_end]
        adesao69_match = re.search(
            r"TERMO DE ADESÃO N° (?P<numero_adesao>69/2024).*?"
            r"ATA DE REGISTRO DE PREÇOS Nº (?P<arp>49/2023).*?"
            r"PREGÃO ELETRÔNICO PARA REGISTRO DE PREÇOS Nº (?P<lic_numero>206/2023).*?"
            r"PROCESSO Nº (?P<processo>0064\.014914\.00006/2024-48).*?"
            r"DO VALOR: O valor total do presente Termo é de R\$ (?P<valor>2\.200\.473,00)",
            adesao69_excerpt,
            re.IGNORECASE,
        )
        if not adesao69_match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        portaria149_start = text.find("Contrato 149/2024")
        if portaria149_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        portaria149_end = text.find("Art. 2º Compete aos gestores", portaria149_start)
        if portaria149_end < 0:
            portaria149_end = portaria149_start + 1600
        portaria149_excerpt = text[portaria149_start:portaria149_end]
        portaria149_match = re.search(
            r"Contrato 149/2024, Processo SEI .*?0819\.012834\.00145/2024-93.*?"
            r"AGRO NORTE IMPORTAÇÃO DE EX-.*?PORTAÇÃO LTDA.*?"
            r"04\.582\.979/0001-04.*?"
            r"veículos tipo caminhonete.*?"
            r"Fundo Nacional de Segurança Pública",
            portaria149_excerpt,
            re.IGNORECASE,
        )
        if not portaria149_match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        return [
            {
                "doc_key": "sejusp_2024_agro_contrato147",
                "ano": 2024,
                "categoria": "viaturas",
                "tipo_documento": "extrato_contrato",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": contrato147_match.group("numero_contrato"),
                "numero_adesao": None,
                "processo": contrato147_match.group("processo"),
                "processo_sei": None,
                "lic_numero": contrato147_match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": contrato147_match.group("arp"),
                "valor_brl": parse_brl(contrato147_match.group("valor")),
                "objeto_resumo": "Extrato do contrato 147/2024 para aquisicao de 4 caminhonetes destinadas a PCAC com recursos do ECV 2020.",
                "excerpt": contrato147_excerpt,
            },
            {
                "doc_key": "sejusp_2024_agro_adesao69",
                "ano": 2024,
                "categoria": "viaturas",
                "tipo_documento": "termo_adesao",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": None,
                "numero_adesao": adesao69_match.group("numero_adesao"),
                "processo": adesao69_match.group("processo"),
                "processo_sei": None,
                "lic_numero": adesao69_match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": adesao69_match.group("arp"),
                "valor_brl": parse_brl(adesao69_match.group("valor")),
                "objeto_resumo": "Termo de adesao 69/2024 para aquisicao de 9 caminhonetes destinadas a PCAC com base na mesma ARP 49/2023 e PE 206/2023.",
                "excerpt": adesao69_excerpt,
            },
            {
                "doc_key": "sejusp_2024_agro_portaria149",
                "ano": 2024,
                "categoria": "viaturas",
                "tipo_documento": "portaria_gestor_fiscal",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "149/2024",
                "numero_adesao": None,
                "processo": None,
                "processo_sei": "0819.012834.00145/2024-93",
                "lic_numero": None,
                "lic_modalidade": None,
                "ata_registro_precos": None,
                "valor_brl": None,
                "objeto_resumo": "Portaria de gestao e fiscalizacao do contrato 149/2024, ligada a aquisicao de caminhonetes com recursos do Fundo Nacional de Seguranca Publica.",
                "excerpt": portaria149_excerpt,
            },
        ]

    if stem == "doe_13483_2023-03-01":
        start = text.find("EXTRATO DE CONTRATO N° 04/2023")
        if start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        end = text.find("SEOP", start)
        if end < 0:
            end = start + 1600
        excerpt = text[start:end]
        match = re.search(
            r"EXTRATO DE CONTRATO N° 04/2023.*?"
            r"AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA.*?"
            r"PREGÃO ELETRÔNICO SRP Nº (?P<lic_numero>318/2022).*?"
            r"ATA DE REGISTRO DE PREÇO Nº (?P<arp>008/2022).*?"
            r"PROCESSO Nº (?P<processo>0853\.013719\.00030/2022-18).*?"
            r"aquisição de 01 \(um\) veículo utilitário \(tipo caminhonete\).*?"
            r"Núcleo de Apoio ao Servidor Penitenciário \(NASP\).*?"
            r"DO VALOR: O valor total deste contrato é de R\$ (?P<valor>250\.000,00)",
            excerpt,
            re.IGNORECASE,
        )
        if not match:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        return [
            {
                "doc_key": "sejusp_2023_agro_contrato004",
                "ano": 2023,
                "categoria": "viaturas",
                "tipo_documento": "extrato_contrato",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "004/2023",
                "numero_adesao": None,
                "processo": match.group("processo"),
                "processo_sei": None,
                "lic_numero": match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": match.group("arp"),
                "valor_brl": parse_brl(match.group("valor")),
                "objeto_resumo": "Extrato do contrato 004/2023 para aquisicao de 1 veiculo utilitario tipo caminhonete destinado ao NASP, com base na ARP 008/2022 e PE 318/2022 - SEPA.",
                "excerpt": excerpt,
            }
        ]

    if stem == "doe_13638_2023-10-19":
        portaria_start = text.find("PORTARIA SEJUSP Nº 557, DE 27 DE SETEMBRO DE 2023")
        if portaria_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        portaria_end = text.find("José Américo de Souza Gaia Secretário de Estado de Justiça e Segurança Pública", portaria_start)
        if portaria_end < 0:
            portaria_end = portaria_start + 2000
        portaria_excerpt = text[portaria_start:portaria_end]
        portaria_processo_match = re.search(r"0819\.012881\.00036/2023-48", portaria_excerpt, re.IGNORECASE)
        if not (
            portaria_processo_match
            and "AGRONORTE" in portaria_excerpt
            and "CIEPS" in portaria_excerpt
            and "aquisição de 01 (um)" in portaria_excerpt
        ):
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        extrato_start = text.find("EXTRATO DE CONTRATO N° 151/2023")
        if extrato_start < 0:
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        extrato_end = text.find("SEMA", extrato_start)
        if extrato_end < 0:
            extrato_end = extrato_start + 2200
        extrato_excerpt = text[extrato_start:extrato_end]
        lic_match = re.search(r"PREGÃO ELETRÔNICO SRP Nº (?P<lic_numero>318/2022)", extrato_excerpt, re.IGNORECASE)
        arp_match = re.search(r"ATA DE REGISTRO DE PREÇO Nº (?P<arp>008/2022)", extrato_excerpt, re.IGNORECASE)
        processo_match = re.search(r"PROCESSO Nº (?P<processo>0819\.012805\.00066/2022-40)", extrato_excerpt, re.IGNORECASE)
        valor_match = re.search(r"R\$.*?(?P<valor>250\.000,00)", extrato_excerpt, re.IGNORECASE)
        if not (
            lic_match
            and arp_match
            and processo_match
            and valor_match
            and "AGRO NOR" in extrato_excerpt
            and "CIEPS" in extrato_excerpt
            and "aquisição de 01 (um)" in extrato_excerpt
        ):
            raise RuntimeError(f"Trecho esperado nao encontrado em {text_path.name}")
        return [
            {
                "doc_key": "sejusp_2023_agro_portaria151",
                "ano": 2023,
                "categoria": "viaturas",
                "tipo_documento": "portaria_gestor_fiscal",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "151/2023",
                "numero_adesao": None,
                "processo": None,
                "processo_sei": portaria_processo_match.group(0),
                "lic_numero": None,
                "lic_modalidade": None,
                "ata_registro_precos": None,
                "valor_brl": None,
                "objeto_resumo": "Portaria de gestao e fiscalizacao do contrato 151/2023, apontando a aquisicao de 1 veiculo utilitario tipo caminhonete para o CIEPS.",
                "excerpt": portaria_excerpt,
            },
            {
                "doc_key": "sejusp_2023_agro_contrato151",
                "ano": 2023,
                "categoria": "viaturas",
                "tipo_documento": "extrato_contrato",
                "fornecedor_nome": "AGRO NORTE IMPORTAÇÃO E EXPORTAÇÃO LTDA",
                "cnpj": "04582979000104",
                "orgao": "SEJUSP",
                "unidade_gestora": "SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP",
                "numero_contrato": "151/2023",
                "numero_adesao": None,
                "processo": processo_match.group("processo"),
                "processo_sei": None,
                "lic_numero": lic_match.group("lic_numero"),
                "lic_modalidade": "PREGAO_ELETRONICO_SRP",
                "ata_registro_precos": arp_match.group("arp"),
                "valor_brl": parse_brl(valor_match.group("valor")),
                "objeto_resumo": "Extrato do contrato 151/2023 para aquisicao de 1 veiculo utilitario tipo caminhonete destinado ao CIEPS, com base na ARP 008/2022 e PE 318/2022 - SEPA.",
                "excerpt": extrato_excerpt,
            },
        ]

    raise RuntimeError(f"Arquivo nao mapeado: {text_path.name}")


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL_BLOCOS)
    con.execute(DDL_DOCS)
    con.execute(DDL_AUDIT)

    blocks = load_current_blocks(con)
    docs = []
    sources = [
        (TMP_DIR, "2020_norte_centro_sejusp_apoio.txt"),
        (TMP_DIR, "2023_jwc_sejusp_contrato33_aditivo.txt"),
        (TMP_DIR, "2025_norte_centro_sejusp_limpeza_aditivo.txt"),
        (FORMAL_DIR, "doe_13931_2024-12-26.txt"),
        (FORMAL_DIR, "doe_13816_2024-07-12.txt"),
        (FORMAL_DIR, "doe_13820_2024-07-17.txt"),
        (FORMAL_DIR, "doe_13883_2024-10-15.txt"),
        (FOLLOWUP_DIR, "doe_13483_2023-03-01.txt"),
        (FOLLOWUP_DIR, "doe_13638_2023-10-19.txt"),
    ]
    for base_dir, name in sources:
        text_path = base_dir / name
        pdf_path = base_dir / f"{Path(name).stem}.pdf"
        for parsed in parse_local_docs(text_path):
            parsed["source_url"] = DOC_SOURCE_URLS.get(Path(name).stem)
            parsed["local_pdf"] = str(pdf_path.relative_to(ROOT)) if pdf_path.exists() else None
            parsed["local_txt"] = str(text_path.relative_to(ROOT))
            docs.append(parsed)

    con.execute("DELETE FROM trace_norte_sejusp_blocos")
    bloco_rows = []
    for row in blocks:
        cnpj, fornecedor_nome, orgao, unidade_gestora, n_contratos, total_brl, contratos, ids_contrato, objetos, terceirizacao_pessoal = row
        categoria = detect_categoria([str(x or "") for x in objetos or []], bool(terceirizacao_pessoal))
        docs_relacionados = []
        if clean_doc(cnpj) == "21813150000194":
            docs_relacionados = [
                "sejusp_2020_adesao_37_apoio",
                "sejusp_2023_jwc_contrato33_aditivo",
                "sejusp_2024_norte_centro_contrato76_aditivo",
                "sejusp_2025_norte_centro_contrato26_aditivo",
            ]
        elif clean_doc(cnpj) == "04582979000104":
            docs_relacionados = [
                "sejusp_2023_agro_contrato004",
                "sejusp_2023_agro_portaria151",
                "sejusp_2023_agro_contrato151",
                "sejusp_2024_agro_portaria82",
                "sejusp_2024_agro_contrato82",
                "sejusp_2024_agro_adesao33",
                "sejusp_2024_agro_contrato147",
                "sejusp_2024_agro_adesao69",
                "sejusp_2024_agro_portaria149",
            ]
        evidence = {
            "contratos": [str(x) for x in contratos or []],
            "ids_contrato": [int(x) for x in ids_contrato or []],
            "objetos": [normalize_space(x) for x in objetos or []],
            "docs_relacionados": docs_relacionados,
        }
        bloco_rows.append(
            (
                row_hash("trace_norte_sejusp_blocos", cnpj, unidade_gestora),
                f"{clean_doc(cnpj)}:{categoria}",
                clean_doc(cnpj),
                fornecedor_nome,
                orgao,
                unidade_gestora,
                categoria,
                int(n_contratos or 0),
                float(total_brl or 0),
                json.dumps([str(x) for x in contratos or []], ensure_ascii=False),
                json.dumps([int(x) for x in ids_contrato or []], ensure_ascii=False),
                json.dumps([normalize_space(x) for x in objetos or []], ensure_ascii=False),
                json.dumps(docs_relacionados, ensure_ascii=False),
                json.dumps(evidence, ensure_ascii=False),
            )
        )
    if bloco_rows:
        con.executemany(
            """
            INSERT INTO trace_norte_sejusp_blocos (
                row_id, bloco_key, cnpj, fornecedor_nome, orgao, unidade_gestora, categoria,
                n_contratos, total_brl, contratos_json, ids_contrato_json, objetos_json,
                docs_relacionados_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            bloco_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_sejusp_blocos AS
        SELECT *
        FROM trace_norte_sejusp_blocos
        ORDER BY total_brl DESC, fornecedor_nome
        """
    )

    con.execute("DELETE FROM trace_norte_sejusp_docs")
    doc_rows = []
    for doc in docs:
        doc_rows.append(
            (
                row_hash("trace_norte_sejusp_docs", doc["doc_key"]),
                doc["doc_key"],
                int(doc["ano"]),
                doc["categoria"],
                doc["tipo_documento"],
                doc["fornecedor_nome"],
                clean_doc(doc["cnpj"]),
                doc["orgao"],
                doc["unidade_gestora"],
                doc["numero_contrato"],
                doc["numero_adesao"],
                doc["processo"],
                doc["processo_sei"],
                doc["lic_numero"],
                doc["lic_modalidade"],
                doc["ata_registro_precos"],
                doc["valor_brl"],
                doc["objeto_resumo"],
                doc["source_url"],
                doc["local_pdf"],
                doc["local_txt"],
                doc["excerpt"],
                json.dumps(
                    {
                        "doc_key": doc["doc_key"],
                        "fornecedor_nome": doc["fornecedor_nome"],
                        "source_url": doc["source_url"],
                    },
                    ensure_ascii=False,
                ),
            )
        )
    if doc_rows:
        con.executemany(
            """
            INSERT INTO trace_norte_sejusp_docs (
                row_id, doc_key, ano, categoria, tipo_documento, fornecedor_nome, cnpj, orgao,
                unidade_gestora, numero_contrato, numero_adesao, processo, processo_sei,
                lic_numero, lic_modalidade, ata_registro_precos, valor_brl, objeto_resumo,
                source_url, local_pdf, local_txt, excerpt, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            doc_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_sejusp_docs AS
        SELECT *
        FROM trace_norte_sejusp_docs
        ORDER BY ano, doc_key
        """
    )

    con.execute("DELETE FROM trace_norte_sejusp_audit")
    audit_docs = {
        doc["numero_contrato"]: doc
        for doc in docs
        if doc["doc_key"] in {"sejusp_2023_agro_contrato004", "sejusp_2023_agro_contrato151"}
    }
    portal_rows = con.execute(
        """
        SELECT numero_contrato, fornecedor_nome, cnpj, valor_brl, raw_contrato_json
        FROM trace_norte_rede_vinculo_exato
        WHERE orgao = 'SEJUSP'
          AND cnpj = '04582979000104'
          AND numero_contrato IN ('004/2023', '151/2023')
        ORDER BY numero_contrato
        """
    ).fetchall()
    audit_rows = []
    for numero_contrato, fornecedor_nome, cnpj, valor_brl, raw_contrato_json in portal_rows:
        raw = json.loads(str(raw_contrato_json))
        doc = audit_docs.get(str(numero_contrato))
        if not doc:
            continue
        portal_objeto = normalize_space(raw.get("objeto"))
        portal_qty = extract_qty(portal_objeto)
        doc_qty = extract_qty(doc.get("excerpt") or doc.get("objeto_resumo"))
        status = "CONSISTENTE"
        observacao = "Portal e DOE mantem a mesma ordem de grandeza no objeto publicado."
        if portal_qty is not None and doc_qty is not None and portal_qty != doc_qty:
            status = "QUANTIDADE_DIVERGENTE"
            observacao = (
                f"Portal publica quantidade {portal_qty} para o contrato {numero_contrato}, "
                f"mas o DOE oficial aponta quantidade {doc_qty}."
            )
        audit_rows.append(
            (
                row_hash("trace_norte_sejusp_audit", numero_contrato),
                numero_contrato,
                clean_doc(cnpj),
                fornecedor_nome,
                portal_objeto,
                float(valor_brl or 0),
                portal_qty,
                doc["doc_key"],
                doc["tipo_documento"],
                doc["objeto_resumo"],
                doc["valor_brl"],
                doc_qty,
                status,
                observacao,
                json.dumps(
                    {
                        "numero_contrato": numero_contrato,
                        "portal_objeto": portal_objeto,
                        "portal_valor_brl": float(valor_brl or 0),
                        "doc_key": doc["doc_key"],
                        "source_url": doc["source_url"],
                    },
                    ensure_ascii=False,
                ),
            )
        )
    if audit_rows:
        con.executemany(
            """
            INSERT INTO trace_norte_sejusp_audit (
                row_id, numero_contrato, cnpj, fornecedor_nome, portal_objeto,
                portal_valor_brl, portal_qty, doc_key, doc_tipo, doc_objeto_resumo,
                doc_valor_brl, doc_qty, status, observacao, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            audit_rows,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_sejusp_audit AS
        SELECT *
        FROM trace_norte_sejusp_audit
        ORDER BY status DESC, numero_contrato
        """
    )

    con.execute(
        """
        DELETE FROM insight
        WHERE kind IN (
            'TRACE_NORTE_SEJUSP_TERCEIRIZACAO_SEM_ID_LICITACAO',
            'TRACE_NORTE_SEJUSP_VIATURAS_RASTRO_DOE',
            'TRACE_NORTE_SEJUSP_PORTAL_OBJETO_DIVERGENTE'
        )
        """
    )
    norte_block = con.execute(
        """
        SELECT cnpj, fornecedor_nome, unidade_gestora, total_brl, contratos_json
        FROM v_trace_norte_sejusp_blocos
        WHERE categoria = 'servico_terceirizado'
        ORDER BY total_brl DESC
        LIMIT 1
        """
    ).fetchone()
    if norte_block:
        cnpj, fornecedor_nome, unidade_gestora, total_brl, contratos_json = norte_block
        contratos = json.loads(str(contratos_json))
        description = (
            f"O portal estadual materializa o bloco atual de **{fornecedor_nome}** na unidade **{unidade_gestora}** "
            f"como **servico terceirizado/apoio operacional** somando **R$ {float(total_brl):,.2f}**, "
            f"com contratos {', '.join(contratos)} e sem `id_licitacao` exposto para o contrato atual. "
            f"O DOE de 26/12/2024 ja liga formalmente o contrato **076/2024** a **ARP 04/2024**, "
            f"ao **Pregao Presencial 053/2023** e ao processo **0819.014451.00277/2024-18**, "
            f"além da linha do tempo local ja congelada de 2020 a 2025."
        ).replace(",", "X", 1).replace(".", ",").replace("X", ".")
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"TRACE_NORTE_SEJUSP:{clean_doc(cnpj)}:terceirizacao",
                "TRACE_NORTE_SEJUSP_TERCEIRIZACAO_SEM_ID_LICITACAO",
                "HIGH",
                95,
                float(total_brl or 0),
                "Bloco SEJUSP de terceirizacao aparece sem id_licitacao exposto",
                description,
                "SEJUSP -> SERVICO_TERCERIZADO_SEM_ID_LICITACAO",
                json.dumps(
                    [
                        {"fonte": "portal_contratos", "fornecedor": fornecedor_nome, "cnpj": cnpj},
                        {
                            "fonte": "docs_publicos_sejusp",
                            "doc_keys": [
                                "sejusp_2020_adesao_37_apoio",
                                "sejusp_2023_jwc_contrato33_aditivo",
                                "sejusp_2024_norte_centro_contrato76_aditivo",
                                "sejusp_2025_norte_centro_contrato26_aditivo",
                            ],
                        },
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_norte", "sejusp", "terceirizacao", cnpj], ensure_ascii=False),
                len(contratos),
                float(total_brl or 0),
                "estadual",
                "AC",
                "SEJUSP",
                None,
                "AC",
                "administracao",
                False,
                float(total_brl or 0),
                2024,
                "portal_transparencia_acre",
            ],
        )

    agro_block = con.execute(
        """
        SELECT cnpj, fornecedor_nome, unidade_gestora, total_brl, contratos_json
        FROM v_trace_norte_sejusp_blocos
        WHERE categoria = 'viaturas'
        ORDER BY total_brl DESC
        LIMIT 1
        """
    ).fetchone()
    if agro_block:
        cnpj, fornecedor_nome, unidade_gestora, total_brl, contratos_json = agro_block
        contratos = json.loads(str(contratos_json))
        description = (
            f"O portal estadual materializa o bloco atual de **{fornecedor_nome}** na unidade **{unidade_gestora}** "
            f"como **aquisicao de viaturas/caminhonetes** somando **R$ {float(total_brl):,.2f}**, "
            f"com contratos {', '.join(contratos)} e sem `id_licitacao` exposto no card atual. "
            f"Mesmo assim, o DOE de 2024 ja fecha parte relevante do rastro documental: "
            f"contrato **082/2024** no processo **0819.016417.00053/2024-94**, "
            f"contrato **147/2024** e adesao **69/2024** na **ARP 49/2023 / PE 206/2023**, "
            f"além da portaria do contrato **149/2024** com recursos do **Fundo Nacional de Seguranca Publica**."
        ).replace(",", "X", 1).replace(".", ",").replace("X", ".")
        con.execute(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"TRACE_NORTE_SEJUSP:{clean_doc(cnpj)}:viaturas",
                "TRACE_NORTE_SEJUSP_VIATURAS_RASTRO_DOE",
                "HIGH",
                94,
                float(total_brl or 0),
                "Bloco SEJUSP de viaturas aparece sem id_licitacao exposto, mas DOE revela o rastro formal",
                description,
                "SEJUSP -> VIATURAS_SEM_ID_LICITACAO_COM_RASTRO_DOE",
                json.dumps(
                    [
                        {"fonte": "portal_contratos", "fornecedor": fornecedor_nome, "cnpj": cnpj},
                        {
                            "fonte": "doe",
                            "doc_keys": [
                                "sejusp_2023_agro_contrato004",
                                "sejusp_2023_agro_portaria151",
                                "sejusp_2023_agro_contrato151",
                                "sejusp_2024_agro_portaria82",
                                "sejusp_2024_agro_contrato82",
                                "sejusp_2024_agro_adesao33",
                                "sejusp_2024_agro_contrato147",
                                "sejusp_2024_agro_adesao69",
                                "sejusp_2024_agro_portaria149",
                            ],
                        },
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_norte", "sejusp", "viaturas", cnpj], ensure_ascii=False),
                len(contratos),
                float(total_brl or 0),
                "estadual",
                "AC",
                "SEJUSP",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(total_brl or 0),
                2024,
                "portal_transparencia_acre",
            ],
        )

    for (
        _row_id,
        numero_contrato,
        cnpj,
        fornecedor_nome,
        portal_objeto,
        portal_valor_brl,
        portal_qty,
        doc_key,
        doc_tipo,
        doc_objeto_resumo,
        doc_valor_brl,
        doc_qty,
        status,
        observacao,
        evidence_json,
    ) in audit_rows:
        if status != "QUANTIDADE_DIVERGENTE":
            continue
        description = (
            f"O contrato **{numero_contrato}** aparece no portal estadual com objeto compatível com **{portal_qty}** unidades, "
            f"enquanto o DOE oficial do mesmo contrato materializa **{doc_qty}** unidade. "
            f"Portal: `{portal_objeto}`. DOE: `{doc_objeto_resumo}`."
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
                f"TRACE_NORTE_SEJUSP_AUDIT:{numero_contrato}",
                "TRACE_NORTE_SEJUSP_PORTAL_OBJETO_DIVERGENTE",
                "HIGH",
                97,
                float(portal_valor_brl or 0),
                f"Contrato {numero_contrato} diverge entre portal e DOE oficial",
                description,
                "SEJUSP -> PORTAL_X_DOE_DIVERGENTE",
                json.dumps(
                    [
                        {"fonte": "portal_contratos", "numero_contrato": numero_contrato, "cnpj": cnpj},
                        {"fonte": "doe", "doc_key": doc_key, "doc_tipo": doc_tipo},
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(["trace_norte", "sejusp", "audit", numero_contrato, cnpj], ensure_ascii=False),
                1,
                float(portal_valor_brl or 0),
                "estadual",
                "AC",
                "SEJUSP",
                None,
                "AC",
                "seguranca_publica",
                False,
                float(portal_valor_brl or 0),
                2023,
                "portal_transparencia_acre",
            ],
        )

    con.close()
    print(f"blocos={len(bloco_rows)}")
    print(f"docs={len(doc_rows)}")
    print(f"audit={len(audit_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
