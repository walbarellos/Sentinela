from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.transparencia_ac_connector import TransparenciaAcConnector

DOE_URL = "https://contilnetnoticias.com.br/wp-content/uploads/2023/12/DO17032527167318.pdf"
DOE_PDF = ROOT / "data" / "tmp" / "pp053" / "doe_homologacao_pp053_2023.pdf"

DDL_AUDIT = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_pp053_audit (
    row_id VARCHAR PRIMARY KEY,
    lic_ano INTEGER,
    lic_numero VARCHAR,
    lic_processo_adm VARCHAR,
    lic_modalidade VARCHAR,
    vencedor_doe_nome VARCHAR,
    vencedor_doe_cnpj VARCHAR,
    valor_homologado_brl DOUBLE,
    contracts_cnpj VARCHAR,
    contracts_fornecedor VARCHAR,
    contracts_n INTEGER,
    contracts_total_brl DOUBLE,
    contracts_ids_json JSON,
    aditivos_json JSON,
    delta_brl DOUBLE,
    winner_is_known_lead BOOLEAN,
    winner_known_lead_nome VARCHAR,
    winner_known_lead_total_contratos INTEGER,
    winner_known_lead_total_valor_brl DOUBLE,
    status VARCHAR,
    doe_url VARCHAR,
    doe_excerpt TEXT,
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


def clean_doc(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def parse_brl(text: str) -> float:
    raw = str(text or "").strip()
    raw = raw.replace(".", "").replace(",", ".")
    return float(re.sub(r"[^\d.]", "", raw) or 0)


def ensure_doe_pdf() -> Path:
    DOE_PDF.parent.mkdir(parents=True, exist_ok=True)
    if DOE_PDF.exists() and DOE_PDF.stat().st_size > 0:
        return DOE_PDF
    response = requests.get(DOE_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    DOE_PDF.write_bytes(response.content)
    return DOE_PDF


def extract_doe_text(pdf_path: Path) -> str:
    proc = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout


def parse_doe_excerpt(text: str) -> tuple[str, str, float, str]:
    lines = [line.strip() for line in text.splitlines()]
    target_idx = None
    for idx, line in enumerate(lines):
        if "Processo nº 0088.016739.00008/2023-72" in line:
            target_idx = idx
            break
    if target_idx is None:
        raise RuntimeError("Trecho do DOE para PP 053/2023 nao encontrado")

    excerpt_lines = [line for line in lines[target_idx : target_idx + 8] if line]
    excerpt = re.sub(r"\s+", " ", " ".join(excerpt_lines)).strip()
    pattern = re.compile(
        r"adjudicado em favor da empresa\s+(?P<nome>.+?),\s*CNPJ n\.º\s*(?P<cnpj>[\d\./-]+),\s*"
        r"vencedora do certame.*?valor total de R\$\s*(?P<valor>[\d\.,]+)",
        re.S,
    )
    match = pattern.search(excerpt)
    if not match:
        raise RuntimeError("Trecho do DOE para PP 053/2023 nao encontrado")
    return (
        match.group("nome").strip(),
        clean_doc(match.group("cnpj")),
        parse_brl(match.group("valor")),
        excerpt,
    )


def fetch_aditivos(ids: list[int]) -> dict[str, list[dict]]:
    bot = TransparenciaAcConnector(delay_entre_requests=0.05)
    out: dict[str, list[dict]] = {}
    for cid in ids:
        rows = bot._portal_post_json(page="contratos", endpoint="aditivo", payload={"id": str(cid)})
        out[str(cid)] = rows if isinstance(rows, list) else []
    return out


def main() -> int:
    pdf_path = ensure_doe_pdf()
    doe_text = extract_doe_text(pdf_path)
    vencedor_nome, vencedor_cnpj, valor_homologado, doe_excerpt = parse_doe_excerpt(doe_text)

    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL_AUDIT)
    con.execute("DELETE FROM trace_norte_rede_pp053_audit")

    contracts = con.execute(
        """
        SELECT
            cnpj,
            fornecedor_nome,
            orgao,
            unidade_gestora,
            lic_ano,
            lic_numero,
            lic_processo_adm,
            lic_modalidade,
            COUNT(*) AS n_contratos,
            SUM(valor_brl) AS total_valor,
            LIST(id_contrato ORDER BY valor_brl DESC, numero_contrato) AS contract_ids,
            LIST(numero_contrato ORDER BY valor_brl DESC, numero_contrato) AS contract_numbers
        FROM trace_norte_rede_vinculo_exato
        WHERE cnpj = '36990588000115'
          AND orgao = 'SEPLANH'
          AND lic_ano = 2023
          AND lic_numero = '053'
        GROUP BY ALL
        """
    ).fetchone()
    if not contracts:
        raise RuntimeError("Bloco CENTRAL NORTE / PP 053/2023 nao encontrado em trace_norte_rede_vinculo_exato")

    (
        contracts_cnpj,
        contracts_fornecedor,
        orgao,
        unidade_gestora,
        lic_ano,
        lic_numero,
        lic_processo_adm,
        lic_modalidade,
        n_contratos,
        contracts_total,
        contract_ids,
        contract_numbers,
    ) = contracts

    aditivos = fetch_aditivos([int(cid) for cid in contract_ids or []])
    winner_lead = con.execute(
        """
        SELECT nome_referencia, total_contratos, total_valor_brl
        FROM v_trace_norte_rede_resumo
        WHERE cnpj = ?
        """,
        [vencedor_cnpj],
    ).fetchone()
    winner_is_known_lead = winner_lead is not None
    winner_known_lead_nome = winner_lead[0] if winner_lead else None
    winner_known_lead_total_contratos = int(winner_lead[1]) if winner_lead else None
    winner_known_lead_total_valor = float(winner_lead[2]) if winner_lead else None
    delta_brl = float(contracts_total or 0) - float(valor_homologado or 0)

    status = "VENCEDOR_DIVERGENTE"
    if clean_doc(contracts_cnpj) == vencedor_cnpj:
        status = "VENCEDOR_COINCIDENTE"

    evidence = {
        "doe": {
            "url": DOE_URL,
            "vencedor_nome": vencedor_nome,
            "vencedor_cnpj": vencedor_cnpj,
            "valor_homologado_brl": valor_homologado,
        },
        "portal_contratos": {
            "contracts_cnpj": contracts_cnpj,
            "contracts_fornecedor": contracts_fornecedor,
            "contracts_n": int(n_contratos or 0),
            "contracts_total_brl": float(contracts_total or 0),
            "contract_ids": [int(cid) for cid in contract_ids or []],
            "contract_numbers": [str(num) for num in contract_numbers or []],
        },
        "winner_known_lead": {
            "match": winner_is_known_lead,
            "nome_referencia": winner_known_lead_nome,
            "total_contratos": winner_known_lead_total_contratos,
            "total_valor_brl": winner_known_lead_total_valor,
        },
        "delta_brl": delta_brl,
    }

    row = (
        row_hash("trace_norte_rede_pp053_audit", lic_ano, lic_numero, lic_processo_adm),
        int(lic_ano),
        str(lic_numero),
        str(lic_processo_adm),
        str(lic_modalidade),
        vencedor_nome,
        vencedor_cnpj,
        float(valor_homologado),
        str(contracts_cnpj),
        str(contracts_fornecedor),
        int(n_contratos),
        float(contracts_total),
        json.dumps(
            [
                {"id_contrato": int(cid), "numero_contrato": str(num)}
                for cid, num in zip(contract_ids or [], contract_numbers or [])
            ],
            ensure_ascii=False,
        ),
        json.dumps(aditivos, ensure_ascii=False),
        float(delta_brl),
        bool(winner_is_known_lead),
        winner_known_lead_nome,
        winner_known_lead_total_contratos,
        winner_known_lead_total_valor,
        status,
        DOE_URL,
        doe_excerpt,
        json.dumps(evidence, ensure_ascii=False),
    )

    con.execute(
        """
        INSERT INTO trace_norte_rede_pp053_audit (
            row_id, lic_ano, lic_numero, lic_processo_adm, lic_modalidade,
            vencedor_doe_nome, vencedor_doe_cnpj, valor_homologado_brl,
            contracts_cnpj, contracts_fornecedor, contracts_n, contracts_total_brl,
            contracts_ids_json, aditivos_json, delta_brl, winner_is_known_lead,
            winner_known_lead_nome, winner_known_lead_total_contratos,
            winner_known_lead_total_valor_brl, status, doe_url, doe_excerpt, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_rede_pp053_audit AS
        SELECT
            lic_ano, lic_numero, lic_processo_adm, lic_modalidade,
            vencedor_doe_nome, vencedor_doe_cnpj, valor_homologado_brl,
            contracts_cnpj, contracts_fornecedor, contracts_n, contracts_total_brl,
            delta_brl, winner_is_known_lead, winner_known_lead_nome,
            winner_known_lead_total_contratos, winner_known_lead_total_valor_brl,
            status, doe_url, doe_excerpt
        FROM trace_norte_rede_pp053_audit
        """
    )

    insight_id = "TRACE_NORTE_REDE:pp053:vencedor_divergente"
    con.execute("DELETE FROM insight WHERE id = ?", [insight_id])
    con.execute(
        """
        INSERT INTO insight (
            id, kind, severity, confidence, exposure_brl, title, description_md,
            pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
            municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            insight_id,
            "TRACE_NORTE_REDE_VENCEDOR_DIVERGENTE",
            "HIGH",
            96,
            float(contracts_total or 0),
            "PP 053/2023 homologado para NORTE-CENTRO, contratos 2024 saem em nome da CENTRAL NORTE",
            (
                f"No **Pregão Presencial {lic_numero}/{lic_ano}** da **{unidade_gestora}** "
                f"(processo **{lic_processo_adm}**), o DOE homologou o certame em favor de "
                f"**{vencedor_nome}** (`{vencedor_cnpj}`), com valor total de **R$ {valor_homologado:,.2f}**. "
                f"Já os **{int(n_contratos)} contrato(s)** de 2024 ligados no próprio portal de contratos ao mesmo "
                f"`id_licitacao` aparecem em nome de **{contracts_fornecedor}** (`{contracts_cnpj}`), somando "
                f"**R$ {float(contracts_total or 0):,.2f}**. Há ainda **1 aditivo** publicado para o contrato `001/2024` "
                f"sob o mesmo processo. O sistema trata isso como divergência documental de vencedor/fornecedor a esclarecer."
            ).replace(",", "X", 1).replace(".", ",").replace("X", "."),
            "TRACE_NORTE_REDE -> DOE_HOMOLOGACAO != FORNECEDOR_DO_CONTRATO",
            json.dumps(
                [
                    {"fonte": "doe", "url": DOE_URL, "processo": lic_processo_adm},
                    {"fonte": "portal_contratos", "licitacao_numero": lic_numero, "licitacao_ano": lic_ano},
                ],
                ensure_ascii=False,
            ),
            json.dumps(
                [
                    "trace_norte",
                    "rede",
                    "pp053",
                    "vencedor_divergente",
                    "central_norte",
                    "norte_centro",
                    "seplanh",
                ],
                ensure_ascii=False,
            ),
            int(n_contratos),
            float(contracts_total or 0),
            "estadual",
            "AC",
            str(orgao),
            None,
            "AC",
            "administracao",
            False,
            float(contracts_total or 0),
            int(lic_ano),
            "doe_acre+portal_transparencia_acre",
        ],
    )

    con.close()
    print("audit_rows=1")
    print(f"status={status}")
    print(f"winner_cnpj={vencedor_cnpj}")
    print(f"contracts_cnpj={contracts_cnpj}")
    print(f"delta_brl={delta_brl:.2f}")
    print("insights=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
