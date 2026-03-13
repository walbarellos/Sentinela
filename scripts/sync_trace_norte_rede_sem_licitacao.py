from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

DDL = """
CREATE TABLE IF NOT EXISTS trace_norte_rede_sem_licitacao (
    row_id VARCHAR PRIMARY KEY,
    cnpj VARCHAR,
    fornecedor_nome VARCHAR,
    orgao VARCHAR,
    unidade_gestora VARCHAR,
    origem VARCHAR,
    modalidade_contrato VARCHAR,
    n_contratos INTEGER,
    total_brl DOUBLE,
    contratos_json JSON,
    ids_contrato_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def row_hash(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def severity_for_total(total: float) -> str:
    if total >= 1_000_000:
        return "HIGH"
    if total >= 500_000:
        return "MEDIUM"
    return "LOW"


def confidence_for_total(total: float) -> int:
    if total >= 1_000_000:
        return 94
    if total >= 500_000:
        return 88
    return 80


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    rows = con.execute(
        """
        SELECT
            cnpj,
            fornecedor_nome,
            orgao,
            unidade_gestora,
            origem,
            modalidade_contrato,
            COUNT(*) AS n_contratos,
            SUM(valor_brl) AS total_brl,
            LIST(numero_contrato ORDER BY valor_brl DESC, numero_contrato) AS contratos,
            LIST(id_contrato ORDER BY valor_brl DESC, numero_contrato) AS ids_contrato
        FROM trace_norte_rede_vinculo_exato
        WHERE COALESCE(lic_numero, '') = ''
          AND COALESCE(id_licitacao, 0) = 0
          AND origem = 'C'
        GROUP BY ALL
        ORDER BY total_brl DESC, fornecedor_nome
        """
    ).fetchall()
    con.execute("DELETE FROM trace_norte_rede_sem_licitacao")
    payload = []
    for row in rows:
        cnpj, fornecedor_nome, orgao, unidade_gestora, origem, modalidade_contrato, n_contratos, total_brl, contratos, ids_contrato = row
        payload.append(
            (
                row_hash("trace_norte_rede_sem_licitacao", cnpj, orgao, unidade_gestora),
                cnpj,
                fornecedor_nome,
                orgao,
                unidade_gestora,
                origem,
                modalidade_contrato,
                int(n_contratos or 0),
                float(total_brl or 0),
                json.dumps([str(x) for x in contratos or []], ensure_ascii=False),
                json.dumps([int(x) for x in ids_contrato or []], ensure_ascii=False),
            )
        )
    if payload:
        con.executemany(
            """
            INSERT INTO trace_norte_rede_sem_licitacao (
                row_id, cnpj, fornecedor_nome, orgao, unidade_gestora, origem,
                modalidade_contrato, n_contratos, total_brl, contratos_json, ids_contrato_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_trace_norte_rede_sem_licitacao AS
        SELECT *
        FROM trace_norte_rede_sem_licitacao
        ORDER BY total_brl DESC, fornecedor_nome
        """
    )

    con.execute("DELETE FROM insight WHERE kind = 'TRACE_NORTE_REDE_CONTRATO_SEM_ID_LICITACAO'")
    insights = []
    for row in rows:
        cnpj, fornecedor_nome, orgao, unidade_gestora, origem, modalidade_contrato, n_contratos, total_brl, contratos, ids_contrato = row
        if float(total_brl or 0) < 500_000:
            continue
        insight_id = f"TRACE_NORTE_REDE:sem_licitacao:{cnpj}:{orgao}:{unidade_gestora}"
        description = (
            f"O portal estadual materializa **{int(n_contratos)} contrato(s)** de **{fornecedor_nome}** "
            f"na unidade **{unidade_gestora}**, somando **R$ {float(total_brl or 0):,.2f}**, "
            f"com `origem = {origem}` e **sem `id_licitacao` exposto** no registro contratual. "
            f"No recorte atual, os contratos listados são: {', '.join(str(x) for x in contratos or [])}."
        ).replace(",", "X", 1).replace(".", ",").replace("X", ".")
        insights.append(
            [
                insight_id,
                "TRACE_NORTE_REDE_CONTRATO_SEM_ID_LICITACAO",
                severity_for_total(float(total_brl or 0)),
                confidence_for_total(float(total_brl or 0)),
                float(total_brl or 0),
                "Contrato alto sem id_licitacao exposto no portal",
                description,
                "TRACE_NORTE_REDE -> CONTRATO_ORIGEM_C_SEM_ID_LICITACAO",
                json.dumps(
                    [
                        {"fonte": "portal_contratos", "unidade_gestora": unidade_gestora, "fornecedor": fornecedor_nome, "cnpj": cnpj},
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(
                    [
                        "trace_norte",
                        "rede",
                        "sem_licitacao",
                        cnpj,
                        orgao,
                    ],
                    ensure_ascii=False,
                ),
                int(n_contratos or 0),
                float(total_brl or 0),
                "estadual",
                "AC",
                orgao,
                None,
                "AC",
                "administracao",
                False,
                float(total_brl or 0),
                None,
                "portal_transparencia_acre",
            ]
        )
    if insights:
        con.executemany(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl, title, description_md,
                pattern, sources, tags, sample_n, unit_total, esfera, ente, orgao,
                municipio, uf, area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insights,
        )

    total_blocks = len(rows)
    total_insights = len(insights)
    con.close()
    print(f"blocks={total_blocks}")
    print(f"insights={total_insights}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
