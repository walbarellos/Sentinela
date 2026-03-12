from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger("sync_sancoes_collapsed")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

DUCKDB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
KIND_ATIVA = "SESACRE_SANCAO_ATIVA"

DDL_COLLAPSED = """
CREATE TABLE IF NOT EXISTS sancoes_collapsed (
    id VARCHAR PRIMARY KEY,
    cnpj_cpf VARCHAR,
    nome_sancionado VARCHAR,
    fonte VARCHAR,
    orgao_ac VARCHAR,
    n_sancoes_total INTEGER,
    n_sancoes_ativas INTEGER,
    tipos_sancao VARCHAR,
    data_inicio_mais_antiga VARCHAR,
    data_fim_mais_recente VARCHAR,
    valor_contratado_ac DOUBLE,
    n_contratos_ac INTEGER,
    ativa BOOLEAN,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def sancao_ativa(status_sancao: object, data_fim: object) -> bool:
    status = str(status_sancao or "").strip().upper()
    if status in {"VIGENTE", "INDEFINIDA"}:
        return True
    if status == "EXPIRADA":
        return False

    text = str(data_fim or "").strip()
    if not text or text in {"N/A", "SEM PRAZO"}:
        return True

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date() >= date.today()
        except ValueError:
            continue
    return True


def short_id(prefix: str, *parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{hashlib.md5(raw.encode('utf-8')).hexdigest()[:16]}"


def build_collapsed(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    con.execute(DDL_COLLAPSED)
    con.execute("DELETE FROM sancoes_collapsed")

    rows = con.execute(
        """
        SELECT
            fornecedor_cnpj,
            COALESCE(nome_sancionado, fornecedor_nome) AS nome_sancionado,
            fonte,
            orgao,
            tipo_sancao,
            data_inicio_sancao,
            data_fim_sancao,
            total_pago,
            n_pagamentos,
            status_sancao
        FROM estado_ac_fornecedor_sancoes
        ORDER BY fornecedor_cnpj, orgao, fonte
        """
    ).fetchall()

    groups: dict[tuple[str, str, str], list[dict]] = {}
    for (
        cnpj,
        nome,
        fonte,
        orgao,
        tipo,
        dt_ini,
        dt_fim,
        valor,
        n_pagamentos,
        status_sancao,
    ) in rows:
        key = (str(cnpj or ""), str(orgao or ""), str(fonte or ""))
        groups.setdefault(key, []).append(
            {
                "nome": str(nome or ""),
                "tipo": str(tipo or ""),
                "dt_ini": str(dt_ini or ""),
                "dt_fim": str(dt_fim or ""),
                "valor": float(valor or 0.0),
                "n_pagamentos": int(n_pagamentos or 0),
                "ativa": sancao_ativa(status_sancao, dt_fim),
            }
        )

    payload = []
    active_count = 0
    for (cnpj, orgao, fonte), items in groups.items():
        ativas = [item for item in items if item["ativa"]]
        tipos = sorted({item["tipo"] for item in items if item["tipo"]})
        datas_ini = sorted(item["dt_ini"] for item in items if item["dt_ini"])
        datas_fim = sorted(
            (item["dt_fim"] for item in items if item["dt_fim"] and item["dt_fim"].strip()),
            reverse=True,
        )
        nome = max((item["nome"] for item in items), key=len, default="")
        valor_total = max((item["valor"] for item in items), default=0.0)
        n_contratos = max((item["n_pagamentos"] for item in items), default=0)
        ativa = bool(ativas)
        if ativa:
            active_count += 1

        payload.append(
            (
                short_id("SC", cnpj, orgao, fonte),
                cnpj,
                nome,
                fonte,
                orgao,
                len(items),
                len(ativas),
                ", ".join(tipos)[:1000],
                datas_ini[0] if datas_ini else "",
                datas_fim[0] if datas_fim else "",
                valor_total,
                n_contratos,
                ativa,
            )
        )

    if payload:
        con.executemany(
            """
            INSERT INTO sancoes_collapsed (
                id, cnpj_cpf, nome_sancionado, fonte, orgao_ac,
                n_sancoes_total, n_sancoes_ativas, tipos_sancao,
                data_inicio_mais_antiga, data_fim_mais_recente,
                valor_contratado_ac, n_contratos_ac, ativa
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    log.info("sancoes_collapsed: %d linhas (%d com sancao ativa)", len(payload), active_count)
    return len(payload), active_count


def build_view(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE OR REPLACE VIEW v_sancoes_ativas AS
        SELECT
            cnpj_cpf,
            nome_sancionado,
            fonte,
            orgao_ac,
            n_sancoes_ativas,
            n_sancoes_total,
            tipos_sancao,
            data_inicio_mais_antiga,
            data_fim_mais_recente,
            valor_contratado_ac,
            n_contratos_ac
        FROM sancoes_collapsed
        WHERE ativa = TRUE
        ORDER BY valor_contratado_ac DESC, nome_sancionado
        """
    )
    n = con.execute("SELECT COUNT(*) FROM v_sancoes_ativas").fetchone()[0]
    log.info("v_sancoes_ativas: %d fornecedores com sancao vigente", n)
    return n


def build_insights(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("DELETE FROM insight WHERE kind = ?", [KIND_ATIVA])
    rows = con.execute(
        """
        SELECT
            cnpj_cpf,
            nome_sancionado,
            fonte,
            orgao_ac,
            n_sancoes_ativas,
            n_sancoes_total,
            tipos_sancao,
            data_inicio_mais_antiga,
            valor_contratado_ac,
            n_contratos_ac
        FROM sancoes_collapsed
        WHERE ativa = TRUE
        ORDER BY valor_contratado_ac DESC, nome_sancionado
        LIMIT 500
        """
    ).fetchall()
    if not rows:
        log.info("Sem sancoes ativas - nenhum insight ATIVA gerado.")
        return 0

    area_map = {
        "SESACRE": "saude",
        "SEE": "educacao",
        "SEFAZ": "financas",
        "SEJUSP": "seguranca",
        "SEINFRA": "infraestrutura",
        "GOVERNO_ACRE": "gestao_estadual",
    }

    payload = []
    now = datetime.now()
    for (
        cnpj,
        nome,
        fonte,
        orgao,
        n_ativas,
        n_total,
        tipos,
        dt_ini,
        valor,
        n_cont,
    ) in rows:
        iid = short_id("INS", KIND_ATIVA, cnpj, orgao, fonte)
        title = f"[{fonte}·ATIVA] {(nome or cnpj)[:50]} - R$ {float(valor or 0):,.0f} contratados com {orgao}"
        description_md = (
            f"**{nome}** (`{cnpj}`) possui **{int(n_ativas or 0)} sancao(oes) ativa(s)** "
            f"no cadastro **{fonte}** (de **{int(n_total or 0)}** total).\n\n"
            f"Tipo(s): _{tipos or 'N/I'}_. Inicio mais antigo: {dt_ini or 'N/I'}.\n\n"
            f"Mesmo sob sancao, o orgao **{orgao}** manteve **{int(n_cont or 0)} contrato(s)** "
            f"totalizando **R$ {float(valor or 0):,.2f}**."
        )
        payload.append(
            (
                iid,
                KIND_ATIVA,
                "CRITICO",
                97,
                float(valor or 0.0),
                title,
                description_md,
                "fornecedor_sancionado_contrato_ativo",
                json.dumps([f"portaldatransparencia.gov.br/{str(fonte).lower()}"], ensure_ascii=False),
                json.dumps([fonte, orgao or "GOVERNO_ACRE", "sancao", "ativa"], ensure_ascii=False),
                int(n_cont or 0),
                float(valor or 0.0),
                now,
                "estadual",
                "Governo do Estado do Acre",
                orgao or "",
                "",
                "AC",
                area_map.get(orgao or "", "gestao_estadual"),
                orgao == "SESACRE",
                float(valor or 0.0),
                None,
                f"portaldatransparencia.gov.br/{str(fonte).lower()}",
            )
        )

    if payload:
        con.executemany(
            """
            INSERT INTO insight (
                id, kind, severity, confidence, exposure_brl,
                title, description_md, pattern, sources, tags,
                sample_n, unit_total, created_at,
                esfera, ente, orgao, municipio, uf,
                area_tematica, sus, valor_referencia, ano_referencia, fonte
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    log.info("=== Resumo por orgao (sancoes ativas) ===")
    rows = con.execute(
        """
        SELECT orgao_ac, fonte, COUNT(*) AS fornecedores, SUM(valor_contratado_ac) AS valor_total
        FROM sancoes_collapsed
        WHERE ativa = TRUE
        GROUP BY orgao_ac, fonte
        ORDER BY valor_total DESC, orgao_ac, fonte
        LIMIT 20
        """
    ).fetchall()
    for orgao, fonte, fornecedores, valor in rows:
        log.info(
            "  [%s] %-20s %3d fornecedores  R$ %s",
            fonte,
            orgao or "",
            int(fornecedores or 0),
            f"{float(valor or 0):,.0f}",
        )

    log.info("=== Top 10 fornecedores SESACRE (ativa) ===")
    rows = con.execute(
        """
        SELECT nome_sancionado, cnpj_cpf, n_sancoes_ativas, tipos_sancao, valor_contratado_ac, n_contratos_ac
        FROM sancoes_collapsed
        WHERE ativa = TRUE AND orgao_ac = 'SESACRE'
        ORDER BY valor_contratado_ac DESC, nome_sancionado
        LIMIT 10
        """
    ).fetchall()
    for nome, cnpj, n_ativas, tipos, valor, n_contratos in rows:
        log.info(
            "  %-45s | %s | %d ativa(s) | R$ %s (%d contratos)",
            (nome or "")[:45],
            cnpj or "",
            int(n_ativas or 0),
            f"{float(valor or 0):,.0f}",
            int(n_contratos or 0),
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            n = con.execute("SELECT COUNT(*) FROM estado_ac_fornecedor_sancoes").fetchone()[0]
            log.info("[dry-run] estado_ac_fornecedor_sancoes: %d linhas", n)
        finally:
            con.close()
        return

    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        build_collapsed(con)
        build_view(con)
        n_ins = build_insights(con)
        log.info("Insights %s gerados: %d", KIND_ATIVA, n_ins)
        print_summary(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
