from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def _date_expr_rb() -> str:
    return """
    COALESCE(
        TRY_STRPTIME(NULLIF(TRIM(c.data_lancamento), ''), '%d/%m/%Y')::DATE,
        TRY_CAST(NULLIF(TRIM(c.data_lancamento), '') AS DATE),
        CASE
            WHEN c.ano IS NOT NULL THEN MAKE_DATE(c.ano, 12, 31)
            WHEN TRY_CAST(c.exercicio AS INTEGER) IS NOT NULL THEN MAKE_DATE(TRY_CAST(c.exercicio AS INTEGER), 12, 31)
            ELSE NULL
        END,
        CAST(c.capturado_em AS DATE)
    )
    """


def _date_expr_sc(column_expr: str) -> str:
    return f"""
    COALESCE(
        TRY_STRPTIME(CAST({column_expr} AS VARCHAR), '%d/%m/%Y')::DATE,
        TRY_CAST(CAST({column_expr} AS VARCHAR) AS DATE)
    )
    """


def run_validation(db_path: str) -> dict[str, object]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        print("\n" + "=" * 70)
        print("VALIDADOR DE LINHA DO TEMPO — SANÇÕES x CONTRATOS SUS RB")
        print("=" * 70)

        tables = set(con.execute("SHOW TABLES").df()["name"].tolist())
        required = {"rb_contratos", "sancoes_collapsed"}
        missing = sorted(required - tables)
        if missing:
            print(f"[ERRO] Tabelas ausentes: {missing}")
            return {"error": f"missing_tables={missing}"}

        sc_cols = {r[1] for r in con.execute("PRAGMA table_info('sancoes_collapsed')").fetchall()}
        di_col = next((c for c in ("data_inicio", "data_inicio_vigencia", "data_inicio_mais_antiga") if c in sc_cols), None)
        df_col = next((c for c in ("data_fim", "data_fim_vigencia", "data_fim_mais_recente") if c in sc_cols), None)
        cnpj_col = next((c for c in ("cnpj", "cnpj_cpf", "cnpj_cpf", "nr_cnpj") if c in sc_cols), None)
        ativa_col = "ativa" if "ativa" in sc_cols else None
        abrang_col = next((c for c in ("abrangencia", "abrangencia_sancao", "ds_abrangencia") if c in sc_cols), None)

        print(f"[i] sancoes_collapsed: cnpj={cnpj_col}, data_inicio={di_col}, data_fim={df_col}, ativa={ativa_col}, abrangencia={abrang_col}")
        if not cnpj_col:
            print("[ERRO] Nenhuma coluna de CNPJ encontrada em sancoes_collapsed.")
            return {"error": "cnpj_col_missing"}

        filtro_ativa = f"AND s.{ativa_col} = TRUE" if ativa_col else ""
        contract_date_expr = _date_expr_rb()
        start_date_expr = _date_expr_sc(f"s.{di_col}") if di_col else "NULL"
        end_date_expr = _date_expr_sc(f"s.{df_col}") if df_col else "NULL"

        total_bruto = con.execute(
            f"""
            SELECT COUNT(*)
            FROM rb_contratos c
            JOIN sancoes_collapsed s
              ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
               = REGEXP_REPLACE(CAST(s.{cnpj_col} AS VARCHAR), '[^0-9]', '', 'g')
            WHERE c.cnpj <> ''
              AND c.sus = TRUE
              {filtro_ativa}
            """
        ).fetchone()[0]
        print(f"[ATUAL] Cruzamentos brutos CEIS x contratos SUS: {total_bruto}")

        posterior = 0
        if di_col:
            posterior_df = con.execute(
                f"""
                SELECT
                    c.numero_contrato,
                    c.numero_processo,
                    c.fornecedor,
                    c.cnpj,
                    {contract_date_expr} AS data_contrato_ref,
                    {start_date_expr} AS sancao_inicio
                FROM rb_contratos c
                JOIN sancoes_collapsed s
                  ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
                   = REGEXP_REPLACE(CAST(s.{cnpj_col} AS VARCHAR), '[^0-9]', '', 'g')
                WHERE c.cnpj <> ''
                  AND c.sus = TRUE
                  {filtro_ativa}
                  AND {start_date_expr} IS NOT NULL
                  AND {start_date_expr} > {contract_date_expr}
                ORDER BY data_contrato_ref DESC, numero_contrato
                """
            ).fetchdf()
            posterior = len(posterior_df)
            print(f"[PROBLEMA] Sanções posteriores ao contrato: {posterior}")
            if posterior:
                print("  Primeiros casos:")
                for _, row in posterior_df.head(10).iterrows():
                    print(
                        f"    contrato={row['numero_contrato']} processo={row['numero_processo']} "
                        f"cnpj={row['cnpj']} data_contrato={row['data_contrato_ref']} sancao_inicio={row['sancao_inicio']}"
                    )
                if posterior > 10:
                    print(f"    ... e mais {posterior - 10} caso(s)")
        else:
            print("[AVISO] Sem coluna de data_inicio em sancoes_collapsed; validação temporal indisponível.")

        abrang_restrita = 0
        if abrang_col:
            abrang_df = con.execute(
                f"""
                SELECT
                    c.numero_contrato,
                    c.cnpj,
                    c.fornecedor,
                    s.{abrang_col} AS abrangencia
                FROM rb_contratos c
                JOIN sancoes_collapsed s
                  ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
                   = REGEXP_REPLACE(CAST(s.{cnpj_col} AS VARCHAR), '[^0-9]', '', 'g')
                WHERE c.cnpj <> ''
                  AND c.sus = TRUE
                  {filtro_ativa}
                  AND s.{abrang_col} IS NOT NULL
                  AND LOWER(CAST(s.{abrang_col} AS VARCHAR)) NOT LIKE '%todas as esferas%'
                  AND LOWER(CAST(s.{abrang_col} AS VARCHAR)) NOT LIKE '%todos os poderes%'
                  AND LOWER(CAST(s.{abrang_col} AS VARCHAR)) NOT LIKE '%nacional%'
                """
            ).fetchdf()
            abrang_restrita = len(abrang_df)
            print(f"[REVISÃO] Abrangência não verificada automaticamente: {abrang_restrita}")
        else:
            print("[REVISÃO] Abrangência ausente em sancoes_collapsed: todos os matches temporais exigem revisão humana de escopo.")

        temporal_ok = 0
        if di_col:
            temporal_ok = con.execute(
                f"""
                SELECT COUNT(*)
                FROM rb_contratos c
                JOIN sancoes_collapsed s
                  ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
                   = REGEXP_REPLACE(CAST(s.{cnpj_col} AS VARCHAR), '[^0-9]', '', 'g')
                WHERE c.cnpj <> ''
                  AND c.sus = TRUE
                  {filtro_ativa}
                  AND ({start_date_expr} IS NULL OR {start_date_expr} <= {contract_date_expr})
                  AND ({end_date_expr} IS NULL OR {end_date_expr} >= {contract_date_expr})
                """
            ).fetchone()[0]
        print(f"[PATCH 01] Matches temporalmente válidos: {temporal_ok}")
        print(f"[PATCH 01] Redução potencial imediata: {int(total_bruto or 0) - int(temporal_ok or 0)}")

        print("\n" + "-" * 70)
        print("DIAGNÓSTICO: contrato 3895 / processo 3044")
        print("-" * 70)
        caso_3895 = con.execute(
            f"""
            SELECT
                c.numero_contrato,
                c.numero_processo,
                c.fornecedor,
                c.cnpj,
                c.ano,
                c.data_lancamento,
                {contract_date_expr} AS data_contrato_ref,
                {start_date_expr if di_col else 'NULL'} AS sancao_inicio,
                {end_date_expr if df_col else 'NULL'} AS sancao_fim,
                {f"s.{abrang_col}" if abrang_col else "NULL"} AS abrangencia,
                {f"s.{ativa_col}" if ativa_col else "NULL"} AS ativa,
                s.orgao_ac
            FROM rb_contratos c
            JOIN sancoes_collapsed s
              ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
               = REGEXP_REPLACE(CAST(s.{cnpj_col} AS VARCHAR), '[^0-9]', '', 'g')
            WHERE c.numero_contrato = '3895'
               OR c.numero_processo = '3044'
            ORDER BY s.orgao_ac
            """
        ).fetchdf()
        if caso_3895.empty:
            print("  contrato 3895 não encontrado.")
        else:
            print(caso_3895.to_string(index=False))

        return {
            "total_bruto": int(total_bruto or 0),
            "falsos_positivos_temporais": int(posterior or 0),
            "abrangencia_restrita": int(abrang_restrita or 0),
            "total_valido_apos_filtros": int(temporal_ok or 0),
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida timeline de sanções CEIS/CNEP contra contratos SUS municipais.")
    parser.add_argument("--db", default=str(ROOT / "data" / "sentinela_analytics.duckdb"))
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"[ERRO] Banco não encontrado: {args.db}")
        return 1

    run_validation(args.db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
