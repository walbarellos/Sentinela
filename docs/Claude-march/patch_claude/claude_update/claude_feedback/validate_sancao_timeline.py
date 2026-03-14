"""
PATCH 03 — Validador de linha do tempo de sanções CEIS
=======================================================
Script standalone que audita o banco atual e lista:
  1. Alertas gerados com sanção POSTERIOR ao contrato (falso positivo confirmado)
  2. Alertas com abrangência restrita (possível falso positivo)
  3. Resumo do impacto do patch 01 antes de aplicá-lo

Uso:
  .venv/bin/python scripts/validate_sancao_timeline.py
  .venv/bin/python scripts/validate_sancao_timeline.py --db data/sentinela_analytics.duckdb
"""

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def run_validation(db_path: str) -> dict:
    con = duckdb.connect(db_path, read_only=True)
    results = {}

    print("\n" + "=" * 70)
    print("VALIDADOR DE LINHA DO TEMPO — SANÇÕES CEIS x CONTRATOS")
    print("=" * 70)

    tables = set(con.execute("SHOW TABLES").df()["name"].tolist())

    # ── 1. Verificar se as tabelas necessárias existem ───────────────────────
    required = {"rb_contratos", "sancoes_collapsed"}
    missing = required - tables
    if missing:
        print(f"\n[!] Tabelas ausentes: {missing}")
        print("    Execute os ingestores antes de validar.")
        return {"error": f"missing_tables: {missing}"}

    # ── 2. Detectar colunas disponíveis em sancoes_collapsed ─────────────────
    sc_cols = {r[1] for r in con.execute("PRAGMA table_info('sancoes_collapsed')").fetchall()}
    rb_cols = {r[1] for r in con.execute("PRAGMA table_info('rb_contratos')").fetchall()}

    di_col = next((c for c in ["data_inicio", "data_inicio_vigencia"] if c in sc_cols), None)
    df_col = next((c for c in ["data_fim", "data_fim_vigencia"] if c in sc_cols), None)
    cnpj_s = next((c for c in ["cnpj", "cnpj_cpf", "nr_cnpj"] if c in sc_cols), None)
    abrang_col = next((c for c in ["abrangencia", "abrangencia_sancao", "ds_abrangencia"] if c in sc_cols), None)
    ativa_col = next((c for c in ["ativa"] if c in sc_cols), None)
    di_rb = next((c for c in ["data_inicio", "data_contrato", "capturado_em"] if c in rb_cols), None)

    print(f"\n[i] Colunas encontradas em sancoes_collapsed:")
    print(f"    data_inicio={di_col}, data_fim={df_col}, cnpj={cnpj_s}")
    print(f"    abrangencia={abrang_col}, ativa={ativa_col}")
    print(f"\n[i] Data de referência do contrato: {di_rb}")

    if not cnpj_s:
        print("\n[!] Coluna CNPJ não encontrada em sancoes_collapsed. Abortando.")
        return {"error": "cnpj_col_missing"}

    # ── 3. Total de cruzamentos brutos (como o sistema atual opera) ───────────
    total_bruto = con.execute(f"""
        SELECT COUNT(*) FROM rb_contratos c
        JOIN sancoes_collapsed s
          ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
           = REGEXP_REPLACE(s.{cnpj_s}, '[^0-9]', '', 'g')
        WHERE c.cnpj <> ''
          AND c.sus = TRUE
          {f"AND s.{ativa_col} = TRUE" if ativa_col else ""}
    """).fetchone()[0]

    results["total_bruto"] = int(total_bruto)
    print(f"\n[ATUAL] Total de cruzamentos CEIS x contratos SUS (sem filtros): {total_bruto}")

    # ── 4. Sanções posteriores ao contrato (falso positivo confirmado) ────────
    if di_col and di_rb:
        try:
            falsos_positivos = con.execute(f"""
                SELECT
                    c.numero_contrato,
                    c.cnpj,
                    c.fornecedor,
                    c.{di_rb}           AS data_contrato,
                    s.{di_col}          AS sancao_inicio,
                    {f"s.{abrang_col}" if abrang_col else "NULL"} AS abrangencia,
                    {f"s.{df_col}" if df_col else "NULL"}         AS sancao_fim
                FROM rb_contratos c
                JOIN sancoes_collapsed s
                  ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
                   = REGEXP_REPLACE(s.{cnpj_s}, '[^0-9]', '', 'g')
                WHERE c.cnpj <> ''
                  AND c.sus = TRUE
                  {f"AND s.{ativa_col} = TRUE" if ativa_col else ""}
                  AND s.{di_col} IS NOT NULL
                  AND s.{di_col} > c.{di_rb}::DATE
                ORDER BY c.{di_rb} DESC, c.numero_contrato
            """).fetchdf()

            results["falsos_positivos_temporais"] = len(falsos_positivos)
            print(f"\n[PROBLEMA] Cruzamentos com sanção POSTERIOR ao contrato: {len(falsos_positivos)}")
            if len(falsos_positivos) > 0:
                print("  ⚠️  ESSES SÃO FALSOS POSITIVOS CONFIRMADOS")
                print("\n  Contratos afetados:")
                for _, r in falsos_positivos.head(10).iterrows():
                    print(f"    Contrato {r['numero_contrato']} | CNPJ {r['cnpj']}")
                    print(f"      Data contrato: {r['data_contrato']} | Sanção início: {r['sancao_inicio']}")
                if len(falsos_positivos) > 10:
                    print(f"    ... e mais {len(falsos_positivos) - 10} casos")
        except Exception as e:
            print(f"\n[!] Erro ao calcular falsos positivos temporais: {e}")
            results["falsos_positivos_temporais"] = "erro"
    else:
        print(f"\n[!] Colunas de data indisponíveis — não foi possível verificar timeline")
        results["falsos_positivos_temporais"] = "indisponivel"

    # ── 5. Sanções com abrangência restrita ───────────────────────────────────
    if abrang_col:
        try:
            abrangencia_restrita = con.execute(f"""
                SELECT
                    c.numero_contrato,
                    c.cnpj,
                    c.fornecedor,
                    s.{abrang_col} AS abrangencia,
                    COUNT(*) AS n_sancoes
                FROM rb_contratos c
                JOIN sancoes_collapsed s
                  ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
                   = REGEXP_REPLACE(s.{cnpj_s}, '[^0-9]', '', 'g')
                WHERE c.cnpj <> ''
                  AND c.sus = TRUE
                  {f"AND s.{ativa_col} = TRUE" if ativa_col else ""}
                  AND s.{abrang_col} IS NOT NULL
                  AND LOWER(s.{abrang_col}) NOT LIKE '%todas as esferas%'
                  AND LOWER(s.{abrang_col}) NOT LIKE '%todos os poderes%'
                  AND LOWER(s.{abrang_col}) NOT LIKE '%nacional%'
                GROUP BY c.numero_contrato, c.cnpj, c.fornecedor, s.{abrang_col}
                ORDER BY n_sancoes DESC
            """).fetchdf()

            results["abrangencia_restrita"] = len(abrangencia_restrita)
            print(f"\n[REVISÃO] Cruzamentos com abrangência possivelmente restrita: {len(abrangencia_restrita)}")
            if len(abrangencia_restrita) > 0:
                print("  Esses casos requerem verificação manual da abrangência")
                for _, r in abrangencia_restrita.head(5).iterrows():
                    print(f"    Contrato {r['numero_contrato']} | {r['fornecedor'] or r['cnpj']}")
                    print(f"      Abrangência: {r['abrangencia']}")
        except Exception as e:
            print(f"\n[!] Erro ao verificar abrangência: {e}")

    # ── 6. Cruzamentos válidos após filtros (impacto do patch) ────────────────
    try:
        filtro_temporal = f"AND (s.{di_col} IS NULL OR s.{di_col} <= c.{di_rb}::DATE)" if di_col and di_rb else ""
        filtro_abrang = f"""AND (
            s.{abrang_col} IS NULL OR TRIM(s.{abrang_col}) = ''
            OR LOWER(s.{abrang_col}) LIKE '%todas as esferas%'
            OR LOWER(s.{abrang_col}) LIKE '%todos os poderes%'
            OR LOWER(s.{abrang_col}) LIKE '%nacional%'
        )""" if abrang_col else ""

        total_valido = con.execute(f"""
            SELECT COUNT(*) FROM rb_contratos c
            JOIN sancoes_collapsed s
              ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
               = REGEXP_REPLACE(s.{cnpj_s}, '[^0-9]', '', 'g')
            WHERE c.cnpj <> ''
              AND c.sus = TRUE
              {f"AND s.{ativa_col} = TRUE" if ativa_col else ""}
              {filtro_temporal}
              {filtro_abrang}
        """).fetchone()[0]

        results["total_valido_apos_filtros"] = int(total_valido)
        reducao = ((total_bruto - total_valido) / total_bruto * 100) if total_bruto > 0 else 0
        print(f"\n[PATCH 01] Cruzamentos válidos após filtros temporais e de abrangência: {total_valido}")
        print(f"[PATCH 01] Redução de falsos positivos: {total_bruto - total_valido} casos ({reducao:.1f}%)")
    except Exception as e:
        print(f"\n[!] Erro ao calcular total válido: {e}")

    # ── 7. Diagnóstico do caso 3895 especificamente ───────────────────────────
    print("\n" + "-" * 70)
    print("DIAGNÓSTICO: Contrato 3895 (NORTE DISTRIBUIDORA / SEMSA)")
    print("-" * 70)
    try:
        caso_3895 = con.execute(f"""
            SELECT
                c.numero_contrato,
                c.fornecedor,
                c.cnpj,
                c.{di_rb if di_rb else "capturado_em"} AS data_contrato,
                s.{di_col if di_col else "NULL"}        AS sancao_inicio,
                s.{df_col if df_col else "NULL"}        AS sancao_fim,
                {f"s.{abrang_col}" if abrang_col else "'N/D'"} AS abrangencia,
                {f"s.{ativa_col}" if ativa_col else "NULL"}    AS ativa
            FROM rb_contratos c
            JOIN sancoes_collapsed s
              ON REGEXP_REPLACE(c.cnpj, '[^0-9]', '', 'g')
               = REGEXP_REPLACE(s.{cnpj_s}, '[^0-9]', '', 'g')
            WHERE c.numero_contrato = '3895'
               OR c.numero_processo = '3044'
            ORDER BY s.{di_col if di_col else "NULL"}
        """).fetchdf()

        if caso_3895.empty:
            print("  Contrato 3895 não encontrado no banco.")
        else:
            for _, r in caso_3895.iterrows():
                print(f"\n  Contrato: {r['numero_contrato']} | Fornecedor: {r['fornecedor']}")
                print(f"  Data contrato: {r['data_contrato']}")
                print(f"  Sanção início: {r['sancao_inicio']} | Fim: {r['sancao_fim']}")
                print(f"  Abrangência: {r['abrangencia']}")
                print(f"  Ativa: {r['ativa']}")

                # Diagnóstico
                if r['sancao_inicio'] and r['data_contrato']:
                    try:
                        from datetime import datetime
                        s_ini = str(r['sancao_inicio'])[:10]
                        d_con = str(r['data_contrato'])[:10]
                        if s_ini > d_con:
                            print(f"  ⚠️  RESULTADO: FALSO POSITIVO — sanção iniciada em {s_ini}, "
                                  f"posterior ao contrato de {d_con}")
                        else:
                            print(f"  ✅ RESULTADO: Sanção preexiste ao contrato — alerta VÁLIDO")
                    except Exception:
                        print("  [?] Não foi possível comparar datas automaticamente")
                else:
                    print("  [?] Datas insuficientes para diagnóstico automático")
    except Exception as e:
        print(f"  [!] Erro ao analisar caso 3895: {e}")

    con.close()

    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"  Cruzamentos brutos (sem filtros): {results.get('total_bruto', 'N/D')}")
    print(f"  Falsos positivos temporais:       {results.get('falsos_positivos_temporais', 'N/D')}")
    print(f"  Cruzamentos válidos (com filtros): {results.get('total_valido_apos_filtros', 'N/D')}")
    print("\nPRÓXIMOS PASSOS:")
    print("  1. Aplique o patch_01_sancao_temporal.py para corrigir as views")
    print("  2. Regenere os insights: .venv/bin/python scripts/sync_rb_contratos.py")
    print("  3. Revalide: .venv/bin/python scripts/validate_sancao_timeline.py")
    print()

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Valida timeline de sanções CEIS vs contratos")
    parser.add_argument("--db", default=str(ROOT / "data" / "sentinela_analytics.duckdb"),
                        help="Caminho do banco DuckDB")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"[ERRO] Banco não encontrado: {args.db}")
        sys.exit(1)

    run_validation(args.db)
