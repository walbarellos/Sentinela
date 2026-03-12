"""
diag_ceis_join.py - diagnostico do JOIN estado x CEIS/CNEP
Roda em read-only contra o DuckDB atual.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"


def table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    return [row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]


def pick_column(cols: list[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)

    fornecedor_cols = table_columns(con, "estado_ac_fornecedores")
    ceis_cols = table_columns(con, "federal_ceis")
    pagamentos_cols = table_columns(con, "estado_ac_pagamentos")

    fornecedor_cnpj_col = pick_column(fornecedor_cols, "cnpjcpf", "cnpj")
    fornecedor_nome_col = pick_column(fornecedor_cols, "razao_social", "nome")
    ceis_cnpj_col = pick_column(ceis_cols, "cnpj", "cnpj_cpf")
    ceis_nome_col = pick_column(ceis_cols, "nome", "nome_sancionado")
    ceis_uf_col = pick_column(ceis_cols, "uf_sancionado", "uf_orgao_sancionador")

    print("=== Colunas usadas no diagnostico ===")
    print("estado_ac_fornecedores:", fornecedor_cnpj_col, fornecedor_nome_col)
    print("federal_ceis:", ceis_cnpj_col, ceis_nome_col, ceis_uf_col)
    print("estado_ac_pagamentos:", pick_column(pagamentos_cols, "cnpjcpf"), pick_column(pagamentos_cols, "credor"))

    print()
    print("=== 10 CNPJs de estado_ac_fornecedores ===")
    rows = con.execute(
        f"""
        SELECT {fornecedor_cnpj_col} AS cnpj, LENGTH({fornecedor_cnpj_col}) AS len, {fornecedor_nome_col} AS nome
        FROM estado_ac_fornecedores
        WHERE {fornecedor_cnpj_col} IS NOT NULL AND TRIM({fornecedor_cnpj_col}) <> ''
        LIMIT 10
        """
    ).fetchall()
    for row in rows:
        print(row)

    print()
    print("=== Distribuicao de formatos em estado_ac_fornecedores ===")
    print(
        con.execute(
            f"""
            SELECT
                LENGTH({fornecedor_cnpj_col}) AS len_bruto,
                LENGTH(REGEXP_REPLACE({fornecedor_cnpj_col}, '[^0-9]', '', 'g')) AS len_digits,
                COUNT(*) AS n
            FROM estado_ac_fornecedores
            WHERE {fornecedor_cnpj_col} IS NOT NULL AND TRIM({fornecedor_cnpj_col}) <> ''
            GROUP BY 1, 2
            ORDER BY n DESC, len_bruto, len_digits
            """
        ).fetchdf().to_string(index=False)
    )

    print()
    print(f"=== 10 {ceis_cnpj_col} de federal_ceis ===")
    rows = con.execute(
        f"""
        SELECT {ceis_cnpj_col} AS cnpj, LENGTH({ceis_cnpj_col}) AS len, {ceis_nome_col} AS nome
        FROM federal_ceis
        WHERE {ceis_cnpj_col} IS NOT NULL AND TRIM({ceis_cnpj_col}) <> ''
        LIMIT 10
        """
    ).fetchall()
    for row in rows:
        print(row)

    print()
    print("=== Distribuicao de formatos em federal_ceis ===")
    print(
        con.execute(
            f"""
            SELECT
                LENGTH({ceis_cnpj_col}) AS len_bruto,
                LENGTH(REGEXP_REPLACE({ceis_cnpj_col}, '[^0-9]', '', 'g')) AS len_digits,
                COUNT(*) AS n
            FROM federal_ceis
            WHERE {ceis_cnpj_col} IS NOT NULL AND TRIM({ceis_cnpj_col}) <> ''
            GROUP BY 1, 2
            ORDER BY n DESC, len_bruto, len_digits
            """
        ).fetchdf().to_string(index=False)
    )

    print()
    print("=== 10 cnpjcpf de estado_ac_pagamentos (se existir) ===")
    rows = con.execute(
        """
        SELECT cnpjcpf, LENGTH(cnpjcpf) AS len, credor
        FROM estado_ac_pagamentos
        WHERE cnpjcpf IS NOT NULL AND TRIM(cnpjcpf) <> ''
        LIMIT 10
        """
    ).fetchall()
    if rows:
        for row in rows:
            print(row)
    else:
        print("  (zero linhas com cnpjcpf preenchido)")

    print()
    print("=== JOIN direto: fornecedores x CEIS (sem normalizacao) ===")
    n_direct = con.execute(
        f"""
        SELECT COUNT(*)
        FROM estado_ac_fornecedores f
        JOIN federal_ceis c
          ON f.{fornecedor_cnpj_col} = c.{ceis_cnpj_col}
        """
    ).fetchone()[0]
    print("Matches diretos:", n_direct)

    print()
    print("=== JOIN com regexp_replace (strip nao-digitos) ===")
    n_strip = con.execute(
        f"""
        SELECT COUNT(*)
        FROM estado_ac_fornecedores f
        JOIN federal_ceis c
          ON REGEXP_REPLACE(f.{fornecedor_cnpj_col}, '[^0-9]', '', 'g')
           = REGEXP_REPLACE(c.{ceis_cnpj_col}, '[^0-9]', '', 'g')
        WHERE LENGTH(REGEXP_REPLACE(f.{fornecedor_cnpj_col}, '[^0-9]', '', 'g')) >= 11
        """
    ).fetchone()[0]
    print("Matches apos strip:", n_strip)

    print()
    print("=== 10 matches apos strip (amostra) ===")
    rows = con.execute(
        f"""
        SELECT
            f.ano,
            f.orgao,
            f.{fornecedor_nome_col} AS fornecedor_nome,
            f.{fornecedor_cnpj_col} AS fornecedor_cnpj,
            c.{ceis_nome_col} AS ceis_nome,
            c.tipo_sancao
        FROM estado_ac_fornecedores f
        JOIN federal_ceis c
          ON REGEXP_REPLACE(f.{fornecedor_cnpj_col}, '[^0-9]', '', 'g')
           = REGEXP_REPLACE(c.{ceis_cnpj_col}, '[^0-9]', '', 'g')
        WHERE LENGTH(REGEXP_REPLACE(f.{fornecedor_cnpj_col}, '[^0-9]', '', 'g')) >= 11
        LIMIT 10
        """
    ).fetchall()
    if rows:
        for row in rows:
            print(row)
    else:
        print("  (sem matches)")

    print()
    print("=== Amostra CEIS filtrado AC ===")
    if ceis_uf_col is None:
        print("  Coluna de UF nao existe em federal_ceis. Colunas:", ceis_cols)
    else:
        rows = con.execute(
            f"""
            SELECT {ceis_cnpj_col}, {ceis_nome_col}, {ceis_uf_col}
            FROM federal_ceis
            WHERE {ceis_uf_col} = 'AC'
            LIMIT 5
            """
        ).fetchall()
        print(f"  Registros CEIS com {ceis_uf_col}='AC': {len(rows)}")
        for row in rows:
            print(" ", row)
        total_ac = con.execute(
            f"SELECT COUNT(*) FROM federal_ceis WHERE {ceis_uf_col} = 'AC'"
        ).fetchone()[0]
        print(f"  Total CEIS {ceis_uf_col}='AC': {total_ac}")

    con.close()


if __name__ == "__main__":
    main()
