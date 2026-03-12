"""
diag_ceis_join.py — diagnóstico do JOIN estado x CEIS/CNEP
Roda em < 5 segundos, read-only.
"""
import duckdb, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
con = duckdb.connect(str(ROOT / "data" / "sentinela_analytics.duckdb"), read_only=True)

print("=== 10 CNPJs de estado_ac_fornecedores ===")
try:
    rows = con.execute("""
        SELECT cnpj, LENGTH(cnpj) as len, nome
        FROM estado_ac_fornecedores
        WHERE cnpj IS NOT NULL AND cnpj != ''
        LIMIT 10
    """).fetchall()
    for r in rows: print(r)
except Exception as e:
    print("ERRO:", e)
    # tenta nome diferente de coluna
    cols = con.execute("PRAGMA table_info(estado_ac_fornecedores)").fetchall()
    print("Colunas:", [c[1] for c in cols])

print()
print("=== 10 cnpj_cpf de federal_ceis ===")
rows = con.execute("""
    SELECT cnpj_cpf, LENGTH(cnpj_cpf) as len
    FROM federal_ceis
    WHERE cnpj_cpf IS NOT NULL AND cnpj_cpf != ''
    LIMIT 10
""").fetchall()
for r in rows: print(r)

print()
print("=== 10 cnpjcpf de estado_ac_pagamentos (se existir) ===")
try:
    rows = con.execute("""
        SELECT cnpjcpf, LENGTH(cnpjcpf) as len, credor
        FROM estado_ac_pagamentos
        WHERE cnpjcpf IS NOT NULL AND cnpjcpf != ''
        LIMIT 10
    """).fetchall()
    if rows:
        for r in rows: print(r)
    else:
        print("  (zero linhas com cnpjcpf preenchido)")
except Exception as e:
    print("ERRO:", e)

print()
print("=== JOIN direto: fornecedores x CEIS (sem normalização) ===")
try:
    n = con.execute("""
        SELECT COUNT(*) FROM estado_ac_fornecedores f
        JOIN federal_ceis c ON f.cnpj = c.cnpj_cpf
    """).fetchone()[0]
    print("Matches diretos:", n)
except Exception as e:
    print("ERRO join direto:", e)

print()
print("=== JOIN com regexp_replace (strip não-dígitos) ===")
try:
    n = con.execute("""
        SELECT COUNT(*)
        FROM estado_ac_fornecedores f
        JOIN federal_ceis c
          ON regexp_replace(f.cnpj,    '[^0-9]', '', 'g')
           = regexp_replace(c.cnpj_cpf,'[^0-9]', '', 'g')
        WHERE LENGTH(regexp_replace(f.cnpj, '[^0-9]', '', 'g')) >= 11
    """).fetchone()[0]
    print("Matches após strip:", n)
except Exception as e:
    print("ERRO join strip:", e)

print()
print("=== Amostra CEIS filtrado AC (uf_sancionado) ===")
rows = con.execute("""
    SELECT cnpj_cpf, nome_sancionado, uf_sancionado
    FROM federal_ceis
    WHERE uf_sancionado = 'AC'
    LIMIT 5
""").fetchall()
print(f"  Registros CEIS com UF=AC: {len(rows)}")
for r in rows: print(" ", r)

# conta total AC
n_ac = con.execute(
    "SELECT COUNT(*) FROM federal_ceis WHERE uf_sancionado = 'AC'"
).fetchone()[0]
print(f"  Total CEIS UF=AC: {n_ac}")

con.close()
