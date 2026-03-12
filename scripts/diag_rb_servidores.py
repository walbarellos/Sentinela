"""
diag_rb_servidores.py - le schema real de rb_servidores_mass e amostras.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
con = duckdb.connect(str(ROOT / "data" / "sentinela_analytics.duckdb"), read_only=True)

print("=== PRAGMA rb_servidores_mass ===")
df = con.execute("PRAGMA table_info('rb_servidores_mass')").fetchdf()
print(df.to_string(index=False))

print()
print("=== 5 linhas de amostra ===")
rows = con.execute("SELECT * FROM rb_servidores_mass LIMIT 5").fetchdf()
print(rows.to_string(index=False))

print()
print("=== Colunas com valores não-nulos (para saber o que chega preenchido) ===")
cols = [r[1] for r in con.execute("PRAGMA table_info('rb_servidores_mass')").fetchall()]
for col in cols:
    n = con.execute(
        f"SELECT COUNT(*) FROM rb_servidores_mass WHERE {col} IS NOT NULL AND CAST({col} AS VARCHAR) != ''"
    ).fetchone()[0]
    print(f"  {col:30s}: {n}")

print()
print("=== SHOW TABLES (rb_*) ===")
tables = [r[0] for r in con.execute("SHOW TABLES").fetchall() if r[0].startswith("rb_")]
print(tables)
for t in tables:
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {n} linhas")

con.close()
