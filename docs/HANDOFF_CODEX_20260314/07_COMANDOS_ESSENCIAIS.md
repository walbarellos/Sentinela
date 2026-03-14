# Comandos Essenciais

## Painel principal

```bash
source .venv/bin/activate
streamlit run app.py
```

## Sync operacional principal

```bash
.venv/bin/python scripts/sync_ops_case_registry.py
.venv/bin/python scripts/sync_ops_source_cache.py
.venv/bin/python scripts/sync_ops_inbox.py
.venv/bin/python scripts/sync_ops_timeline.py
.venv/bin/python scripts/sync_ops_search_index.py
.venv/bin/python scripts/sync_ops_burden.py
.venv/bin/python scripts/sync_ops_semantic.py
.venv/bin/python scripts/sync_ops_contradiction.py
.venv/bin/python scripts/sync_ops_checklist.py
.venv/bin/python scripts/sync_ops_guard.py
.venv/bin/python scripts/sync_ops_export_gate.py
.venv/bin/python scripts/sync_ops_rulebook.py
.venv/bin/python scripts/sync_ops_calibration.py
.venv/bin/python scripts/sync_ops_sentinel.py
```

## Validação

```bash
.venv/bin/python scripts/validate_ops_output_guard.py
.venv/bin/python scripts/validate_ops_rulebook.py
.venv/bin/python scripts/validate_ops_calibration.py
.venv/bin/python scripts/validate_ops_sentinel.py
.venv/bin/python scripts/validate_cross_reference_engine.py
.venv/bin/python scripts/validate_insights_engine.py
```

## Saneamento do acervo legado

```bash
.venv/bin/python scripts/sync_legacy_alerts_quarantine.py
```

## Congelar export seguro

```bash
.venv/bin/python scripts/freeze_ops_case_export.py --case-id rb:contrato:3898 --mode NOTICIA_FATO
.venv/bin/python scripts/freeze_ops_case_export.py --case-id cedimp:saude_societario:13325100000130 --mode PEDIDO_DOCUMENTAL
```

## Ver estado do banco

```bash
.venv/bin/python - <<'PY'
import duckdb
con=duckdb.connect('data/sentinela_analytics.duckdb', read_only=True)
print(con.execute("SELECT family, count(*) n FROM ops_case_registry GROUP BY 1 ORDER BY 1").fetchdf().to_string(index=False))
print(con.execute("SELECT estagio_operacional, count(*) n FROM ops_case_registry GROUP BY 1 ORDER BY 1").fetchdf().to_string(index=False))
PY
```
