# Arquitetura Ativa

## Fluxo principal

1. ingestão e sync
2. materialização no DuckDB
3. registro operacional em `ops_case_registry`
4. artefatos, inbox, timeline e busca
5. burden, semantic, contradiction, checklist
6. gate de linguagem e exportação
7. congelamento da peça gerada

## Camadas centrais

### Banco

- `data/sentinela_analytics.duckdb`

### Backend operacional

- `src/core/ops_registry.py`
- `src/core/ops_runtime.py`
- `src/core/ops_inbox.py`
- `src/core/ops_timeline.py`
- `src/core/ops_search.py`
- `src/core/ops_burden.py`
- `src/core/ops_semantic.py`
- `src/core/ops_contradiction.py`
- `src/core/ops_checklist.py`
- `src/core/ops_guard.py`
- `src/core/ops_export.py`
- `src/core/ops_rulebook.py`
- `src/core/ops_calibration.py`
- `src/core/ops_sentinel.py`

### UI

- `app.py`
- `src/ui/streamlit_ops.py`
- `src/ui/ops_*`

### Casos e dossiês

- `docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/`

## Tabelas principais

- `ops_case_registry`
- `ops_case_artifact`
- `ops_case_inbox_document`
- `ops_case_generated_export`
- `ops_pipeline_run`
- `ops_source_cache`
- `ops_case_burden_item`
- `ops_case_semantic_issue`
- `ops_case_contradiction`
- `ops_case_checklist`
- `ops_artifact_text_index`
- `v_ops_case_timeline_event`

## Tabelas históricas congeladas

- `alerts`
  - agora somente acervo em quarentena
- `v_alerts_legacy_quarantine`
  - view sanitizada para visualização histórica
