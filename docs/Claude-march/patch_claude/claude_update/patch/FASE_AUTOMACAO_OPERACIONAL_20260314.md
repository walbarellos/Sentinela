# Fase Atual - Automacao Operacional

## Onde estamos

O projeto ja tem:
- painel real em `app.py` via Streamlit
- registry operacional de casos:
  - `ops_case_registry`
  - `ops_case_artifact`
- API operacional:
  - `/ops/summary`
  - `/ops/cases`
  - `/ops/cases/{case_id}`
  - `/ops/cases/{case_id}/artifacts`

## Correcao importante

A trilha correta do produto e:
- evoluir o Streamlit existente
- nao abrir UI paralela sem necessidade

## Inspiracao util

- `USAspending`
  - filtro forte
  - drill-down por recebedor / award
- `Oversight.gov`
  - status do caso
  - relatorio-fonte
  - trilha operacional
- `SEC EDGAR`
  - documento primeiro
  - anexo acessivel
  - timeline de evidencia

## Concluido nesta fase

1. `ops_pipeline_run` implementado
2. `ops_source_cache` implementado
3. integracao concluida no Streamlit existente
4. execucoes recentes e fontes monitoradas exibidas na aba `📂 OPERAÇÕES`
5. `app.py` desmontado em modulos de UI
6. status da sidebar passou a refletir estado real do banco operacional
7. `ops_case_inbox_document` implementado
8. inbox operacional entregue para o caso `CEDIMP`
9. rerun de workflow do `CEDIMP` entregue sem shell
10. timeline documental por caso implementada
11. diff textual de artefatos suportados implementado

## TODO imediato

1. implementar `inbox operacional`
2. generalizar timeline e inbox para outros casos
3. melhorar diff documental por tipo de artefato
4. ligar rerun de maturidade/gate ao fluxo de resposta oficial
5. continuar quebrando modulos grandes somente quando houver ganho real de manutencao
6. generalizar a caixa operacional para outros casos alem de `CEDIMP`

## Regra de produto

- nada de destruir o painel atual
- nada de score cosmetico
- tudo deve aproximar o operador do documento-fonte
- hipotese nao pode parecer fato
- cache e run log devem ser aditivos e idempotentes

## Nota do plano

- `ops_pipeline_run`: `9.5/10`
- `ops_source_cache`: `9/10`
- `timeline/diff`: `8.5/10`
- `inbox operacional`: `10/10`

## Proxima implementacao em curso

- `src/ui/streamlit_ops.py` reduzido a orquestrador
- novos modulos:
  - `src/ui/ops_shared.py`
  - `src/ui/ops_data.py`
  - `src/ui/ops_preview.py`
  - `src/ui/ops_sections.py`
- inbox operacional:
  - `src/core/ops_inbox.py`
  - `src/ui/ops_inbox.py`
  - `scripts/sync_ops_inbox.py`
- timeline e diff:
  - `src/core/ops_timeline.py`
  - `scripts/sync_ops_timeline.py`
  - `src/ui/ops_diff.py`
- proximos modulos de produto:
  - timeline agrupada por fase
  - diff documental semantico
