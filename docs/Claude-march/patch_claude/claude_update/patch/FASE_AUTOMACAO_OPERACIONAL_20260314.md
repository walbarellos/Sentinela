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

## TODO imediato

1. implementar `ops_pipeline_run`
2. implementar `ops_source_cache`
3. integrar ambos ao Streamlit existente
4. exibir execucoes recentes e fontes monitoradas na aba `📂 OPERAÇÕES`
5. depois seguir para:
   - inbox operacional
   - timeline documental
   - diff documental

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

- modulo `src/core/ops_runtime.py`
- script `scripts/sync_ops_case_registry.py` com log de execucao
- script `scripts/sync_ops_source_cache.py`
- modulo `src/ui/streamlit_ops.py`
- integracao do resumo operacional ao Streamlit sem manter a logica no `app.py`
