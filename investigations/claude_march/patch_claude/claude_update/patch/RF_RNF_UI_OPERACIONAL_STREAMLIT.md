# RF/RNF - UI Operacional Streamlit

## Norte

O painel deve funcionar como mesa de triagem documental:
- fila de casos
- detalhe probatorio
- artefatos visualizaveis no proprio app
- proximo passo objetivo

Nao deve funcionar como vitrine vazia de graficos.

## Inspiracoes uteis

- `USAspending`: busca e drill-down por award/recipient, com filtros fortes e resumo executivo por caso.
- `Oversight.gov`: foco em recomendacao, status, relatorio-fonte e encadeamento institucional.
- `SEC EDGAR`: leitura documental primeiro, com timeline e anexos acessiveis.

## RF

- O Streamlit deve ter uma aba operacional dedicada a casos reais.
- O Streamlit deve ter uma area de inbox operacional para anexar respostas oficiais sem shell quando o caso tiver caixa configurada.
- O usuario deve conseguir filtrar por:
  - familia
  - estagio operacional
  - orgao
  - uso externo
  - texto livre
- O usuario deve abrir o caso sem sair da fila.
- O usuario deve ver, no mesmo fluxo:
  - resumo curto
  - limite da conclusao
  - proximo passo
  - bundle
  - artefatos
  - timeline documental
  - diff de artefatos suportados
- O usuario deve conseguir visualizar localmente:
  - `md`
  - `txt`
  - `json`
  - `csv`
  - `html`
  - `pdf`
- O painel deve mostrar hash e caminho do artefato.
- O painel deve permitir refresh manual do registry sem shell.
- O painel deve permitir sincronizar a caixa e rerodar workflow conhecido do caso sem shell.
- O painel deve mostrar timeline sem exigir leitura de log bruto.
- O painel so deve oferecer diff quando houver texto extraivel.

## RNF

- Nao esconder a fonte primaria atras de score opaco.
- Nao usar grafico quando tabela ou documento for mais util.
- Nao promover `HIPOTESE_INVESTIGATIVA` como caso pronto para representacao.
- Nao depender de chamada remota para abrir caso ja materializado.
- Nao obrigar operador a abrir dezenas de arquivos fora do painel para entender um caso.

## DER funcional

- `ops_case_registry`
  - fila principal
- `ops_case_artifact`
  - trilha de evidencia
- `ops_pipeline_run` (futuro)
  - status de execucao
- `ops_source_cache`
  - ttl / etag / hash de fontes remotas
- `ops_case_inbox_document`
  - diligencias e respostas oficiais por caso
- `v_ops_case_timeline_event`
  - linha do tempo operacional e documental do caso

## Proxima etapa de UX

- adicionar timeline do caso
- adicionar checklists de diligencia
- adicionar busca full-text nos artefatos locais
- melhorar o diff entre documentos de um mesmo caso com recortes e filtros por tipo
- adicionar historico visual do workflow do caso ao lado da inbox
