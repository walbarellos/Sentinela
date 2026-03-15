# RF/RNF - Onus Probatorio, Timeline por Fase, Diff Semantico e Busca Operacional

## RF

1. O sistema deve materializar `ops_case_burden_item` por `case_id`, com:
   - `item_label`
   - `status`
   - `evidence_grade`
   - `legal_anchors_json`
   - `source_refs_json`
   - `rationale`
   - `next_action`

2. O sistema deve usar apenas ancoras normativas oficiais para a matriz de onus:
   - Constituicao Federal
   - Lei 12.527/2011
   - Lei 14.133/2021
   - Lei 12.846/2013
   - Decreto 11.129/2022

3. O sistema deve distinguir claramente:
   - `COMPROVADO_DOCUMENTAL`
   - `PENDENTE_DOCUMENTO`
   - `PENDENTE_ENQUADRAMENTO`
   - `SEM_BASE_ATUAL`

4. O sistema deve agrupar a timeline do caso por fase investigativa:
   - materializacao do caso
   - evidencia local
   - diligencia documental
   - processamento analitico
   - resposta oficial
   - consolidacao externa

5. O sistema deve materializar comparacoes semanticas por caso em `ops_case_semantic_issue`.

6. O diff semantico deve priorizar:
   - `contrato x licitacao`
   - `contrato x edital/publicacao`
   - `item critico x edital`
   - `item critico x propostas`

7. O sistema deve marcar `INSUFICIENTE` quando nao houver base documental local congelada para concluir.

8. A busca full-text deve suportar filtros por:
   - familia
   - tipo de arquivo
   - origem
   - evento
   - orgao
   - case_id

9. A busca deve gerar snippet denso e nao apenas primeira ocorrencia.

## RNF

1. O sistema nao pode promover automaticamente `hipotese` a `representacao`.
2. O sistema nao pode usar nome parecido como prova juridica.
3. O sistema nao pode afirmar nepotismo, fraude penal ou acumulacao ilicita sem documento especifico.
4. O sistema nao pode depender de API fechada ou credencial privada para a operacao basica do caso.
5. O sistema nao pode esconder ausencia de base; deve marcar `SEM_BASE_ATUAL` ou `INSUFICIENTE`.

## MER

- `ops_case_registry` 1:N `ops_case_artifact`
- `ops_case_registry` 1:N `ops_case_inbox_document`
- `ops_case_registry` 1:N `ops_case_burden_item`
- `ops_case_registry` 1:N `ops_case_semantic_issue`
- `ops_case_registry` 1:N `v_ops_case_timeline_event`

## DER

- `ops_case_burden_item`
  - `burden_id` PK
  - `case_id`
  - `family`
  - `item_key`
  - `item_label`
  - `status`
  - `status_order`
  - `evidence_grade`
  - `legal_anchors_json`
  - `source_refs_json`
  - `rationale`
  - `next_action`
  - `updated_at`

- `ops_case_semantic_issue`
  - `issue_id` PK
  - `case_id`
  - `comparator`
  - `field_key`
  - `status`
  - `severity`
  - `left_label`
  - `left_value`
  - `center_label`
  - `center_value`
  - `right_label`
  - `right_value`
  - `rationale`
  - `source_refs_json`
  - `updated_at`

## Regra de uso externo

- `COMPROVADO_DOCUMENTAL` e `DIVERGENTE` podem sustentar apuracao.
- `PENDENTE_DOCUMENTO` exige diligencia, nao acusacao.
- `PENDENTE_ENQUADRAMENTO` exige leitura juridica humana.
- `SEM_BASE_ATUAL` bloqueia tese.
- `INSUFICIENTE` bloqueia conclusao semantica.
