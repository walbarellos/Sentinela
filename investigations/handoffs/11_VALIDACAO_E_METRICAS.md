# Validação e Métricas

## Métricas confirmadas

- `ops_case_registry = 12`
- `ops_case_artifact = 35`
- `ops_case_inbox_document = 85`
- `ops_case_generated_export = 2`
- `alerts = 1809`
- `alerts em QUARENTENA = 1809`
- `alerts com uso_externo != REVISAO_INTERNA = 0`

## Estado de famílias

- `rb_sus_contrato = 1`
- `sesacre_sancao = 10`
- `saude_societario = 1`

## Estágio operacional

- `APTO_A_NOTICIA_DE_FATO = 11`
- `APTO_OFICIO_DOCUMENTAL = 1`

## Guardas

Validações que devem continuar verdes:

- `validate_ops_output_guard.py`
- `validate_ops_rulebook.py`
- `validate_ops_calibration.py`
- `validate_ops_sentinel.py`
- `validate_cross_reference_engine.py`
- `validate_insights_engine.py`

## Interpretação

Se qualquer uma dessas começar a falhar:

- não emitir peça externa nova
- revisar `ops_case_registry`
- revisar documentação e UI antes de seguir
