# Suite Sentinela por Regra

Data: `2026-03-14`

Objetivo:
- detectar regressao de comportamento nas regras mais sensiveis do backend;
- travar retorno de falso positivo conhecido;
- manter as familias ativas em linguagem e enquadramento conservadores.

## Sentinelas ativos

1. `RB_TEMPORAL_FALSE_POSITIVE`
- regra: `FAMILY_CONFIDENCE_GUARD`
- garante que `3895` nao retorne como caso ativo

2. `RB_3898_SEMANTIC_TRIAD`
- regra: `RB_SEMANTIC_TRIAD`
- garante que o `3898` continue com:
  - `item_x_edital = DIVERGENTE`
  - `item_x_propostas = DIVERGENTE`
  - `contrato_x_edital.objeto = COERENTE`
  - `contrato_x_licitacao.numero_processo = COERENTE`

3. `SAUDE_NO_NEPOTISM_LABEL`
- regra: `SAUDE_SOCIETARIO_CNES`
- garante que a trilha societaria nao reintroduza rotulo operacional de `nepotismo`

4. `SAUDE_NOTICIA_FATO_BLOCKED`
- regra: `SAUDE_SOCIETARIO_CNES`
- garante que `CEDIMP` continue bloqueado para `NOTICIA_FATO`

5. `SESACRE_NO_OVERCLAIM_LANGUAGE`
- regra: `SESACRE_SANCTION_CROSS`
- garante que a familia estadual nao volte a usar linguagem temporal forte como `concomitante`

## Resultado

- `sentinel_rows = 5`
- `sentinel_result_rows = 5`
- `sentinel_fail_rows = 0`
- `sentinel_warn_rows = 0`

Resumo:
- `FAMILY_CONFIDENCE_GUARD = 1 PASS`
- `RB_SEMANTIC_TRIAD = 1 PASS`
- `SAUDE_SOCIETARIO_CNES = 2 PASS`
- `SESACRE_SANCTION_CROSS = 1 PASS`

## Leitura correta

Essa suite nao mede “verdade final”.
Ela mede regressao das cercas de seguranca do backend.

Ela prova que:
- o sistema nao reintroduziu o falso positivo municipal conhecido;
- o comportamento semantico do `3898` continua estavel;
- `CEDIMP` continua contido no modo documental;
- `SESACRE` continua em linguagem conservadora.

## Uso correto

Sempre que alterar:
- `ops_burden`
- `ops_registry`
- `ops_semantic`
- `ops_export_gate`
- `ops_guard`

rodar:
1. `scripts/sync_ops_case_registry.py`
2. `scripts/validate_ops_rulebook.py`
3. `scripts/validate_ops_calibration.py`
4. `scripts/validate_ops_sentinel.py`
