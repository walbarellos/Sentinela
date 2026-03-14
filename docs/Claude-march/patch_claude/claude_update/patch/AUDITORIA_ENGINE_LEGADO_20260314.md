# Auditoria do Engine Legado

Data: `2026-03-14`

Objetivo:
- reduzir risco do `cross_reference_engine.py`;
- impedir uso acidental do motor legado como se fosse camada externa do produto;
- manter compatibilidade apenas para triagem tecnica interna.

## Decisao

Todos os detectores do engine legado passaram a ficar bloqueados por padrao.

Lista atual:
- `fracionamento`
- `outlier_salarial`
- `viagem_bloco`
- `concentracao_mercado`
- `empresa_suspensa`
- `doacao_contrato`
- `fim_de_semana`
- `nepotismo_sobrenome`

Regra:
- so rodam com `--allow-internal`
- qualquer `Alert` legado fica com `uso_externo = REVISAO_INTERNA`

## Motivo

Esses detectores:
- podem orientar triagem;
- mas nao devem operar como camada padrao do produto;
- e nao devem ser confundidos com a trilha `ops`, que ja possui gate, burden, calibracao, sentinela e saida nao-acusatoria.

## Validacao

- `scripts/validate_cross_reference_engine.py`
- smoke:
  - `python -m src.core.cross_reference_engine --detector fracionamento`
  - resultado esperado: bloqueio por padrao e nenhum alerta gerado

## Consequencia pratica

Uso normal do produto:
- `ops_*`

Uso excepcional, tecnico e interno:
- `cross_reference_engine.py --allow-internal`
