# Legado Congelado

## Situação

O legado foi mantido só para:

- compatibilidade histórica
- auditoria técnica
- referência de evolução

## cross_reference_engine.py

Mapa final:

- `COBERTO_OPS`
  - `empresa_suspensa`
- `APOSENTADO`
  - `fracionamento`
  - `outlier_salarial`
  - `viagem_bloco`
  - `concentracao_mercado`
  - `fim_de_semana`
  - `nepotismo_sobrenome`
  - `doacao_contrato`

Consequência:

- não existe mais detector legado executável com utilidade operacional corrente
- o arquivo pode continuar no repositório, mas não deve ser expandido

## insights_engine.py

Situação:

- desligado por padrão
- apenas triagem interna explícita
- não é motor do produto

## tabela alerts

Situação:

- `1809` linhas
- `uso_externo = REVISAO_INTERNA`
- `status = QUARENTENA`
- visualização pela `v_alerts_legacy_quarantine`

## O que não fazer

- não reativar `cross_reference_engine.py` como fonte operacional
- não reintroduzir `alerts` brutos como KPI central
- não usar qualquer `alert` legado como base automática de peça externa
