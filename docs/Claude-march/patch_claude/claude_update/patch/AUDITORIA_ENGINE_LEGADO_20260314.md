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
- `fracionamento` fica em `CANDIDATO_OPS` e so roda com `--allow-internal`
- `outlier_salarial` fica em `LAB_INTERNO` e so roda com `--allow-internal`
- `empresa_suspensa` fica em `COBERTO_OPS` e nao roda mais no legado
- `viagem_bloco`, `concentracao_mercado`, `fim_de_semana`, `nepotismo_sobrenome` e `doacao_contrato` ficam em `APOSENTADO` e nao rodam mais nem com `--allow-internal`
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

Detectores aposentados:
- `viagem_bloco`
- `concentracao_mercado`
- `fim_de_semana`
- `nepotismo_sobrenome`
- `doacao_contrato`

Detector endurecido mas mantido em laboratorio:
- `outlier_salarial`
  - `n >= 30`
  - `z > 4.0`
  - `delta absoluto >= R$ 5.000,00`

Detector coberto na trilha principal:
- `empresa_suspensa`
  - o cruzamento sancionatório confiável permanece em `ops` e nos syncs canônicos, não no engine legado

Detector candidato a reescrita futura em `ops`:
- `fracionamento`
  - mínimo de `4` contratos
  - janela máxima de `90` dias
  - saída apenas interna até existir cotejo de objeto, modalidade e processo integral

Mapa final de sobrevivencia do legado:
- `CANDIDATO_OPS`: `fracionamento`
- `LAB_INTERNO`: `outlier_salarial`
- `COBERTO_OPS`: `empresa_suspensa`
- `APOSENTADO`: `viagem_bloco`, `concentracao_mercado`, `fim_de_semana`, `nepotismo_sobrenome`, `doacao_contrato`
