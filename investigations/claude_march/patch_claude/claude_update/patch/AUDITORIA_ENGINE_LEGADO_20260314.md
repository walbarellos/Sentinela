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
- `empresa_suspensa` fica em `COBERTO_OPS` e nao roda mais no legado
- `fracionamento`, `outlier_salarial`, `viagem_bloco`, `concentracao_mercado`, `fim_de_semana`, `nepotismo_sobrenome` e `doacao_contrato` ficam em `APOSENTADO` e nao rodam mais nem com `--allow-internal`
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
- `fracionamento`
- `viagem_bloco`
- `concentracao_mercado`
- `fim_de_semana`
- `nepotismo_sobrenome`
- `doacao_contrato`

Detector coberto na trilha principal:
- `empresa_suspensa`
  - o cruzamento sancionatório confiável permanece em `ops` e nos syncs canônicos, não no engine legado

Mapa final de sobrevivencia do legado:
- `COBERTO_OPS`: `empresa_suspensa`
- `APOSENTADO`: `fracionamento`, `outlier_salarial`, `viagem_bloco`, `concentracao_mercado`, `fim_de_semana`, `nepotismo_sobrenome`, `doacao_contrato`

Conclusao operacional:
- o `cross_reference_engine.py` ficou efetivamente congelado;
- ele permanece no repositório apenas por compatibilidade histórica e auditoria técnica;
- a trilha viva do produto é `ops_*`.

Saneamento do acervo historico:
- a tabela `alerts` foi integralmente rebaixada para `REVISAO_INTERNA`;
- o status historico ativo passou a `QUARENTENA`;
- a UI de quarentena passou a ler `v_alerts_legacy_quarantine`, e nao a tabela bruta.

Motivo adicional para aposentar `fracionamento` no legado:
- recorte real de `obras` sem candidatos compactos;
- recortes municipal e estadual atuais sem blocos pequenos e compactos que justificassem promover o detector sem processo integral e cotejo de objeto.

Motivo adicional para aposentar `outlier_salarial` no legado:
- tabela `servidores` sem competência/mês explícitos;
- múltiplas linhas por pessoa/cargo;
- candidatos fortes concentrados em carreiras com alta incidência de verbas e eventos de folha, sem trilha funcional suficiente no legado.
