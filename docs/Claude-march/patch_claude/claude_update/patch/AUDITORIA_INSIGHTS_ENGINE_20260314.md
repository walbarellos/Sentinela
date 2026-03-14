# Auditoria do `insights_engine.py` — 2026-03-14

## Objetivo

Alinhar o motor legado `insights_engine.py` ao mesmo padrão de quarentena já aplicado ao `cross_reference_engine.py`.

## Correções aplicadas

- Todos os detectores do `insights_engine.py` passaram a ficar desligados por padrão.
- O uso agora exige `allow_internal=True`.
- `outlier_salarial` foi endurecido para `n >= 30` e `z > 3.0`.
- `viagem_bloco` foi endurecido para agrupamento mínimo de `4` registros.
- `concentracao_mercado` e `fracionamento` em obras ficaram explicitamente como triagem interna.
- O threshold exploratório de fracionamento foi corrigido para `R$ 50.000,00` no motor legado.
- As descrições e bases normativas foram reescritas para não sugerir conclusão automática de fraude, improbidade ou direcionamento.

## Validação

Executado:

```bash
python -m py_compile insights_engine.py scripts/validate_insights_engine.py
.venv/bin/python scripts/validate_insights_engine.py
```

Resultado esperado e confirmado:

- chamadas padrão retornam `0` insights;
- chamadas com `allow_internal=True` materializam apenas triagem interna;
- o motor legado deixa de se apresentar como camada probatória.

## Conclusão

`insights_engine.py` permanece no repositório apenas como laboratório exploratório explícito. Ele não deve ser tratado como motor confiável de uso externo nem como base primária de notícia de fato.
