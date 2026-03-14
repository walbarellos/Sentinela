# Protocolo de Saida Nao-Acusatoria

## Regra central

O produto nao acusa. O produto:
- relata fatos objetivos
- aponta contradicoes documentais
- organiza diligencias
- prepara noticia de fato, pedido documental e apuracao

## O que e vedado na saida operacional

- imputar culpa
- afirmar fraude consumada
- chamar agente ou empresa de corrupto, bandido ou criminoso
- afirmar nepotismo sem documento especifico
- afirmar acumulacao ilicita sem confronto juridico-funcional

## O que e permitido

- `ha divergencia documental objetiva`
- `ha cruzamento sancionatorio objetivo`
- `ha fato formalmente publicado`
- `ha necessidade de apuracao`
- `ha documento pendente essencial`
- `ha contradicao entre contrato, edital e proposta`

## Rotulos preferidos

- `noticia de fato`
- `relato para apuracao`
- `pedido documental`
- `apuracao preliminar`
- `divergencia documental`
- `contradicao objetiva`

## Rotulos bloqueados

- `denuncia imediata`
- `representacao preliminar` como padrao automatico
- `fraude consumada`
- `crime`
- `corrupto`
- `bandido`
- `roubo`

## Gate tecnico

O sistema materializa `ops_case_language_guard` e bloqueia saidas externas acusatorias por texto.

Estado alvo:
- `language_guard_rows = 0`

## Uso institucional correto

- controle interno
- auditoria
- CGU
- TCE
- MP
- PF

sempre com:
- fato
- fonte
- limite da conclusao
- proxima diligencia
