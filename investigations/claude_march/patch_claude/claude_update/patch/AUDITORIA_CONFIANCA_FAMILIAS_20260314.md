# Auditoria de Confianca por Familia de Caso

Data: `2026-03-14`

Objetivo:
- verificar se as regras ativas do backend permanecem uteis e legitimas;
- separar o que e `prova`, `cruzamento conservador` e `hipotese`;
- registrar cercas contra falso positivo e linguagem excessiva.

## Estado atual

- `ops_case_registry = 12`
- `ops_rule_catalog = 6`
- `ops_rule_validation = 11`
- `ops_rule_validation_fail = 0`
- `ops_rule_validation_warn = 0`

## Familia: `rb_sus_contrato`

### Casos ativos
- `1` caso
- caso ativo: `rb:contrato:3898`

### O que a familia prova hoje
- contrato publicado
- trilha licitatoria materializada
- divergencia objetiva entre item contratual e base licitatoria

### O que a familia nao prova sozinha
- fraude penal
- dolo
- favorecimento pessoal

### Cerca de confianca aplicada
- nenhum caso municipal ativo pode permanecer como `CRUZAMENTO_SANCIONATORIO`
- o falso positivo temporal do `3895` segue bloqueado

### Resultado
- `PASS`

## Familia: `sesacre_sancao`

### Casos ativos
- `10` casos
- todos em `APTO_A_NOTICIA_DE_FATO`

### O que a familia prova hoje
- fornecedor contratado pela `SESACRE`
- fornecedor cruzado com sancao ativa em base publica
- valor contratado e recorte financeiro materializados

### O que a familia nao prova sozinha
- que a sancao ja estava vigente na decisao administrativa especifica
- nulidade automatica do contrato
- omissao deliberada da administracao

### Cerca de confianca aplicada
- linguagem ativa deixou de usar `contratacao concomitante`
- cada caso precisa manter itens minimos de:
  - processo integral
  - consulta de integridade
  - justificativa administrativa
  - execucao/fiscalizacao/pagamentos

### Resultado
- `PASS`

## Familia: `saude_societario`

### Casos ativos
- `1` caso
- caso ativo: `CEDIMP`

### O que a familia prova hoje
- sobreposicao societaria documentada
- historico oficial `CNES`
- concomitancia publico-privada documentada

### O que a familia nao prova sozinha
- nepotismo
- acumulacao ilicita
- impedimento funcional conclusivo
- fraude penal

### Cerca de confianca aplicada
- a trilha ativa nao usa mais rotulo operacional de `nepotismo`
- `NOTICIA_FATO` continua bloqueada para essa familia
- o caso continua limitado a `PEDIDO_DOCUMENTAL` / `APTO_OFICIO_DOCUMENTAL`

### Resultado
- `PASS`

## Julgamento honesto

O backend ativo esta util e nao esta fake, desde que seja lido assim:
- `rb_sus_contrato`: divergencia documental objetiva
- `sesacre_sancao`: cruzamento conservador de sancao ativa com contratacao estadual
- `saude_societario`: triagem funcional documentada, sem qualificacao ilicita automatica

O sistema ainda nao deve ser usado para:
- acusacao penal
- afirmacao automatica de nepotismo
- afirmacao automatica de fraude consumada
- afirmacao automatica de nulidade contratual

## Decisao

- manter as tres familias ativas
- preservar as cercas novas no `ops_rulebook`
- continuar expandindo so depois de validacao empirica por familia
