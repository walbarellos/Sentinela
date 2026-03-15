# Protocolo Probatorio do Sentinela

## Regra central

O sistema nao deve transformar hipotese em acusacao.

Cada insight passa a sair com:
- `classe_achado`
- `grau_probatorio`
- `fonte_primaria`
- `uso_externo`
- `inferencia_permitida`
- `limite_conclusao`

## Classes de achado

- `FATO_DOCUMENTAL`
  - contrato, termo, portaria, extrato, homologacao ou vinculo formal publicado em fonte oficial
- `DIVERGENCIA_DOCUMENTAL`
  - conflito objetivo entre portal, DOE, DJE, edital, proposta, homologacao ou outro ato publico
- `CRUZAMENTO_SANCIONATORIO`
  - match objetivo de CNPJ/entidade com CEIS, CNEP, CEPIM ou CEAF
- `RASTRO_CONTRATUAL`
  - cadeia contratual, origem parcial, compatibilidade material ou rastro util para aprofundamento
- `HIPOTESE_INVESTIGATIVA`
  - pista societaria, relacional, nominal ou contratual que ainda nao fecha prova

## Graus probatorios

- `DOCUMENTAL_CORROBORADO`
  - ha documento primario e corroboracao por outra fonte publica
- `DOCUMENTAL_PRIMARIO`
  - ha documento oficial suficiente para fato objetivo, mas sem segunda fonte forte
- `INDICIARIO`
  - ha compatibilidade relevante, mas nao fechamento formal
- `EXPLORATORIO`
  - triagem interna; nao serve para conclusao externa sozinho

## Uso externo

- `APTO_A_NOTICIA_DE_FATO`
  - pode integrar noticia de fato tecnicamente fundamentada
- `APTO_APURACAO`
  - bom para auditoria, correcao, pedido de informacao e aprofundamento
- `REVISAO_INTERNA`
  - nao deve sair como conclusao externa sem revisao humana

## Limites operacionais

O sistema nao deve afirmar automaticamente:
- nepotismo
- conluio oculto
- ligacao com MP, PF ou politico especifico sem documento
- fraude penal consumada
- "pedalada fiscal" sem modulo fiscal proprio

## O que e aceitavel afirmar

- ha divergencia documental objetiva
- ha cruzamento sancionatorio objetivo
- ha rastro contratual relevante
- ha fato documental formalmente publicado
- ha hipotese investigativa que demanda revisao humana

## Leituras corretas

- `RB_CONTRATO_LICITACAO_INCONSISTENTE`
  - divergencia documental
- `RB_CONTRATO_SANCIONADO`
  - cruzamento sancionatorio
- `SESACRE_SANCAO_ATIVA`
  - cruzamento sancionatorio
- `TRACE_NORTE_SEJUSP_170_ORIGEM_ADESAO_TJAC`
  - fato documental
- `TRACE_AGRO_DETRAN_FROTA_CADEIA`
  - rastro contratual

## Objetivo

Entregar material fiel para CGU, MP, PF, TCE e controle interno:
- fato
- fonte
- cadeia de evidencia
- inferencia permitida
- limite da conclusao
