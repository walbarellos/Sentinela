# RF/RNF - Runbook Operacional por Caso

## Objetivo

Transformar cada caso operacional em uma sequencia institucional minima, com:
- peca recomendada;
- destinatario principal;
- destinatarios secundarios;
- dossie minimo;
- documentos a requerer;
- passos ordenados de diligencia.

O modulo nao acusa, nao decide culpa e nao substitui analise juridica humana.

## Requisitos funcionais

- RF01: materializar `1` runbook por caso em `ops_case_runbook`.
- RF02: materializar `N` passos por caso em `ops_case_runbook_step`.
- RF03: derivar `recommended_mode` do `ops_case_export_gate`.
- RF04: derivar destinatario e canal conforme a familia do caso.
- RF05: anexar base normativa oficial ao runbook e aos passos.
- RF06: expor o runbook no painel Streamlit em aba propria.
- RF07: manter compatibilidade com o `registry`, `burden`, `checklist`, `timeline` e `inbox`.

## Regras de negocio

- RN01: `rb_sus_contrato`
  - prioriza `NOTICIA_FATO` quando liberada pelo gate;
  - destinatario principal: `Controladoria Geral do Municipio de Rio Branco`;
  - foco: contradicao documental e rastro licitatorio.

- RN02: `sesacre_sancao`
  - prioriza `NOTICIA_FATO` quando liberada pelo gate;
  - destinatario principal: `Controladoria-Geral do Estado do Acre`;
  - foco: manutencao contratual diante de sancao ativa e due diligence documental.

- RN03: `saude_societario`
  - limita a `PEDIDO_DOCUMENTAL` enquanto houver pendencia funcional/juridica;
  - destinatario principal: `SEMSA/RH`;
  - foco: ficha funcional, escalas, declaracoes e processo integral.

- RN04: o runbook deve listar somente diligencias proporcionais ao caso.
- RN05: o runbook deve preservar linguagem nao-acusatoria.

## Requisitos nao funcionais

- RNF01: leitura deterministica a partir do banco local; sem inferencia opaca.
- RNF02: compativel com DuckDB e Streamlit atuais.
- RNF03: sem quebrar a cadeia de evidencia ja materializada.
- RNF04: sem depender de shell para o operador final compreender o proximo passo.

## Limites do modulo

O runbook pode:
- orientar protocolo;
- orientar pedido documental;
- ordenar diligencias;
- resumir o que anexar.

O runbook nao pode:
- concluir fraude;
- concluir nepotismo;
- concluir crime;
- promover caso alem do permitido pelo `export gate`.
