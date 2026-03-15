# Dossie Prioritario SUS Rio Branco

Gerado em: `2026-03-14 09:52:02`

Este dossie consolida o estado municipal atual do recorte SUS em Rio Branco.

## Resumo operacional

- Caso municipal ativo: `3898`.
- Natureza do caso ativo: `DIVERGENCIA_DOCUMENTAL`.
- Uso externo recomendado: `noticia de fato` ou `pedido de apuracao preliminar`.
- Caso `3895` rebaixado para nota historica apos validacao temporal do cruzamento sancionatorio.

## Caso ativo - Contrato 3898 / anomalia documental de licitacao

- Contrato: `3898`
- Processo: `3006`
- Termo: `1100007`
- Ano: `2024`
- Secretaria: `01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA`
- Objeto: Contratação de empresa especializada na confecção de material e serviços gráficos, para atender a demanda da Secretaria Municipal de Saúde, no Município de Rio Branco - AC
- Valor de referencia: R$ 31.200,00
- Fila final: `auditoria_documental_licitacao`
- Prioridade final: `95`
- Link do contrato: https://transparencia.riobranco.ac.gov.br/contrato/ver/2274688/

### Fatos objetivos

- A licitacao-mae do processo `3006` foi confirmada no portal municipal: `2274334`.
- O edital e a retificacao oficiais usados no confronto foram as publicacoes `1554` e `1640` da CPL.
- O contrato segue sem fornecedor/CNPJ confirmado por fonte aberta, mas a divergencia documental do item foi preservada por auditoria.

### Itens auditados

- Item `1`: Coletor de Material Perfurocortante | qtd `3000.0` | unit `R$ 5,00` | total `R$ 15.000,00` | propostas `False` | edital `False` | candidatos `0` | anomalia `ITEM_FORA_EDITAL_E_PROPOSTAS` | severidade `HIGH`
  termos auditados: COLETOR, COLETOR DE MATERIAL PERFUROCORTANTE, PERFUROCORTANTE
- Item `2`: Broche / Boton / Pin | qtd `18000.0` | unit `R$ 0,90` | total `R$ 16.200,00` | propostas `True` | edital `True` | candidatos `3`
  termos auditados: BOTON, BROCHE, BROCHE BOTON PIN

### Insight consolidado

- Severidade: `HIGH`
- Confianca: `92`
- Titulo: RB SUS: contrato 3898 diverge da licitação mãe do processo 3006
- Fontes: rb_contratos, rb_contratos_item_audit, transparencia.riobranco.ac.gov.br/contrato, https://transparencia.riobranco.ac.gov.br/licitacao/ver/2274334/, https://cpl.riobranco.ac.gov.br/publicacao/1554, https://cpl.riobranco.ac.gov.br/publicacao/1640

## Nota historica - Contrato 3895 / cruzamento sancionatorio invalidado

- Contrato: `3895`
- Processo: `3044`
- Secretaria: `01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA`
- Fornecedor: `NORTE DISTRIBUIDORA DE PRODUTOS LTDA`
- CNPJ: `37306014000148`
- Valor de referencia: `R$ 82.950,00`
- Data de referencia do contrato: `2024-12-31`
- Primeira data de sancao encontrada: `2025-06-05`
- Linhas invalidadas: `5`
- Motivo de exclusao: `SANCAO_POSTERIOR_AO_CONTRATO`

Leitura correta: o contrato foi preservado apenas como trilha historica de auditoria. Ele nao integra mais a fila municipal ativa nem sustenta insight sancionatorio valido.

## Proximos atos uteis

- Protocolar noticia de fato ou pedido de apuracao com foco na divergencia documental do contrato `3898`.
- Preservar HTML/PDF do contrato, da licitacao-mae e das publicacoes da CPL.
- Manter o `3895` apenas como historico de validacao de falso positivo temporal.
