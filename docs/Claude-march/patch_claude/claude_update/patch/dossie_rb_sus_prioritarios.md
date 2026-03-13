# Dossie Prioritario SUS Rio Branco

Gerado em: `2026-03-13 10:08:42`

Este dossie consolida os dois casos municipais prioritarios do recorte SUS em Rio Branco com base no banco local do projeto e nas fontes publicas ja integradas.

## Caso 1 — Contrato 3895 / fornecedor sancionado

- Contrato: `3895`
- Processo: `3044`
- Termo: `1100004`
- Ano: `2024`
- Secretaria: `01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA`
- Objeto: A aquisição de Gêneros de natureza alimentícia, quais sejam, café e açúcar, para atender as demandas da Secretaria Municipal de Saúde – SEMSA.
- Valor de referencia: R$ 82.950,00
- Fornecedor: **NORTE DISTRIBUIDORA DE PRODUTOS LTDA**
- CNPJ: `37306014000148`
- Fila final: `denuncia_imediata`
- Prioridade final: `100`
- Link do contrato: https://transparencia.riobranco.ac.gov.br/contrato/ver/1916838/

### Fatos objetivos

- O fornecedor aparece no banco com `5` sancao(oes) ativa(s).
- Fontes de sancao materializadas: `CEIS`.
- O contrato esta vinculado a `SEMSA` e ja consta como caso de denuncia imediata na triagem final.

### Sancoes materializadas

- `CEIS` | Impedimento/proibição de contratar com prazo determinado | inicio `05/06/2025` | fim `29/07/2026` | orgao sancionador `GOVERNO_ACRE` | status `ativa`
- `CEIS` | Impedimento/proibição de contratar com prazo determinado | inicio `05/06/2025` | fim `29/07/2026` | orgao sancionador `SEDET` | status `ativa`
- `CEIS` | Impedimento/proibição de contratar com prazo determinado | inicio `05/06/2025` | fim `29/07/2026` | orgao sancionador `SEE` | status `ativa`
- `CEIS` | Impedimento/proibição de contratar com prazo determinado | inicio `05/06/2025` | fim `29/07/2026` | orgao sancionador `SEJUSP` | status `ativa`
- `CEIS` | Impedimento/proibição de contratar com prazo determinado | inicio `05/06/2025` | fim `29/07/2026` | orgao sancionador `SESACRE` | status `ativa`

### Insight consolidado

- Severidade: `CRITICAL`
- Confianca: `95`
- Titulo: RB SUS: fornecedor sancionado NORTE DISTRIBUIDORA DE PRODUTOS LTDA contratado por 01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA
- Fontes: rb_contratos, sancoes_collapsed, transparencia.riobranco.ac.gov.br/contrato

## Caso 2 — Contrato 3898 / anomalia documental de licitacao

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

- A licitacao mae do processo `3006` ja foi confirmada no portal municipal: `2274334`.
- O edital e a retificacao oficiais da CPL usados no confronto foram as publicacoes `1554` e `1640`.
- O contrato permanece sem fornecedor/CNPJ confirmado por fonte aberta.

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

## Leitura operacional

- `3895` ja esta pronto para representacao como contratacao municipal com fornecedor sancionado.
- `3898` nao fecha fornecedor por fonte aberta, mas fecha uma inconsistencia documental forte: item fora do edital e fora das propostas publicas da licitacao mae.
- Os dois casos permanecem juntos na fila prioritaria municipal, com prioridades `100` e `95`.

## Proximos atos uteis

- Protocolo de representacao com anexacao dos links e evidencias acima.
- Se necessario, diligencia complementar manual em ambiente autenticado do `licitacoes-e` apenas para tentar identificar o lote/fornecedor do `3898`.
- Preservacao de captura PDF/HTML das telas do contrato, da licitacao mae e das publicacoes da CPL.
