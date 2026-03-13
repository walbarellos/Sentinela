# Trace NORTE - Vinculo Exato Contrato x Licitacao

Gerado em: `2026-03-13 16:11:26`

Este arquivo substitui a leitura heuristica quando o proprio portal de contratos entrega `id_licitacao` resolvido.

- Vinculos exatos materializados: `47`
- Empresas com vinculo exato: `3`
- Valor agregado com vinculo exato: `R$ 25.999.359,65`
- Divergencias entre heuristica e vinculo exato: `5`
- Valor agregado dessas divergencias: `R$ 2.020.878,88`

## Bloco confirmado - CENTRAL NORTE / SEPLAN

- Fornecedor: `CENTRAL NORTE COMERCIO E SERVICOS DE APOIO ADMINISTRATI` (`36990588000115`)
- Orgão: `SEPLANH` / `SECRETARIA DE ESTADO DE PLANEJAMENTO - SEPLAN`
- Licitação exata no portal: `053/2023`
- Processo administrativo: `0088.016739.00008/2023-72`
- Modalidade: `PREGAO_PRESENCIAL`
- Abertura: `14/11/2023`
- Situação: `HOMOLOGADA`
- Contratos ligados: `5`
- Valor total dos contratos ligados: `R$ 2.020.878,88`
- Vigências iniciais observadas: `01/01/2024` até `27/03/2024`

O portal de contratos devolve `id_licitacao = 27462` para os 5 contratos da `CENTRAL NORTE` em `SEPLAN`, e o portal de licitações resolve esse id como `Pregão Presencial 053/2023`.

## Divergencia com a heuristica anterior

O bloco `CENTRAL NORTE x SEPLAN` tinha sido aproximado heurísticamente do `processo 282/2024`. Esse vínculo deve ser descartado no recorte atual, porque o próprio portal resolve os contratos para outro processo.

- contrato `001/2024` | valor `R$ 1.404.178,40`
  heuristica: processo `282` / modalidade `PREGAO_ELETRONICO`
  portal exato: licitacao `053/2023` / processo `0088.016739.00008/2023-72` / modalidade `PREGAO_PRESENCIAL`
  motivo: match heuristico diverge do id_licitacao presente no portal de contratos
- contrato `032/2024` | valor `R$ 313.401,48`
  heuristica: processo `282` / modalidade `PREGAO_ELETRONICO`
  portal exato: licitacao `053/2023` / processo `0088.016739.00008/2023-72` / modalidade `PREGAO_PRESENCIAL`
  motivo: match heuristico diverge do id_licitacao presente no portal de contratos
- contrato `007/2024` | valor `R$ 169.149,68`
  heuristica: processo `282` / modalidade `PREGAO_ELETRONICO`
  portal exato: licitacao `053/2023` / processo `0088.016739.00008/2023-72` / modalidade `PREGAO_PRESENCIAL`
  motivo: match heuristico diverge do id_licitacao presente no portal de contratos
- contrato `013/2024` | valor `R$ 95.283,24`
  heuristica: processo `282` / modalidade `PREGAO_ELETRONICO`
  portal exato: licitacao `053/2023` / processo `0088.016739.00008/2023-72` / modalidade `PREGAO_PRESENCIAL`
  motivo: match heuristico diverge do id_licitacao presente no portal de contratos
- contrato `015/2024` | valor `R$ 38.866,08`
  heuristica: processo `282` / modalidade `PREGAO_ELETRONICO`
  portal exato: licitacao `053/2023` / processo `0088.016739.00008/2023-72` / modalidade `PREGAO_PRESENCIAL`
  motivo: match heuristico diverge do id_licitacao presente no portal de contratos

## Arquivos gerados

- `trace_norte_rede_vinculo_exato.csv`
- `trace_norte_rede_vinculo_divergencias.csv`
- diretório `trace_norte_rede_vinculo_exato_raw` com JSON bruto do portal para o bloco `CENTRAL NORTE / SEPLAN`

