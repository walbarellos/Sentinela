# Entrega Atual - Resultados para Denuncias

Esta pasta reune, em um unico lugar, o que o software ja produziu de forma utilizavel para o recorte atual `Acre / Rio Branco / SUS`.

## O que ja temos de concreto

Sim, ja existem elementos concretos para representacao preliminar.

### 1. Rio Branco / SUS / municipal

- Caso `3895`:
  fornecedor `NORTE DISTRIBUIDORA DE PRODUTOS LTDA`
  CNPJ `37.306.014/0001-48`
  contrato SUS municipal com sancoes ativas cruzadas e materializadas
- trilha dedicada `NORTE` ja materializada:
  `10` contratos no Acre
  `R$ 905.873,95` mapeados
  `3` sancoes CEIS ativas
  `0` coincidencias nominais exatas da socia nas bases locais carregadas
  `0` contratos com sinal textual direto de terceirizacao de pessoal para este CNPJ
  `11` leads separados por nome semelhante (`NORTE`) em contratos estaduais de terceirizacao, somando `R$ 5.156.810,37`, apenas como fila de triagem
  `3` empresas-lead ja enriquecidas com QSA e sem compartilhamento exato de socio com a `NORTE` focal
  `7` matches `contrato -> licitacao candidata` ja materializados para os dois leads prioritarios
  `47` vinculos exatos `contrato -> id_licitacao -> processo` ja resolvidos a partir do proprio portal estadual
  bloco `CENTRAL NORTE / SEPLAN` agora fechado com vinculo exato para `Pregao Presencial 053/2023`, processo `0088.016739.00008/2023-72`, total `R$ 2.020.878,88`
  o match heuristico anterior com `282/2024` foi descartado como divergente para esse bloco
  auditoria `PP 053/2023` materializada: DOE homologa o certame para `NORTE COMERCIO E SERVICOS` (`21.813.150/0001-94`), enquanto o portal de contratos mostra `CENTRAL NORTE` (`36.990.588/0001-15`) nos 5 contratos de 2024 ligados ao mesmo `id_licitacao`
  fila de pendencias de vinculo exato ja exportada com `22` blocos, puxando principalmente `AGRO NORTE` e `NORTE - CENTRO`
  camada `sem id_licitacao` materializada: `20` blocos e `13` insights para contratos altos em que o proprio portal nao expõe a origem licitatória
  recorte `SEJUSP` agora isolado em dossie proprio:
  `NORTE-CENTRO / SEJUSP` = `R$ 1.215.990,24` em servico terceirizado de apoio operacional/administrativo no contrato `076/2024`, sem `id_licitacao` exposto no portal, mas ja ligado no DOE a `ARP 04/2024`, `PP 053/2023` e ao processo `0819.014451.00277/2024-18`
  `AGRO NORTE / SEJUSP` = `R$ 2.987.976,00` em `6` contratos de viaturas/caminhonetes, sem `id_licitacao` exposto no card do portal, mas com atos formais ja fechados para `004/2023`, `151/2023`, `082/2024`, `147/2024`, `149/2024`, `33/2024`, `69/2024` e agora tambem o `170/2023`, ligado ao `DJE/TJAC 24/10/2023`, `ARP 304/2022`, `PE SRP 74/2022` e DOE `08/12/2023`
  linha do tempo publica congelada para o eixo `SEJUSP`:
  `2020` NORTE-CENTRO / PE SRP `023/2019` / `R$ 2.069.261,04` / apoio administrativo-logistica-operacional
  `2023` JWC / PE SRP `241/2021` / `R$ 339.313,85` mensal repactuado / apoio operacional-administrativo com dedicacao exclusiva
  `2023` AGRO NORTE / contratos `004/2023` e `151/2023` / `ARP 008/2022` / `PE 318/2022 - SEPA`
  `2024` NORTE-CENTRO / contrato `076/2024` / `ARP 04/2024` / `PP 053/2023`
  `2024` AGRO NORTE / contratos e adesoes ligados a `ARP 49/2023` / `PE 206/2023` / `FNSP`
  `2025` NORTE-CENTRO / contrato `26/2021` / limpeza com mao de obra
  auditoria `portal x DOE` materializada:
  `004/2023` consistente
  `151/2023` divergente: portal publica `10` caminhonetes, DOE oficial publica `1`
- pacote `AGRO / unidades prioritarias` agora separado do bloco `SEJUSP`:
  `DETRAN` = `R$ 2.716.895,28` em `5` contratos
  cadeia objetiva: aquisicao principal `022/2023` (`R$ 2.296.950,00`) sem `id_licitacao` exposto no portal + manutencoes `001/2023`, `071/2023`, `007/2024` e `086/2024` no mesmo CNPJ
  o DOE fecha o `022/2023` com `6` viaturas a `R$ 382.825,00` por unidade, e o `071/2023` ja referencia manutencao de `6` veiculos L200 2023/2024
  `execucao penal / socioeducativo` = `R$ 4.068.000,00` em `3` contratos
  blocos altos do eixo:
    `038/2023` / `FUNPENACRE` / `R$ 1.330.000,00` / agora fechado como contrato exato no DOE, com `5` caminhonetes a `R$ 266.000,00` por unidade
    `073/2023` / `IAPEN` / `R$ 254.000,00` / agora fechado como contrato exato no DOE de `04/08/2023`, com termo de adesao `26/2023/IAPEN`, processo `4005.014141.00047/2023-70`, `PE SRP 258/2022`, `ARP 010/2022-SEPA`; o card estadual `82564` corrobora a mesma CIAP
    `072/2024` / `ISE` / `R$ 2.484.000,00`
  o DOE do `072/2024` materializa `10` caminhonetes e `R$ 2.480.000,00`, enquanto o portal publica `R$ 2.484.000,00`
- Caso `3898`:
  contrato com anomalia documental forte
  item fora do edital e fora das propostas da licitacao mae

### 2. SESACRE / estadual

- `67` fornecedores com sancao ativa
- `R$ 101.329.947,27` em contratos agregados no recorte atual
- top `10` estadual ja empacotado com dossie, representacao preliminar e extratos CSV

## O que esta nesta pasta

- `relatorio_final_acre_rio_branco_sus.md`
- `painel_prioridades_acre_rio_branco_sus.csv`
- `denuncia_preliminar_3895.txt`
- `denuncia_preliminar_3898.txt`
- `representacao_preliminar_sesacre_top10.txt`
- `acre_rio_branco_sus_master_bundle_20260313.tar.gz`
- `CASO_NORTE_DISTRIBUIDORA.md`
- `TRACE_NORTE_DOSSIE.md`
- `TRACE_NORTE_MANIFEST.json`
- `trace_norte_contratos.csv`
- `TRACE_NORTE_REDE_DOSSIE.md`
- `TRACE_NORTE_REDE_MANIFEST.json`
- `trace_norte_rede_empresas.csv`
- `trace_norte_rede_contratos.csv`
- `TRACE_NORTE_REDE_MATCH_DOSSIE.md`
- `TRACE_NORTE_REDE_MATCH_MANIFEST.json`
- `trace_norte_rede_match_best.csv`
- `TRACE_NORTE_REDE_VINCULO_EXATO_DOSSIE.md`
- `TRACE_NORTE_REDE_VINCULO_EXATO_MANIFEST.json`
- `trace_norte_rede_vinculo_exato.csv`
- `trace_norte_rede_vinculo_divergencias.csv`
- `trace_norte_rede_vinculo_exato_raw/`
- `TRACE_NORTE_PP053_DOSSIE.md`
- `TRACE_NORTE_PP053_MANIFEST.json`
- `trace_norte_pp053_audit.csv`
- `trace_norte_pp053/`
- `TRACE_NORTE_REDE_PENDENCIAS.md`
- `TRACE_NORTE_REDE_PENDENCIAS_MANIFEST.json`
- `trace_norte_rede_pendencias.csv`
- `TRACE_NORTE_REDE_SEM_LICITACAO.md`
- `TRACE_NORTE_REDE_SEM_LICITACAO_MANIFEST.json`
- `trace_norte_rede_sem_licitacao.csv`
- `TRACE_NORTE_SEJUSP_PRIORITARIOS.md`
- `TRACE_NORTE_SEJUSP_PRIORITARIOS_MANIFEST.json`
- `trace_norte_sejusp_blocos.csv`
- `trace_norte_sejusp_docs.csv`
- `trace_norte_sejusp_audit.csv`
- `trace_norte_sejusp/`
- `trace_norte_sejusp_bundle_20260313.tar.gz`
- `TRACE_AGRO_UNIDADES_FOLLOWUP.md`
- `TRACE_AGRO_UNIDADES_FOLLOWUP_MANIFEST.json`
- `trace_agro_unidades_resumo.csv`
- `trace_agro_unidades_followup.csv`
- `trace_agro_unidades_docs.csv`
- `trace_agro_unidades_audit.csv`
- `trace_agro_unidades/`
- `trace_agro_unidades/073_2023_iapen_1.pdf`
- `trace_agro_unidades/073_2023_iapen_1.txt`
- `trace_agro_unidades/agro_iapen_073_2023_portal_card.json`
- `trace_agro_unidades_bundle_20260313.tar.gz`
- `PROXIMO_PASSO.md`

## Limite do que o software prova hoje

O software hoje ja prova:
- existencia de contratos
- existencia de sancoes ativas cruzadas com esses contratos
- existencia de anomalia documental em contrato municipal
- trilha de QSA/socios em parte relevante do top estadual
- trilha focal da `NORTE` com recorte municipal + estadual + CEIS + QSA
- camada inicial da rede de leads `NORTE` em terceirizacao, com QSA e descarte preliminar de socio exato compartilhado
- ponte inicial `contrato -> licitacao candidata` para os dois leads prioritarios
- camada de vinculo exato `contrato -> id_licitacao -> processo` para os leads prioritarios da rede
- descarte objetivo do falso vinculo `CENTRAL NORTE / SEPLAN -> 282/2024`
- divergencia documental objetiva no `PP 053/2023`: vencedor homologado no DOE diferente do fornecedor que aparece nos contratos ligados ao mesmo `id_licitacao`
- fila objetiva dos proximos blocos a resolver, por valor e unidade gestora
- camada explicita de contratos altos sem `id_licitacao` exposto no portal estadual
- separacao objetiva do eixo `SEJUSP` em dois blocos diferentes:
  terceirizacao/apoio (`NORTE-CENTRO`) x aquisicao de viaturas (`AGRO NORTE`)
- linha do tempo publica localmente congelada para o bloco `SEJUSP`, agora com documentos de `2020`, `2023`, `2024` e `2025`
- base formal do `076/2024` fechada no DOE de `26/12/2024`, ligando o contrato a `ARP 04/2024`, `PP 053/2023` e ao processo `0819.014451.00277/2024-18`
- base formal do bloco `AGRO NORTE / SEJUSP` fechada em parte relevante para `2023` e `2024`, com processo, ARP, pregão e fonte de recurso para `004/2023`, `151/2023`, `082/2024`, `147/2024`, `149/2024`, `33/2024` e `69/2024`
- `170/2023` agora fechado com origem formal: o `DJE/TJAC` de `24/10/2023` autorizou a `SEJUSP` a aderir a `ARP 304/2022`, oriunda do `PE SRP 74/2022`, nos exatos quantitativos do contrato (`2` pick-ups, `R$ 532.000,00`), e o DOE de `08/12/2023` materializa a portaria de gestao/fiscalizacao no processo SEI `0819.012803.00093/2023-03`
- divergencia documental objetiva no contrato `151/2023`: portal estadual publica objeto de `10` caminhonetes, enquanto o DOE oficial materializa `1` caminhonete para o `CIEPS`
- cadeia objetiva de frota da `AGRO` no `DETRAN`: aquisicao principal `022/2023` sem `id_licitacao` exposto no portal, seguida por quatro contratos de revisao/manutencao no mesmo CNPJ, somando `R$ 2.716.895,28`
- auditoria formal do bloco `DETRAN`: extrato oficial do `022/2023` com `6` viaturas e compatibilidade quantitativa com o `071/2023`, que ja publica manutencao de `6` veiculos L200
- expansao objetiva da `AGRO` no eixo `FUNPENACRE / IAPEN / ISE`, somando `R$ 4.068.000,00` em tres contratos altos de caminhonete sem `id_licitacao` exposto no portal
- `038/2023` do `FUNPENACRE` ja tem contrato exato fechado no DOE, consistente com o `Termo de Adesao 5/2023/IAPEN`, no mesmo processo `4005.014135.00006/2023-90`
- `073/2023` do `IAPEN` ja tem contrato exato fechado no DOE de `04/08/2023`, com termo de adesao `26/2023/IAPEN`, processo `4005.014141.00047/2023-70`, `PE SRP 258/2022`, `ARP 010/2022-SEPA` e card estadual convergente para a `CIAP / Convenio 905916/2020 MJ/DEPEN`
- divergencia nominal objetiva no `072/2024`: portal estadual com `R$ 2.484.000,00` e DOE oficial com `R$ 2.480.000,00`, mantendo `10` unidades no extrato

O software ainda nao prova sozinho:
- nepotismo da `NORTE`
- fraude licitatoria especifica da `NORTE`
- substituicao de servidores efetivos por empregados dessa empresa

Essas tres linhas seguem como hipoteses investigativas fortes, mas ainda precisam de prova adicional especifica.
