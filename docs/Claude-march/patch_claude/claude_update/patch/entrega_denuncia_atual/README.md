# Entrega Atual - Resultados para Denuncias

Esta pasta reune, em um unico lugar, o que o software ja produziu de forma utilizavel para o recorte atual `Acre / Rio Branco / SUS`.

## O que ja temos de concreto

Sim, ja existem elementos concretos para representacao preliminar.

## Estado do painel

Hoje nao existe UI/painel web real. O que passou a existir nesta fase foi a fundacao operacional para esse painel:
- `registry` de casos no banco
- `registry` de artefatos
- API operacional para listar casos e anexos
- tipos de cliente prontos em `frontend/lib`

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
- camada conservadora de vinculo politico/societario agora materializada:
  `37` empresas com QSA + contratos publicos avaliadas
  `1` caso com match objetivo exato em base publica
  caso atual: `CEDIMP` (`13.325.100/0001-30`) com `R$ 6.604.510,00` em `1` contrato da `SESACRE`, e `2` socios aparecendo de forma exata como servidores concursados da `SEMSA`
  este achado ficou classificado como `HIPOTESE_INVESTIGATIVA / EXPLORATORIO / REVISAO_INTERNA`, nao como denuncia pronta
- follow-up de saude ja fechado para esse caso:
  `CNES` oficial `6861849`
  estabelecimento `SADT ISOLADO`, gestao `ESTADUAL`, em `RIO BRANCO`
  CNAE `Servicos de tomografia`
  contrato `779/2023` da `SESACRE`
  `2` socios com cargo medico de `20h` na `SEMSA`
  `8` classificacoes CNES de diagnostico por imagem, incluindo `RADIOLOGIA`, `TOMOGRAFIA COMPUTADORIZADA` e `ULTRASONOGRAFIA`
  o modulo de profissionais do proprio `CNES` lista nominalmente os `2` socios como profissionais ativos do estabelecimento, ambos com `CBO 225320 - MEDICO EM RADIOLOGIA E DIAGNOSTICO POR IMAGEM` e `20h` ambulatoriais
  isso gerou um segundo insight factual: `FATO_DOCUMENTAL / DOCUMENTAL_CORROBORADO / APTO_APURACAO`
  a ficha individual oficial desses mesmos profissionais no proprio `CNES` agora materializa `131` competencias com concomitancia documental entre linha `ESTATUTARIO / SERVIDOR PROPRIO` e linha da `CEDIMP`
  isso gerou um terceiro insight factual: `VINCULO_EXATO_CNES_HISTORICO_PUBLICO_PRIVADO_SAUDE`, classificado como `FATO_DOCUMENTAL / DOCUMENTAL_PRIMARIO / APTO_APURACAO`
  a mesma trilha agora foi convertida em metrica documental de carga:
  `2` profissionais com concomitancia
  `262` competencias concomitantes no total
  `234` competencias `>=60h`
  `28` competencias `>=80h`
  pico documental de `100h` no proprio historico oficial do `CNES`
  isso gerou um quarto insight factual: `VINCULO_EXATO_CNES_CARGA_CONCOMITANTE_SAUDE`, ainda tratado como fato documental para apuracao, e nao como juizo automatico de ilegalidade
  a camada seguinte agora virou matriz juridico-funcional separada, com normas oficiais, perguntas de apuracao e destaque para `1` socio-administradora em coincidencia exata com cargo publico municipal
  a camada seguinte agora virou tambem triagem funcional prioritaria, separada do motor acusatorio:
  score interno `80`
  prioridade `ALTA`
  flags centrais: `socio_administrador_em_base_publica`, `carga_documentada_ge_80h`, `delta_publico_cnes_local_ge_20h`, `multiplos_estabelecimentos_publicos_cnes`
  isso gerou `1` insight interno em `REVISAO_INTERNA`, adequado para diligencia e nao para acusacao automatica
  por fim, o caso ja ganhou pacote de diligencias dirigidas:
  dossie de requisicoes
  csv de itens por destino
  modelo de pedido preliminar para `SEMSA/RH`
  modelo de pedido preliminar para `SESACRE`
  e ganhou tambem matriz de maturidade probatoria, separando o que hoje esta:
  `COMPROVADO_DOCUMENTAL`
  `PENDENTE_DOCUMENTO`
  `PENDENTE_ENQUADRAMENTO`
  `SEM_BASE_ATUAL`
  a partir de agora tambem existe uma camada de respostas oficiais, com indice de documentos esperados, hash local e cobertura por eixo
  e agora existe tambem um gate operacional conservador, que decide se o caso esta em `TRIAGEM_INTERNA`, `APTO_OFICIO_DOCUMENTAL`, `APTO_ANALISE_JURIDICO_FUNCIONAL` ou `APTO_REPRESENTACAO_PRELIMINAR`
  e agora esta empacotado em bundle proprio do caso `CEDIMP`, com hash unico e todos os manifests internos
  o sistema continua conservador: essa camada prova coexistencia documental em fonte primaria de saude, nao prova sozinha acumulo ilicito, impedimento legal ou nepotismo

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
- `TRACE_VINCULO_SOCIETARIO_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_MANIFEST.json`
- `trace_vinculo_societario_resumo.csv`
- `trace_vinculo_societario_matches.csv`
- `trace_vinculo_societario_contratos.csv`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_MANIFEST.json`
- `trace_vinculo_societario_saude_followup.csv`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_MANIFEST.json`
- `trace_vinculo_societario_saude_juridico.csv`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_MANIFEST.json`
- `trace_vinculo_societario_saude_apuracao_funcional.csv`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS_MANIFEST.json`
- `trace_vinculo_societario_saude_diligencias.csv`
- `pedido_preliminar_semsa_cedimp.txt`
- `pedido_preliminar_sesacre_cedimp.txt`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_MANIFEST.json`
- `trace_vinculo_societario_saude_maturidade.csv`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_MANIFEST.json`
- `trace_vinculo_societario_saude_respostas.csv`
- `cedimp_respostas/`
- `cedimp_respostas/README.md`
- `cedimp_respostas/cedimp_respostas_index.csv`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_MANIFEST.json`
- `trace_vinculo_societario_saude_gate.csv`
- `nota_operacional_cedimp.txt`
- `cedimp_case_bundle_20260313.tar.gz`
- `CEDIMP_CASE_BUNDLE_MANIFEST.json`
- `RF_RNF_VINCULO_SOCIETARIO_SAUDE.md`
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
- um caso objetivo de sobreposicao societaria exata entre contrato estadual e base municipal de servidores:
  `CEDIMP` (`13.325.100/0001-30`) em contrato `779/2023` da `SESACRE`, com `2` socios aparecendo nominalmente como servidores concursados da `SEMSA`
  o sistema trata este achado apenas como triagem conservadora, sem afirmar nepotismo, conflito ilegal ou favorecimento
- follow-up oficial do mesmo caso em fonte primaria de saude:
  `CNES 6861849`, ficha oficial aberta do Ministerio da Saude
  estabelecimento `UNIDADE DE APOIO DIAGNOSE E TERAPIA (SADT ISOLADO)`, gestao `ESTADUAL`
  `8` classificacoes oficiais de diagnostico por imagem no `CNES`
  o modulo oficial de profissionais do mesmo `CNES` lista `MAIRA SANTIAGO PIRES PARENTE` e `MARCOS PAULO PARENTE ARAUJO` como profissionais ativos do estabelecimento, ambos com `CBO 225320`
  isso eleva a trilha de saude para fato documental corroborado sobre coincidencia funcional, mas ainda nao fecha ilicitude
  a ficha individual oficial desses profissionais agora materializa `131` competencias com concomitancia entre historico `ESTATUTARIO / SERVIDOR PROPRIO` e historico da `CEDIMP`, elevando a trilha para um terceiro fato documental, ainda sem concluir ilegalidade por si so
  a camada seguinte ja ficou quantificada em metrica documental: `262` competencias concomitantes no total, `234` com `>=60h`, `28` com `>=80h` e pico documental de `100h`, sempre como triagem tecnica e nao como veredito juridico automatico
  a matriz juridico-funcional agora organiza esse mesmo caso contra as normas oficiais da `Constituicao Federal` e da `Lei Municipal 1.794/2009`, com perguntas de apuracao sobre compatibilidade de horarios, alcance do art. 107, X, e eventual procedimento disciplinar
  a camada de triagem funcional agora organiza o que deve ser atacado primeiro em diligencia: diferenca `base local x CNES publico`, socio-administradora em base publica e carga documental extrema, mas tudo ainda em `REVISAO_INTERNA`
  o pacote de diligencias agora traduz essa trilha em pedidos objetivos de documentos para `SEMSA/RH` e `SESACRE`, prontos para aprofundamento probatorio sem formular acusacao pronta
  a matriz de maturidade agora diz expressamente o que o sistema pode afirmar e o que ele nao pode afirmar hoje: por exemplo, `carga_concomitante_extrema` esta `COMPROVADO_DOCUMENTAL`, enquanto `compatibilidade_horarios` esta `PENDENTE_DOCUMENTO`, e `nepotismo` e `fraude_penal` estao `SEM_BASE_ATUAL`
  a camada de respostas oficiais agora trava o ciclo seguinte: nada sobe por impressao ou memoria; sobe quando o arquivo entra, ganha hash e passa a contar na cobertura documental do caso

O software ainda nao prova sozinho:
- nepotismo da `NORTE`
- fraude licitatoria especifica da `NORTE`
- substituicao de servidores efetivos por empregados dessa empresa
- impedimento legal do caso `CEDIMP` sem CPF completo, carga horaria, regime e compatibilidade funcional

Essas linhas seguem como hipoteses investigativas fortes, mas ainda precisam de prova adicional especifica.
