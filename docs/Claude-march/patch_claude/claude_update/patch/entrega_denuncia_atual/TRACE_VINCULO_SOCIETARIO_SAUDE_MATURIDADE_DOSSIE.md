# Trace Vinculo Societario em Saude - Maturidade Probatoria

Matriz de resposta para dizer com clareza o que esta provado, o que ainda depende de documento e o que nao tem base atual.

## Regra

- `COMPROVADO_DOCUMENTAL`: fato sustentado por fonte primaria ou base publica objetiva.
- `PENDENTE_DOCUMENTO` / `PENDENTE_ENQUADRAMENTO`: exige documento ou analise juridica adicional.
- `SEM_BASE_ATUAL` / `SEM_CONCLUSAO_AUTOMATICA`: o sistema nao deve afirmar isso hoje.

## Linhas: `11`

### acumulacao_ilegal

- status: `SEM_CONCLUSAO_AUTOMATICA`
- uso externo: `REVISAO_INTERNA`
- evidencia: A prova atual nao basta para concluir acumulacao ilegal sem confrontar regime, autorizacoes e horarios.
- proximo documento/ato: Processo funcional, declaracoes e validacao juridica.

### carga_concomitante_extrema

- status: `COMPROVADO_DOCUMENTAL`
- uso externo: `APTO_APURACAO`
- evidencia: 28 competencias com >=80h documentadas; pico de 100h.
- proximo documento/ato: Confrontar jornadas com norma funcional e autorizacoes.

### cnes_historico_concomitante

- status: `COMPROVADO_DOCUMENTAL`
- uso externo: `APTO_APURACAO`
- evidencia: 262 competencias concomitantes documentadas no historico oficial do CNES.
- proximo documento/ato: Escalas, ponto e compatibilidade por competencia.

### cnes_profissional_ativo

- status: `COMPROVADO_DOCUMENTAL`
- uso externo: `APTO_APURACAO`
- evidencia: 2 profissional(is) coincidente(s) listado(s) no modulo oficial de profissionais do CNES.
- proximo documento/ato: Fichas individuais dos profissionais no CNES.

### compatibilidade_horarios

- status: `PENDENTE_DOCUMENTO`
- uso externo: `REVISAO_INTERNA`
- evidencia: A prova atual documenta carga e concomitancia, mas nao fecha compatibilidade formal de horarios.
- proximo documento/ato: Escalas, ponto, declaracoes e pareceres de compatibilidade.

### contrato_estadual

- status: `COMPROVADO_DOCUMENTAL`
- uso externo: `APTO_APURACAO`
- evidencia: Contrato 779/2023 da SECRETARIA DE ESTADO DE SAÚDE - SESACRE identificado no valor de R$ 6.604.510,00.
- proximo documento/ato: Processo integral do contrato e anexos de execucao.

### fraude_penal

- status: `SEM_BASE_ATUAL`
- uso externo: `NAO_USAR_EXTERNAMENTE`
- evidencia: Nao ha base automatizada atual para afirmar fraude penal consumada.
- proximo documento/ato: Depende de apuracao humana, contraditorio e prova adicional.

### nepotismo

- status: `SEM_BASE_ATUAL`
- uso externo: `NAO_USAR_EXTERNAMENTE`
- evidencia: Nao ha base objetiva atual para afirmar nepotismo neste caso.
- proximo documento/ato: Somente se surgirem documentos especificos de parentesco ou nomeacao cruzada.

### qsa_socios

- status: `COMPROVADO_DOCUMENTAL`
- uso externo: `APTO_APURACAO`
- evidencia: 2 coincidencia(s) nominal(is) exata(s) entre QSA e base municipal local.
- proximo documento/ato: Confirmacao funcional completa e declaracoes de acumulacao.

### socio_administrador

- status: `COMPROVADO_DOCUMENTAL`
- uso externo: `APTO_APURACAO`
- evidencia: Ha socio-administrador com coincidencia exata em base publica municipal.
- proximo documento/ato: Ficha funcional e eventual autorizacao para gerencia/administracao societaria.

### vedacao_art_107_x

- status: `PENDENTE_ENQUADRAMENTO`
- uso externo: `REVISAO_INTERNA`
- evidencia: Ha dado objetivo de socio-administrador, mas a incidência juridica do art. 107, X, ainda depende de enquadramento funcional e excecoes aplicaveis.
- proximo documento/ato: Analise juridica do estatuto municipal e da situacao funcional concreta.
