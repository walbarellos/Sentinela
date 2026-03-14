# Caso Norte Distribuidora - Nota Historica

Este arquivo resume o estado correto e atual da `NORTE DISTRIBUIDORA DE PRODUTOS LTDA` no projeto.

## Identificacao basica

- Razao social: `NORTE DISTRIBUIDORA DE PRODUTOS LTDA`
- Nome fantasia: `NORTE DISTRIBUIDORA`
- CNPJ: `37.306.014/0001-48`
- Municipio/UF: `Ariquemes/RO`

## O que permaneceu objetivo

- O CNPJ continua aparecendo na base `CEIS`, com sancoes publicas fora do Acre.
- O CNPJ continua aparecendo em contratos estaduais do Acre, inclusive em `SEE`, `GOVERNO_ACRE`, `SESACRE`, `SEJUSP` e `SEDET`.
- A trilha `NORTE` continua util como eixo exploratorio para rede empresarial e contratos estaduais.

## O que foi corrigido

- O contrato municipal `3895` / processo `3044` nao integra mais a fila municipal ativa.
- O cruzamento sancionatorio municipal foi invalidado por filtro temporal.
- O insight `RB_CONTRATO_SANCIONADO` foi zerado para esse caso.

Leitura correta:
- a sancao encontrada comecou depois da data de referencia do contrato;
- portanto, o caso `3895` nao sustenta noticia de fato sancionatoria municipal;
- ele permanece apenas como historico de auditoria sobre falso positivo temporal.

## O que ainda nao esta provado

As afirmacoes abaixo continuam sem base fechada no banco atual e nao devem ser tratadas como fato:

- nepotismo da empresa
- fraude de licitacao atribuida especificamente a esse CNPJ
- substituicao de servidores efetivos por empregados da empresa
- fornecimento de mao de obra terceirizada com indicacao politica

## Direcao correta

O recorte `NORTE` continua valido apenas em dois eixos:

- contratos estaduais e rede empresarial correlata;
- trilha historica municipal do `3895`, agora usada so para validacao e controle de falso positivo.
