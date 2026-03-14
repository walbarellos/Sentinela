# Trace Vinculo Politico/Societario

Camada conservadora para sobreposicoes exatas entre quadro societario, contratos publicos e bases publicas.

## Resumo

- empresas-alvo com QSA e contratos: `37`
- empresas com match objetivo: `1`
- matches brutos materializados: `4`
- contratos relacionados exportados: `1`
- insights gerados: `1`

## Leitura de rigor

- Esta camada nao prova nepotismo, conflito ilegal ou favorecimento por si so.
- Ela prova apenas coincidencia exata entre nome societario e base publica carregada.
- Quando o QSA mascara CPF, a coincidencia por nome permanece objetiva, mas exige confirmacao documental externa para elevar o achado.

## Casos

### CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA

- CNPJ: `13325100000130`
- exposicao publica mapeada: `R$ 6.604.510,00`
- socios no QSA: `2`
- socios com match objetivo: `2`
- pessoas distintas com match: `2`
- bases objetivas atingidas: `2`
- contratos relacionados exportados: `1`
- flags conservadoras: `sobreposicao_socio_servidor`
- orgaos/contratos mapeados:
  - `estado_ac_contratos` / `SECRETARIA DE ESTADO DE SAÚDE - SESACRE` / `1` contrato(s) / `R$ 6.604.510,00`
- matches objetivos:
  - socio `MAIRA SANTIAGO PIRES PARENTE` -> `servidores` -> `MAIRA SANTIAGO PIRES PARENTE` / `1617 - MEDICO ULTRASONOGRAFISTA` / `Prefeitura de Rio Branco` / `CONCURSADO`
  - socio `MAIRA SANTIAGO PIRES PARENTE` -> `rb_servidores_lotacao` -> `MAIRA SANTIAGO PIRES PARENTE` / `1617 - MEDICO ULTRASONOGRAFISTA` / `SEMSA` / `CONCURSADO`
  - socio `MARCOS PAULO PARENTE ARAUJO` -> `servidores` -> `MARCOS PAULO PARENTE ARAUJO` / `706 - MEDICO RADIOLOGIA 20H GPO-5B` / `Prefeitura de Rio Branco` / `CONCURSADO`
  - socio `MARCOS PAULO PARENTE ARAUJO` -> `rb_servidores_lotacao` -> `MARCOS PAULO PARENTE ARAUJO` / `706 - MEDICO RADIOLOGIA 20H GPO-5B` / `SEMSA` / `CONCURSADO`
- contratos relacionados:
  - `estado_ac_contratos` / `2023` / `779/2023` / `SECRETARIA DE ESTADO DE SAÚDE - SESACRE` / `R$ 6.604.510,00`
- limites do achado:
  - inferencia permitida: Ha achado preliminar para triagem.
  - limite: Sem corroboracao adicional, o sistema nao deve elevar este achado para conclusao acusatoria.
