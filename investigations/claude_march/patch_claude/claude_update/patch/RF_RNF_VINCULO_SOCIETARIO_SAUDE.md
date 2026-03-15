# RF / RNF - Vinculo Societario em Saude

## Objetivo

Elevar o modulo `vinculo_societario_saude_followup` para um padrao probatorio util a `CGU / PF / TCE`, sem transformar coincidencia em acusacao.

## RF

- `RF-01`: captar a ficha oficial do estabelecimento no `CNES` por `CNPJ + municipio + UF`.
- `RF-02`: captar o modulo oficial de profissionais do mesmo estabelecimento.
- `RF-03`: quando houver match exato de nome com socio e base publica local, abrir a ficha individual oficial do profissional usando a mesma sessao HTTP.
- `RF-04`: extrair `CNS`, `CNS master`, data de atribuicao e historico profissional oficial.
- `RF-05`: separar no historico as linhas:
  - publicas (`ESTATUTARIO` ou `SERVIDOR PROPRIO`)
  - da empresa correlata (`CNES` ou nome do estabelecimento da empresa)
- `RF-06`: calcular por profissional:
  - numero de competencias concomitantes
  - maximo de carga publica documentada
  - maximo de carga da empresa documentada
  - maximo de carga concomitante documentada
  - numero de competencias `>=60h`
  - numero de competencias `>=80h`
- `RF-07`: gravar as metricas no DuckDB em formato idempotente.
- `RF-08`: gerar insight documental apenas quando a concomitancia for comprovada por fonte primaria.
- `RF-09`: manter separado o que e:
  - sobreposicao societaria
  - coincidencia nominal em profissional do CNES
  - historico oficial concomitante
  - carga documental concomitante

## RNF

- `RNF-01`: nenhuma saida pode afirmar automaticamente nepotismo, conflito ilicito, acumulacao vedada ou fraude penal.
- `RNF-02`: o modulo deve operar so com `match exato` de nome nesta etapa; nada de heuristica frouxa.
- `RNF-03`: toda conclusao precisa citar explicitamente o limite do achado.
- `RNF-04`: a execucao deve ser reprodutivel e idempotente no mesmo banco.
- `RNF-05`: a coleta do `CNES` deve usar sessao persistente quando a ficha individual depender disso.
- `RNF-06`: a camada de metricas deve usar marcos `>=60h` e `>=80h` apenas como triagem documental, nunca como juizo legal automatico.
- `RNF-07`: o dossie exportado deve ser legivel e curto; tabelas completas ficam no CSV/JSON.
- `RNF-08`: o modulo nao pode degradar casos anteriores nem sobrescrever classificacao probatoria mais forte com uma mais fraca.

## Regra de ouro

O sistema pode provar:
- coexistencia documental
- carga documentada
- vinculo contratual
- origem primaria da evidencia

O sistema nao pode provar sozinho:
- dolo
- fraude penal
- nepotismo
- impedimento legal consumado
- acumulacao ilicita

Esses pontos exigem norma aplicavel, contexto funcional e revisao humana.
