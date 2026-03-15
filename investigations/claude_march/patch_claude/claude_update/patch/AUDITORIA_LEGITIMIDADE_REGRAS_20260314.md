# Auditoria de Legitimidade das Regras - 2026-03-14

## Conclusao executiva

O produto, no estado atual, **e util e autentico para**:

- noticia de fato
- pedido documental
- apuracao preliminar
- triagem probatoria interna

O produto, no estado atual, **nao e legitimo para**:

- acusacao penal
- afirmacao automatica de fraude consumada
- afirmacao automatica de nepotismo
- afirmacao automatica de acumulacao ilicita
- afirmacao automatica de "pedalada fiscal"

Em outras palavras: o sistema **serve** para controle, auditoria, noticia de fato e provocacao institucional. **Nao serve** para substituir analise juridica, instrucao formal ou juizo.

## Estado validado

Validacao materializada no DuckDB principal em `2026-03-14`:

- `cases = 13`
- `artifacts = 39`
- `burden_rows = 66`
- `semantic_rows = 4`
- `contradiction_rows = 2`
- `checklist_rows = 66`
- `language_guard_rows = 0`
- `export_gate_rows = 39`
- `generated_export_rows = 2`
- `generated_export_diff_rows = 0`
- `rule_rows = 5`
- `rule_validation_rows = 6`
- `rule_validation_fail_rows = 0`

Leitura:

- o sistema tem regra expressa de saida nao-acusatoria
- toda contradicao operacional atual tem origem rastreavel em divergencia semantica
- todo caso com uso externo hoje tem ao menos um nucleo comprovado documentalmente
- exportacoes congeladas passam por gate, hash, tamanho, linguagem e secoes minimas obrigatorias

## O que e realmente forte

### 1. Matriz de onus probatorio

E forte porque separa:

- `COMPROVADO_DOCUMENTAL`
- `PENDENTE_DOCUMENTO`
- `PENDENTE_ENQUADRAMENTO`
- `SEM_BASE_ATUAL`

Isso aproxima o sistema da logica de auditoria e evita que hipotese seja tratada como fato.

### 2. Diff semantico orientado a documento

E forte quando compara:

- contrato
- edital/publicacao
- licitacao
- proposta congelada, quando houver

O ganho aqui nao e "IA adivinhando"; e **contradicao documental objetiva**.

### 3. Gate de saida nao-acusatoria

E forte porque a saida externa so nasce em 3 modos:

- `NOTA_INTERNA`
- `PEDIDO_DOCUMENTAL`
- `NOTICIA_FATO`

E ainda exige:

- rationale
- disclaimer
- limite da conclusao
- linguagem limpa

### 4. Cadeia de custodia da exportacao

Agora a peca gerada pode ser congelada como artefato governado do caso, com:

- `sha256`
- `size_bytes`
- `created_at`
- `actor`
- timeline
- preview
- busca local

## O que continua limitado

### 1. Saude societaria

O caso `CEDIMP` e forte como:

- coincidencia societaria
- coincidencia profissional
- concomitancia historica CNES
- carga documental extrema

Mas continua limitado para:

- declarar acumulacao ilicita
- declarar impedimento juridico consumado
- declarar nepotismo

Sem:

- ficha funcional integral
- declaracao de acumulacao
- escala/jornada
- enquadramento juridico humano

o sistema deve permanecer em `pedido documental` e `apuração`.

### 2. Pedalada fiscal

Hoje nao ha motor fiscal-contabil serio. Para isso, faltam:

- `SICONFI`
- `RREO`
- `RGF`
- restos a pagar
- cadeia empenho -> liquidacao -> pagamento

### 3. Nepotismo

Hoje o sistema pode produzir:

- coincidencia nominal exata
- coincidencia societaria
- coincidencia com nomeacao/cargo, se existir

Mas nao pode provar parentesco civil ou biologico sozinho.

## Falhas reais encontradas e corrigidas nesta fase

### 1. Comparacao de objeto contratual era estrita demais

Antes:

- igualdade textual exata

Agora:

- igualdade
- contencao
- forte sobreposicao lexical conservadora

Isso reduz falso positivo em divergencia de objeto.

### 2. Validador de exportacao congelada estava sensivel demais

Problema:

- o proprio disclaimer continha `fraude consumada` e `crime` em contexto negativo
- o validador tratava isso como linguagem imprópria

Correcao:

- o validador passou a respeitar contexto seguro do tipo `nao imputa`

Resultado:

- `rule_validation_fail_rows = 0`

## Julgamento de utilidade

### Serve para MP, PF, PC, CGU, TCE e juiz?

Serve **como insumo tecnico**, se usado do jeito certo:

- fato objetivo
- fonte primaria
- contradicao materializada
- limite da conclusao
- diligencia sugerida

Nao serve, sozinho, como:

- prova pericial conclusiva
- denuncia criminal pronta
- juizo definitivo sobre dolo, fraude ou corrupcao

## Referencias oficiais que realmente fazem sentido

### Brasil

- Constituicao Federal, art. 5o, XXXIII e art. 37  
  https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm
- Lei 12.527/2011  
  https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm
- Lei 14.133/2021  
  https://www.planalto.gov.br/ccivil_03/_Ato2019-2022/2021/Lei/L14133.htm
- Lei 12.846/2013  
  https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2013/lei/l12846.htm
- Decreto 11.129/2022  
  https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2022/decreto/d11129.htm
- Resolucao CNMP 174/2017  
  https://www.cnmp.mp.br/portal/todas-as-noticias/10513-resolucao-disciplina-a-instauracao-e-tramitacao-da-noticia-de-fato-e-do-procedimento-administrativo
- IN PF 255/2023  
  https://www.gov.br/mj/pt-br/acesso-a-informacao/acoes-e-programas/recupera/instrucao_normativa___in_34963175_in_255_2023___regulamenta_as_atividades_de_policia_judiciaria_da_pf.pdf

### Internacional - o que vale importar

- `Oversight.gov`: fila de pendencias, recomendacoes abertas, status claro  
  https://www.oversight.gov/reports/recommendations
- `USAspending`: filtro forte e drill-down por recebedor/award  
  https://api.usaspending.gov/docs/endpoints
- `SEC EDGAR`: documento primeiro, anexo acessivel, busca por evidencia  
  https://www.sec.gov/search-filings
- `OECD`: integridade em procurement, riscos em todo o ciclo, accountability e e-procurement  
  https://www.oecd.org/en/topics/sub-issues/integrity-in-public-procurement.html
- `INTOSAI ISSAI 100`: evidencia suficiente e apropriada, materialidade e julgamento profissional  
  https://www.intosaifipp.org/wp-content/uploads/2023/10/Final-ISSAI-100-after-FIPP-approval-Oct-2023.pdf
- `Israel Ombudsman`: reclamacao contra orgao publico, anexos relevantes, resposta previa do orgao, protecao a whistleblower  
  https://www.mevaker.gov.il/en/ombudsman/activity
  https://library.mevaker.gov.il/En/Ombudsman/Guidecomplainant/Filing-a-Complaint/Pages/default.aspx

## O que nao deve ser importado

- taxonomias federais americanas irrelevantes para municipio pequeno
- dashboards cenograficos
- scoring opaco sem documento
- NLP "acusatorio"
- rede/grafo como enfeite sem lastro primario

## Melhor leitura honesta do produto hoje

O sistema **nao e fake**.

Ele e:

- um motor de triagem probatoria
- um organizador de cadeia documental
- um gerador controlado de noticia de fato e pedido documental
- uma bancada de caso operacional

Ele **nao e**, e nao deve tentar ser:

- delegacia automatica
- acusador automatico
- juiz automatizado
- detector magico de corrupcao

## Proximo passo institucional correto

1. validar empiricamente os falsos positivos e falsos negativos com um conjunto fechado de casos reais
2. ampliar o rulebook com familias novas so quando houver base documental primaria estavel
3. abrir um modulo fiscal-contabil separado para "pedalada", sem contaminar o motor atual
4. manter a regra principal: sem documento primario, sem promocao de estagio
