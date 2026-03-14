# Trace Vinculo Societario em Saude - Gate Operacional

Camada de decisao conservadora para dizer se o caso esta pronto para ofício documental, analise interna ou representacao preliminar.

## Regra

- O gate nao decide ilegalidade.
- O gate so decide o proximo uso operacional permitido pelo estado atual da prova.
- Se os bloqueios remanescentes forem funcionais/juridicos, o caso nao sobe para representacao.

## Casos: `1`

### CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA

- CNPJ: `13325100000130`
- contrato: `779/2023` / `SECRETARIA DE ESTADO DE SAÚDE - SESACRE` / `R$ 6.604.510,00`
- score funcional: `80`
- estagio operacional: `APTO_OFICIO_DOCUMENTAL`
- uso recomendado: `PEDIDO_DOCUMENTAL` / externo `True`
- resumo: O caso CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA (13325100000130) esta em estagio `APTO_OFICIO_DOCUMENTAL`. Contrato 779/2023 da SECRETARIA DE ESTADO DE SAÚDE - SESACRE no valor de R$ 6.604.510,00. Base documental minima=sim; documentos oficiais recebidos=0; documentos localizados=0; score funcional=80.
- requisitos cumpridos:
  - base_documental_minima_fechada
  - triagem_funcional_alta
- bloqueios remanescentes:
  - nenhum_documento_oficial_recebido
  - compatibilidade_horarios_nao_fechada
  - enquadramento_art_107_x_pendente
  - acumulacao_ilegal_sem_conclusao
- recomendacoes:
  - Protocolar pedidos objetivos para SEMSA/RH e SESACRE.
  - Anexar respostas oficiais na caixa local do caso com hash.
  - Nao escalar para representacao preliminar sem documento funcional recebido.
- limite: Esta camada decide fluxo operacional, nao ilegalidade. Ela nao autoriza afirmar nepotismo, fraude penal ou acumulacao ilicita sem fechamento juridico-funcional.
