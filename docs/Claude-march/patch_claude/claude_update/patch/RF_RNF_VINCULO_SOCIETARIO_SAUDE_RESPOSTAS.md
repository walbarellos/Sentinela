# RF/RNF - Vinculo Societario em Saude / Respostas Oficiais

## RF

- O sistema deve manter uma caixa de recepcao controlada para documentos oficiais do caso `CEDIMP`.
- O sistema deve registrar `documento_chave`, `destino`, `eixo`, `status_documento`, `file_relpath`, `sha256` e `size`.
- O sistema deve distinguir `PENDENTE`, `RECEBIDO`, `ANALISADO`, `VALIDADO` e `ARQUIVO_NAO_LOCALIZADO`.
- O sistema deve resumir a cobertura por eixo probatorio:
  - `compatibilidade_horarios`
  - `acumulacao_ilegal`
  - `vedacao_art_107_x`
  - `contrato_estadual`
- O sistema deve permitir que a maturidade do caso seja atualizada quando documentos forem efetivamente recebidos.

## RNF

- Nao elevar automaticamente `compatibilidade_horarios`, `vedacao_art_107_x` ou `acumulacao_ilegal` para `COMPROVADO_DOCUMENTAL` apenas porque um PDF chegou.
- Nao aceitar caminho quebrado como documento valido.
- Nao misturar documento recebido com documento analisado.
- Nao usar `ARQUIVO_NAO_LOCALIZADO` como prova negativa; isso e falha operacional, nao achado.
- Manter a trilha local com hash para cada arquivo recebido.

## Regra Operacional

- Documento sem arquivo local continua `PENDENTE`.
- Documento com caminho informado e arquivo inexistente vira `ARQUIVO_NAO_LOCALIZADO`.
- Documento com arquivo local pode virar `RECEBIDO` ou `ANALISADO`, mas a mudanca de status probatorio do caso depende de rerun da maturidade e leitura humana.
