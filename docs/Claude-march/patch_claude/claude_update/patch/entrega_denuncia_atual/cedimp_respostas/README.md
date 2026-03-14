# Cedimp - Caixa de Respostas Oficiais

Use esta pasta para anexar documentos recebidos de `SEMSA/RH`, `SESACRE` ou controle interno.

## Como usar

- Preencha `cedimp_respostas_index.csv`.
- Se houver arquivo, grave em `cedimp_respostas/anexos/...` e informe `file_relpath` relativo a `entrega_denuncia_atual`.
- Depois rode `scripts/sync_vinculo_societario_saude_respostas.py`.
- Em seguida rerode `scripts/sync_vinculo_societario_saude_maturidade.py` e os exports do caso.

## Regras

- Nao sobrescreva o `documento_chave`; ele ancora a trilha probatoria.
- So marque `ANALISADO` quando o arquivo estiver localmente presente.
- `ARQUIVO_NAO_LOCALIZADO` e gerado automaticamente se o caminho apontado nao existir.
