# Relatorio Final - Acre / Rio Branco / SUS

Gerado em `2026-03-14`.

Este relatorio consolida o estado operacional atual do recorte `Acre / Rio Branco / SUS`, com distincao entre caso ativo, caso historicamente invalidado e frentes estaduais/funcionais.

## Resumo executivo

- Rio Branco: `20403` servidores com lotacao materializada, sendo `4427` classificados como SUS.
- Rio Branco: `13` contratos SUS mapeados.
- Rio Branco: `1` caso municipal ativo para noticia de fato: contrato `3898`.
- Rio Branco: `1` caso municipal rebaixado para nota historica: contrato `3895`.
- SESACRE: `67` fornecedores com sancao ativa, somando `R$ 101.329.947,27`.
- SESACRE top 10: `10/10` com `QSA`, `9/10` com socios e `1/10` com detalhe financeiro.
- CEDIMP: caso apto a `PEDIDO_DOCUMENTAL`, nao a noticia de fato.

## Fila priorizada consolidada

- prioridade `95` | `municipal` | `SEMSA` | `contrato_3898` | `(fornecedor nao resolvido)` | `R$ 31.200,00` | `divergencia_documental_licitacao`
- prioridade `89` | `estadual` | `SESACRE` | `sesacre_top_1` | `CIENTIFICA MEDICA HOSPITAL LTDA EM RECUPERACAO JUDICIAL` | `R$ 29.410.248,30` | `cruzamento_sancionatorio`
- prioridade `88` | `estadual` | `SESACRE` | `sesacre_top_2` | `R. BISPO AGUIAR` | `R$ 12.329.014,18` | `cruzamento_sancionatorio`
- prioridade `87` | `estadual` | `SESACRE` | `sesacre_top_3` | `HOSPSHOP PRODUTOS HOSPITALARES LTDA` | `R$ 7.577.625,00` | `cruzamento_sancionatorio`
- prioridade `80` | `estadual` | `SESACRE` | `sesacre_top_10` | `PREVIX PRODUTOS PARA SAUDE LTDA` | `R$ 2.129.400,00` | `cruzamento_sancionatorio`
- prioridade `80` | `estadual` | `SESACRE` | `cedimp` | `CEDIMP` | `R$ 6.604.510,00` | `pedido_documental`

## Caso municipal ativo

- contrato `3898` / processo `3006` / valor `R$ 31.200,00`
- natureza: `DIVERGENCIA_DOCUMENTAL`
- fato central: item `Coletor de Material Perfurocortante` apareceu no contrato, mas nao apareceu no edital nem nas propostas publicas da licitacao-mae
- uso externo recomendado: `noticia de fato` ou `pedido de apuracao preliminar`

## Caso municipal historico rebaixado

- contrato `3895` / processo `3044` / valor `R$ 82.950,00`
- fornecedor: `NORTE DISTRIBUIDORA DE PRODUTOS LTDA`
- situacao atual: `cruzamento sancionatorio invalidado por filtro temporal`
- leitura correta: o contrato permanece apenas como trilha historica de validacao de falso positivo; nao integra a fila municipal ativa nem sustenta mais `RB_CONTRATO_SANCIONADO`

## Top 10 SESACRE por valor contratado sob sancao ativa

- `1`. `CIENTIFICA MEDICA HOSPITAL LTDA EM RECUPERACAO JUDICIAL` | CNPJ `07847837000110` | `3` sancao(oes) ativa(s) | `64` contratos | `R$ 29.410.248,30`
- `2`. `R. BISPO AGUIAR` | CNPJ `32595581000148` | `1` sancao(oes) ativa(s) | `11` contratos | `R$ 12.329.014,18`
- `3`. `HOSPSHOP PRODUTOS HOSPITALARES LTDA` | CNPJ `07094705000164` | `1` sancao(oes) ativa(s) | `11` contratos | `R$ 7.577.625,00`
- `4`. `GOLDENPLUS - COMERCIO DE MEDICAMENTOS E PRODUTOS HOSPITALARES LTDA` | CNPJ `17472278000164` | `1` sancao(oes) ativa(s) | `16` contratos | `R$ 6.590.265,00`
- `5`. `INDUSTRIA BRASILEIRA DE EQUIPAMENTOS MEDICOS HOSPITALARES LTDA - (LUANNA FREIRE FELIX LTDA)` | CNPJ `13200879000167` | `1` sancao(oes) ativa(s) | `8` contratos | `R$ 6.230.171,66`
- `6`. `ASCLE BRASIL LTDA` | CNPJ `28911309000152` | `9` sancao(oes) ativa(s) | `15` contratos | `R$ 5.828.123,20`
- `7`. `COSTA CAMARGO COMERCIO DE PRODUTOS HOSPITALARES LTDA` | CNPJ `36325157000134` | `1` sancao(oes) ativa(s) | `13` contratos | `R$ 4.954.446,50`
- `8`. `RUPO CRESCERE COM E SERV LTDA` | CNPJ `51801426000185` | `1` sancao(oes) ativa(s) | `4` contratos | `R$ 2.653.478,89`
- `9`. `MCW PRODUTOS MEDICOS E HOSPITALARES LTDA` | CNPJ `94389400000184` | `8` sancao(oes) ativa(s) | `29` contratos | `R$ 2.480.306,00`
- `10`. `PREVIX PRODUTOS PARA SAUDE LTDA` | CNPJ `11877124000176` | `1` sancao(oes) ativa(s) | `1` contrato | `R$ 2.129.400,00`

## Caso funcional em saude

- `CEDIMP` (`13.325.100/0001-30`) / contrato `779/2023` / `SESACRE`
- fato documental: coincidencia exata entre quadro societario, profissionais ativos no `CNES` e historico de concomitancia publico-privada
- gate atual: `APTO_OFICIO_DOCUMENTAL`
- uso externo recomendado: `pedido documental`

## Artefatos canonicos

- Dossie municipal: `docs/Claude-march/patch_claude/claude_update/patch/dossie_rb_sus_prioritarios.md`
- Relato municipal: `docs/Claude-march/patch_claude/claude_update/patch/relato_apuracao_3898.txt`
- Nota historica 3895: `docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/nota_historica_3895_sancao_invalidada.txt`
- Dossie SESACRE: `docs/Claude-march/patch_claude/claude_update/patch/sesacre_prioritarios/dossie_sesacre_sancoes_prioritarias.md`
- Relato SESACRE: `docs/Claude-march/patch_claude/claude_update/patch/sesacre_prioritarios/relato_apuracao_sesacre_top10.txt`
- Dossie CEDIMP: `docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md`

## Pendencias remanescentes

- Municipal: identificar fornecedor/CNPJ do `3898` por fonte aberta autenticada ou diligencia formal.
- Estadual: ampliar detalhe financeiro dos sancionados da `SESACRE` alem do `1/10` ja coberto.
- Funcional: receber e analisar documentos de `SEMSA/RH` e `SESACRE` para o caso `CEDIMP`.
