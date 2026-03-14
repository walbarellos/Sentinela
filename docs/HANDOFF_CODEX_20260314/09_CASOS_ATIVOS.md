# Casos Ativos

## 1. Rio Branco SUS

### `rb:contrato:3898`

- família: `rb_sus_contrato`
- estágio: `APTO_A_NOTICIA_DE_FATO`
- uso externo: `APTO_APURACAO`
- peça recomendada: `NOTICIA_FATO`
- destinatário principal: `Controladoria Geral do Municipio de Rio Branco`
- artefatos: `9`
- núcleo do caso:
  - divergência documental entre contrato, edital e propostas da licitação-mãe

## 2. SESACRE sancionatório

### família `sesacre_sancao`

- total: `10` casos
- estágio: `APTO_A_NOTICIA_DE_FATO`
- uso externo: `APTO_APURACAO`
- peça recomendada: `NOTICIA_FATO`
- destinatário principal: `Controladoria-Geral do Estado do Acre`

Exemplos:

- `sesacre:sancao:07847837000110`
  - `CIENTIFICA MEDICA HOSPITAL LTDA EM RECUPERACAO JUDICIAL`
  - `3` sanções ativas
  - `64` contratos
  - `R$ 29.410.248,30`

- `sesacre:sancao:32595581000148`
  - `R. BISPO AGUIAR`
  - `1` sanção ativa
  - `11` contratos
  - `R$ 12.329.014,18`

## 3. CEDIMP

### `cedimp:saude_societario:13325100000130`

- família: `saude_societario`
- estágio: `APTO_OFICIO_DOCUMENTAL`
- uso externo: `PEDIDO_DOCUMENTAL`
- peça recomendada: `PEDIDO_DOCUMENTAL`
- destinatário principal: `Secretaria Municipal de Saude - RH / SEMSA`
- artefatos: `6`

Estado:

- base documental mínima: sim
- documentos oficiais recebidos: `0`
- próximo passo: alimentar a inbox com respostas de `SEMSA/RH` e `SESACRE`

## 4. Caso histórico desativado

### `3895`

Situação:

- não é mais caso ativo
- ficou como nota histórica
- motivo: falso positivo temporal no cruzamento sancionatório

Arquivo:

- `docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/nota_historica_3895_sancao_invalidada.txt`
