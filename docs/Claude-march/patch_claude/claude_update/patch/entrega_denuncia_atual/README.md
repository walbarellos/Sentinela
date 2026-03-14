# Entrega Atual - Acre / Rio Branco / SUS

Esta pasta reune o subconjunto curado dos artefatos mais uteis para uso externo controlado e revisao interna.

## Estado atual correto

- Existe `1` caso municipal ativo para noticia de fato: contrato `3898`.
- O antigo cruzamento sancionatorio do contrato `3895` foi invalidado por filtro temporal e saiu da fila ativa.
- Existe `1` frente estadual consolidada: top `10` de fornecedores com sancao ativa na `SESACRE`.
- Existe `1` caso funcional/societario relevante: `CEDIMP`, hoje restrito a `PEDIDO_DOCUMENTAL`.
- O painel operacional Streamlit existe e esta ligado ao `ops_case_registry`, `ops_case_artifact`, inbox, timeline, diff, busca e gate nao-acusatorio.

## Arquivos canonicos desta pasta

- `relatorio_final_acre_rio_branco_sus.md`
- `relato_apuracao_3898.txt`
- `relato_apuracao_sesacre_top10.txt`
- `nota_historica_3895_sancao_invalidada.txt`
- `CASO_NORTE_DISTRIBUIDORA.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md`
- `TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_DOSSIE.md`
- `cedimp_case_bundle_20260313.tar.gz`
- `PROXIMO_PASSO.md`

## Leitura correta por eixo

### Rio Branco / SUS

- Caso ativo: `3898`
- Classe: `DIVERGENCIA_DOCUMENTAL`
- Uso externo: `noticia de fato` ou `pedido de apuracao preliminar`
- Fato central: item presente no contrato e ausente do edital/propostas da licitacao-mae

### Norte Distribuidora

- O CNPJ continua relevante como trilha historica e estadual.
- O contrato municipal `3895` nao e mais caso sancionatorio ativo.
- O arquivo `CASO_NORTE_DISTRIBUIDORA.md` passou a ser nota historica fiel.

### SESACRE

- `67` fornecedores com sancao ativa
- `R$ 101.329.947,27` no recorte agregado atual
- top `10` com dossie e relato para apuracao

### CEDIMP

- fato documental forte em saude, mas ainda sem acusacao automatica
- gate atual: `APTO_OFICIO_DOCUMENTAL`
- uso externo correto: `PEDIDO_DOCUMENTAL`

## O que esta fora da fila ativa

- `3895` como caso sancionatorio municipal
- qualquer saida que trate esse contrato como `RB_CONTRATO_SANCIONADO`
- qualquer linguagem de `denuncia imediata` ou `representacao preliminar` como rotulo operacional ativo

## Regra de uso

Use esta pasta assim:
- `3898` para noticia de fato municipal
- `SESACRE top 10` para noticia de fato estadual
- `CEDIMP` para pedido documental
- `3895` apenas como historico de auditoria sobre falso positivo temporal
