# Estado Atual

Data de referência: `2026-03-14`

## Produto vivo

O produto ativo hoje é a trilha operacional `ops_*` integrada ao Streamlit.

Peças principais:

- painel: `app.py`
- aba operacional: `src/ui/streamlit_ops.py`
- backend operacional: `src/core/ops_*`
- banco principal: `data/sentinela_analytics.duckdb`

## Banco

Estado confirmado:

- `ops_case_registry = 12`
- `ops_case_artifact = 35`
- `ops_case_inbox_document = 85`
- `ops_case_generated_export = 2`
- `alerts = 1809`, todas em `QUARENTENA`

Famílias ativas:

- `rb_sus_contrato = 1`
- `sesacre_sancao = 10`
- `saude_societario = 1`

Estágios:

- `APTO_A_NOTICIA_DE_FATO = 11`
- `APTO_OFICIO_DOCUMENTAL = 1`

## Casos ativos

- `rb:contrato:3898`
  - divergência documental entre contrato, edital e propostas
  - uso atual: `NOTICIA_FATO`
- `sesacre:sancao:*`
  - 10 fornecedores da `SESACRE` cruzados com sanção ativa em base pública
  - uso atual: `NOTICIA_FATO`
- `cedimp:saude_societario:13325100000130`
  - sobreposição societária/saúde documentada com `CNES`
  - uso atual: `PEDIDO_DOCUMENTAL`

## O que não está mais vivo

- `cross_reference_engine.py` não é mais trilha operacional
- `insights_engine.py` não é mais motor ativo
- tabela `alerts` não é mais camada principal do sistema
