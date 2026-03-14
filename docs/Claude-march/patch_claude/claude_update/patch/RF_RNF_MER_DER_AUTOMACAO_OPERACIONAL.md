# RF/RNF/MER/DER - Automacao Operacional

## RF

- O sistema deve materializar um `registry` de casos operacionais reutilizavel por API e painel.
- O painel-alvo v1 e v2 deve ser o `Streamlit` existente em `app.py`, sem abrir uma segunda interface paralela.
- O sistema deve expor:
  - lista de casos
  - detalhe do caso
  - artefatos do caso
  - resumo operacional por estagio/familia
- O sistema deve separar pelo menos estas familias:
  - `rb_sus_contrato`
  - `sesacre_sancao`
  - `saude_societario`
- O sistema deve trazer `estagio_operacional`, `uso_externo`, `prioridade`, `valor`, `resumo_curto` e `proximo_passo`.
- O sistema deve registrar artefatos locais com `path`, `sha256`, `exists` e `size_bytes`.
- O sistema deve ser idempotente: o sync do registry pode ser rerodado sem duplicar casos.

## RNF

- Nao promover automaticamente um caso de `pedido documental` para `representacao preliminar`.
- Nao depender de leitura manual de pasta para montar painel; tudo deve sair de tabela/view.
- Nao bloquear a API por chamadas de rede externas; o registry deve operar sobre o que ja esta no banco e nos artefatos locais.
- Nao recalcular o mundo inteiro para listar casos; o painel deve ler views prontas.
- O sync do registry deve ser rapido o suficiente para rodar no startup da API.
- A visualizacao de artefatos deve priorizar leitura local no proprio painel (`md`, `txt`, `json`, `csv`, `html`, `pdf`) antes de exigir download manual.
- O painel deve favorecer triagem e cadeia de evidencia, nao score cosmetico.

## MER

### Entidades

- `ops_case_registry`
  - 1 linha por caso operacional
- `ops_case_artifact`
  - N linhas por caso, uma por artefato local

### Relacoes

- `ops_case_registry (1) -> (N) ops_case_artifact`

### Fontes de alimentacao v1

- `v_rb_contratos_prioritarios`
- `sancoes_collapsed`
- `v_vinculo_societario_saude_gate`

### Fontes previstas v2

- `ops_pipeline_run`
- `ops_source_cache`
- `ops_case_comment`
- `ops_case_assignment`

## DER

### ops_case_registry

- `case_id` PK
- `family`
- `title`
- `subtitle`
- `subject_name`
- `subject_doc`
- `esfera`
- `ente`
- `orgao`
- `municipio`
- `uf`
- `area_tematica`
- `severity`
- `classe_achado`
- `uso_externo`
- `estagio_operacional`
- `status_operacional`
- `prioridade`
- `valor_referencia_brl`
- `source_table`
- `source_row_ref`
- `resumo_curto`
- `proximo_passo`
- `bundle_path`
- `bundle_sha256`
- `artifact_count`
- `evidence_json`
- `updated_at`

### ops_case_artifact

- `artifact_id` PK
- `case_id` FK logico
- `label`
- `kind`
- `path`
- `exists`
- `sha256`
- `size_bytes`
- `metadata_json`
- `updated_at`

## Proxima Fase

- expandir `ops_pipeline_run` com retries, scheduler e serializacao de writers
- expandir `ops_source_cache` com conditional request e invalidacao por TTL/etag/hash
- evoluir a aba `📂 OPERAÇÕES` do Streamlit para um centro de casos com visualizador documental e trilha de diligencias
- manter `/ops/summary`, `/ops/cases` e `/ops/cases/{id}/artifacts` como contrato estavel para API e possivel frontend futuro
