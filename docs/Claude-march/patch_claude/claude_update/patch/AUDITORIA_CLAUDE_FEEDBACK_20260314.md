## Auditoria de Correção — Claude Feedback (2026-03-14)

Escopo desta fase:
- portar os patches de `claude_feedback` ao schema real do projeto;
- cortar o falso positivo temporal do caso `3895`;
- endurecer o `cross_reference_engine` legado;
- tirar `alerts` brutos da posição de camada principal no painel;
- preservar apenas saídas compatíveis com `notícia de fato`, `pedido documental` e `apuração preliminar`.

### Achado crítico confirmado

O caso municipal `3895 / processo 3044 / NORTE DISTRIBUIDORA` era um falso positivo temporal no cruzamento sancionatório municipal.

Diagnóstico validado com [validate_sancao_timeline.py](/home/walbarellos/Projetos/Sentinela/scripts/validate_sancao_timeline.py):
- cruzamentos brutos CEIS x contratos SUS: `5`
- sanções posteriores ao contrato: `5`
- matches temporalmente válidos após filtro: `0`

Referência temporal usada no contrato:
- `data_lancamento`, quando disponível;
- fallback conservador para `31/12` do `ano` do contrato;
- `capturado_em` só como último fallback técnico.

Resultado:
- `v_rb_contrato_ceis_valida = 0`
- `v_rb_contrato_ceis_revisao = 0`
- `v_rb_contrato_ceis_invalida = 5`
- `RB_CONTRATO_SANCIONADO = 0`

### Correções aplicadas

#### 1. Cruzamento sancionatório municipal

Arquivo: [sync_rb_contratos.py](/home/walbarellos/Projetos/Sentinela/scripts/sync_rb_contratos.py)

Implementado:
- `v_rb_contrato_ceis`
- `v_rb_contrato_ceis_valida`
- `v_rb_contrato_ceis_revisao`
- `v_rb_contrato_ceis_invalida`

Regras novas:
- sanção precisa preexistir ao contrato;
- sanção precisa estar vigente na data de referência do contrato;
- ausência de campo de abrangência em `sancoes_collapsed` impede promoção automática para saída externa;
- insights municipais sancionatórios usam apenas `v_rb_contrato_ceis_valida`.

#### 2. Engine legado de detectores

Arquivo: [cross_reference_engine.py](/home/walbarellos/Projetos/Sentinela/src/core/cross_reference_engine.py)

Implementado:
- schema real do banco atual;
- campos probatórios no `alerts`:
  - `classe_achado`
  - `grau_probatorio`
  - `fonte_primaria`
  - `uso_externo`
  - `inferencia_permitida`
  - `limite_conclusao`
- threshold de fracionamento corrigido para `R$ 50.000,00`;
- outlier salarial exige `n >= 30`;
- nepotismo por sobrenome virou triagem interna exclusiva;
- detectores sensíveis bloqueados por padrão sem `--allow-internal`.

Validação técnica:
- `fracionamento`: compila e roda no schema atual;
- `outlier_salarial`: compila e roda no schema atual;
- `nepotismo_sobrenome`: compila e roda no schema atual;
- `empresa_suspensa`: compila e roda no schema atual.

Limite mantido:
- o engine legado continua sendo triagem interna;
- a camada principal de uso externo segue sendo `ops_registry`.

#### 3. UI de quarentena

Arquivos:
- [app.py](/home/walbarellos/Projetos/Sentinela/app.py)
- [streamlit_sidebar.py](/home/walbarellos/Projetos/Sentinela/src/ui/streamlit_sidebar.py)
- [streamlit_alerts.py](/home/walbarellos/Projetos/Sentinela/src/ui/streamlit_alerts.py)
- [streamlit_home.py](/home/walbarellos/Projetos/Sentinela/src/ui/streamlit_home.py)

Implementado:
- `🚩 ALERTAS CRÍTICOS` foi rebaixado para `🧪 ALERTAS LEGADOS (QUARENTENA)`;
- a aba não gera mais texto de relato/exportação;
- o dashboard principal deixou de tratar `alerts` como KPI central;
- a UI aponta o operador para `📂 OPERAÇÕES` como fluxo probatório oficial.

### Estado operacional após correção

Após rerodar [sync_ops_case_registry.py](/home/walbarellos/Projetos/Sentinela/scripts/sync_ops_case_registry.py):
- `cases = 12`
- `artifacts = 35`
- `burden_rows = 61`
- `semantic_rows = 4`
- `contradiction_rows = 2`
- `checklist_rows = 61`
- `language_guard_rows = 0`
- `export_gate_rows = 36`
- `generated_export_rows = 2`
- `rule_validation_fail_rows = 0`
- `indexed_docs = 32`

Famílias ativas no registry:
- `sesacre_sancao = 10`
- `rb_sus_contrato = 1`
- `saude_societario = 1`

Casos ativos relevantes:
- `rb:contrato:3898`
- `cedimp:saude_societario:13325100000130`
- top `10` de `sesacre_sancao`

Estágios operacionais ativos após rerun final:
- `APTO_A_NOTICIA_DE_FATO`
- `APTO_OFICIO_DOCUMENTAL`

### Leitura final

O feedback estava correto em pontos materiais.

Hoje, depois desta fase:
- o caso `3895` não deve mais sair como caso sancionatório municipal;
- `alerts` brutos não são mais apresentados como camada principal;
- o motor legado foi rebaixado para triagem interna com metadados probatórios;
- a camada operacional continua apta para uso institucional conservador.

O que continua **não** autorizado automaticamente:
- acusação penal;
- nepotismo automático;
- fraude consumada automática;
- sanção municipal automática sem data e abrangência verificadas.
