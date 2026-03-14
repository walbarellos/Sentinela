# Fase Atual - Automacao Operacional

## Onde estamos

O projeto ja tem:
- painel real em `app.py` via Streamlit
- registry operacional de casos:
  - `ops_case_registry`
  - `ops_case_artifact`
- API operacional:
  - `/ops/summary`
  - `/ops/cases`
  - `/ops/cases/{case_id}`
  - `/ops/cases/{case_id}/artifacts`

## Correcao importante

A trilha correta do produto e:
- evoluir o Streamlit existente
- nao abrir UI paralela sem necessidade

## Inspiracao util

- `USAspending`
  - filtro forte
  - drill-down por recebedor / award
- `Oversight.gov`
  - status do caso
  - relatorio-fonte
  - trilha operacional
- `SEC EDGAR`
  - documento primeiro
  - anexo acessivel
  - timeline de evidencia

## Concluido nesta fase

1. `ops_pipeline_run` implementado
2. `ops_source_cache` implementado
3. integracao concluida no Streamlit existente
4. execucoes recentes e fontes monitoradas exibidas na aba `📂 OPERAÇÕES`
5. `app.py` desmontado em modulos de UI
6. status da sidebar passou a refletir estado real do banco operacional
7. `ops_case_inbox_document` implementado
8. inbox operacional entregue para o caso `CEDIMP`
9. rerun de workflow do `CEDIMP` entregue sem shell
10. timeline documental por caso implementada
11. diff textual de artefatos suportados implementado
12. inbox generalizada para `RB_SUS` e `SESACRE`
13. fila de pendencias documentais por caso implementada
14. indice textual local implementado
15. aba `Busca` operacional implementada
16. matriz de onus probatorio implementada
17. timeline agrupada por fase investigativa implementada
18. diff semantico materializado para contrato x licitacao x edital/publicacao x proposta congelada
19. busca full-text refinada com filtros operacionais e snippet denso
20. gate de linguagem nao-acusatoria implementado
21. motor de contradicoes objetivas implementado
22. checklist probatorio por caso implementado
23. gate de exportacao segura implementado
24. gerador on-demand de texto neutro por caso implementado

## TODO imediato

1. diff semantico orientado a proposta congelada quando houver espelho local do documento
2. timeline com agrupamento visual por fase e contadores por etapa no overview
3. busca full-text com ranking por evento e preview semantico por tipo documental
4. ligar rerun de maturidade/gate ao fluxo de resposta oficial
5. continuar quebrando modulos grandes somente quando houver ganho real de manutencao
6. generalizar a caixa operacional para outros casos alem de `CEDIMP`
7. reduzir retrabalho em exports legados para que bundles reemitidos ja nascam com linguagem neutra
8. concluido: exportacao on-demand agora pode ser congelada como artefato controlado do caso
9. concluido: infraestrutura de diff entre versoes congeladas da mesma peca
10. concluido: auditoria de legitimidade das regras com rulebook e validador

## Regra de produto

- nada de destruir o painel atual
- nada de score cosmetico
- tudo deve aproximar o operador do documento-fonte
- hipotese nao pode parecer fato
- cache e run log devem ser aditivos e idempotentes

## Nota do plano

- `ops_pipeline_run`: `9.5/10`
- `ops_source_cache`: `9/10`
- `timeline/diff`: `8.5/10`
- `inbox operacional`: `10/10`

## Proxima implementacao em curso

- `src/ui/streamlit_ops.py` reduzido a orquestrador
- novos modulos:
  - `src/ui/ops_shared.py`
  - `src/ui/ops_data.py`
  - `src/ui/ops_preview.py`
  - `src/ui/ops_sections.py`
- inbox operacional:
  - `src/core/ops_inbox.py`
  - `src/ui/ops_inbox.py`
  - `scripts/sync_ops_inbox.py`
- timeline e diff:
  - `src/core/ops_timeline.py`
  - `scripts/sync_ops_timeline.py`
  - `src/ui/ops_diff.py`
- inbox multi-caso:
  - `RB_SUS`
  - `SESACRE`
  - `CEDIMP`
- busca local:
- `src/core/ops_search.py`
  - `src/ui/ops_search.py`
  - `scripts/sync_ops_search_index.py`
- onus probatorio:
  - `src/core/ops_legal.py`
  - `src/core/ops_burden.py`
  - `src/ui/ops_burden.py`
  - `scripts/sync_ops_burden.py`
- diff semantico:
  - `src/core/ops_semantic.py`
  - `src/ui/ops_semantic.py`
  - `scripts/sync_ops_semantic.py`
- timeline por fase:
  - `src/core/ops_timeline.py`
  - `src/ui/ops_timeline.py`
- saida nao-acusatoria:
  - `src/core/ops_guard.py`
  - `src/core/ops_contradiction.py`
  - `src/core/ops_checklist.py`
  - `src/core/ops_export.py`
  - `src/ui/ops_checklist.py`
  - `src/ui/ops_export.py`
  - `scripts/sync_ops_guard.py`
  - `scripts/sync_ops_contradiction.py`
  - `scripts/sync_ops_checklist.py`
  - `scripts/sync_ops_export_gate.py`
  - `scripts/validate_ops_output_guard.py`
- congelamento da saida controlada:
  - `scripts/freeze_ops_case_export.py`
  - `ops_case_generated_export`
  - `src/core/ops_export.py`
- governanca de regras:
  - `src/core/ops_rulebook.py`
  - `scripts/sync_ops_rulebook.py`
  - `scripts/validate_ops_rulebook.py`
  - `AUDITORIA_LEGITIMIDADE_REGRAS_20260314.md`
- proximos modulos de produto:
  - motor de contradicoes
  - checklist probatorio por caso
  - diff semantico com proposta congelada quando existir
  - calibracao empirica de falso positivo/falso negativo por familia

## Correção crítica de legitimidade — feedback audit (2026-03-14)

escopo:
- portar patches de `claude_feedback` ao schema real;
- remover falso positivo temporal do caso municipal `3895`;
- endurecer `cross_reference_engine`;
- tirar `alerts` brutos da posição de camada principal no streamlit.

resultado:
- `validate_sancao_timeline.py` confirmou `5` cruzamentos brutos e `5` falsos positivos temporais no contrato `3895`
- `v_rb_contrato_ceis_valida = 0`
- `v_rb_contrato_ceis_invalida = 5`
- `RB_CONTRATO_SANCIONADO = 0`
- `rb:contrato:3898` permaneceu vivo no registry
- registry recalculado:
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

arquivos centrais:
- `scripts/sync_rb_contratos.py`
- `scripts/validate_sancao_timeline.py`
- `src/core/cross_reference_engine.py`
- `app.py`
- `src/ui/streamlit_alerts.py`
- `src/ui/streamlit_home.py`
- `src/ui/streamlit_sidebar.py`
- `AUDITORIA_CLAUDE_FEEDBACK_20260314.md`

## Saneamento do acervo ativo (2026-03-14)

escopo:
- neutralizar nomes legados acusatorios;
- remover o `3895` da documentacao ativa;
- alinhar scripts/exportadores ao estado verdadeiro do banco;
- preservar o `3895` apenas como nota historica.

resultado:
- `scripts/export_rb_casos_prioritarios.py` passou a gerar:
  - `relato_apuracao_3898.txt`
  - `nota_historica_3895_sancao_invalidada.txt`
- `scripts/export_sesacre_prioritarios.py` passou a gerar:
  - `sesacre_prioritarios/relato_apuracao_sesacre_top10.txt`
- `src/core/ops_registry.py` passou a indexar os nomes neutros
- `INDEX_PRIORITARIOS.md`, `relatorio_final_acre_rio_branco_sus.md`, `entrega_denuncia_atual/README.md` e `CASO_NORTE_DISTRIBUIDORA.md` foram reescritos para refletir:
  - `3898` como unico caso municipal ativo
  - `3895` como falso positivo temporal historicamente documentado
- `painel_prioridades_acre_rio_branco_sus.csv` foi corrigido:
  - `3895` removido
  - `3898` mantido como `divergencia_documental_licitacao`

validacao executada:
- `python -m py_compile scripts/export_rb_casos_prioritarios.py scripts/export_sesacre_prioritarios.py scripts/export_relatorio_final_acre_sus.py src/core/ops_registry.py`
- `.venv/bin/python scripts/export_rb_casos_prioritarios.py`
- `.venv/bin/python scripts/export_sesacre_prioritarios.py`
- `.venv/bin/python scripts/export_relatorio_final_acre_sus.py`
- `.venv/bin/python scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/validate_ops_output_guard.py`
- `.venv/bin/python scripts/validate_sancao_timeline.py`
- `streamlit run app.py --server.headless true --server.port 8781`
- `curl -I http://localhost:8781 -> 200`

estado final:
- `cases = 12`
- `artifacts = 35`
- `language_guard_rows = 0`
- `rule_validation_fail_rows = 0`
- `RB_CONTRATO_SANCIONADO = 0`
- `v_rb_contrato_ceis_valida = 0`
- `v_rb_contrato_ceis_invalida = 5`

consolidacao final dos nomes:
- arquivos legados `denuncia_preliminar_*` e `representacao_preliminar_*` foram removidos do acervo ativo
- sobraram apenas dois mecanismos de compatibilidade intencionais:
  - `src/ui/ops_shared.py` para mapear estagios antigos eventualmente existentes no banco
  - `src/core/ops_guard.py` para detectar linguagem legada em qualquer saida nova

## Runbook operacional por caso (2026-03-14)

escopo:
- ligar `ops_case_registry` a um runbook institucional por caso;
- derivar destinatario, peca, dossie minimo, documentos a requerer e passos de diligencia;
- expor isso no Streamlit sem criar mais um fluxo paralelo de scripts.

resultado:
- `src/core/ops_runbook.py`
- `src/ui/ops_runbook.py`
- `scripts/sync_ops_runbook.py`
- aba nova `Runbook` dentro da bancada de caso
- contagem validada:
  - `runbook_rows = 12`
  - `runbook_steps = 60`

amostras validadas:
- `rb:contrato:3898`
  - `recommended_mode = NOTICIA_FATO`
  - `destinatario_principal = Controladoria Geral do Municipio de Rio Branco`
- `cedimp:saude_societario:13325100000130`
  - `recommended_mode = PEDIDO_DOCUMENTAL`
  - `destinatario_principal = Secretaria Municipal de Saude - RH / SEMSA`
- `sesacre:sancao:*`
  - `recommended_mode = NOTICIA_FATO`
  - `destinatario_principal = Controladoria-Geral do Estado do Acre`

validacao executada:
- `python -m py_compile src/core/ops_runbook.py src/ui/ops_runbook.py src/core/ops_registry.py src/ui/ops_data.py src/ui/ops_sections.py src/ui/streamlit_ops.py scripts/sync_ops_runbook.py`
- `.venv/bin/python scripts/sync_ops_runbook.py`
- `.venv/bin/python scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/validate_ops_output_guard.py`
- `streamlit run app.py --server.headless true --server.port 8782`
- `curl -I http://localhost:8782 -> 200`

## Auditoria de utilidade e poda segura (2026-03-14)

escopo:
- revisar o stack operacional para separar `essencial / util / excesso`;
- cortar destaque de camada derivada sem perder funcionalidade;
- evitar proliferacao de UI e automacao sem ganho probatorio.

resultado:
- matriz registrada em `AUDITORIA_UTILIDADE_MODULOS_20260314.md`
- `ops_runbook` rebaixado para `apoio`
- aba `Runbook` removida do primeiro nivel da bancada
- `Encaminhamento operacional` agora fica dentro de `Exportacao`
- metricas de `runbook` removidas do overview principal
- mensagem de refresh do painel ficou menos ruidosa

decisao de produto:
- manter `runbook`, mas nao expandir agora
- priorizar sempre `prova > seguranca juridica > reducao de trabalho manual > conveniencia`

validacao executada:
- `python -m py_compile src/ui/ops_export.py src/ui/ops_runbook.py src/ui/ops_sections.py src/ui/ops_data.py src/ui/streamlit_ops.py`
- `.venv/bin/python scripts/sync_ops_case_registry.py`
- `streamlit run app.py --server.headless true --server.port 8783`
- `curl -I http://localhost:8783 -> 200`

## Auditoria de confianca do backend por familia (2026-03-14)

escopo:
- verificar se as regras ativas continuam legitimas para `MP / PF / CGU / controle interno`;
- travar repeticao de falso positivo conhecido;
- rebaixar linguagem que sugeria mais certeza do que o dado comporta.

mudancas:
- `src/core/ops_burden.py`
  - `sesacre_sancao`: `contratacao concomitante` -> `cruzado com sancao ativa em base publica`
  - `saude_societario`: remove rotulo operacional de `nepotismo`
- `src/core/ops_registry.py`
  - titulos e resumos estaduais rebaixados para linguagem conservadora
- `src/core/ops_runbook.py`
  - encaminhamento estadual passa a explicitar dependencia de processo integral e due diligence
- `src/core/ops_rulebook.py`
  - nova camada `FAMILY_CONFIDENCE_GUARD`
  - validacoes novas por familia ativa

resultado:
- matriz familiar em `AUDITORIA_CONFIANCA_FAMILIAS_20260314.md`
- `rule_rows = 6`
- `rule_validation_rows = 11`
- `rule_validation_fail_rows = 0`
- `rule_validation_warn_rows = 0`
- `rb_sancao_ativos = 0`
- `sesacre_overclaim = 0`
- `saude_nf_allowed = 0`
- `labels_saude_nepot = 0`

validacao executada:
- `python -m py_compile src/core/ops_burden.py src/core/ops_registry.py src/core/ops_runbook.py src/core/ops_rulebook.py`
- `.venv/bin/python scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/validate_ops_rulebook.py`

## Calibracao empirica minima (2026-03-14)

escopo:
- travar regressao em casos conhecidos;
- medir o backend contra `3898`, `3895`, `CEDIMP` e `SESACRE` de referencia;
- expor `fail/warn` objetivos de calibracao.

resultado:
- `src/core/ops_calibration.py`
- `scripts/sync_ops_calibration.py`
- `scripts/validate_ops_calibration.py`
- relatorio em `CALIBRACAO_EMPIRICA_20260314.md`

benchmarks:
- confirmados:
  - `RB_3898_ACTIVE_DOCUMENTAL`
  - `CEDIMP_DOCUMENT_REQUEST_ONLY`
  - `SESACRE_REFERENCE_CASE_ACTIVE`
  - `GLOBAL_LANGUAGE_GUARD_CLEAN`
- descartado:
  - `RB_3895_FALSE_POSITIVE_REMOVED`
- inconclusivos:
  - `RB_3898_CORE_DOCS_PENDING`
  - `CEDIMP_PARENTESCO_UNPROVEN`
  - `SESACRE_REFERENCE_DUE_DILIGENCE_PENDING`

estado:
- `calibration_benchmark_rows = 8`
- `calibration_result_rows = 8`
- `calibration_fail_rows = 0`
- `calibration_warn_rows = 0`
- `confirmado = 4 PASS`
- `descartado = 1 PASS`
- `inconclusivo = 3 PASS`

validacao executada:
- `python -m py_compile src/core/ops_calibration.py src/core/ops_registry.py src/ui/ops_data.py src/ui/ops_sections.py src/ui/streamlit_ops.py scripts/sync_ops_calibration.py scripts/validate_ops_calibration.py scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/validate_ops_calibration.py`
- `.venv/bin/python scripts/validate_ops_rulebook.py`
- `streamlit run app.py --server.headless true --server.port 8785`
- `curl -I http://localhost:8785 -> 200`
