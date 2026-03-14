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

## Suite sentinela por regra (2026-03-14)

escopo:
- travar regressao nas regras mais sensiveis do backend;
- validar comportamento por regra, nao apenas por caso;
- manter linguagem e cercas conservadoras no motor operacional.

resultado:
- `src/core/ops_sentinel.py`
- `scripts/sync_ops_sentinel.py`
- `scripts/validate_ops_sentinel.py`
- relatorio em `SUITE_SENTINELA_REGRAS_20260314.md`

sentinelas:
- `RB_TEMPORAL_FALSE_POSITIVE`
- `RB_3898_SEMANTIC_TRIAD`
- `SAUDE_NO_NEPOTISM_LABEL`
- `SAUDE_NOTICIA_FATO_BLOCKED`
- `SESACRE_NO_OVERCLAIM_LANGUAGE`

estado:
- `sentinel_rows = 5`
- `sentinel_result_rows = 5`
- `sentinel_fail_rows = 0`
- `sentinel_warn_rows = 0`

validacao executada:
- `python -m py_compile src/core/ops_sentinel.py src/core/ops_registry.py src/ui/ops_data.py src/ui/ops_sections.py src/ui/streamlit_ops.py scripts/sync_ops_sentinel.py scripts/validate_ops_sentinel.py scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/sync_ops_case_registry.py`
- `.venv/bin/python scripts/validate_ops_sentinel.py`
- `streamlit run app.py --server.headless true --server.port 8787`
- `curl -I http://localhost:8787 -> 200`

## Contencao do engine legado (2026-03-14)

escopo:
- alinhar `cross_reference_engine.py` ao padrao atual de rigor;
- impedir que o legado volte a produzir qualquer saida com cara de uso externo;
- manter o legado apenas como triagem interna.

resultado:
- `src/core/cross_reference_engine.py`
  - `uso_externo` agora e sempre `REVISAO_INTERNA`
- `scripts/validate_cross_reference_engine.py`
  - trava configuracao minima do legado

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- smoke direto de `Alert(...).uso_externo -> REVISAO_INTERNA`

## Legado em modo opt-in total (2026-03-14)

escopo:
- bloquear por padrao os detectores fracos do `cross_reference_engine.py`;
- impedir execucao “normal” do legado como se fosse motor ativo do produto;
- alinhar o uso do legado ao padrao `triagem interna apenas`.

resultado:
- todos os `8` detectores do engine legado ficaram em `INTERNAL_ONLY_DEFAULT`
- o CLI sem `--allow-internal` agora informa bloqueio por padrao
- relatorio em `AUDITORIA_ENGINE_LEGADO_20260314.md`

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- `.venv/bin/python -m src.core.cross_reference_engine --detector fracionamento`

## Quarentena do insights_engine legado (2026-03-14)

escopo:
- alinhar `insights_engine.py` ao mesmo padrao de quarentena do legado;
- impedir que `outlier_salarial`, `viagem_bloco` e `concentracao_mercado` sigam ativos por padrao;
- corrigir a comunicacao do `README.md`.

resultado:
- `insights_engine.py`
  - todos os detectores ficam desligados por padrao;
  - uso exige `allow_internal=True`;
  - `outlier_salarial` endurecido para `n >= 30` e `z > 3.0`;
  - `viagem_bloco` endurecido para `n >= 4`;
  - `fracionamento` legado corrigido para threshold exploratorio de `R$ 50.000,00`;
- `README.md`
  - ingestoes legadas reescritas como triagem interna/exploratoria;
  - `insights_engine.py` reclassificado como motor legado;
- `scripts/sync_v2.py`
  - passou a chamar o legado com `allow_internal=False` de forma explicita;
- relatorio novo em `AUDITORIA_INSIGHTS_ENGINE_20260314.md`
- validador novo em `scripts/validate_insights_engine.py`

validacao executada:
- `python -m py_compile insights_engine.py scripts/validate_insights_engine.py`
- `python -m py_compile scripts/sync_v2.py`
- `.venv/bin/python scripts/validate_insights_engine.py`

## Aposentadoria de detectores fracos do legado (2026-03-14)

escopo:
- definir destino final de `viagem_bloco` e `concentracao_mercado` no `cross_reference_engine.py`;
- impedir que esses detectores voltem a rodar nem mesmo com `--allow-internal`;
- reduzir ruído operacional no legado.

resultado:
- `src/core/cross_reference_engine.py`
  - `RETIRED_DEFAULT` criado com:
    - `viagem_bloco`
    - `concentracao_mercado`
  - `DETECTOR_STATUS` criado para todos os `8` detectores
  - detectores aposentados saem antes mesmo de abrir o DuckDB no CLI
- `scripts/validate_cross_reference_engine.py`
  - agora valida tambem status e aposentadoria
- `README.md`
  - obras/diarias deixaram de sugerir esses detectores como eixo ativo
- `AUDITORIA_ENGINE_LEGADO_20260314.md`
  - atualizada com a distincao `LAB_INTERNO` vs `APOSENTADO`

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- `.venv/bin/python -m src.core.cross_reference_engine --detector viagem_bloco --allow-internal`
- `.venv/bin/python -m src.core.cross_reference_engine --detector concentracao_mercado --allow-internal`

## Segunda rodada de poda do legado (2026-03-14)

escopo:
- retirar `fim_de_semana` do fluxo legado;
- endurecer `outlier_salarial` como laboratorio interno conservador;
- travar thresholds minimos no validador.

resultado:
- `src/core/cross_reference_engine.py`
  - `fim_de_semana` movido para `RETIRED_DEFAULT`
  - `outlier_salarial` agora exige:
    - `n >= 30`
    - `z > 4.0`
    - `delta >= R$ 5.000,00`
- `scripts/validate_cross_reference_engine.py`
  - valida aposentadoria do `fim_de_semana`
  - valida thresholds minimos do `outlier_salarial`
- `README.md`
  - folha descrita como triagem conservadora
- `AUDITORIA_ENGINE_LEGADO_20260314.md`
  - atualizada com a nova classificacao

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- `.venv/bin/python -m src.core.cross_reference_engine --detector fim_de_semana --allow-internal`

## Destino final de empresa_suspensa e fracionamento (2026-03-14)

escopo:
- decidir se `empresa_suspensa` ainda faz sentido no engine legado;
- decidir se `fracionamento` sobrevive como laboratorio ou se deve ser aposentado;
- travar essa decisao no codigo e no validador.

resultado:
- `empresa_suspensa`
  - movido para `RETIRED_DEFAULT`
  - status `COBERTO_OPS`
  - deixa de rodar no legado porque o cruzamento sancionatorio confiavel ja vive na trilha `ops`
- `fracionamento`
  - status `CANDIDATO_OPS`
  - segue apenas com `--allow-internal`
  - endurecido para:
    - minimo de `4` contratos
    - janela maxima de `90` dias
    - uso externo sempre bloqueado
- `scripts/validate_cross_reference_engine.py`
  - ganhou sample guard de fracionamento com base sintetica
  - valida o status `COBERTO_OPS`

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- `.venv/bin/python -m src.core.cross_reference_engine --detector empresa_suspensa --allow-internal`

## Aposentadoria do detector de sobrenome (2026-03-14)

escopo:
- remover `nepotismo_sobrenome` do fluxo executavel do legado;
- impedir que coincidencia nominal volte a ser usada como triagem operacional corrente.

resultado:
- `src/core/cross_reference_engine.py`
  - `nepotismo_sobrenome` movido para `RETIRED_DEFAULT`
  - status `APOSENTADO`
- `AUDITORIA_ENGINE_LEGADO_20260314.md`
  - atualizada com a nova classificacao

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- `.venv/bin/python -m src.core.cross_reference_engine --detector nepotismo_sobrenome --allow-internal`

## Fechamento do mapa do legado (2026-03-14)

escopo:
- decidir o destino final de `doacao_contrato`;
- consolidar o mapa de sobrevivencia do `cross_reference_engine.py`.

resultado:
- `doacao_contrato`
  - movido para `RETIRED_DEFAULT`
  - status `APOSENTADO`
- `fracionamento`
  - movido para `RETIRED_DEFAULT`
  - status `APOSENTADO`
  - motivo: sem superficie minima confiavel nos dados reais atuais
- `outlier_salarial`
  - movido para `RETIRED_DEFAULT`
  - status `APOSENTADO`
  - motivo: base consolidada sem competencia explicita e com duplicidade suficiente para gerar ruido estrutural
- mapa final do legado:
  - `COBERTO_OPS`: `empresa_suspensa`
  - `APOSENTADO`: `fracionamento`, `outlier_salarial`, `viagem_bloco`, `concentracao_mercado`, `fim_de_semana`, `nepotismo_sobrenome`, `doacao_contrato`

validacao executada:
- `python -m py_compile src/core/cross_reference_engine.py scripts/validate_cross_reference_engine.py`
- `.venv/bin/python scripts/validate_cross_reference_engine.py`
- `.venv/bin/python -m src.core.cross_reference_engine --detector doacao_contrato --allow-internal`
- `.venv/bin/python -m src.core.cross_reference_engine --detector fracionamento --allow-internal`
- `.venv/bin/python -m src.core.cross_reference_engine --detector outlier_salarial --allow-internal`

## Quarentena do acervo alerts (2026-03-14)

escopo:
- saneiar o acervo historico da tabela `alerts`;
- impedir que linhas antigas com `APTO_APURACAO` sobrevivam no banco;
- fazer a UI ler uma view sanitizada, e nao a tabela bruta.

resultado:
- `scripts/sync_legacy_alerts_quarantine.py`
  - rebaixa `uso_externo` para `REVISAO_INTERNA`
  - marca `status = QUARENTENA`
  - cria `v_alerts_legacy_quarantine`
- `src/ui/streamlit_alerts.py`
  - passou a ler a view sanitizada
  - exibe `detector_status` e metricas de quarentena

validacao executada:
- `python -m py_compile scripts/sync_legacy_alerts_quarantine.py src/ui/streamlit_alerts.py`
- `.venv/bin/python scripts/sync_legacy_alerts_quarantine.py`
- `streamlit run app.py --server.headless true --server.port 8776`
- checagem final:
  - `alerts_external_rows=0`
  - `alerts_quarantined_rows=1809`
  - `v_alerts_legacy_quarantine -> APOSENTADO=1809`
