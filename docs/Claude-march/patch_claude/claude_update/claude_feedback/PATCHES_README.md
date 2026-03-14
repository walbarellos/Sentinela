Vou fazer as correções críticas direto no código. Três arquivos: o cruzamento CEIS (bug mais grave), o engine de detecção, e um validador novo.Todos compilam limpos. Aqui está o que foi entregue:

**Patch 01 — `patch_01_sancao_temporal.py`**
O bug mais grave. Substitui `_build_views` e `_build_insights` no `sync_rb_contratos.py`. Cria três views: `v_rb_contrato_ceis_valida` (só usa essa para alertas externos), `v_rb_contrato_ceis_revisao` (abrangência ambígua, triagem interna) e `v_rb_contrato_ceis_invalida` (prova documental de quais casos foram descartados e por quê). Confiança dos alertas CEIS reduzida de 95 → 90 para refletir que ainda precisa de revisão humana da due diligence.

**Patch 02 — `patch_02_detectores.py`**
Corrige o `cross_reference_engine.py` inteiro: threshold de fracionamento para R$ 50.000 com base legal correta, nepotismo por sobrenome vira triagem interna pura com lista de sobrenomes comuns do Acre excluídos (Silva, Lima, Santos, Kaxinawá e mais 50+), outlier salarial exige n >= 30, e todos os detectores agora saem com os campos `classe_achado`, `grau_probatorio`, `uso_externo` etc. que o protocolo probatório exige mas o engine antigo não implementava.

**Patch 03 — `validate_sancao_timeline.py`**
Script de diagnóstico standalone — rode antes de aplicar qualquer coisa. Ele lê o banco atual e mostra exatamente quantos alertas são falsos positivos temporais, qual é o diagnóstico específico do contrato 3895, e qual seria o impacto do patch 01. Copie para `scripts/` do projeto.

**Ordem de execução:**
```bash
# Primeiro diagnostique sem mudar nada
.venv/bin/python scripts/validate_sancao_timeline.py

# Aplique os patches e regenere
.venv/bin/python scripts/sync_rb_contratos.py --anos 2024 2025
.venv/bin/python scripts/sync_ops_case_registry.py
.venv/bin/python scripts/validate_ops_output_guard.py
```

# Sentinela — Patches Críticos

Esses três arquivos corrigem os problemas identificados na auditoria de março/2026.
Aplique na ordem abaixo antes de qualquer uso externo do sistema.

---

## Pré-requisito: diagnóstico

Rode primeiro para saber o impacto real no seu banco:

```bash
.venv/bin/python sentinela_patches/validate_sancao_timeline.py
```

Isso vai mostrar quantos alertas atuais são falsos positivos e qual o impacto do patch.

---

## Patch 01 — Cruzamento CEIS com verificação temporal e de abrangência

**Arquivo:** `patch_01_sancao_temporal.py`

**Problema corrigido:** O sistema gerava alerta CRÍTICO mesmo quando a sanção
da empresa começou DEPOIS do contrato, e mesmo quando a abrangência da sanção
não alcançava municípios (ex: sanção restrita ao Governo de Rondônia).

**Aplicação:**

Opção A — standalone (regenera só as views e insights de contratos):
```bash
.venv/bin/python sentinela_patches/patch_01_sancao_temporal.py data/sentinela_analytics.duckdb
```

Opção B — integrar ao `sync_rb_contratos.py` (recomendado):
Substituir a função `_build_views` e `_build_insights` pela versão corrigida.
As funções `build_views_corrigido` e `build_insights_corrigido` deste arquivo
são drop-in replacements.

**Novas views criadas:**
- `v_rb_contrato_ceis_valida` → use esta para alertas externos (sanção preexiste + abrangência verificada)
- `v_rb_contrato_ceis_revisao` → triagem interna (abrangência ambígua — revisar)
- `v_rb_contrato_ceis_invalida` → documentação de falsos positivos removidos

---

## Patch 02 — Detectores com campos probatórios e thresholds corretos

**Arquivo:** `patch_02_detectores.py`

**Problemas corrigidos:**

1. **Fracionamento:** threshold corrigido de R$ 57.278,16 (sem base legal)
   para R$ 50.000 (Lei 14.133/2021, art. 75, I). Base legal corrigida.

2. **Nepotismo por sobrenome:** rebaixado para `REVISAO_INTERNA` exclusiva,
   com exclusão de sobrenomes comuns no Acre. NUNCA gera alerta externo.

3. **Outlier salarial:** exige n >= 30 servidores por cargo (antes era 5).
   Adiciona nota sobre adicionais legítimos.

4. **Empresa suspensa:** verifica data da sanção vs data do contrato.

5. **Todos os detectores:** agora incluem `classe_achado`, `grau_probatorio`,
   `uso_externo`, `inferencia_permitida`, `limite_conclusao`.

**Aplicação:**

```python
# Substituir em src/core/cross_reference_engine.py:

# Trocar LEGAL["fracionamento"] → base_legal da função detect_fracionamento_corrigido
# Trocar LIMITE_DISPENSA_SERVICOS → 50_000.00
# Substituir detect_fracionamento → detect_fracionamento_corrigido
# Substituir detect_outlier_salarial → detect_outlier_salarial_corrigido
# Substituir detect_nepotismo_sobrenome → detect_nepotismo_triagem_interna
# Substituir detect_empresa_suspensa → detect_empresa_suspensa_corrigido
# Substituir Alert → AlertaExpanded (adiciona campos probatórios)
# Substituir save_alerts → save_alerts_com_campos_probatorios
```

---

## Patch 03 — Validador standalone

**Arquivo:** `validate_sancao_timeline.py`

Script de diagnóstico que pode ser rodado a qualquer momento.
Não modifica o banco — apenas lê e reporta.

**Copiar para o projeto:**
```bash
cp sentinela_patches/validate_sancao_timeline.py scripts/validate_sancao_timeline.py
```

**Uso:**
```bash
.venv/bin/python scripts/validate_sancao_timeline.py
# ou
.venv/bin/python scripts/validate_sancao_timeline.py --db data/sentinela_analytics.duckdb
```

---

## Após aplicar os patches

Regenerar tudo na ordem correta:

```bash
# 1. Regenerar contratos e views CEIS
.venv/bin/python scripts/sync_rb_contratos.py --anos 2024 2025

# 2. Validar timeline
.venv/bin/python scripts/validate_sancao_timeline.py

# 3. Regenerar registry operacional (vai puxar os novos insights)
.venv/bin/python scripts/sync_ops_case_registry.py

# 4. Regenerar gate de linguagem e exportações
.venv/bin/python scripts/sync_ops_guard.py
.venv/bin/python scripts/validate_ops_output_guard.py
```

---

## O que NÃO foi alterado (funciona como esperado)

- Case 3898 (item fora do edital) → divergência documental objetiva, mantido
- Protocolo probatório e matriz de ônus → corretos
- Gate de exportação e language guard → funcionam
- Caso SESACRE top 10 → cruzamento válido (verificar abrangência individualmente)
- CEDIMP → correto como `APTO_OFICIO_DOCUMENTAL` / `PEDIDO_DOCUMENTAL`
