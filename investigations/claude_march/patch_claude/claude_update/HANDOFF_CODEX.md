# Handoff Claude → Codex  (2026-03-13)
# Estado do pipeline + próximos passos precisos

## O que está pronto e funcionando no banco

| Etapa | Tabela / View | Status |
|-------|--------------|--------|
| SESACRE sanções | v_sancoes_ativas, sancoes_collapsed | ✅ 263 insights, 67 fornecedores, R$101mi |
| CEIS/CNEP | federal_ceis (22.444), federal_cnep (1.586) | ✅ |
| Lotação Rio Branco | rb_servidores_lotacao | ✅ 20.403 servidores, 4.427 SUS |
| Despesas SUS RB | rb_despesas_unidade, v_rb_despesas_sus | ✅ FMS 2024=R$320mi, 2025=R$51mi |

## O que falta (ordem de valor para denúncia)

### 1. Contratos de Rio Branco — use sync_rb_contratos.py

```bash
# Copiar para o projeto:
cp docs/Claude-march/patch_claude/sync_rb_contratos.py scripts/

# Compilar (deve ser silencioso):
python -m py_compile scripts/sync_rb_contratos.py

# Teste sem gravar (SEMSA 2025):
.venv/bin/python scripts/sync_rb_contratos.py --anos 2025 --dry-run

# Carga real SEMSA 2024+2025:
.venv/bin/python scripts/sync_rb_contratos.py --anos 2024 2025 --unit-contains semsa

# Carga completa todas as unidades:
.venv/bin/python scripts/sync_rb_contratos.py --anos 2023 2024 2025 --unit-contains ""
```

**O que este script faz diferente dos anteriores com erros:**
- Usa POST completo (não partial/ajax) — confirmado funcionar em /licitacao/
- Extrai fornecedor/CNPJ do detalhe /contrato/ver/{id}/ com regex de CNPJ
- Se CNPJ não vier (portal inconsistente), grava mesmo assim e insight ainda é gerado
- JOIN com v_sancoes_ativas por CNPJ → v_rb_contrato_ceis (automático se existir)

**Campos JSF confirmados na sessão anterior:**
- form_id = j_idt35 (igual em /licitacao/ e /contrato/)
- SEMSA no autocomplete = id 4065, label "01.10.00.00000.000.00 - Secretaria Municipal de Saúde - SEMSA"
- 510 contratos SEMSA 2024 confirmados

---

### 2. Licitações de Rio Branco — /licitacao/

O endpoint /licitacao/ já foi validado:
- POST completo retornou 10 linhas incluindo "Serviços de outsourcing e outros - SEMSA."
- O submit que funciona é Formulario:j_idt90=Pesquisar (diferente do AJAX)

```bash
# Script ainda não existe — criar sync_rb_licitacoes.py
# Reaproveitar exatamente a mesma lógica de sync_rb_contratos.py
# Trocar URL_CONT por URL_LIC = BASE + "/licitacao/"
# Ajustar _parse_table para layout de licitação:
# [0] Nº Processo  [1] Modalidade  [2] Objeto  [3] Secretaria
# [4] Situação     [5] Data Aber.  [6] Val.Est [7] Val.Hom
# [8] Fornecedor   [9] CNPJ venc
```

---

### 3. PNCP — contratos federais (Etapa 5)

Zero no banco. API pública, sem autenticação:

```python
# Endpoint contratos por CNPJ:
GET https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/contratos?dataInicial=20240101&dataFinal=20251231&pagina=1&tamanhoPagina=50

# Endpoint licitações:
GET https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras?...

# CNPJs a consultar (pelos fornecedores SESACRE sancionados):
# Extrair da tabela sancoes_collapsed: SELECT DISTINCT cnpj FROM sancoes_collapsed WHERE ativa
```

Script a criar: `scripts/sync_pncp.py`
- Lê CNPJs de sancoes_collapsed WHERE ativa=TRUE
- Consulta PNCP por CNPJ
- Persiste em pncp_contratos
- Gera insight kind=PNCP_SANCIONADO quando CNPJ está em v_sancoes_ativas

---

### 4. Receitas Rio Branco

```bash
curl -I https://transparencia.riobranco.ac.gov.br/receita/
# Espera 200 — mesma lógica JSF
```

Script a criar: `scripts/sync_rb_receitas.py` — menor prioridade para denúncia.

---

## Correção necessária em sources_registry.py

O registry ainda aponta para `/contratacao/` que retorna 404.
Corrigir para:
```python
# DE:
"url": "https://transparencia.riobranco.ac.gov.br/contratacao/"
# PARA:
"url": "https://transparencia.riobranco.ac.gov.br/contrato/"
```

---

## Resumo da prioridade para denúncia

1. **sync_rb_contratos.py** → contratos SEMSA + cruzamento CEIS = denúncia municipal forte
2. **sync_pncp.py** → fecha o cruzamento federal (PNCP × CEIS)
3. **sync_rb_licitacoes.py** → completa a trilha de compras municipais
4. **sync_rb_receitas.py** → contexto orçamentário (menor urgência)
