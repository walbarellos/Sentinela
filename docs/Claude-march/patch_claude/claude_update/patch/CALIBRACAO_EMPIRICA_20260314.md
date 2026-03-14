# Calibracao Empirica do Backend

Data: `2026-03-14`

Objetivo:
- medir o backend contra casos conhecidos;
- travar regressao de falso positivo;
- confirmar que familias ativas continuam no enquadramento correto.

## Benchmarks materializados

### Confirmado

1. `RB_3898_ACTIVE_DOCUMENTAL`
- esperado: caso ativo
- familia: `rb_sus_contrato`
- classe: `DIVERGENCIA_DOCUMENTAL`
- estagio: `APTO_A_NOTICIA_DE_FATO`

2. `CEDIMP_DOCUMENT_REQUEST_ONLY`
- esperado: caso ativo
- familia: `saude_societario`
- estagio: `APTO_OFICIO_DOCUMENTAL`
- `NOTICIA_FATO = False`
- `PEDIDO_DOCUMENTAL = True`

3. `SESACRE_REFERENCE_CASE_ACTIVE`
- esperado: caso ativo
- familia: `sesacre_sancao`
- classe: `CRUZAMENTO_SANCIONATORIO`
- burden central presente: `cruzamento_sancao_ativa`

4. `GLOBAL_LANGUAGE_GUARD_CLEAN`
- esperado: `0` flags ativas em `ops_case_language_guard`

### Descartado

5. `RB_3895_FALSE_POSITIVE_REMOVED`
- esperado: caso ausente da fila ativa
- historico preservado em nota especifica

### Inconclusivo

6. `RB_3898_CORE_DOCS_PENDING`
- esperado: `3` burden items centrais ainda em `PENDENTE_DOCUMENTO`

7. `CEDIMP_PARENTESCO_UNPROVEN`
- esperado: burden `parentesco_ou_designacao_cruzada = SEM_BASE_ATUAL`

8. `SESACRE_REFERENCE_DUE_DILIGENCE_PENDING`
- esperado: burden items de processo/due diligence ainda em `PENDENTE_DOCUMENTO`

## Resultado

- `calibration_benchmark_rows = 8`
- `calibration_result_rows = 8`
- `calibration_fail_rows = 0`
- `calibration_warn_rows = 0`
- resumo:
  - `confirmado = 4 PASS`
  - `descartado = 1 PASS`
  - `inconclusivo = 3 PASS`

Todos os benchmarks passaram.

## Leitura correta

Essa calibracao nao prova que o sistema esta “pronto para tudo”.
Ela prova que:
- o falso positivo conhecido do `3895` nao reapareceu;
- o `3898` continua no enquadramento documental correto;
- o `3898` continua sem processo integral artificialmente “resolvido”;
- `CEDIMP` continua bloqueado para `NOTICIA_FATO`;
- `CEDIMP` continua sem base para tese pessoal/parentesco;
- a familia `SESACRE` continua de pe no modo conservador;
- a familia `SESACRE` continua com due diligence pendente, e nao foi “fechada” de forma artificial;
- o `language guard` continua limpo.

## Arquivos principais

- `src/core/ops_calibration.py`
- `scripts/sync_ops_calibration.py`
- `scripts/validate_ops_calibration.py`

## Proximo uso correto

Sempre que uma regra nova entrar em producao:
1. adicionar benchmark correspondente;
2. rodar `sync_ops_case_registry.py`;
3. rodar `validate_ops_rulebook.py`;
4. rodar `validate_ops_calibration.py`.
