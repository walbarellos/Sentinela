# Dificuldades e Riscos

## 1. Lock do DuckDB

Problema recorrente:

- `streamlit` e scripts de escrita podem disputar lock do `sentinela_analytics.duckdb`

Mitigação:

- evitar rodar sync de escrita ao mesmo tempo que outras rotinas de escrita
- para diagnóstico, abrir em `read_only=True`

## 2. Acervo grande e histórico

Problema:

- há muito material em `docs/Claude-march/.../patch`
- nem tudo ali é fluxo ativo

Mitigação:

- usar esta pasta de handoff como índice curto
- tratar `entrega_denuncia_atual` como acervo principal de casos

## 3. Legado congelado ainda existe no repositório

Problema:

- arquivos legados ainda podem confundir quem entrar sem contexto

Mitigação:

- `alerts` em quarentena
- `cross_reference_engine` congelado
- `insights_engine` desligado por padrão

## 4. Dependência de documento oficial

Especialmente em `CEDIMP`, o avanço real depende de:

- resposta de `SEMSA/RH`
- resposta de `SESACRE`

Sem isso, o caso não deve subir de estágio.

## 5. Risco de regressão narrativa

Perigo:

- algum script, docs antigo ou UI voltar a tratar acervo legado como camada operacional

Mitigação:

- rerodar:
  - `scripts/validate_ops_output_guard.py`
  - `scripts/validate_ops_rulebook.py`
  - `scripts/validate_ops_calibration.py`
  - `scripts/validate_ops_sentinel.py`
  - `scripts/validate_cross_reference_engine.py`
  - `scripts/sync_legacy_alerts_quarantine.py`
