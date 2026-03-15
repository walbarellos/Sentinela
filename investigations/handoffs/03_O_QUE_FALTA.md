# O Que Falta

## Falta alta prioridade

### 1. Continuar no produto vivo, não no legado

O próximo trabalho útil deve acontecer em `ops_*`, não em `cross_reference_engine.py`.

### 2. Auditar o resto fora de `ops`

Ainda vale revisar:

- rotas/backend que tratem `alerts` como KPI principal
- documentação antiga que ainda venda detectores legados como úteis
- scripts antigos que possam recriar linhas em `alerts`

### 3. Ampliar operação por documento recebido

Para o caso `CEDIMP`, o próximo passo real depende de resposta oficial:

- `SEMSA/RH`
- `SESACRE`

### 4. Refinar casos vivos

- `rb:contrato:3898`
  - continuar como divergência documental forte
- `sesacre:sancao:*`
  - manter notícia de fato técnica
  - evitar sobreafirmação temporal/jurídica

## Falta média prioridade

- melhorar UX da aba `🧪 ALERTAS LEGADOS (QUARENTENA)` para deixar ainda mais explícito que é acervo histórico
- consolidar alguns documentos repetidos do acervo de `docs/Claude-march/.../patch`
- criar rotina automática de re-saneamento de `alerts` após qualquer execução antiga

## Falta baixa prioridade

- protocolo/manual de operação por múltiplos usuários
- refinamento visual adicional do Streamlit
- limpeza cosmética de docs históricos não ativos
