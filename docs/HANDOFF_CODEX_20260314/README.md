# Handoff Final — 2026-03-14

Esta pasta concentra o estado real do projeto para continuidade sem depender da memória da conversa.

## Leitura rápida

1. [01_ESTADO_ATUAL.md](./01_ESTADO_ATUAL.md)
2. [02_O_QUE_FOI_FEITO.md](./02_O_QUE_FOI_FEITO.md)
3. [03_O_QUE_FALTA.md](./03_O_QUE_FALTA.md)
4. [04_ARQUITETURA_ATIVA.md](./04_ARQUITETURA_ATIVA.md)
5. [05_REGRAS_E_LIMITES.md](./05_REGRAS_E_LIMITES.md)
6. [06_LEGADO_CONGELADO.md](./06_LEGADO_CONGELADO.md)
7. [07_COMANDOS_ESSENCIAIS.md](./07_COMANDOS_ESSENCIAIS.md)
8. [08_MAPA_DE_ARQUIVOS.md](./08_MAPA_DE_ARQUIVOS.md)
9. [09_CASOS_ATIVOS.md](./09_CASOS_ATIVOS.md)
10. [10_DIFICULDADES_E_RISCOS.md](./10_DIFICULDADES_E_RISCOS.md)
11. [11_VALIDACAO_E_METRICAS.md](./11_VALIDACAO_E_METRICAS.md)
12. [manifest_handoff_20260314.json](./manifest_handoff_20260314.json)

## Estado resumido

- trilha viva do produto: `ops_*`
- painel principal: `Streamlit` em `app.py`
- casos operacionais atuais: `12`
- artefatos operacionais indexados: `35`
- inbox operacional: `85` documentos
- exports congelados: `2`
- acervo legado `alerts`: `1809` linhas, todas em `QUARENTENA`
- engine legado `cross_reference_engine.py`: congelado

## Casos vivos

- `rb:contrato:3898` -> `APTO_A_NOTICIA_DE_FATO`
- `10` casos `sesacre:sancao:*` -> `APTO_A_NOTICIA_DE_FATO`
- `cedimp:saude_societario:13325100000130` -> `APTO_OFICIO_DOCUMENTAL`

## Dossiês e artefatos principais

O acervo detalhado continua em:

- [patch/entrega_denuncia_atual/README.md](../Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/README.md)
- [patch/INDEX_PRIORITARIOS.md](../Claude-march/patch_claude/claude_update/patch/INDEX_PRIORITARIOS.md)
- [patch/FASE_AUTOMACAO_OPERACIONAL_20260314.md](../Claude-march/patch_claude/claude_update/patch/FASE_AUTOMACAO_OPERACIONAL_20260314.md)
- [patch/AUDITORIA_ENGINE_LEGADO_20260314.md](../Claude-march/patch_claude/claude_update/patch/AUDITORIA_ENGINE_LEGADO_20260314.md)

## Regra central

O sistema não deve acusar. Ele deve:

- descrever fato documental
- descrever contradição objetiva
- limitar a inferência
- apontar a próxima diligência
- produzir `PEDIDO_DOCUMENTAL` ou `NOTICIA_FATO`, nunca condenação automática
