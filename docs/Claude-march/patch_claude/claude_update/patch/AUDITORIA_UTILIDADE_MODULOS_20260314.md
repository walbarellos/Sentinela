# Auditoria de Utilidade dos Modulos Operacionais

Data: `2026-03-14`

Objetivo:
- separar `essencial / util / excesso`;
- evitar implementacao que nao aumente verdade, prova, seguranca juridica ou eficiencia operacional;
- registrar o que deve ser mantido, simplificado, congelado ou removido.

Regra de admissao:
- um modulo novo so faz sentido se responder `sim` para pelo menos um item:
  - melhora a verdade do caso;
  - melhora a prova do caso;
  - melhora a seguranca juridica da saida;
  - reduz trabalho manual sem perder rigor.

## Classificacao

| Modulo | Papel | Nota | Classe | Decisao |
|---|---|---:|---|---|
| `ops_case_registry` | fila canonica de casos e artefatos | 10.0 | ESSENCIAL | manter |
| `ops_burden` | separa comprovado, pendente e sem base | 10.0 | ESSENCIAL | manter |
| `ops_semantic` | detecta contradicao objetiva entre contrato, edital e proposta | 10.0 | ESSENCIAL | manter |
| `ops_contradiction` | transforma divergencia em nucleo objetivo do caso | 9.8 | ESSENCIAL | manter |
| `ops_checklist` | controla se o caso esta minimamente maduro | 9.4 | ESSENCIAL | manter |
| `ops_export_gate` | bloqueia saida indevida e linguagem acusatoria | 10.0 | ESSENCIAL | manter |
| `ops_guard` | valida linguagem e reduz risco institucional | 9.7 | ESSENCIAL | manter |
| `ops_inbox` | fecha o loop documental e reduz shell manual | 9.5 | ESSENCIAL | manter |
| `ops_search` | localiza evidencias em acervo crescente | 9.0 | UTIL | manter |
| `ops_timeline` | organiza eventos e cadeia de custodia | 8.9 | UTIL | manter |
| `ops_export` | congela saidas seguras e diff de versoes | 9.2 | UTIL | manter |
| `ops_rulebook` | governanca das regras e benchmark oficial | 8.6 | UTIL | manter |
| `ops_runtime` | observabilidade basica de pipelines e cache | 8.8 | UTIL | manter |
| `ops_runbook` | apoio de encaminhamento por caso | 6.8 | UTIL / APOIO | simplificar e nao expandir |

## Diagnostico

### O que e indispensavel
- `registry + burden + semantic + contradiction + checklist + export_gate`
- esse bloco ja forma o nucleo de uma ferramenta util para `CGU / MP / PF / controle interno`.

### O que ajuda muito, mas nao pode dominar o produto
- `inbox`, `timeline`, `search`, `export`
- esses modulos tornam o trabalho humano viavel e rastreavel.

### O que estava com risco de virar excesso
- `runbook`
- problema: ele deriva fortemente de `gate + checklist + burden + proximo_passo`.
- risco: duplicar informacao e parecer mais importante do que realmente e.

## Decisao de produto

### Manter sem inflar
- `ops_runbook` fica como camada de apoio.
- nao deve ser tratado como camada probatoria.
- nao deve ganhar mais automacao antes de lacunas mais criticas.

### Simplificacao aplicada
- o `Runbook` saiu do primeiro nivel da bancada de caso.
- o conteudo foi rebaixado para expander dentro de `Exportacao`, como `Encaminhamento operacional`.
- as metricas de `runbook` sairam do overview principal.
- a mensagem de refresh do painel deixou de destacar `runbooks/passos`.

## O que nao implementar agora
- BPM completo de protocolo.
- score cosmetico.
- grafo bonito sem utilidade probatoria.
- automacao extra de runbook.
- mais tabs se o conteudo for derivado de algo que ja existe.

## Proximo criterio de desenvolvimento

Antes de qualquer feature nova, responder:
1. ela aumenta prova?
2. ela reduz falso positivo?
3. ela reduz trabalho manual real?
4. ela melhora a peca segura de saida?

Se a resposta for `nao` para as quatro, a feature nao entra.
