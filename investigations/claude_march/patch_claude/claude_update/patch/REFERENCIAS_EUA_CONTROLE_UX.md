# Referencias EUA - Controle e UX

## Fontes oficiais

- `USAspending`
  - https://www.usaspending.gov/search
  - https://api.usaspending.gov/docs/endpoints
  - ponto util: busca avancada, drill-down por award e perfil do recebedor

- `Oversight.gov`
  - https://www.oversight.gov/
  - ponto util: busca por relatorios e recomendacoes abertas, com status e fonte oficial

- `SEC / EDGAR`
  - https://www.sec.gov/search-filings
  - ponto util: leitura documental primeiro, timeline e anexos acessiveis
  - pagina oficial de ajuda: https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data

## O que vale trazer para o Sentinela

- filtro forte antes do grafico
- um caso por vez, com drill-down claro
- documento e cadeia de evidencia visiveis no mesmo fluxo
- status operacional objetivo:
  - triagem
  - pedido documental
  - analise juridico-funcional
  - representacao preliminar
- export simples do caso

## O que ja foi incorporado

- `Oversight.gov`
  - fila de pendencias documentais por caso
  - inbox operacional com status objetivo
  - workflow do caso com trilha de execucao
- `USAspending`
  - filtros fortes na fila de casos
  - visao de sujeito/valor/orgao antes do detalhe
  - escalabilidade por caso/fornecedor para o municipio
- `SEC / EDGAR`
  - documento primeiro
  - inspetor de artefatos no mesmo fluxo
  - timeline documental e diff textual quando houver texto extraivel
- `EDGAR + search-first`
  - busca textual por documento e anexo local antes de abrir arquivo bruto

## O que faz sentido para Rio Branco

- sim:
  - fila de casos
  - caixa de diligencias
  - timeline documental
  - diff e preview local
  - perfil por fornecedor/contrato
- nao:
  - navegação burocrática em muitas telas
  - taxonomia federal excessiva para o operador municipal
  - graficos de mercado sem efeito probatorio

## O que nao vale copiar

- excesso de paginas intermediarias
- visualizacao bonita sem utilidade investigativa
- score fechado sem explicar fonte e limite da conclusao

## Regra de produto

No Sentinela, UI boa e a que reduz:
- numero de cliques para achar o documento-fonte
- tempo para entender o limite do achado
- risco de o operador confundir hipotese com fato documental
