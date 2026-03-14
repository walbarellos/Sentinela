
<img width="1653" height="793" alt="1" src="https://github.com/user-attachments/assets/52a85202-3cf2-46fb-aa47-72f0a9d30be5" />
<img width="1653" height="793" alt="2" src="https://github.com/user-attachments/assets/04809f6e-0e6c-4114-9b85-e2240591eff7" />
<img width="1653" height="793" alt="3" src="https://github.com/user-attachments/assets/eab487b3-adb3-4723-8c9b-334369e94f3f" />


# 🛡️ SENTINELA // COMMAND CENTER

Sistema de inteligência e auditoria de gastos públicos focado no Estado do Acre e Município de Rio Branco. O projeto utiliza **DuckDB** para análise massiva de dados e **Neo4j** para mapeamento de grafos de influência.

## 🚀 Pré-requisitos

- **Python 3.10+**
- **Docker & Docker Compose** (para o banco Neo4j)
- **Venv** (ambiente virtual)

---

## 🛠️ Configuração Inicial

1. **Clonar o repositório e entrar na pasta:**
   ```bash
   cd Projetos/Sentinela
   ```

2. **Criar e ativar o ambiente virtual:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 🚦 Operação (Dois Terminais)

Para o funcionamento completo, você deve manter dois processos rodando simultaneamente:

### Terminal 1: Banco de Dados (Grafo)
Sobe o container do Neo4j para análises de relacionamentos.
```bash
chmod +x start_db.sh
./start_db.sh
```
*Acesse a interface visual em: `http://localhost:7474`*

### Terminal 2: Dashboard (Streamlit)
Interface principal de monitoramento e alertas.
```bash
source .venv/bin/activate
streamlit run app.py
```
*Acesse o painel em: `http://localhost:8501`*

Observação:
- O painel operacional ativo do projeto é o `Streamlit` em `app.py`.
- Hoje não há aplicação `Dash` separada versionada como interface principal.
- As camadas novas de casos e artefatos operacionais devem ser integradas nesse painel, não em uma UI paralela.

---

## 📡 Ingestão de Dados (Os Motores)

O dashboard só exibirá sinais se os dados forem capturados. Rode os scripts abaixo conforme a necessidade de atualização de cada aba:

### 1. Janela: Inteligência de Pessoal & Salários
Captura a folha de pagamento completa (CSV massivo) e alimenta triagem interna estatística conservadora de remuneração.
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/ingest/riobranco_servidores_mass.py
```

### 2. Janela: Radar de Obras Públicas
Captura a lista de obras e detalhes de contratos para análise exploratória e cruzamentos futuros.
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/ingest/riobranco_obras_list.py
```

### 3. Janela: Rastreio de Diárias
Captura o histórico de diárias para revisão documental e cruzamentos posteriores.
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/ingest/riobranco_diarias.py
```

---

## 📂 Camada Operacional

O Streamlit agora também pode operar como fila de casos probatórios:
- `ops_case_registry`: registro materializado de casos
- `ops_case_artifact`: artefatos locais com `path`, `sha256` e tamanho
- `ops_pipeline_run`: trilha de execuções dos syncs operacionais
- `ops_source_cache`: catálogo/cache de fontes públicas com `ttl`, `etag`, `last_modified` e snapshot local quando disponível
- `ops_case_inbox_document`: caixa operacional de respostas oficiais por caso
- `v_ops_case_timeline_event`: timeline documental por caso
- `ops_artifact_text_index`: índice textual local dos artefatos e anexos suportados
- `ops_case_burden_item`: matriz de ônus probatório por caso
- `ops_case_semantic_issue`: diff semântico materializado por caso
- `ops_case_contradiction`: contradições objetivas materializadas
- `ops_case_checklist`: checklist probatório por caso
- `ops_case_language_guard`: gate de linguagem externa não-acusatória
- `ops_case_generated_export`: exportações seguras congeladas como artefato controlado do caso
- aba `📂 OPERAÇÕES`: resumo, filtros, detalhe do caso, busca textual, inbox, timeline, diff e visualização local de artefatos
- cobertura atual da inbox: `13` casos (`RB_SUS`, `SESACRE` e `CEDIMP`)

Para rematerializar essa camada:
```bash
.venv/bin/python scripts/sync_ops_case_registry.py
.venv/bin/python scripts/sync_ops_source_cache.py
.venv/bin/python scripts/sync_ops_inbox.py
.venv/bin/python scripts/sync_ops_timeline.py
.venv/bin/python scripts/sync_ops_search_index.py
.venv/bin/python scripts/sync_ops_burden.py
.venv/bin/python scripts/sync_ops_semantic.py
.venv/bin/python scripts/sync_ops_contradiction.py
.venv/bin/python scripts/sync_ops_checklist.py
.venv/bin/python scripts/sync_ops_guard.py
.venv/bin/python scripts/sync_ops_export_gate.py
.venv/bin/python scripts/sync_ops_rulebook.py
.venv/bin/python scripts/validate_ops_output_guard.py
.venv/bin/python scripts/validate_ops_rulebook.py
```

Para congelar uma saída segura como artefato versionado do caso:
```bash
.venv/bin/python scripts/freeze_ops_case_export.py --case-id rb:contrato:3898 --mode NOTICIA_FATO
```

---

## 📊 Estrutura de Dados

- **`data/sentinela_analytics.duckdb`**: Banco principal onde os dados normalizados são armazenados.
- **`src/core/analytics_db.py`**: Gerenciador de conexão e esquemas SQL.
- **`insights_engine.py`**: Motor legado e exploratório. Fica desligado por padrão e serve apenas para triagem interna explícita.
- **`jsf_client.py`**: Driver customizado para interagir com portais de transparência baseados em Java (JSF/PrimeFaces).

---
**AVISO:** Este sistema é para uso em auditoria e controle social. Respeite os termos de uso dos portais de transparência.
