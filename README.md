
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

---

## 📡 Ingestão de Dados (Os Motores)

O dashboard só exibirá sinais se os dados forem capturados. Rode os scripts abaixo conforme a necessidade de atualização de cada aba:

### 1. Janela: Inteligência de Pessoal & Salários
Captura a folha de pagamento completa (CSV massivo) e detecta outliers salariais.
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/ingest/riobranco_servidores_mass.py
```

### 2. Janela: Radar de Obras Públicas
Captura a lista de obras e detalhes de contratos para detectar concentração de mercado.
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/ingest/riobranco_obras_list.py
```

### 3. Janela: Rastreio de Diárias
Captura o histórico de diárias e detecta "Viagens em Bloco" (servidores viajando juntos).
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 src/ingest/riobranco_diarias.py
```

---

## 📊 Estrutura de Dados

- **`data/sentinela_analytics.duckdb`**: Banco principal onde os dados normalizados são armazenados.
- **`src/core/analytics_db.py`**: Gerenciador de conexão e esquemas SQL.
- **`insights_engine.py`**: O "cérebro" do sistema, contendo as heurísticas de detecção de fraude e anomalias.
- **`jsf_client.py`**: Driver customizado para interagir com portais de transparência baseados em Java (JSF/PrimeFaces).

---
**AVISO:** Este sistema é para uso em auditoria e controle social. Respeite os termos de uso dos portais de transparência.
