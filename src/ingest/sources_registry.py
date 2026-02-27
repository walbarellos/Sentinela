"""
SENTINELA // REGISTRY DE FONTES PÚBLICAS
Mapa completo de todas as fontes disponíveis + seus métodos de coleta.
Cada fonte tem: url, método (REST/CSV/SCRAPE/JSF), tabela destino DuckDB.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class CollectMethod(str, Enum):
    REST_JSON = "rest_json"       # API REST retorna JSON
    CSV_DIRECT = "csv_direct"     # CSV disponível para download direto
    CSV_BULK = "csv_bulk"         # CSV massivo (bulk download)
    JSF_SCRAPE = "jsf_scrape"     # Portal JSF/PrimeFaces (Rio Branco)
    HTML_SCRAPE = "html_scrape"   # HTML estático
    QUERIDO_DIARIO = "qd"         # Via API Querido Diário
    SICONFI = "siconfi"           # API SICONFI/STN


@dataclass
class DataSource:
    id: str
    name: str
    method: CollectMethod
    base_url: str
    table: str                     # tabela DuckDB destino
    priority: int = 3              # 1=crítico, 2=alto, 3=médio
    active: bool = True
    notes: str = ""
    params: dict = field(default_factory=dict)


SOURCES: list[DataSource] = [

    # ─── RIO BRANCO (JSF) ─────────────────────────────────────────────────────
    DataSource(
        id="rb_servidores",
        name="Portal RB — Pessoal/Salários",
        method=CollectMethod.JSF_SCRAPE,
        base_url="https://transparencia.riobranco.ac.gov.br/servidor/",
        table="servidores",
        priority=1,
        notes="JSF PrimeFaces. Requer ViewState + session. Ver jsf_client.py",
    ),
    DataSource(
        id="rb_diarias",
        name="Portal RB — Diárias",
        method=CollectMethod.JSF_SCRAPE,
        base_url="https://transparencia.riobranco.ac.gov.br/diaria/",
        table="diarias",
        priority=1,
    ),
    DataSource(
        id="rb_obras",
        name="Portal RB — Obras Públicas",
        method=CollectMethod.JSF_SCRAPE,
        base_url="https://transparencia.riobranco.ac.gov.br/obra/",
        table="obras",
        priority=1,
    ),
    DataSource(
        id="rb_despesas",
        name="Portal RB — Despesas",
        method=CollectMethod.JSF_SCRAPE,
        base_url="https://transparencia.riobranco.ac.gov.br/despesa/",
        table="despesas",
        priority=1,
    ),
    DataSource(
        id="rb_licitacoes",
        name="Portal RB — Licitações/Contratos",
        method=CollectMethod.JSF_SCRAPE,
        base_url="https://transparencia.riobranco.ac.gov.br/contratacao/",
        table="licitacoes",
        priority=1,
    ),
    DataSource(
        id="rb_receitas",
        name="Portal RB — Receitas",
        method=CollectMethod.JSF_SCRAPE,
        base_url="https://transparencia.riobranco.ac.gov.br/receita/",
        table="receitas",
        priority=2,
    ),

    # ─── CNPJ / RECEITA FEDERAL ───────────────────────────────────────────────
    DataSource(
        id="cnpj_brasilapi",
        name="BrasilAPI — CNPJ + QSA",
        method=CollectMethod.REST_JSON,
        base_url="https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
        table="empresas_cnpj",
        priority=1,
        notes="Rate limit: ~3 req/s. Buscar CNPJ a partir das obras/contratos já coletados.",
    ),

    # ─── TSE ──────────────────────────────────────────────────────────────────
    DataSource(
        id="tse_candidaturas",
        name="TSE — Candidaturas AC/Rio Branco",
        method=CollectMethod.CSV_BULK,
        base_url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2024_AC.zip",
        table="tse_candidatos",
        priority=2,
        notes="CSV em ZIP. Filtrar: SG_UF='AC', NM_MUNICIPIO='RIO BRANCO'",
    ),
    DataSource(
        id="tse_bens",
        name="TSE — Bens Declarados",
        method=CollectMethod.CSV_BULK,
        base_url="https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2024_AC.zip",
        table="tse_bens",
        priority=3,
    ),
    DataSource(
        id="tse_doacoes",
        name="TSE — Doações Eleitorais 2024",
        method=CollectMethod.CSV_BULK,
        base_url="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2024_AC.zip",
        table="tse_doacoes",
        priority=1,
        notes="CRÍTICO: cruzar doador_cnpj com empresas contratadas pós-eleição",
    ),
    DataSource(
        id="tse_resultados",
        name="TSE — Resultados Eleições",
        method=CollectMethod.CSV_BULK,
        base_url="https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes-2024/buweb/bweb_2t_AC_311020241535.zip",
        table="tse_resultados",
        priority=3,
    ),

    # ─── CGU ──────────────────────────────────────────────────────────────────
    DataSource(
        id="cgu_ceis",
        name="CGU — CEIS (Empresas Inidôneas)",
        method=CollectMethod.CSV_DIRECT,
        base_url="https://dadosabertos.cgu.gov.br/api/publico/ceis?pagina=1",
        table="cgu_ceis",
        priority=1,
        notes="API paginada. Cruzar empresa_cnpj com obras/contratos.",
    ),
    DataSource(
        id="cgu_cnep",
        name="CGU — CNEP (Penalidades)",
        method=CollectMethod.CSV_DIRECT,
        base_url="https://dadosabertos.cgu.gov.br/api/publico/cnep?pagina=1",
        table="cgu_cnep",
        priority=1,
    ),
    DataSource(
        id="cgu_cepim",
        name="CGU — CEPIM (Entidades Impedidas)",
        method=CollectMethod.CSV_DIRECT,
        base_url="https://dadosabertos.cgu.gov.br/api/publico/cepim?pagina=1",
        table="cgu_cepim",
        priority=2,
    ),
    DataSource(
        id="cgu_servidores_federal",
        name="CGU — Servidores Federais no AC",
        method=CollectMethod.CSV_BULK,
        base_url="https://dadosabertos.cgu.gov.br/api/publico/servidores/servidores?orgaoExercicio=26000&pagina=1",
        table="servidores_federais",
        priority=3,
        notes="Filtrar por AC. Detectar duplo vínculo (federal + municipal).",
    ),

    # ─── PNCP (COMPRASNET) ────────────────────────────────────────────────────
    DataSource(
        id="pncp_contratos",
        name="PNCP — Contratos Rio Branco",
        method=CollectMethod.REST_JSON,
        base_url="https://pncp.gov.br/api/pncp/v1/contratos?codigoMunicipio=1200401&dataInicial=2022-01-01&pagina=1",
        table="pncp_contratos",
        priority=1,
        notes="IBGE código RB=1200401. API REST paginada. Cruzar com CEIS.",
    ),
    DataSource(
        id="pncp_licitacoes",
        name="PNCP — Licitações Rio Branco",
        method=CollectMethod.REST_JSON,
        base_url="https://pncp.gov.br/api/pncp/v1/contratacoes?codigoMunicipio=1200401&pagina=1",
        table="pncp_licitacoes",
        priority=1,
    ),

    # ─── SICONFI ──────────────────────────────────────────────────────────────
    DataSource(
        id="siconfi_rreo",
        name="SICONFI — RREO Rio Branco (RCL)",
        method=CollectMethod.SICONFI,
        base_url="https://siconfi.tesouro.gov.br/siconfi/api/public/relatorio/rreo",
        table="siconfi_rreo",
        priority=1,
        params={"co_municipio": "1200401", "co_poder": "E", "no_periodo": "2024"},
        notes="Receita Corrente Líquida para cálculo do limite LRF art.19",
    ),
    DataSource(
        id="siconfi_rgf",
        name="SICONFI — RGF (Despesa Pessoal)",
        method=CollectMethod.SICONFI,
        base_url="https://siconfi.tesouro.gov.br/siconfi/api/public/relatorio/rgf",
        table="siconfi_rgf",
        priority=1,
        params={"co_municipio": "1200401"},
        notes="Relatório Gestão Fiscal — verificar se pessoal > 60% RCL",
    ),

    # ─── QUERIDO DIÁRIO ───────────────────────────────────────────────────────
    DataSource(
        id="qd_diario_rb",
        name="Querido Diário — D.O. Rio Branco",
        method=CollectMethod.QUERIDO_DIARIO,
        base_url="https://queridodiario.ok.org.br/api/gazettes",
        table="diario_oficial",
        priority=1,
        params={"territory_id": "1200401", "excerpt_size": 500},
        notes="Buscar: 'diária', 'portaria', 'concessão', 'dispensa'. Correlacionar com viagens.",
    ),

    # ─── TCE-ACRE ─────────────────────────────────────────────────────────────
    DataSource(
        id="tce_acre",
        name="TCE-Acre — Auditorias e Julgamentos",
        method=CollectMethod.HTML_SCRAPE,
        base_url="https://tce.ac.gov.br/portal/index.php/julgamento-de-contas",
        table="tce_julgamentos",
        priority=2,
        notes="HTML estático. BeautifulSoup. Empresas/gestores com contas rejeitadas.",
    ),

    # ─── IBGE ─────────────────────────────────────────────────────────────────
    DataSource(
        id="ibge_populacao",
        name="IBGE — Censo/População Rio Branco",
        method=CollectMethod.REST_JSON,
        base_url="https://servicodados.ibge.gov.br/api/v1/localidades/municipios/1200401",
        table="ibge_municipio",
        priority=3,
    ),

    # ─── DATAJUD / CNJ ────────────────────────────────────────────────────────
    DataSource(
        id="datajud_processos",
        name="DataJud CNJ — Processos RB",
        method=CollectMethod.REST_JSON,
        base_url="https://api-publica.datajud.cnj.jus.br/api_publica_tjac/_search",
        table="processos_judiciais",
        priority=2,
        notes="Buscar processos por CPF/CNPJ das entidades detectadas como suspeitas.",
    ),

    # ─── TCU ──────────────────────────────────────────────────────────────────
    DataSource(
        id="tcu_acordaos",
        name="TCU — Acórdãos (AC/RB)",
        method=CollectMethod.REST_JSON,
        base_url="https://contas.tcu.gov.br/etcu/ObterDocumentoSisdoc?codArqCatalogado=",
        table="tcu_acordaos",
        priority=2,
        notes="API de pesquisa do TCU. Buscar entidade='Rio Branco'.",
    ),

    # ─── DADOS ABERTOS FEDERAIS ───────────────────────────────────────────────
    DataSource(
        id="dados_abertos_rais",
        name="RAIS — Vínculos Empregatícios AC",
        method=CollectMethod.CSV_BULK,
        base_url="https://basedosdados.org/api/3/action/datastore_search",
        table="rais_vinculos",
        priority=3,
        notes="Via basedosdados.org BigQuery ou download direto MTE. Detectar servidores com vínculo privado.",
    ),

    # ─── CÂMARA MUNICIPAL ─────────────────────────────────────────────────────
    DataSource(
        id="cmrb_vereadores",
        name="Câmara Municipal RB — Vereadores/Votações",
        method=CollectMethod.HTML_SCRAPE,
        base_url="https://www.riobranco.ac.leg.br/transparencia",
        table="cmrb_vereadores",
        priority=2,
        notes="Cruzar com TSE doações. Vereadores que votaram em contratos suspeitos.",
    ),
]

# Index rápido
SOURCE_BY_ID = {s.id: s for s in SOURCES}
SOURCES_BY_PRIORITY = sorted(SOURCES, key=lambda s: s.priority)
