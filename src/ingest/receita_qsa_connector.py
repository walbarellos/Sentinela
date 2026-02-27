"""
haEmet — Conector Receita Federal: QSA (Quadro Societário)

Fonte: dados.gov.br — "Empresas"
Dataset público sem necessidade de autenticação.
Download: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

O dataset completo são ~30 arquivos ZIP de ~250MB cada (total ~5GB comprimido).
Para o Acre, filtramos CNPJ com município do Acre OU socios com CPF já no grafo.

Estratégia de dois modos:
  1. FULL: baixa dataset nacional e filtra por UF=AC (uso offline/pesquisa)
  2. LOOKUP: consulta a API pública CNPJ.ws para CNPJs individuais (uso online)
     https://publica.cnpj.ws/cnpj/{cnpj}  — sem autenticação, rate limit ~3 req/s
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger(__name__)

CNPJWS_URL = "https://publica.cnpj.ws/cnpj/{cnpj}"
RATE_LIMIT_DELAY = 0.4   # 3 req/s = ~0.33s; usamos 0.4 para segurança


@dataclass
class Socio:
    cpf_cnpj: str          # CPF (PF) ou CNPJ (PJ sócia)
    nome: str
    qualificacao: str      # "Sócio-Administrador", "Sócio", etc.
    entrada: str = ""      # data de entrada na sociedade
    faixa_etaria: str = ""


@dataclass
class EmpresaQSA:
    cnpj: str              # 14 dígitos
    razao_social: str
    nome_fantasia: str
    cnae_principal: str
    cnae_descricao: str
    situacao_cadastral: str
    municipio: str
    uf: str
    data_abertura: str
    capital_social: float
    socios: list[Socio] = field(default_factory=list)
    # Flag: True se algum sócio é político conhecido no grafo
    tem_socio_politico: bool = False


class ReceitaQSAConnector:
    """
    Resolve CNPJs para Empresas com QSA completo.

    Uso recomendado:
        connector = ReceitaQSAConnector()
        empresas = connector.lookup_many(cnpjs_do_transparencia_ac)

    O resultado alimenta o grafo Neo4j com:
      (:Pessoa {cpf})-[:SOCIO_DE]->(:Empresa {cnpj})
      (:Empresa {cnpj})-[:RECEBEU_PAGAMENTO {valor}]->(:OrgaoEstadual)
    """

    def __init__(
        self,
        cache_dir: str = "./data/qsa_cache",
        force: bool = False,
        delay: float = RATE_LIMIT_DELAY,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.force = force
        self.delay = delay

        self.session = requests.Session()
        self.session.headers["User-Agent"] = "haEmet/1.0 (anticorrupcao-acre)"

    # ── Lookup de um único CNPJ ───────────────────────────────────────────────

    def lookup(self, cnpj: str) -> Optional[EmpresaQSA]:
        """
        Consulta a API pública cnpj.ws para um CNPJ.
        Cacheia resultado em disco para evitar re-consultas.
        """
        cnpj_digits = re.sub(r"\D", "", cnpj)
        if len(cnpj_digits) != 14:
            log.debug("CNPJ inválido: %s", cnpj)
            return None

        cache_file = self.cache_dir / f"{cnpj_digits}.json"
        if cache_file.exists() and not self.force:
            data = json.loads(cache_file.read_text())
            return self._parse_cnpjws(data)

        url = CNPJWS_URL.format(cnpj=cnpj_digits)
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 429:
                log.warning("Rate limit atingido — aguardando 10s")
                time.sleep(10)
                resp = self.session.get(url, timeout=15)

            if not resp.ok:
                log.debug("CNPJ %s: HTTP %d", cnpj_digits, resp.status_code)
                return None

            data = resp.json()
            cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))
            time.sleep(self.delay)
            return self._parse_cnpjws(data)

        except Exception as e:
            log.error("Erro ao consultar CNPJ %s: %s", cnpj_digits, e)
            return None

    # ── Lookup em lote ────────────────────────────────────────────────────────

    def lookup_many(
        self,
        cnpjs: set[str],
        cpfs_politicos: set[str] = None,
    ) -> list[EmpresaQSA]:
        """
        Resolve uma lista de CNPJs.

        Se cpfs_politicos for fornecido, marca empresas onde um político
        do grafo aparece como sócio — isso gera o insight EMENDA_FAMILIA.
        """
        cpfs_politicos = cpfs_politicos or set()
        resultados = []
        total = len(cnpjs)

        log.info("QSA lookup: %d CNPJs", total)
        for i, cnpj in enumerate(sorted(cnpjs), 1):
            if i % 50 == 0:
                log.info("  %d/%d processados...", i, total)

            empresa = self.lookup(cnpj)
            if empresa:
                # Marca se algum sócio é político
                for socio in empresa.socios:
                    if socio.cpf_cnpj in cpfs_politicos:
                        empresa.tem_socio_politico = True
                        log.info(
                            "  ⚠️  SÓCIO POLÍTICO: %s é sócio de %s (%s)",
                            socio.nome, empresa.razao_social, empresa.cnpj
                        )
                        break
                resultados.append(empresa)

        politicas = [e for e in resultados if e.tem_socio_politico]
        log.info(
            "QSA concluído: %d empresas resolvidas, %d com sócios políticos",
            len(resultados), len(politicas)
        )
        return resultados

    # ── Parser da resposta cnpj.ws ────────────────────────────────────────────

    def _parse_cnpjws(self, data: dict) -> Optional[EmpresaQSA]:
        """
        Converte resposta da API cnpj.ws para EmpresaQSA.

        Estrutura da API:
        {
          "cnpj": "...",
          "razao_social": "...",
          "nome_fantasia": "...",
          "data_inicio_atividade": "...",
          "cnae_fiscal": 1234567,
          "cnae_fiscal_descricao": "...",
          "situacao_cadastral": 2,
          "descricao_situacao_cadastral": "ATIVA",
          "municipio": "...",
          "uf": "AC",
          "capital_social": 10000.0,
          "qsa": [
            {
              "identificador_de_socio": 2,  # 1=PJ, 2=PF
              "nome_socio": "...",
              "cnpj_cpf_do_socio": "...",
              "qualificacao_socio": "...",
              "data_entrada_sociedade": "...",
              "faixa_etaria": "..."
            }
          ]
        }
        """
        if not data or "cnpj" not in data:
            return None

        socios = []
        for s in data.get("qsa", []):
            cpf_cnpj = re.sub(r"\D", "", s.get("cnpj_cpf_do_socio", ""))
            socios.append(Socio(
                cpf_cnpj=cpf_cnpj,
                nome=s.get("nome_socio", "").strip(),
                qualificacao=s.get("qualificacao_socio", "").strip(),
                entrada=s.get("data_entrada_sociedade", ""),
                faixa_etaria=s.get("faixa_etaria", ""),
            ))

        return EmpresaQSA(
            cnpj=re.sub(r"\D", "", data.get("cnpj", "")),
            razao_social=data.get("razao_social", "").strip(),
            nome_fantasia=data.get("nome_fantasia", "").strip(),
            cnae_principal=str(data.get("cnae_fiscal", "")),
            cnae_descricao=data.get("cnae_fiscal_descricao", ""),
            situacao_cadastral=data.get("descricao_situacao_cadastral", ""),
            municipio=data.get("municipio", ""),
            uf=data.get("uf", ""),
            data_abertura=data.get("data_inicio_atividade", ""),
            capital_social=float(data.get("capital_social", 0) or 0),
            socios=socios,
        )
