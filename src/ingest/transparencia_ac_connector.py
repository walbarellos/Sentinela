"""
haEmet — Conector Portal de Transparência do Acre
Etapa 2: extração de orgao real do Governo do Acre.
"""
import json
import logging
import os
import re
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL = "https://transparencia.ac.gov.br"
API_PREFIX_CANDIDATES = ["/transparencia/api", "/api"]
CACHE_FILE = Path("./data/transparencia_ac/api_config.json")
VERSAO_CANDIDATES = ["v1", "v2", "v3"]
CODIGO_CANDIDATES = list(range(1, 51))


ORGAO_KEYWORDS: list[tuple[str, list[str]]] = [
    ("SESACRE", [
        "SESACRE",
        "SECRETARIA DE ESTADO DE SAUDE",
        "SEC EST SAUDE",
        "FUNDO ESTADUAL DE SAUDE",
        "FES ",
        "HOSPITAL DO JURUA",
        "HOSPITAL DE URGENCIA",
        "HUERB",
        "CAPS ESTADUAL",
        "LABORATORIO CENTRAL",
        "LACEN",
    ]),
    ("SEE", [
        "SECRETARIA DE ESTADO DE EDUCACAO",
        "SEC EST EDUC",
        "SEE ",
        "FUNDO ESTADUAL DE EDUCACAO",
        "FUDEB",
    ]),
    ("SEFAZ", [
        "SEFAZ",
        "SECRETARIA DE ESTADO DA FAZENDA",
        "SEC FAZENDA",
        "TESOURO ESTADUAL",
    ]),
    ("SEJUSP", [
        "SEJUSP",
        "SECRETARIA DE ESTADO DE JUSTICA",
        "POLICIA CIVIL",
        "POLICIA MILITAR",
        "CORPO DE BOMBEIROS",
        "DETRAN",
        "IAPEN",
    ]),
    ("SEPLANH", [
        "SEPLANH",
        "SECRETARIA DE ESTADO DE PLANEJAMENTO",
        "SEC EST PLAN",
    ]),
    ("SEINFRA", [
        "SEINFRA",
        "SECRETARIA DE ESTADO DE INFRAESTRUTURA",
        "DAER",
        "DEPASA",
    ]),
    ("SEOP", [
        "SEOP",
        "SECRETARIA DE ESTADO DE OBRAS",
        "OBRAS E SERVICOS PUBLICOS",
    ]),
    ("SEMA", [
        "SEMA ",
        "SECRETARIA DE ESTADO DE MEIO AMBIENTE",
        "IMAC",
    ]),
    ("SEAGRO", [
        "SEAGRO",
        "SECRETARIA DE ESTADO DE AGROPECUARIA",
        "IDAF",
    ]),
    ("SEAS", [
        "SEAS ",
        "SECRETARIA DE ESTADO DE ASSISTENCIA",
        "FUNDO ESTADUAL DE ASSISTENCIA",
    ]),
    ("SECD", [
        "SECD",
        "SECRETARIA DE ESTADO DE CULTURA",
        "ESPORTE E LAZER",
        "FUNDACAO CULTURAL",
    ]),
    ("SEDET", [
        "SEDET",
        "SECRETARIA DE ESTADO DE DESENVOLVIMENTO",
        "TURISMO",
        "FUNTAC",
    ]),
    ("CASA CIVIL", [
        "CASA CIVIL",
        "GABINETE DO GOVERNADOR",
        "VICE GOVERNADOR",
        "GOVERNO DO ESTADO",
    ]),
    ("PGE", [
        "PROCURADORIA GERAL DO ESTADO",
        "PGE ",
    ]),
    ("ALEAC", ["ASSEMBLEIA LEGISLATIVA"]),
    ("TCE-AC", ["TRIBUNAL DE CONTAS"]),
    ("MPAC", ["MINISTERIO PUBLICO DO ESTADO"]),
    ("TJ-AC", ["TRIBUNAL DE JUSTICA"]),
]


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).strip()


_ORGAO_KEYWORDS_NORM: list[tuple[str, list[str]]] = [
    (orgao, [_norm(keyword) for keyword in keywords])
    for orgao, keywords in ORGAO_KEYWORDS
]


def _keyword_matches(text: str, keyword: str) -> bool:
    if not text or not keyword:
        return False
    if re.fullmatch(r"[A-Z0-9/-]+", keyword):
        return re.search(rf"(?<![A-Z0-9]){re.escape(keyword)}(?![A-Z0-9])", text) is not None
    return keyword in text


def resolve_orgao(
    *,
    unidade_gestora: str = "",
    credor: str = "",
    natureza_despesa: str = "",
) -> str:
    """
    Resolve o orgao canônico do Governo do Acre.
    Ordem: unidade_gestora > credor > natureza_despesa.
    """
    candidates = [
        _norm(unidade_gestora),
        _norm(credor),
        _norm(natureza_despesa),
    ]

    for orgao, keywords in _ORGAO_KEYWORDS_NORM:
        for text in candidates:
            if not text:
                continue
            for keyword in keywords:
                if _keyword_matches(text, keyword):
                    return orgao

    return "GOVERNO_ACRE"


@dataclass
class ApiConfig:
    versao: str
    codigo: int
    base_url: str = BASE_URL
    api_prefix: str = "/transparencia/api"

    def url(self, endpoint: str) -> str:
        return f"{self.base_url}{self.api_prefix}/{self.versao}/json/{endpoint}"


@dataclass
class PagamentoRow:
    data_movimento: str
    numero_empenho: str
    credor: str
    cnpjcpf: str
    natureza_despesa: str
    modalidade_licitacao: str
    valor: float
    id_empenho: int
    orgao: str = ""
    unidade_gestora: str = ""
    codigo_unidade: str = ""


@dataclass
class ContratoRow:
    numero: str
    tipo: str
    data_inicio_vigencia: str
    data_fim_vigencia: str
    valor: float
    credor: str = ""
    cnpjcpf: str = ""
    objeto: str = ""
    orgao: str = ""
    unidade_gestora: str = ""


@dataclass
class LicitacaoRow:
    numero_processo: str
    modalidade: str
    objeto: str
    valor_estimado: float
    valor_real: float
    situacao: str
    data_abertura: str
    fornecedores: list[dict] = field(default_factory=list)
    orgao: str = ""
    unidade_gestora: str = ""


@dataclass
class FornecedorResumoRow:
    razao_social: str
    cnpjcpf: str
    empenhado: float
    liquidado: float
    pago: float
    ano: int
    orgao: str = ""
    entidade: str = ""


@dataclass
class FornecedorDetalheRow:
    ano: int
    entidade: str
    orgao: str
    razao_social: str
    cnpjcpf: str
    numero_empenho: str
    ano_empenho: str
    data_empenho: str
    total_empenho: float
    historico: str
    despesa_orcamentaria: str
    funcao: str
    subfuncao: str
    fonte_recurso: str
    numero_liquidacao: str
    data_liquidacao: str
    valor_liquidacao: float
    numero_pagamento: str
    data_pagamento: str
    valor_pagamento: float


class TransparenciaAcConnector:
    def __init__(
        self,
        data_dir: str = "./data/transparencia_ac",
        force: bool = False,
        pagesize: int = 500,
        delay_entre_requests: float = 0.5,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.force = force
        self.pagesize = pagesize
        self.delay = delay_entre_requests
        self._cfg: Optional[ApiConfig] = None

        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self.session.headers.update(
            {"User-Agent": "Sentinela/1.0", "Accept": "application/json"}
        )
        self._portal_tokens: dict[str, str] = {}

    def get_config(self) -> ApiConfig:
        if self._cfg:
            return self._cfg

        env_v = os.getenv("TRANSPARENCIA_AC_VERSAO")
        env_c = os.getenv("TRANSPARENCIA_AC_CODIGO")
        env_p = os.getenv("TRANSPARENCIA_AC_PREFIX")
        env_b = os.getenv("TRANSPARENCIA_AC_BASE_URL")
        if env_v and env_c and env_p:
            self._cfg = ApiConfig(
                versao=env_v,
                codigo=int(env_c),
                api_prefix=env_p,
                base_url=env_b or BASE_URL,
            )
            return self._cfg

        if CACHE_FILE.exists() and not self.force:
            try:
                self._cfg = ApiConfig(**json.loads(CACHE_FILE.read_text()))
                return self._cfg
            except Exception:
                pass

        cfg = self._discover()
        if not cfg:
            raise RuntimeError("API não encontrada")
        CACHE_FILE.write_text(
            json.dumps(
                {
                    "versao": cfg.versao,
                    "codigo": cfg.codigo,
                    "base_url": cfg.base_url,
                    "api_prefix": cfg.api_prefix,
                }
            )
        )
        self._cfg = cfg
        return cfg

    def _discover(self) -> Optional[ApiConfig]:
        for prefix in API_PREFIX_CANDIDATES:
            for versao in VERSAO_CANDIDATES:
                for codigo in [1285490, 383443] + CODIGO_CANDIDATES:
                    url = f"{BASE_URL}{prefix}/{versao}/json/exercicios/{codigo}"
                    try:
                        response = self.session.get(url, timeout=5)
                        if response.status_code == 200 and isinstance(response.json(), list):
                            log.info(
                                "API encontrada: prefix=%s versao=%s codigo=%d",
                                prefix,
                                versao,
                                codigo,
                            )
                            return ApiConfig(
                                versao=versao,
                                codigo=codigo,
                                api_prefix=prefix,
                            )
                    except Exception:
                        pass
        return None

    def _safe_get(self, url: str, params: dict = None):
        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code < 500:
                return response
            return None
        except Exception as exc:
            log.warning("GET %s falhou: %s", url, exc)
            return None

    def _clean_doc(self, raw: str) -> str:
        return re.sub(r"\D", "", raw or "")

    def _parse_valor(self, raw) -> float:
        try:
            text = str(raw).strip()
            if not text:
                return 0.0
            if "," in text and "." in text:
                if text.rfind(",") > text.rfind("."):
                    text = text.replace(".", "").replace(",", ".")
                else:
                    text = text.replace(",", "")
            elif "," in text:
                text = text.replace(".", "").replace(",", ".")
            return float(text)
        except Exception:
            return 0.0

    def _extract_unidade(self, item: dict) -> tuple[str, str]:
        nome = (
            item.get("unidadeGestora")
            or item.get("nomeUnidade")
            or item.get("orgao")
            or item.get("secretaria")
            or item.get("unidade")
            or ""
        )
        codigo = (
            str(item.get("codigoUnidade") or "")
            or str(item.get("codigoOrgao") or "")
            or str(item.get("codUnidade") or "")
        )
        return str(nome).strip(), str(codigo).strip()

    def _portal_csrf(self, page: str) -> str:
        if page in self._portal_tokens and not self.force:
            return self._portal_tokens[page]

        response = self.session.get(f"{BASE_URL}/{page}", timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        meta = soup.find("meta", {"name": "csrf-token"})
        if not meta or not meta.get("content"):
            raise RuntimeError(f"CSRF não encontrado na página {page}")
        token = meta["content"]
        self._portal_tokens[page] = token
        return token

    def _portal_headers(self, page: str) -> dict[str, str]:
        token = self._portal_csrf(page)
        return {
            "X-CSRF-TOKEN": token,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE_URL}/{page}",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

    def _portal_list(
        self,
        *,
        page: str,
        extra_payload: dict[str, str],
        order_dir: str = "desc",
        page_size: int = 500,
    ) -> list[dict]:
        token = self._portal_csrf(page)
        headers = self._portal_headers(page)
        rows: list[dict] = []
        start = 0
        records_total = None

        while True:
            payload = {
                "_token": token,
                "draw": "1",
                "start": str(start),
                "length": str(page_size),
                "search[value]": "",
                "search[regex]": "false",
                "order[0][column]": "0",
                "order[0][dir]": order_dir,
                "columns[0][data]": "",
                "columns[0][name]": "",
                "columns[0][searchable]": "true",
                "columns[0][orderable]": "true",
                "columns[0][search][value]": "",
                "columns[0][search][regex]": "false",
            }
            payload.update(extra_payload)
            response = self.session.post(
                f"{BASE_URL}/{page}/listar",
                data=payload,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            items = data.get("data", [])
            if records_total is None:
                try:
                    records_total = int(data.get("recordsFiltered") or data.get("recordsTotal") or 0)
                except Exception:
                    records_total = 0

            if not items:
                break

            rows.extend(items)
            start += len(items)
            if records_total and start >= records_total:
                break

            time.sleep(self.delay)

        return rows

    def _portal_post_json(
        self,
        *,
        page: str,
        endpoint: str,
        payload: dict[str, str],
    ):
        token = self._portal_csrf(page)
        headers = self._portal_headers(page)
        data = {"_token": token}
        data.update(payload)
        response = self.session.post(
            f"{BASE_URL}/{page}/{endpoint}",
            data=data,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _portal_pagamentos(self, ano: int) -> list[PagamentoRow]:
        items = self._portal_list(
            page="despesas",
            extra_payload={
                "ano": str(ano),
                "orgao": "",
                "busca": "",
                "filtro": "",
                "fonte": "",
                "despesa": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "bimestre": "",
                "quadrimestre": "",
                "semestre": "",
                "trimestre": "",
                "nr_empenho": "",
                "motivo": "",
                "programa": "",
            },
        )
        rows: list[PagamentoRow] = []
        for item in items:
            unidade = str(item.get("completo") or item.get("descricao") or "").strip()
            orgao = resolve_orgao(unidade_gestora=unidade)
            rows.append(
                PagamentoRow(
                    data_movimento=str(ano),
                    numero_empenho="",
                    credor=unidade,
                    cnpjcpf="",
                    natureza_despesa="DESPESAS_AGREGADAS_POR_ORGAO",
                    modalidade_licitacao="",
                    valor=self._parse_valor(item.get("pago", "0")),
                    id_empenho=0,
                    orgao=orgao,
                    unidade_gestora=unidade,
                    codigo_unidade="",
                )
            )
        return rows

    def _portal_contratos(self, ano: int) -> list[ContratoRow]:
        items = self._portal_list(
            page="contratos",
            extra_payload={
                "ano": str(ano),
                "orgao": "",
                "busca": "",
                "filtro": "",
                "fonte": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "modalidade": "",
                "fornecedor": "",
            },
        )
        rows: list[ContratoRow] = []
        for item in items:
            unidade = str(item.get("entidade") or "").strip()
            credor = str(item.get("nome_licitante") or "").strip()
            rows.append(
                ContratoRow(
                    numero=str(item.get("numero_contrato") or ""),
                    tipo=str(item.get("modalidade_licitacao") or ""),
                    data_inicio_vigencia=str(item.get("vigencia_inicial") or ""),
                    data_fim_vigencia=str(item.get("vigencia_final") or ""),
                    valor=self._parse_valor(item.get("valor_global_contrato", "0")),
                    credor=credor,
                    cnpjcpf=self._clean_doc(item.get("cpf_cnpj", "")),
                    objeto=str(item.get("objeto") or ""),
                    orgao=resolve_orgao(unidade_gestora=unidade, credor=credor),
                    unidade_gestora=unidade,
                )
            )
        return rows

    def _portal_licitacoes(self, ano: int) -> list[LicitacaoRow]:
        items = self._portal_list(
            page="licitacoes",
            extra_payload={
                "ano": str(ano),
                "orgao": "",
                "busca": "",
                "filtro": "",
                "fonte": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "modalidade": "",
                "natureza": "",
                "status": "",
            },
        )
        rows: list[LicitacaoRow] = []
        for item in items:
            unidade = str(item.get("entidade") or "").strip()
            rows.append(
                LicitacaoRow(
                    numero_processo=str(item.get("numero_licitacao") or ""),
                    modalidade=str(item.get("modalidade") or ""),
                    objeto=str(item.get("objeto_licitacao") or ""),
                    valor_estimado=0.0,
                    valor_real=0.0,
                    situacao=str(item.get("status_licitacao_atual") or ""),
                    data_abertura=str(item.get("data_abertura") or ""),
                    fornecedores=[],
                    orgao=resolve_orgao(unidade_gestora=unidade),
                    unidade_gestora=unidade,
                )
            )
        return rows

    def _portal_fornecedores(self, ano: int) -> list[FornecedorResumoRow]:
        return self._portal_fornecedores_filtrados(ano=ano)

    def _portal_fornecedores_filtrados(
        self,
        *,
        ano: int,
        busca: str = "",
        filtro: str = "",
        entidade: str = "",
        orgao: str = "",
    ) -> list[FornecedorResumoRow]:
        items = self._portal_list(
            page="fornecedores",
            extra_payload={
                "ano": str(ano),
                "busca": busca,
                "filtro": filtro,
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "bimestre": "",
                "quadrimestre": "",
                "semestre": "",
                "trimestre": "",
            },
        )
        rows: list[FornecedorResumoRow] = []
        for item in items:
            rows.append(
                FornecedorResumoRow(
                    razao_social=str(item.get("razaosocial") or item.get("descricao") or "").strip(),
                    cnpjcpf=self._clean_doc(item.get("cpfcnpjcredor", "")),
                    empenhado=self._parse_valor(item.get("empenhado", "0")),
                    liquidado=self._parse_valor(item.get("liquidado", "0")),
                    pago=self._parse_valor(item.get("pago", "0")),
                    ano=ano,
                    orgao=orgao or resolve_orgao(unidade_gestora=entidade or busca),
                    entidade=entidade or (busca if filtro == "orgao" else ""),
                )
            )
        return rows

    def _portal_orgaos(self, *, ano: int, page: str = "despesas") -> list[str]:
        rows: list[str] = []
        current_page = 1

        while True:
            response = self.session.get(
                f"{BASE_URL}/{page}/orgaos",
                params={
                    "busca": "",
                    "ano": str(ano),
                    "page": str(current_page),
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            items = data.get("data", [])
            if not items:
                break

            rows.extend(str(item.get("entidade") or "").strip() for item in items if item.get("entidade"))
            last_page = int(data.get("last_page") or current_page)
            if current_page >= last_page:
                break
            current_page += 1
            time.sleep(self.delay)

        return rows

    def _portal_despesas_fornecedores_por_orgao(
        self,
        *,
        ano: int,
        entidade: str,
        orgao_canonico: str,
    ) -> list[FornecedorResumoRow]:
        items = self._portal_list(
            page="despesas",
            extra_payload={
                "ano": str(ano),
                "orgao": entidade,
                "busca": "",
                "filtro": "fornecedor",
                "fonte": "",
                "despesa": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "bimestre": "",
                "quadrimestre": "",
                "semestre": "",
                "trimestre": "",
                "nr_empenho": "",
                "motivo": "",
                "programa": "",
            },
        )
        rows: list[FornecedorResumoRow] = []
        for item in items:
            rows.append(
                FornecedorResumoRow(
                    razao_social=str(item.get("razaosocial") or item.get("descricao") or "").strip(),
                    cnpjcpf=self._clean_doc(item.get("cpfcnpjcredor", "")),
                    empenhado=self._parse_valor(item.get("empenhado", "0")),
                    liquidado=self._parse_valor(item.get("liquidado", "0")),
                    pago=self._parse_valor(item.get("pago", "0")),
                    ano=ano,
                    orgao=orgao_canonico,
                    entidade=entidade,
                )
            )
        return rows

    def _portal_fornecedor_dados_exportacao(
        self,
        *,
        ano: int,
        fornecedor: str,
    ) -> list[FornecedorDetalheRow]:
        if not fornecedor.strip():
            return []

        items = self._portal_post_json(
            page="fornecedores",
            endpoint="dados-exportacao",
            payload={
                "ano": str(ano),
                "fornecedor": fornecedor,
                "busca": "",
                "busca_card": "",
                "filtro": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "bimestre": "",
                "trimestre": "",
                "quadrimestre": "",
                "semestre": "",
            },
        )
        return [
            self._to_fornecedor_detalhe_row(item, ano=ano, fornecedor_fallback=fornecedor)
            for item in (items or [])
        ]

    def _to_fornecedor_detalhe_row(
        self,
        item: dict,
        *,
        ano: int,
        entidade_fallback: str = "",
        fornecedor_fallback: str = "",
    ) -> FornecedorDetalheRow:
        entidade = str(item.get("entidade") or entidade_fallback or "").strip()
        razao_social = str(item.get("razaosocial") or fornecedor_fallback or "").strip()
        return FornecedorDetalheRow(
            ano=ano,
            entidade=entidade,
            orgao=resolve_orgao(unidade_gestora=entidade, credor=razao_social),
            razao_social=razao_social,
            cnpjcpf=self._clean_doc(item.get("cpfcnpjcredor", "")),
            numero_empenho=str(item.get("numeroempenho") or ""),
            ano_empenho=str(item.get("anoempenho") or ""),
            data_empenho=str(item.get("dataempenho") or ""),
            total_empenho=self._parse_valor(item.get("totalempenho", "0")),
            historico=str(item.get("historico") or ""),
            despesa_orcamentaria=str(item.get("despesaorcamentaria") or ""),
            funcao=str(item.get("funcao") or ""),
            subfuncao=str(item.get("subfuncao") or ""),
            fonte_recurso=str(item.get("fonterecurso") or ""),
            numero_liquidacao=str(item.get("numeroliquidacao") or ""),
            data_liquidacao=str(item.get("dataemissao") or ""),
            valor_liquidacao=self._parse_valor(item.get("valordaliquidacao", "0")),
            numero_pagamento=str(item.get("numeropagamento") or ""),
            data_pagamento=str(item.get("datapagamento") or ""),
            valor_pagamento=self._parse_valor(item.get("valorpagamento", "0")),
        )

    def _portal_despesas_dados_exportacao(
        self,
        *,
        ano: int,
        entidade: str,
    ) -> list[FornecedorDetalheRow]:
        items = self._portal_post_json(
            page="despesas",
            endpoint="dados-exportacao",
            payload={
                "ano": str(ano),
                "orgao": entidade,
                "unidade": "",
                "descricao": "",
                "fornecedor": "",
                "busca": "",
                "busca_card": "",
                "filtro": "orgao",
                "fonte": "",
                "despesa": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "bimestre": "",
                "trimestre": "",
                "quadrimestre": "",
                "semestre": "",
                "nr_empenho": "",
                "motivo": "",
                "programa": "",
            },
        )
        return [
            self._to_fornecedor_detalhe_row(item, ano=ano, entidade_fallback=entidade)
            for item in (items or [])
        ]

    def _portal_despesa_fornecedor_dados_exportacao(
        self,
        *,
        ano: int,
        entidade: str,
        fornecedor: str,
    ) -> list[FornecedorDetalheRow]:
        items = self._portal_post_json(
            page="despesas",
            endpoint="dados-exportacao",
            payload={
                "ano": str(ano),
                "orgao": entidade,
                "unidade": "",
                "descricao": "",
                "fornecedor": fornecedor,
                "busca": "",
                "busca_card": "",
                "filtro": "fornecedor",
                "fonte": "",
                "despesa": "",
                "periodo": "",
                "inicio": "",
                "fim": "",
                "mes": "",
                "bimestre": "",
                "trimestre": "",
                "quadrimestre": "",
                "semestre": "",
                "nr_empenho": "",
                "motivo": "",
                "programa": "",
            },
        )
        return [
            self._to_fornecedor_detalhe_row(
                item,
                ano=ano,
                entidade_fallback=entidade,
                fornecedor_fallback=fornecedor,
            )
            for item in (items or [])
        ]

    def get_exercicios(self):
        cfg = self.get_config()
        response = self._safe_get(cfg.url(f"exercicios/{cfg.codigo}"))
        return response.json() if response else []

    def _get_id_exercicio(self, ano):
        for exercicio in self.get_exercicios():
            if str(exercicio.get("nome", "")) == str(ano):
                return exercicio["id"]
        return None

    def get_pagamentos(self, ano: int) -> list[PagamentoRow]:
        try:
            cfg = self.get_config()
            id_exercicio = self._get_id_exercicio(ano)
        except Exception as exc:
            log.warning("Falha na API legada de pagamentos (%d): %s", ano, exc)
            return self._portal_pagamentos(ano)
        if not id_exercicio:
            log.warning("Exercício %d não encontrado na API legada; usando portal público", ano)
            return self._portal_pagamentos(ano)

        inicio, fim = f"01/01/{ano}", f"31/12/{ano}"
        response = self._safe_get(
            cfg.url(f"pagamentos/{cfg.codigo}/count"),
            {"exer": id_exercicio, "inicio": inicio, "fim": fim},
        )
        if not response:
            return self._portal_pagamentos(ano)

        try:
            total = int(response.json().get("totalDeRegistros", 0))
        except Exception:
            total = 0
        if total == 0:
            return self._portal_pagamentos(ano)

        rows: list[PagamentoRow] = []
        for page in range(1, (total // self.pagesize) + 2):
            response = self._safe_get(
                cfg.url(f"pagamentos/{cfg.codigo}"),
                {
                    "exer": id_exercicio,
                    "inicio": inicio,
                    "fim": fim,
                    "page": page,
                    "pagesize": self.pagesize,
                },
            )
            if not response:
                continue
            items = response.json()
            if not items:
                break

            for item in items:
                nome_unidade, codigo_unidade = self._extract_unidade(item)
                credor = item.get("credor", "").strip()
                natureza = item.get("naturezaDaDespesa", "")
                orgao = resolve_orgao(
                    unidade_gestora=nome_unidade,
                    credor=credor,
                    natureza_despesa=natureza,
                )
                rows.append(
                    PagamentoRow(
                        data_movimento=item.get("dataMovimento", ""),
                        numero_empenho=item.get("numeroEmpenho", ""),
                        credor=credor,
                        cnpjcpf=self._clean_doc(item.get("cnpjcpf", "")),
                        natureza_despesa=natureza,
                        modalidade_licitacao=item.get("modalidadeLicitacao", ""),
                        valor=self._parse_valor(item.get("valor", "0")),
                        id_empenho=int(item.get("idEmpenho", 0) or 0),
                        orgao=orgao,
                        unidade_gestora=nome_unidade,
                        codigo_unidade=codigo_unidade,
                    )
                )

            time.sleep(self.delay)

        return rows or self._portal_pagamentos(ano)

    def get_contratos(self, ano: int) -> list[ContratoRow]:
        try:
            cfg = self.get_config()
            id_exercicio = self._get_id_exercicio(ano)
        except Exception as exc:
            log.warning("Falha na API legada de contratos (%d): %s", ano, exc)
            return self._portal_contratos(ano)
        if not id_exercicio:
            return self._portal_contratos(ano)

        response = self._safe_get(
            cfg.url(f"contratos/{cfg.codigo}/count"),
            {"exer": id_exercicio, "inicio": f"01/01/{ano}", "fim": f"31/12/{ano}"},
        )
        try:
            total = int(response.json().get("totalDeRegistros", 0)) if response else 0
        except Exception:
            total = 0
        if total == 0:
            return self._portal_contratos(ano)

        rows: list[ContratoRow] = []
        for page in range(1, (total // self.pagesize) + 2):
            response = self._safe_get(
                cfg.url(f"contratos/{cfg.codigo}"),
                {"exer": id_exercicio, "page": page, "pagesize": self.pagesize},
            )
            if not response:
                continue
            items = response.json()
            if not items:
                break

            for item in items:
                nome_unidade, _ = self._extract_unidade(item)
                credor = item.get("credor", "").strip()
                orgao = resolve_orgao(
                    unidade_gestora=nome_unidade,
                    credor=credor,
                )
                rows.append(
                    ContratoRow(
                        numero=item.get("numContr", ""),
                        tipo=item.get("descTipoContr", ""),
                        data_inicio_vigencia=item.get("dataInicVigen", ""),
                        data_fim_vigencia=item.get("dataFinaVegen", ""),
                        valor=self._parse_valor(item.get("valor", "0")),
                        credor=credor,
                        cnpjcpf=self._clean_doc(item.get("cnpjcpf", "")),
                        objeto=item.get("objeto", ""),
                        orgao=orgao,
                        unidade_gestora=nome_unidade,
                    )
                )

            time.sleep(self.delay)

        return rows or self._portal_contratos(ano)

    def get_licitacoes(self, ano: int) -> list[LicitacaoRow]:
        try:
            cfg = self.get_config()
            response = self._safe_get(
                cfg.url(f"licitacoes/count/{cfg.codigo}"),
                {"inicio": f"01/01/{ano}", "fim": f"31/12/{ano}"},
            )
            total = int(response.json().get("totalDeRegistros", 0)) if response else 0
        except Exception as exc:
            log.warning("Falha na API legada de licitações (%d): %s", ano, exc)
            return self._portal_licitacoes(ano)
        if total == 0:
            return self._portal_licitacoes(ano)

        rows: list[LicitacaoRow] = []
        for page in range(1, (total // self.pagesize) + 2):
            response = self._safe_get(
                cfg.url(f"licitacoes/{cfg.codigo}"),
                {"page": page, "pagesize": self.pagesize},
            )
            if not response:
                continue
            items = response.json()
            if not items:
                break

            for item in items:
                nome_unidade, _ = self._extract_unidade(item)
                fornecedores = [
                    {
                        "nome": fornecedor.get("nome", ""),
                        "cnpjcpf": self._clean_doc(fornecedor.get("cnpjcpf", "")),
                        "vencedor": fornecedor.get("vencedor", False),
                    }
                    for fornecedor in item.get("fornecedores", [])
                ]
                rows.append(
                    LicitacaoRow(
                        numero_processo=item.get("numprocecompra", ""),
                        modalidade=item.get("modalidade", ""),
                        objeto=item.get("objeto", ""),
                        valor_estimado=self._parse_valor(item.get("valestimado", "0")),
                        valor_real=self._parse_valor(item.get("vlreal", "0")),
                        situacao=item.get("situacao", ""),
                        data_abertura=item.get("datainicio", ""),
                        fornecedores=fornecedores,
                        orgao=resolve_orgao(unidade_gestora=nome_unidade),
                        unidade_gestora=nome_unidade,
                    )
                )

            time.sleep(self.delay)

        return rows or self._portal_licitacoes(ano)

    def get_fornecedores(self, ano: int) -> list[FornecedorResumoRow]:
        return self._portal_fornecedores(ano)

    def get_fornecedores_por_orgao(
        self,
        ano: int,
        orgao_canonico: str,
    ) -> list[FornecedorResumoRow]:
        entidades = [
            entidade
            for entidade in self._portal_orgaos(ano=ano, page="despesas")
            if resolve_orgao(unidade_gestora=entidade) == orgao_canonico
        ]
        rows: list[FornecedorResumoRow] = []
        total = len(entidades)
        for idx, entidade in enumerate(entidades, start=1):
            log.info(
                "Coletando fornecedores de %s %d/%d (%s)",
                orgao_canonico,
                idx,
                total,
                entidade[:120],
            )
            rows.extend(
                self._portal_despesas_fornecedores_por_orgao(
                    ano=ano,
                    entidade=entidade,
                    orgao_canonico=orgao_canonico,
                )
            )
            time.sleep(self.delay)
        return rows

    def get_despesa_detalhes_por_orgao(
        self,
        ano: int,
        orgao_canonico: str,
        max_entidades: Optional[int] = None,
    ) -> list[FornecedorDetalheRow]:
        entidades = [
            entidade
            for entidade in self._portal_orgaos(ano=ano, page="despesas")
            if resolve_orgao(unidade_gestora=entidade) == orgao_canonico
        ]
        if max_entidades is not None:
            entidades = entidades[:max_entidades]
        rows: list[FornecedorDetalheRow] = []
        total = len(entidades)
        for idx, entidade in enumerate(entidades, start=1):
            log.info(
                "Coletando despesas detalhadas de %s %d/%d (%s)",
                orgao_canonico,
                idx,
                total,
                entidade[:120],
            )
            rows.extend(
                self._portal_despesas_dados_exportacao(
                    ano=ano,
                    entidade=entidade,
                )
            )
            time.sleep(self.delay)
        return rows

    def get_fornecedor_detalhes(
        self,
        ano: int,
        fornecedores: Optional[list[FornecedorResumoRow]] = None,
        max_fornecedores: Optional[int] = None,
    ) -> list[FornecedorDetalheRow]:
        fornecedores = fornecedores or self.get_fornecedores(ano)
        if max_fornecedores is not None:
            fornecedores = fornecedores[:max_fornecedores]

        rows: list[FornecedorDetalheRow] = []
        total = len(fornecedores)
        for idx, fornecedor in enumerate(fornecedores, start=1):
            if not fornecedor.razao_social:
                continue
            if idx == 1 or idx % 25 == 0 or idx == total:
                log.info(
                    "Detalhando fornecedores %d/%d (%s)",
                    idx,
                    total,
                    fornecedor.razao_social[:120],
                )
            try:
                if fornecedor.entidade:
                    rows.extend(
                        self._portal_despesa_fornecedor_dados_exportacao(
                            ano=ano,
                            entidade=fornecedor.entidade,
                            fornecedor=fornecedor.razao_social,
                        )
                    )
                else:
                    rows.extend(
                        self._portal_fornecedor_dados_exportacao(
                            ano=ano,
                            fornecedor=fornecedor.razao_social,
                        )
                    )
            except Exception as exc:
                log.warning(
                    "Falha ao detalhar fornecedor %s (%d/%d): %s",
                    fornecedor.razao_social,
                    idx,
                    total,
                    exc,
                )
            time.sleep(self.delay)
        return rows

    def run(self, anos=None):
        anos = anos or [2024, 2023]
        result = {
            "pagamentos": [],
            "contratos": [],
            "licitacoes": [],
            "cnpjs_unicos": set(),
        }
        for ano in anos:
            result["pagamentos"].extend(self.get_pagamentos(ano))
            result["contratos"].extend(self.get_contratos(ano))
            result["licitacoes"].extend(self.get_licitacoes(ano))
        result["cnpjs_unicos"] = {
            row.cnpjcpf for row in result["pagamentos"] if len(row.cnpjcpf) == 14
        }
        return result
