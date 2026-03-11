"""
haEmet — Conector Portal de Transparência do Acre
Etapa 2: extração de `orgao` real (SESACRE, SEFAZ, etc.) a partir dos campos
da API e fallback por heurística sobre credor/natureza_despesa.
"""
import json
import logging
import re
import time
import os
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://transparencia.ac.gov.br"
API_PREFIX_CANDIDATES = ["/transparencia/api", "/api"]
CACHE_FILE = Path("./data/transparencia_ac/api_config.json")
VERSAO_CANDIDATES = ["v1", "v2", "v3"]
CODIGO_CANDIDATES = list(range(1, 51))

# ── Mapeamento canônico de órgãos do Governo do Acre ─────────────────────────
# Chaves são substrings normalizadas (uppercase, sem acento) a buscar em:
#   unidadeGestora / nomeUnidade / credor / naturezaDaDespesa
# Valores são o nome canônico do órgão.
ORGAO_KEYWORDS: list[tuple[str, list[str]]] = [
    # Saúde — SESACRE (prioridade alta, checar primeiro)
    ("SESACRE", [
        "SESACRE",
        "SECRETARIA DE ESTADO DE SAUDE",
        "SEC EST SAUDE",
        "FUNDO ESTADUAL DE SAUDE",
        "FES ",
        "HOSPITAL DO JURUÁ",
        "HOSPITAL DE URGÊNCIA",
        "HUERB",
        "CAPS ESTADUAL",
        "LABORATÓRIO CENTRAL",
        "LACEN",
    ]),
    # Educação
    ("SEE", [
        "SECRETARIA DE ESTADO DE EDUCACAO",
        "SEC EST EDUC",
        "SEE ",
        "FUNDO ESTADUAL DE EDUCACAO",
        "FUDEB",
    ]),
    # Fazenda
    ("SEFAZ", [
        "SEFAZ",
        "SECRETARIA DE ESTADO DA FAZENDA",
        "SEC FAZENDA",
        "TESOURO ESTADUAL",
    ]),
    # Segurança Pública
    ("SEJUSP", [
        "SEJUSP",
        "SECRETARIA DE ESTADO DE JUSTICA",
        "POLICIA CIVIL",
        "POLICIA MILITAR",
        "CORPO DE BOMBEIROS",
        "DETRAN",
        "IAPEN",
    ]),
    # Planejamento e Gestão
    ("SEPLANH", [
        "SEPLANH",
        "SECRETARIA DE ESTADO DE PLANEJAMENTO",
        "SEC EST PLAN",
        "IMAC",
    ]),
    # Infraestrutura
    ("SEINFRA", [
        "SEINFRA",
        "SECRETARIA DE ESTADO DE INFRAESTRUTURA",
        "DAER",
        "DEPASA",
    ]),
    # Obras
    ("SEOP", [
        "SEOP",
        "SECRETARIA DE ESTADO DE OBRAS",
        "OBRAS E SERVICOS PUBLICOS",
    ]),
    # Meio Ambiente
    ("SEMA", [
        "SEMA ",
        "SECRETARIA DE ESTADO DE MEIO AMBIENTE",
        "IMAC",
        "IPAAM",
    ]),
    # Agropecuária
    ("SEAGRO", [
        "SEAGRO",
        "SECRETARIA DE ESTADO DE AGROPECUARIA",
        "EMBRAPA",
        "IDAF",
    ]),
    # Assistência Social
    ("SEAS", [
        "SEAS ",
        "SECRETARIA DE ESTADO DE ASSISTENCIA",
        "FUNDO ESTADUAL DE ASSISTENCIA",
    ]),
    # Cultura / Esporte
    ("SECD", [
        "SECD",
        "SECRETARIA DE ESTADO DE CULTURA",
        "ESPORTE E LAZER",
        "FUNDAÇÃO CULTURAL",
    ]),
    # Turismo / Desenvolvimento
    ("SEDET", [
        "SEDET",
        "SECRETARIA DE ESTADO DE DESENVOLVIMENTO",
        "TURISMO",
        "FUNTAC",
    ]),
    # Casa Civil / Governo
    ("CASA CIVIL", [
        "CASA CIVIL",
        "GABINETE DO GOVERNADOR",
        "VICE GOVERNADOR",
        "GOVERNO DO ESTADO",
    ]),
    # Procuradoria
    ("PGE", [
        "PROCURADORIA GERAL DO ESTADO",
        "PGE ",
    ]),
    # Assembleia / TCE / MP (esfera estadual mas não executivo)
    ("ALEAC", ["ASSEMBLEIA LEGISLATIVA"]),
    ("TCE-AC", ["TRIBUNAL DE CONTAS"]),
    ("MPAC", ["MINISTERIO PUBLICO DO ESTADO"]),
    ("TJ-AC", ["TRIBUNAL DE JUSTICA"]),
]

# Pré-compilar em uppercase normalizado para performance
def _norm(s: str) -> str:
    s = s.upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()

_ORGAO_KEYWORDS_NORM: list[tuple[str, list[str]]] = [
    (orgao, [_norm(k) for k in kws])
    for orgao, kws in ORGAO_KEYWORDS
]


def resolve_orgao(
    *,
    unidade_gestora: str = "",
    credor: str = "",
    natureza_despesa: str = "",
) -> str:
    """
    Resolve o órgão canônico a partir de campos da API.
    Ordem de prioridade: unidade_gestora > credor > natureza_despesa.
    Retorna '' se não conseguir mapear.
    """
    candidates = [
        _norm(unidade_gestora or ""),
        _norm(credor or ""),
        _norm(natureza_despesa or ""),
    ]
    for orgao, keywords in _ORGAO_KEYWORDS_NORM:
        for text in candidates:
            if not text:
                continue
            for kw in keywords:
                if kw in text:
                    return orgao
    return "GOVERNO_ACRE"


# ── Dataclasses ───────────────────────────────────────────────────────────────

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
    # Etapa 2: campos de recorte institucional
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


# ── Conector ──────────────────────────────────────────────────────────────────

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
        self.session.headers.update({
            "User-Agent": "Sentinela/2.0",
            "Accept": "application/json",
        })

    # ── Configuração ──────────────────────────────────────────────────────────

    def get_config(self) -> ApiConfig:
        if self._cfg:
            return self._cfg
        env_v = os.getenv("TRANSPARENCIA_AC_VERSAO")
        env_c = os.getenv("TRANSPARENCIA_AC_CODIGO")
        env_p = os.getenv("TRANSPARENCIA_AC_PREFIX")
        env_b = os.getenv("TRANSPARENCIA_AC_BASE_URL")
        if env_v and env_c and env_p:
            self._cfg = ApiConfig(
                versao=env_v, codigo=int(env_c), api_prefix=env_p,
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
            raise RuntimeError(
                "API Transparência AC não encontrada. "
                "Defina TRANSPARENCIA_AC_VERSAO / TRANSPARENCIA_AC_CODIGO / "
                "TRANSPARENCIA_AC_PREFIX como variáveis de ambiente."
            )
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(
            json.dumps({
                "versao": cfg.versao,
                "codigo": cfg.codigo,
                "base_url": cfg.base_url,
                "api_prefix": cfg.api_prefix,
            })
        )
        self._cfg = cfg
        return cfg

    def _discover(self) -> Optional[ApiConfig]:
        log.info("Descobrindo config da API Transparência AC...")
        for p in API_PREFIX_CANDIDATES:
            for v in VERSAO_CANDIDATES:
                for c in [1285490, 383443] + CODIGO_CANDIDATES:
                    url = f"{BASE_URL}{p}/{v}/json/exercicios/{c}"
                    try:
                        r = self.session.get(url, timeout=5)
                        if r.status_code == 200 and isinstance(r.json(), list):
                            log.info("API encontrada: prefix=%s versao=%s codigo=%d", p, v, c)
                            return ApiConfig(versao=v, codigo=c, api_prefix=p)
                    except Exception:
                        pass
        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _safe_get(self, url: str, params: dict = None):
        try:
            r = self.session.get(url, params=params, timeout=30)
            if r.status_code < 500:
                return r
            return None
        except Exception as e:
            log.warning("GET %s falhou: %s", url, e)
            return None

    def _clean_doc(self, raw: str) -> str:
        return re.sub(r"\D", "", raw or "")

    def _parse_valor(self, raw) -> float:
        try:
            return float(str(raw).replace(".", "").replace(",", "."))
        except Exception:
            return 0.0

    def _extract_unidade(self, item: dict) -> tuple[str, str]:
        """
        Extrai (nome_unidade, codigo_unidade) do item da API.
        Campos tentados em ordem de preferência.
        """
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

    # ── Exercícios ────────────────────────────────────────────────────────────

    def get_exercicios(self) -> list:
        cfg = self.get_config()
        r = self._safe_get(cfg.url(f"exercicios/{cfg.codigo}"))
        return r.json() if r else []

    def _get_id_exercicio(self, ano: int) -> Optional[int]:
        for ex in self.get_exercicios():
            if str(ex.get("nome", "")) == str(ano):
                return ex["id"]
        return None

    # ── Pagamentos ────────────────────────────────────────────────────────────

    def get_pagamentos(self, ano: int) -> list[PagamentoRow]:
        cfg = self.get_config()
        id_exer = self._get_id_exercicio(ano)
        if not id_exer:
            log.warning("Exercício %d não encontrado", ano)
            return []

        inicio, fim = f"01/01/{ano}", f"31/12/{ano}"
        r = self._safe_get(
            cfg.url(f"pagamentos/{cfg.codigo}/count"),
            {"exer": id_exer, "inicio": inicio, "fim": fim},
        )
        if not r:
            return []
        try:
            total = int(r.json().get("totalDeRegistros", 0))
        except Exception:
            total = 0
        if total == 0:
            return []

        log.info("Buscando %d pagamentos do ano %d...", total, ano)
        rows: list[PagamentoRow] = []
        n_pages = (total // self.pagesize) + 2

        for page in range(1, n_pages):
            r = self._safe_get(
                cfg.url(f"pagamentos/{cfg.codigo}"),
                {
                    "exer": id_exer,
                    "inicio": inicio,
                    "fim": fim,
                    "page": page,
                    "pagesize": self.pagesize,
                },
            )
            if not r:
                continue
            items = r.json()
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

                rows.append(PagamentoRow(
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
                ))

            time.sleep(self.delay)

        log.info("Pagamentos %d: %d registros coletados", ano, len(rows))
        return rows

    # ── Contratos ─────────────────────────────────────────────────────────────

    def get_contratos(self, ano: int) -> list[ContratoRow]:
        cfg = self.get_config()
        id_exer = self._get_id_exercicio(ano)
        if not id_exer:
            return []

        r = self._safe_get(
            cfg.url(f"contratos/{cfg.codigo}/count"),
            {"exer": id_exer, "inicio": f"01/01/{ano}", "fim": f"31/12/{ano}"},
        )
        try:
            total = int(r.json().get("totalDeRegistros", 0)) if r else 0
        except Exception:
            total = 0
        if total == 0:
            return []

        rows: list[ContratoRow] = []
        for page in range(1, (total // self.pagesize) + 2):
            r = self._safe_get(
                cfg.url(f"contratos/{cfg.codigo}"),
                {"exer": id_exer, "page": page, "pagesize": self.pagesize},
            )
            if not r:
                continue
            items = r.json()
            if not items:
                break

            for item in items:
                nome_unidade, _ = self._extract_unidade(item)
                credor = item.get("credor", "").strip()
                orgao = resolve_orgao(
                    unidade_gestora=nome_unidade,
                    credor=credor,
                )
                rows.append(ContratoRow(
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
                ))
            time.sleep(self.delay)

        log.info("Contratos %d: %d registros", ano, len(rows))
        return rows

    # ── Licitações ────────────────────────────────────────────────────────────

    def get_licitacoes(self, ano: int) -> list[LicitacaoRow]:
        cfg = self.get_config()
        r = self._safe_get(
            cfg.url(f"licitacoes/count/{cfg.codigo}"),
            {"inicio": f"01/01/{ano}", "fim": f"31/12/{ano}"},
        )
        try:
            total = int(r.json().get("totalDeRegistros", 0)) if r else 0
        except Exception:
            total = 0
        if total == 0:
            return []

        rows: list[LicitacaoRow] = []
        for page in range(1, (total // self.pagesize) + 2):
            r = self._safe_get(
                cfg.url(f"licitacoes/{cfg.codigo}"),
                {"page": page, "pagesize": self.pagesize},
            )
            if not r:
                continue
            items = r.json()
            if not items:
                break

            for item in items:
                nome_unidade, _ = self._extract_unidade(item)
                fornecedores = [
                    {
                        "nome": f.get("nome", ""),
                        "cnpjcpf": self._clean_doc(f.get("cnpjcpf", "")),
                        "vencedor": f.get("vencedor", False),
                    }
                    for f in item.get("fornecedores", [])
                ]
                orgao = resolve_orgao(unidade_gestora=nome_unidade)

                rows.append(LicitacaoRow(
                    numero_processo=item.get("numprocecompra", ""),
                    modalidade=item.get("modalidade", ""),
                    objeto=item.get("objeto", ""),
                    valor_estimado=self._parse_valor(item.get("valestimado", "0")),
                    valor_real=self._parse_valor(item.get("vlreal", "0")),
                    situacao=item.get("situacao", ""),
                    data_abertura=item.get("datainicio", ""),
                    fornecedores=fornecedores,
                    orgao=orgao,
                    unidade_gestora=nome_unidade,
                ))
            time.sleep(self.delay)

        log.info("Licitações %d: %d registros", ano, len(rows))
        return rows

    # ── Run completo ──────────────────────────────────────────────────────────

    def run(self, anos: list[int] = None) -> dict:
        anos = anos or [2024, 2023]
        res: dict = {
            "pagamentos": [],
            "contratos": [],
            "licitacoes": [],
            "cnpjs_unicos": set(),
        }
        for ano in anos:
            res["pagamentos"].extend(self.get_pagamentos(ano))
            res["contratos"].extend(self.get_contratos(ano))
            res["licitacoes"].extend(self.get_licitacoes(ano))
        res["cnpjs_unicos"] = {
            p.cnpjcpf for p in res["pagamentos"] if len(p.cnpjcpf) == 14
        }
        return res
