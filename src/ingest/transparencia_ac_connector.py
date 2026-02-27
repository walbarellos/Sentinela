"""
haEmet — Conector Portal de Transparência do Acre
"""
import json
import logging
import time
import os
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

class TransparenciaAcConnector:
    def __init__(self, data_dir: str = "./data/transparencia_ac", force: bool = False, pagesize: int = 500, delay_entre_requests: float = 0.5):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.force = force
        self.pagesize = pagesize
        self.delay = delay_entre_requests
        self._cfg: Optional[ApiConfig] = None

        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        self.session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self.session.headers.update({"User-Agent": "Sentinela/1.0", "Accept": "application/json"})

    def get_config(self) -> ApiConfig:
        if self._cfg: return self._cfg
        env_v, env_c, env_p, env_b = os.getenv("TRANSPARENCIA_AC_VERSAO"), os.getenv("TRANSPARENCIA_AC_CODIGO"), os.getenv("TRANSPARENCIA_AC_PREFIX"), os.getenv("TRANSPARENCIA_AC_BASE_URL")
        if env_v and env_c and env_p:
            self._cfg = ApiConfig(versao=env_v, codigo=int(env_c), api_prefix=env_p, base_url=env_b or BASE_URL)
            return self._cfg
        if CACHE_FILE.exists() and not self.force:
            try:
                self._cfg = ApiConfig(**json.loads(CACHE_FILE.read_text()))
                return self._cfg
            except: pass
        cfg = self._discover()
        if not cfg: raise RuntimeError("API não encontrada")
        CACHE_FILE.write_text(json.dumps({"versao": cfg.versao, "codigo": cfg.codigo, "base_url": cfg.base_url, "api_prefix": cfg.api_prefix}))
        self._cfg = cfg
        return cfg

    def _discover(self) -> Optional[ApiConfig]:
        for p in API_PREFIX_CANDIDATES:
            for v in VERSAO_CANDIDATES:
                for c in [1285490, 383443] + CODIGO_CANDIDATES:
                    url = f"{BASE_URL}{p}/{v}/json/exercicios/{c}"
                    try:
                        r = self.session.get(url, timeout=5)
                        if r.status_code == 200 and isinstance(r.json(), list):
                            return ApiConfig(versao=v, codigo=c, api_prefix=p)
                    except: pass
        return None

    def _safe_get(self, url, params=None):
        try:
            r = self.session.get(url, params=params, timeout=30)
            return r if r.status_code < 500 else None
        except: return None

    def get_exercicios(self):
        cfg = self.get_config()
        r = self._safe_get(cfg.url(f"exercicios/{cfg.codigo}"))
        return r.json() if r else []

    def _get_id_exercicio(self, ano):
        for ex in self.get_exercicios():
            if str(ex.get("nome", "")) == str(ano): return ex["id"]
        return None

    def get_pagamentos(self, ano):
        cfg = self.get_config()
        id_exer = self._get_id_exercicio(ano)
        if not id_exer: return []
        inicio, fim = f"01/01/{ano}", f"31/12/{ano}"
        r = self._safe_get(cfg.url(f"pagamentos/{cfg.codigo}/count"), {"exer": id_exer, "inicio": inicio, "fim": fim})
        if not r: return []
        try: total = int(r.json().get("totalDeRegistros", 0))
        except: total = 0
        if total == 0: return []
        rows = []
        for page in range(1, (total // self.pagesize) + 2):
            r = self._safe_get(cfg.url(f"pagamentos/{cfg.codigo}"), {"exer": id_exer, "inicio": inicio, "fim": fim, "page": page, "pagesize": self.pagesize})
            if r:
                for item in r.json():
                    rows.append(PagamentoRow(data_movimento=item.get("dataMovimento", ""), numero_empenho=item.get("numeroEmpenho", ""), credor=item.get("credor", "").strip(), cnpjcpf=self._clean_doc(item.get("cnpjcpf", "")), natureza_despesa=item.get("naturezaDaDespesa", ""), modalidade_licitacao=item.get("modalidadeLicitacao", ""), valor=self._parse_valor(item.get("valor", "0")), id_empenho=int(item.get("idEmpenho", 0))))
        return rows

    def get_contratos(self, ano):
        cfg = self.get_config()
        id_exer = self._get_id_exercicio(ano)
        if not id_exer: return []
        r = self._safe_get(cfg.url(f"contratos/{cfg.codigo}/count"), {"exer": id_exer, "inicio": f"01/01/{ano}", "fim": f"31/12/{ano}"})
        try: total = int(r.json().get("totalDeRegistros", 0)) if r else 0
        except: total = 0
        if total == 0: return []
        rows = []
        for page in range(1, (total // self.pagesize) + 2):
            r = self._safe_get(cfg.url(f"contratos/{cfg.codigo}"), {"exer": id_exer, "page": page, "pagesize": self.pagesize})
            if r:
                for item in r.json():
                    rows.append(ContratoRow(numero=item.get("numContr", ""), tipo=item.get("descTipoContr", ""), data_inicio_vigencia=item.get("dataInicVigen", ""), data_fim_vigencia=item.get("dataFinaVegen", ""), valor=self._parse_valor(item.get("valor", "0"))))
        return rows

    def get_licitacoes(self, ano):
        cfg = self.get_config()
        r = self._safe_get(cfg.url(f"licitacoes/count/{cfg.codigo}"), {"inicio": f"01/01/{ano}", "fim": f"31/12/{ano}"})
        try: total = int(r.json().get("totalDeRegistros", 0)) if r else 0
        except: total = 0
        if total == 0: return []
        rows = []
        for page in range(1, (total // self.pagesize) + 2):
            r = self._safe_get(cfg.url(f"licitacoes/{cfg.codigo}"), {"page": page, "pagesize": self.pagesize})
            if r:
                for item in r.json():
                    fornecedores = [{"nome": f.get("nome", ""), "cnpjcpf": self._clean_doc(f.get("cnpjcpf", "")), "vencedor": f.get("vencedor", False)} for f in item.get("fornecedores", [])]
                    rows.append(LicitacaoRow(numero_processo=item.get("numprocecompra", ""), modalidade=item.get("modalidade", ""), objeto=item.get("objeto", ""), valor_estimado=self._parse_valor(item.get("valestimado", "0")), valor_real=self._parse_valor(item.get("vlreal", "0")), situacao=item.get("situacao", ""), data_abertura=item.get("datainicio", ""), fornecedores=fornecedores))
        return rows

    def run(self, anos=None):
        anos = anos or [2024, 2023]
        res = {"pagamentos": [], "contratos": [], "licitacoes": [], "cnpjs_unicos": set()}
        for ano in anos:
            res["pagamentos"].extend(self.get_pagamentos(ano))
            res["contratos"].extend(self.get_contratos(ano))
            res["licitacoes"].extend(self.get_licitacoes(ano))
        res["cnpjs_unicos"] = {p.cnpjcpf for p in res["pagamentos"] if len(p.cnpjcpf) == 14}
        return res

    def _clean_doc(self, raw):
        import re
        return re.sub(r"\D", "", raw or "")

    def _parse_valor(self, raw):
        try: return float(str(raw).replace(".", "").replace(",", "."))
        except: return 0.0
