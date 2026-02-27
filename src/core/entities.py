"""
haEmet — Modelos de domínio
"HaEmet" (האמת) = "A Verdade" em hebraico
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class Severidade(str, Enum):
    CRITICO = "CRITICO"
    ALTO = "ALTO"
    MEDIO = "MEDIO"
    BAIXO = "BAIXO"


class InsightTipo(str, Enum):
    VARIACAO_PATRIMONIAL = "VARIACAO_PATRIMONIAL"
    EMENDA_FAMILIA = "EMENDA_FAMILIA"
    DUPLO_VINCULO = "DUPLO_VINCULO"
    ESCOLA_FANTASMA = "ESCOLA_FANTASMA"
    CIRCUITO_DOACAO_SUS = "CIRCUITO_DOACAO_SUS"
    OFFSHORE = "OFFSHORE"


@dataclass
class Candidatura:
    ano: int
    cargo: str
    partido: str
    numero_urna: str
    situacao: str
    uf: str
    total_bens: float = 0.0


@dataclass
class PatrimonioSnapshot:
    ano: int
    total_declarado: float
    fonte_sha256: str = ""


@dataclass
class Pessoa:
    cpf: str                                      # 11 dígitos ou 'seq:...' ou 'parcial:...'
    nome_canonico: str                            # uppercase sem acento
    nome_urna: str = ""
    data_nascimento: str = ""                     # DD/MM/AAAA
    candidaturas: list[Candidatura] = field(default_factory=list)
    historico_patrimonio: list[PatrimonioSnapshot] = field(default_factory=list)
    cnpjs_como_socio: list[str] = field(default_factory=list)
    fonte: str = ""
    ingested_at: datetime = field(default_factory=datetime.utcnow)
    proveniencia_sha256: str = ""

    @property
    def variacao_patrimonial(self) -> float:
        if len(self.historico_patrimonio) < 2:
            return 0.0
        hist = sorted(self.historico_patrimonio, key=lambda x: x.ano)
        return hist[-1].total_declarado - hist[0].total_declarado

    @property
    def is_agente_publico(self) -> bool:
        return bool(self.candidaturas)


@dataclass
class Empresa:
    cnpj: str
    razao_social: str
    nome_fantasia: str = ""
    cnae_principal: str = ""
    situacao_cadastral: str = ""
    municipio: str = ""
    uf: str = ""
    socios: list[dict] = field(default_factory=list)   # [{cpf_cnpj, nome, qualificacao}]
    fonte: str = ""
    ingested_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Insight:
    id: str
    tipo: InsightTipo
    cpf_sujeito: str
    descricao: str
    score: float                                  # 0.0–1.0
    severidade: Severidade
    evidencias: dict
    detectado_em: datetime = field(default_factory=datetime.utcnow)
    versao_regra: str = "1.0"
