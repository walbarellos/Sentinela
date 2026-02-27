"""
haEmet — Detecção: EMENDA_FAMILIA e CONTRATO_SUSPEITO

Padrão EMENDA_FAMILIA:
  Político (CPF) → sócio de Empresa (CNPJ) → empresa recebeu pagamento do estado

Padrão CONTRATO_SUSPEITO:
  Empresa venceu licitação por DISPENSA ou INEXIGIBILIDADE
  E tem sócio que é parente/associado de político
  E valor > threshold

Requer:
  - pessoas (TSE) com candidaturas → CPFs dos políticos
  - empresas (QSA) com socios → quais CNPJs têm políticos como sócios
  - pagamentos (Transparência AC) → quais CNPJs receberam dinheiro
"""

import uuid
import logging
from src.core.entities import Insight, InsightTipo, Pessoa, Severidade
from src.ingest.transparencia_ac_connector import PagamentoRow, LicitacaoRow
from src.ingest.receita_qsa_connector import EmpresaQSA

log = logging.getLogger(__name__)

# Licitações por dispensa/inexigibilidade são red flags para direcionamento
MODALIDADES_SUSPEITAS = {
    "DISPENSA",
    "INEXIGIBILIDADE",
    "DISPENSA DE LICITAÇÃO",
    "INEXIGIBILIDADE DE LICITAÇÃO",
}

VALOR_MINIMO_ALERTA = 50_000   # R$50k — abaixo disso é ruído


def detectar_emenda_familia(
    pessoas: dict[str, Pessoa],
    empresas_qsa: list[EmpresaQSA],
    pagamentos: list[PagamentoRow],
) -> list[Insight]:
    """
    Detecta o padrão:
      político é sócio de empresa → empresa recebeu pagamento do estado

    Severidade depende do valor total recebido e se o político
    estava em exercício no período dos pagamentos.
    """
    insights = []

    # Indexa pagamentos por CNPJ → soma de valores
    pagamentos_por_cnpj: dict[str, float] = {}
    pagamentos_por_cnpj_raw: dict[str, list[PagamentoRow]] = {}
    for p in pagamentos:
        if not p.cnpjcpf or len(p.cnpjcpf) != 14:
            continue
        pagamentos_por_cnpj[p.cnpjcpf] = pagamentos_por_cnpj.get(p.cnpjcpf, 0) + p.valor
        pagamentos_por_cnpj_raw.setdefault(p.cnpjcpf, []).append(p)

    # Para cada empresa com sócio político
    for empresa in empresas_qsa:
        if not empresa.tem_socio_politico:
            continue

        valor_recebido = pagamentos_por_cnpj.get(empresa.cnpj, 0.0)
        if valor_recebido < VALOR_MINIMO_ALERTA:
            continue

        # Identifica qual sócio é político
        socios_politicos = []
        for socio in empresa.socios:
            if socio.cpf_cnpj in pessoas:
                pessoa = pessoas[socio.cpf_cnpj]
                socios_politicos.append({
                    "cpf": socio.cpf_cnpj,
                    "nome": pessoa.nome_canonico,
                    "qualificacao": socio.qualificacao,
                    "cargos": [c.cargo for c in pessoa.candidaturas[-3:]],
                })

        if not socios_politicos:
            continue

        score = _score_emenda_familia(valor_recebido)
        severidade = _severidade(score)

        # Pega os maiores pagamentos para evidência
        top_pagamentos = sorted(
            pagamentos_por_cnpj_raw.get(empresa.cnpj, []),
            key=lambda x: x.valor, reverse=True
        )[:5]

        for sp in socios_politicos:
            insight = Insight(
                id=str(uuid.uuid4()),
                tipo=InsightTipo.EMENDA_FAMILIA,
                cpf_sujeito=sp["cpf"],
                score=score,
                severidade=severidade,
                descricao=(
                    f"{sp['nome']} é {sp['qualificacao']} em "
                    f"{empresa.razao_social} (CNPJ {_fmt_cnpj(empresa.cnpj)}), "
                    f"que recebeu R$ {valor_recebido:,.2f} do erário estadual."
                ),
                evidencias={
                    "politico":          sp,
                    "empresa": {
                        "cnpj":            empresa.cnpj,
                        "razao_social":    empresa.razao_social,
                        "cnae":            empresa.cnae_descricao,
                        "situacao":        empresa.situacao_cadastral,
                        "uf":              empresa.uf,
                        "capital_social":  empresa.capital_social,
                    },
                    "valor_total_recebido": valor_recebido,
                    "top_pagamentos": [
                        {
                            "data":           p.data_movimento,
                            "valor":          p.valor,
                            "natureza":       p.natureza_despesa,
                            "modalidade":     p.modalidade_licitacao,
                            "num_empenho":    p.numero_empenho,
                        }
                        for p in top_pagamentos
                    ],
                },
                versao_regra="EMENDA_FAMILIA_v1.0",
            )
            insights.append(insight)
            log.info(
                "EMENDA_FAMILIA [%s]: %s → %s R$%.0f",
                severidade.value, sp["nome"], empresa.razao_social, valor_recebido
            )

    insights.sort(key=lambda i: i.score, reverse=True)
    log.info("EMENDA_FAMILIA: %d insights gerados", len(insights))
    return insights


def detectar_contrato_suspeito(
    pessoas: dict[str, Pessoa],
    empresas_qsa: list[EmpresaQSA],
    licitacoes: list[LicitacaoRow],
) -> list[Insight]:
    """
    Detecta contratos ganhos por dispensa/inexigibilidade por empresas
    com sócios políticos.
    """
    insights = []

    # Indexa empresas com sócios políticos por CNPJ
    empresas_politicas: dict[str, EmpresaQSA] = {
        e.cnpj: e for e in empresas_qsa if e.tem_socio_politico
    }

    for lic in licitacoes:
        modalidade = lic.modalidade.upper()
        if not any(m in modalidade for m in MODALIDADES_SUSPEITAS):
            continue
        if lic.valor_real < VALOR_MINIMO_ALERTA:
            continue

        # Verifica se algum vencedor tem sócio político
        for fornecedor in lic.fornecedores:
            cnpj = fornecedor.get("cnpjcpf", "")
            if cnpj not in empresas_politicas:
                continue
            if not fornecedor.get("vencedor", False):
                continue

            empresa = empresas_politicas[cnpj]
            socios_politicos = [
                s for s in empresa.socios if s.cpf_cnpj in pessoas
            ]
            if not socios_politicos:
                continue

            for sp_raw in socios_politicos:
                pessoa = pessoas[sp_raw.cpf_cnpj]
                score = min(1.0, 0.65 + (lic.valor_real / 5_000_000) * 0.3)

                insight = Insight(
                    id=str(uuid.uuid4()),
                    tipo=InsightTipo.EMENDA_FAMILIA,  # reutiliza tipo — refinar em v2
                    cpf_sujeito=sp_raw.cpf_cnpj,
                    score=round(score, 4),
                    severidade=_severidade(score),
                    descricao=(
                        f"Contrato por {lic.modalidade} "
                        f"(R$ {lic.valor_real:,.2f}) para {empresa.razao_social}, "
                        f"onde {pessoa.nome_canonico} é sócio. "
                        f"Objeto: {lic.objeto[:100]}"
                    ),
                    evidencias={
                        "politico": {
                            "cpf":  sp_raw.cpf_cnpj,
                            "nome": pessoa.nome_canonico,
                            "qualificacao": sp_raw.qualificacao,
                        },
                        "empresa": {
                            "cnpj":         empresa.cnpj,
                            "razao_social": empresa.razao_social,
                        },
                        "licitacao": {
                            "numero":         lic.numero_processo,
                            "modalidade":     lic.modalidade,
                            "objeto":         lic.objeto,
                            "valor_estimado": lic.valor_estimado,
                            "valor_real":     lic.valor_real,
                            "data_abertura":  lic.data_abertura,
                            "situacao":       lic.situacao,
                        },
                    },
                    versao_regra="CONTRATO_SUSPEITO_v1.0",
                )
                insights.append(insight)
                log.info(
                    "CONTRATO_SUSPEITO [%s]: %s → %s R$%.0f [%s]",
                    insight.severidade.value,
                    pessoa.nome_canonico,
                    empresa.razao_social,
                    lic.valor_real,
                    lic.modalidade,
                )

    insights.sort(key=lambda i: i.score, reverse=True)
    log.info("CONTRATO_SUSPEITO: %d insights gerados", len(insights))
    return insights


# ── Helpers ───────────────────────────────────────────────────────────────────

def _score_emenda_familia(valor_recebido: float) -> float:
    """Score crescente com o valor recebido. R$10M+ → score ~0.97."""
    import math
    if valor_recebido <= 0:
        return 0.0
    # log10(50k)=4.7, log10(10M)=7.0 → normaliza para [0.6, 0.97]
    log_v = math.log10(max(valor_recebido, 1))
    score = 0.5 + min(0.47, (log_v - 4.0) / 6.0)
    return round(max(0.5, score), 4)


def _severidade(score: float) -> Severidade:
    if score >= 0.85:
        return Severidade.CRITICO
    if score >= 0.70:
        return Severidade.ALTO
    return Severidade.MEDIO


def _fmt_cnpj(cnpj: str) -> str:
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return cnpj
