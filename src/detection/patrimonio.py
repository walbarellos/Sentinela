"""
haEmet — Detecção: Variação Patrimonial Incompatível

Padrão mais poderoso e simples de implementar:
compara patrimônio declarado ao TSE com renda acumulada estimada.
"""
import uuid
import logging
from src.core.entities import Insight, InsightTipo, Pessoa, Severidade

log = logging.getLogger(__name__)

# Salários brutos anuais estimados por cargo (conservador)
RENDA_ANUAL_POR_CARGO: dict[str, float] = {
    "SENADOR":              936_000,   # R$46.3k bruto/mês × 8 anos = R$4.4M
    "GOVERNADOR":           780_000,
    "DEPUTADO FEDERAL":     528_000,   # R$44k/mês
    "DEPUTADO ESTADUAL":    312_000,   # Acre: ~R$26k/mês
    "PREFEITO":             156_000,   # média municipal AC
    "VEREADOR":              96_000,
    "DEFAULT":              120_000,
}

THRESHOLD_MINIMO_GAP = 500_000       # R$500k de gap mínimo para gerar alerta
THRESHOLD_MULTIPLO   = 1.5           # gap deve ser > 1.5× a renda acumulada


def detectar_variacao_patrimonial(pessoas: dict[str, Pessoa]) -> list[Insight]:
    """
    Para cada pessoa com >= 2 snapshots patrimoniais:
    1. Calcula variação total
    2. Estima renda acumulada pelo cargo e período
    3. Calcula gap = variação - renda
    4. Se gap > threshold → gera Insight com score e evidências
    """
    insights = []

    for cpf, pessoa in pessoas.items():
        hist = sorted(pessoa.historico_patrimonio, key=lambda x: x.ano)
        if len(hist) < 2:
            continue

        variacao = hist[-1].total_declarado - hist[0].total_declarado
        if variacao <= 0:
            continue

        anos_periodo = max(1, hist[-1].ano - hist[0].ano)

        # Determina cargo mais recente para estimar renda
        cargo = "DEFAULT"
        if pessoa.candidaturas:
            ultimo = sorted(pessoa.candidaturas, key=lambda c: c.ano)[-1]
            cargo_upper = ultimo.cargo.upper()
            for key in RENDA_ANUAL_POR_CARGO:
                if key in cargo_upper:
                    cargo = key
                    break

        renda_anual = RENDA_ANUAL_POR_CARGO[cargo]
        renda_acumulada = renda_anual * anos_periodo

        gap = variacao - renda_acumulada

        if gap < THRESHOLD_MINIMO_GAP or gap < renda_acumulada * THRESHOLD_MULTIPLO:
            continue

        score = _calcular_score(gap, renda_acumulada)
        severidade = _severidade(score)

        insight = Insight(
            id=str(uuid.uuid4()),
            tipo=InsightTipo.VARIACAO_PATRIMONIAL,
            cpf_sujeito=cpf,
            score=score,
            severidade=severidade,
            descricao=(
                f"Variação patrimonial de R$ {variacao:,.0f} "
                f"com renda estimada de R$ {renda_acumulada:,.0f} "
                f"no período {hist[0].ano}–{hist[-1].ano}. "
                f"Gap sem origem identificada: R$ {gap:,.0f}."
            ),
            evidencias={
                "nome":                pessoa.nome_canonico,
                "patrimonio_inicial":  {"ano": hist[0].ano,  "valor": hist[0].total_declarado},
                "patrimonio_final":    {"ano": hist[-1].ano, "valor": hist[-1].total_declarado},
                "variacao_total":      variacao,
                "renda_acumulada":     renda_acumulada,
                "gap":                 gap,
                "cargo_referencia":    cargo,
                "anos_periodo":        anos_periodo,
                "snapshots":           [{"ano": s.ano, "valor": s.total_declarado} for s in hist],
            },
            versao_regra="VARIACAO_PATRIMONIAL_v1.0",
        )
        insights.append(insight)

    insights.sort(key=lambda i: i.score, reverse=True)
    log.info("Variação patrimonial: %d insights gerados de %d pessoas", len(insights), len(pessoas))
    return insights


def _calcular_score(gap: float, renda_acumulada: float) -> float:
    # ratio = quantas vezes o gap supera a renda
    # ratio 1x → score ~0.67  | ratio 2x → score ~0.75 | ratio 5x → score ~0.89
    ratio = gap / (renda_acumulada + 1.0)
    score = 0.5 + (ratio / (ratio + 2.0)) * 0.5
    return round(min(1.0, score), 4)


def _severidade(score: float) -> Severidade:
    if score >= 0.90:
        return Severidade.CRITICO
    if score >= 0.75:
        return Severidade.ALTO
    return Severidade.MEDIO
