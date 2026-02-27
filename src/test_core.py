import pytest
from src.core.normalizer import normalize_cpf, normalize_name, normalize_currency
from src.core.entities import Candidatura, PatrimonioSnapshot, Pessoa
from src.detection.patrimonio import detectar_variacao_patrimonial


# ── CPF ───────────────────────────────────────────────────────────────────────

def test_cpf_valido():
    assert normalize_cpf("529.982.247-25") == "52998224725"

def test_cpf_dv_errado():
    assert normalize_cpf("529.982.247-26") is None

def test_cpf_mascarado_tse():
    assert normalize_cpf("***982247**") == "parcial:982247"

def test_cpf_trivial():
    assert normalize_cpf("111.111.111-11") is None
    assert normalize_cpf("00000000000") is None

def test_cpf_com_zero():
    assert normalize_cpf("048.033.990-60") == "04803399060"


# ── Nome ─────────────────────────────────────────────────────────────────────

def test_nome_uppercase_sem_acento():
    assert normalize_name("João da Silva") == "JOAO DA SILVA"

def test_nome_acento_cedilha():
    assert normalize_name("Ângela Conceição") == "ANGELA CONCEICAO"

def test_nome_squash_espacos():
    assert normalize_name("  JOSÉ   MARIA  ") == "JOSE MARIA"


# ── Moeda ─────────────────────────────────────────────────────────────────────

def test_currency_brasileiro():
    assert normalize_currency("1.234.567,89") == pytest.approx(1_234_567.89)

def test_currency_nulo():
    assert normalize_currency("#NULO#") == 0.0
    assert normalize_currency("") == 0.0


# ── Variação Patrimonial ──────────────────────────────────────────────────────

def _make_pessoa(cpf, nome, snapshots, cargo="DEPUTADO ESTADUAL"):
    p = Pessoa(cpf=cpf, nome_canonico=nome)
    p.historico_patrimonio = [PatrimonioSnapshot(ano=a, total_declarado=v) for a, v in snapshots]
    p.candidaturas = [Candidatura(2014, cargo, "PXX", "1", "ELEITO", "AC")]
    return p


def test_insight_critico_gerado():
    pessoas = {
        "52998224725": _make_pessoa(
            "52998224725", "DEPUTADO FICTICIO",
            [(2014, 800_000), (2018, 2_800_000), (2022, 8_000_000)],
        )
    }
    insights = detectar_variacao_patrimonial(pessoas)
    assert len(insights) == 1
    assert insights[0].severidade.value == "CRITICO"
    assert insights[0].score >= 0.80


def test_sem_insight_quando_normal():
    pessoas = {
        "04803399060": _make_pessoa(
            "04803399060", "VEREADOR HONESTO",
            [(2016, 120_000), (2020, 350_000)],
            cargo="VEREADOR",
        )
    }
    insights = detectar_variacao_patrimonial(pessoas)
    assert len(insights) == 0


def test_sem_insight_com_apenas_um_snapshot():
    pessoas = {
        "52998224725": _make_pessoa("52998224725", "X", [(2022, 5_000_000)])
    }
    assert detectar_variacao_patrimonial(pessoas) == []


def test_insights_ordenados_por_score():
    p1 = _make_pessoa("52998224725", "RICO", [(2014, 500_000), (2022, 15_000_000)])
    p2 = _make_pessoa("04803399060", "MEDIO", [(2014, 200_000), (2022, 2_000_000)])
    pessoas = {p1.cpf: p1, p2.cpf: p2}
    insights = detectar_variacao_patrimonial(pessoas)
    if len(insights) >= 2:
        assert insights[0].score >= insights[1].score


def test_evidencias_completas():
    p = _make_pessoa("52998224725", "TEST", [(2014, 800_000), (2022, 8_000_000)])
    insights = detectar_variacao_patrimonial({"52998224725": p})
    assert insights
    ev = insights[0].evidencias
    assert "patrimonio_inicial" in ev
    assert "patrimonio_final" in ev
    assert "gap" in ev
    assert "nome" in ev
