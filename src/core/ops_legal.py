from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LegalAnchor:
    anchor_id: str
    label: str
    scope: str
    url: str
    note: str


LEGAL_ANCHORS: dict[str, LegalAnchor] = {
    "CF88_ART5_XXXIII": LegalAnchor(
        anchor_id="CF88_ART5_XXXIII",
        label="Constituicao Federal - art. 5o, XXXIII",
        scope="acesso a informacao publica",
        url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        note="Base para exigibilidade de documentos e respostas oficiais.",
    ),
    "CF88_ART37_CAPUT": LegalAnchor(
        anchor_id="CF88_ART37_CAPUT",
        label="Constituicao Federal - art. 37, caput",
        scope="legalidade, impessoalidade, moralidade, publicidade e eficiencia",
        url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        note="Ancora geral de integridade administrativa; nao autoriza conclusao automatica de ilicitude.",
    ),
    "CF88_ART37_XVI": LegalAnchor(
        anchor_id="CF88_ART37_XVI",
        label="Constituicao Federal - art. 37, XVI",
        scope="acumulacao remunerada de cargos",
        url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        note="Usado apenas como referencia de apuracao funcional; exige leitura humana de compatibilidade.",
    ),
    "CF88_ART37_XXI": LegalAnchor(
        anchor_id="CF88_ART37_XXI",
        label="Constituicao Federal - art. 37, XXI",
        scope="licitacao publica",
        url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        note="Base constitucional do dever de contratar conforme processo licitatorio e condicoes publicadas.",
    ),
    "LAI_12527_2011": LegalAnchor(
        anchor_id="LAI_12527_2011",
        label="Lei 12.527/2011 - Lei de Acesso a Informacao",
        scope="transparencia ativa e passiva",
        url="https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm",
        note="Usada para caixa de respostas, diligencias documentais e cobranca de processo integral.",
    ),
    "CNMP_RES174_2017": LegalAnchor(
        anchor_id="CNMP_RES174_2017",
        label="Resolucao CNMP 174/2017 - noticia de fato e procedimento administrativo",
        scope="tratamento ministerial de noticia de fato",
        url="https://www.cnmp.mp.br/portal/todas-as-noticias/10513-resolucao-disciplina-a-instauracao-e-tramitacao-da-noticia-de-fato-e-do-procedimento-administrativo",
        note="Ancora para saida em formato de noticia de fato ou apuracao preliminar, sem acusacao automatica.",
    ),
    "PF_IN255_ART9_2023": LegalAnchor(
        anchor_id="PF_IN255_ART9_2023",
        label="IN PF 255/2023 - art. 9o",
        scope="analise de noticias de fato pela Policia Federal",
        url="https://www.gov.br/mj/pt-br/acesso-a-informacao/acoes-e-programas/recupera/instrucao_normativa___in_34963175_in_255_2023___regulamenta_as_atividades_de_policia_judiciaria_da_pf.pdf",
        note="Exige plausibilidade, tipicidade, atribuicao e justa causa; usado como limite de triagem, nao como fundamento penal.",
    ),
    "L14133_PLANEJAMENTO": LegalAnchor(
        anchor_id="L14133_PLANEJAMENTO",
        label="Lei 14.133/2021 - planejamento, edital e vinculacao ao instrumento convocatorio",
        scope="compras publicas",
        url="https://www.planalto.gov.br/ccivil_03/_Ato2019-2022/2021/Lei/L14133.htm",
        note="Base para comparar contrato, edital, proposta e fiscalizacao.",
    ),
    "L14133_FISCALIZACAO": LegalAnchor(
        anchor_id="L14133_FISCALIZACAO",
        label="Lei 14.133/2021 - fiscalizacao, execucao e gestao contratual",
        scope="execucao contratual",
        url="https://www.planalto.gov.br/ccivil_03/_Ato2019-2022/2021/Lei/L14133.htm",
        note="Base para exigir atesto, glosa, fiscalizacao e processo integral de execucao.",
    ),
    "L12846_2013": LegalAnchor(
        anchor_id="L12846_2013",
        label="Lei 12.846/2013 - Lei Anticorrupcao",
        scope="responsabilizacao administrativa e civil de pessoas juridicas",
        url="https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2013/lei/l12846.htm",
        note="Usada apenas como contexto normativo de integridade; nao gera imputacao automatica.",
    ),
    "D11129_2022": LegalAnchor(
        anchor_id="D11129_2022",
        label="Decreto 11.129/2022 - regulamentacao da Lei 12.846/2013",
        scope="integridade e parametros sancionatorios",
        url="https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2022/decreto/d11129.htm",
        note="Ancora complementar para cruzamentos sancionatorios e due diligence documental.",
    ),
}


def get_legal_anchor(anchor_id: str) -> LegalAnchor | None:
    return LEGAL_ANCHORS.get(anchor_id)


def legal_anchor_payload(anchor_ids: list[str]) -> list[dict[str, str]]:
    payload: list[dict[str, str]] = []
    for anchor_id in anchor_ids:
        anchor = get_legal_anchor(anchor_id)
        if not anchor:
            continue
        payload.append(
            {
                "anchor_id": anchor.anchor_id,
                "label": anchor.label,
                "scope": anchor.scope,
                "url": anchor.url,
                "note": anchor.note,
            }
        )
    return payload
