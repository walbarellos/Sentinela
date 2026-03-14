"""
Motor legado de insights exploratórios.

Este arquivo não compõe a trilha probatória principal do produto. Os
detectores daqui ficam desligados por padrão e só devem ser usados em
triagem interna explícita (`allow_internal=True`).
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import math
import pandas as pd

# --- REFERENCIAS NORMATIVAS PARA TRIAGEM INTERNA ---
LEGAL = {
    "fracionamento": "Lei 14.133/21 art. 75 — Referencia para diligencia sobre parcelamento indevido; exige enquadramento do objeto e do regime da contratacao.",
    "teto_constitucional": "CF art. 37, XI — Remuneração não pode exceder o subsídio dos Ministros do STF / Prefeito.",
    "nepotismo": "Súmula Vinculante n° 13 STF — Vedada nomeação de parentes até 3º grau da autoridade nomeante.",
    "empresa_jovem": "Lei 14.133/21 art. 67 — Exige comprovação de aptidão técnica compatível com o objeto.",
    "doacao_contrato": "Lei 12.846/13 art. 5° e Lei 9.840/99 art. 41-A — Conflito de interesse e captação ilícita de sufrágio.",
    "concentracao_mercado": "Lei 14.133/21 art. 5° — Referencia para triagem de competitividade; concentracao, por si so, nao comprova direcionamento ou fraude.",
    "viagem_bloco": "Lei 8.429/92 art. 10 e art. 11 — Referencia para triagem de economicidade; agrupamento de diarias nao comprova irregularidade sem evento, missao ou justificativa.",
    "empresa_suspensa": "Lei 14.133/21 art. 156 — Impedimento de licitar e contratar com a Administração.",
    "outlier_salarial": "CF art. 37, X e XI — Referencia para conferencia de base legal remuneratoria; anomalia estatistica nao comprova irregularidade sozinha.",
}

DEFAULT_ALLOW_INTERNAL = False
FRACIONAMENTO_THRESHOLD_BENS_SERVICOS = 50_000.0

@dataclass
class Insight:
    id: str
    tipo: str                
    severidade: str          
    confianca: int           
    exposicao: float         
    titulo: str
    descricao: str
    pattern: str
    fontes: List[str]
    evidencias: List[Dict[str, Any]]  
    n_amostra: int = 0
    total_unidade: float = 0.0
    base_legal: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def _fmt_brl(v: float) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return "R$0"
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R${s}"

def _clamp(n: float, a: float, b: float) -> float:
    return max(a, min(b, n))

def _safe_num(x: Any) -> float:
    try:
        if x is None: return 0.0
        if isinstance(x, str):
            t = x.strip().replace(".", "").replace(",", ".")
            return float(t)
        return float(x)
    except: return 0.0

def _pick_cols(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
    return None

def generate_insights_for_obras(
    obras: pd.DataFrame,
    *,
    allow_internal: bool = DEFAULT_ALLOW_INTERNAL,
    min_exposicao: float = 200_000.0,
    conc_threshold: float = 0.35,
    min_n_secretaria: int = 8,
    max_evidencias: int = 25,
    fracionamento_threshold: float = FRACIONAMENTO_THRESHOLD_BENS_SERVICOS,
) -> List[Insight]:
    if not allow_internal:
        return []
    if obras is None or len(obras) == 0: return []
    val_col = _pick_cols(obras, ["valor_total", "valor"])
    sec_col = _pick_cols(obras, ["secretaria", "unidade"])
    emp_col = _pick_cols(obras, ["empresa_nome", "empresa"])
    
    if not val_col: return []
    df = obras.copy()
    df[val_col] = df[val_col].apply(_safe_num)
    insights: List[Insight] = []
    
    # 1. Triagem interna de concentração de mercado
    if sec_col and emp_col:
        g = df.groupby([sec_col, emp_col], dropna=False)[val_col].sum().reset_index()
        total_sec = df.groupby(sec_col, dropna=False)[val_col].sum().reset_index().rename(columns={val_col: "total_sec"})
        m = g.merge(total_sec, on=sec_col, how="left")
        m["share"] = m[val_col] / m["total_sec"].replace({0: 1})
        top = m.sort_values(["share"], ascending=False).groupby(sec_col, dropna=False).head(1)
        for _, row in top.iterrows():
            share = float(row["share"])
            exposicao = float(row[val_col])
            if exposicao < min_exposicao or share < conc_threshold: continue
            secretaria = str(row[sec_col])
            empresa = str(row[emp_col])
            
            # Filtro por amostragem mínima na secretaria
            total_obras_sec = len(df[df[sec_col] == secretaria])
            if total_obras_sec < min_n_secretaria: continue

            evid = df[(df[sec_col] == secretaria) & (df[emp_col] == empresa)].copy()
            
            conf = int(_clamp(50 + (share * 40), 0, 99))
            insights.append(Insight(
                id=f"OB_CONC_{hash(secretaria+empresa) & 0xffffffff}",
                tipo="TRIAGEM INTERNA - CONCENTRACAO DE MERCADO",
                severidade="MEDIO" if share <= 0.5 else "ALTO",
                confianca=conf, exposicao=exposicao, 
                titulo=f"Triagem interna: {empresa}",
                descricao=(
                    f"Na secretaria **{secretaria}**, a empresa concentra **{int(share*100)}%** do volume monitorado. "
                    "Esse sinal e exploratorio e exige analise do mercado relevante, do objeto e da base competitiva antes de qualquer conclusao."
                ),
                pattern="SECRETARIA → FORNECEDOR DOMINANTE",
                fontes=["Portal Transparência (Obras)"],
                evidencias=evid.head(max_evidencias).to_dict('records'),
                base_legal=LEGAL["concentracao_mercado"]
            ))
            
    # 2. Triagem interna de fracionamento
    try:
        frac = df[df[val_col] < fracionamento_threshold].groupby([emp_col, sec_col]).agg({
            val_col: ['count', 'sum'],
            'id': 'list'
        }).reset_index()
        frac.columns = [emp_col, sec_col, 'qtd', 'total', 'ids']
        
        for _, row in frac[frac['qtd'] >= 3].iterrows():
            if row['total'] > fracionamento_threshold:
                insights.append(Insight(
                    id=f"OB_FRAC_{hash(str(row[emp_col])+str(row[sec_col])) & 0xffffffff}",
                    tipo="TRIAGEM INTERNA - FRACIONAMENTO",
                    severidade="ALTO",
                    confianca=85, exposicao=row['total'],
                    titulo=f"Triagem interna: {row[emp_col]}",
                    descricao=(
                        f"Detectados {row['qtd']} contratos sucessivos na {row[sec_col]} com valores somados "
                        f"({_fmt_brl(float(row['total']))}) acima do threshold exploratorio de {_fmt_brl(fracionamento_threshold)}. "
                        "O caso exige revisar o enquadramento do objeto, a natureza da despesa e o regime juridico aplicavel."
                    ),
                    pattern="CONTRATOS SUCESSIVOS → DISPENSA → LIMITE EXCEDIDO",
                    fontes=["Portal Transparência"],
                    evidencias=[{'obra_id': id} for id in row['ids'][:10]],
                    base_legal=LEGAL["fracionamento"]
                ))
    except: pass
            
    return sorted(insights, key=lambda x: x.confianca, reverse=True)

def generate_insights_for_servidores(
    df: pd.DataFrame,
    *,
    allow_internal: bool = DEFAULT_ALLOW_INTERNAL,
    min_group_n: int = 30,
    z_threshold: float = 3.0,
) -> List[Insight]:
    if not allow_internal:
        return []
    if df is None or df.empty: return []
    group_cols = ['cargo', 'ch', 'vinculo']
    target_col = 'salario_liquido'
    if not all(col in df.columns for col in group_cols + [target_col, 'servidor']): return []
        
    df_stats = df.copy()
    counts = df_stats.groupby(group_cols)[target_col].transform('count')
    df_stats = df_stats[counts >= min_group_n]
    if df_stats.empty: return []
    
    def compute_z(group):
        if len(group) < 2 or group.std() == 0 or pd.isna(group.std()): 
            return pd.Series(0.0, index=group.index)
        return (group - group.mean()) / group.std()
        
    df_stats['z_score'] = df_stats.groupby(group_cols)[target_col].transform(compute_z)
    outliers = df_stats[df_stats['z_score'] > z_threshold].sort_values('z_score', ascending=False)
    
    insights = []
    for idx, row in outliers.head(50).iterrows():
        z = float(row['z_score'])
        val = float(row[target_col])
        conf = int(_clamp(45 + (z * 8), 0, 99))
        ir = float(row.get('imposto_de_renda', 0))
        ir_flag = f" | ⚠️ IR descontado: {_fmt_brl(ir)}" if ir > 0 else ""
        serv_str = str(row['servidor'])
        matricula, nome = serv_str.split('-', 1) if '-' in serv_str else ("N/A", serv_str)

        insights.append(Insight(
            id=f"SAL_{matricula}_{idx}", tipo="TRIAGEM INTERNA - OUTLIER ESTATISTICO",
            severidade="ALTO" if z > 4.5 else "MEDIO",
            confianca=conf, exposicao=val, titulo=f"Indício: {nome}",
            descricao=(
                f"Registro aponta valor líquido de **{_fmt_brl(val)}**, equivalente a **{z:.1f}x** o desvio padrão "
                f"da média do grupo **{row['cargo']}** ({row['ch']}h, {row['vinculo']}).{ir_flag} "
                "Esse sinal é apenas estatístico e exige conferência de base legal, rubricas e histórico funcional."
            ),
            pattern="CARGO+CH+VINCULO → Z-SCORE → ANOMALIA",
            fontes=["Portal da Transparência (Rio Branco)"], evidencias=[row.to_dict()],
            base_legal=LEGAL["outlier_salarial"]
        ))
    return insights

def generate_insights_for_diarias(
    df: pd.DataFrame,
    *,
    allow_internal: bool = DEFAULT_ALLOW_INTERNAL,
    min_group_n: int = 4,
) -> List[Insight]:
    if not allow_internal:
        return []
    if df is None or df.empty: return []
    group_cols = ['destino', 'data_saida', 'data_retorno', 'motivo']
    if not all(col in df.columns for col in group_cols): return []
        
    agrupado = df.groupby(group_cols).agg({'servidor_nome': 'count', 'valor': 'sum'}).reset_index()
    agrupado.columns = group_cols + ['qtd_servidores', 'total_gasto']
    blocos = agrupado[agrupado['qtd_servidores'] >= min_group_n].sort_values('total_gasto', ascending=False)
    
    insights = []
    for idx, row in blocos.head(20).iterrows():
        n = int(row['qtd_servidores'])
        val = float(row['total_gasto'])
        conf = int(_clamp(50 + (n * 5) + (val / 5000), 0, 99))
        insights.append(Insight(
            id=f"DIA_AGRUP_{idx}", tipo="TRIAGEM INTERNA - AGRUPAMENTO DE DIARIAS",
            severidade="ALTO" if n >= 8 or val > 20000 else "MEDIO",
            confianca=conf, exposicao=val, titulo=f"Triagem interna: {n} registros → {row['destino']}",
            descricao=(
                f"Identificado agrupamento de **{n} diárias** para **{row['destino']}** com saída em "
                f"**{row['data_saida'].strftime('%d/%m/%Y')}**. Custo agregado: **{_fmt_brl(val)}**. "
                "O achado exige confirmação de evento, missão oficial, escala e justificativa administrativa."
            ),
            pattern="DESTINO+DATA → AGRUPAMENTO DE REGISTROS",
            fontes=["Portal da Transparência (Diárias)"],
            evidencias=df[(df['destino'] == row['destino']) & (df['data_saida'] == row['data_saida'])].head(10).to_dict('records'),
            base_legal=LEGAL["viagem_bloco"]
        ))
    return insights
