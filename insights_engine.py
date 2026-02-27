# insights_engine.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import math
import pandas as pd

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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def _fmt_brl(v: float) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return "R$0"
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R${s}"

def _severity_from(conf: int, exposicao: float, min_exposicao: float) -> str:
    if exposicao < min_exposicao: return "BAIXO"
    if conf >= 85: return "CRITICO"
    if conf >= 70: return "ALTO"
    if conf >= 45: return "MEDIO"
    return "BAIXO"

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
    min_exposicao: float = 200_000.0,
    conc_threshold: float = 0.35,
    z_threshold: float = 3.0,
    min_n_secretaria: int = 5,
    max_evidencias: int = 25,
) -> List[Insight]:
    if obras is None or len(obras) == 0: return []
    val_col = _pick_cols(obras, ["valor_total", "valor", "pago"])
    sec_col = _pick_cols(obras, ["secretaria", "unidade"])
    emp_col = _pick_cols(obras, ["empresa_nome", "empresa", "fornecedor"])
    id_col = _pick_cols(obras, ["id", "obra_id"])
    nome_col = _pick_cols(obras, ["nome", "objeto"])
    if not val_col: return []
    df = obras.copy()
    df[val_col] = df[val_col].apply(_safe_num)
    insights: List[Insight] = []
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
            evid = df[(df[sec_col] == secretaria) & (df[emp_col] == empresa)].copy()
            total_obras_sec = len(df[df[sec_col] == secretaria])
            if total_obras_sec < min_n_secretaria: continue
            base_conf = 50 + (share * 40) + math.log10(max(exposicao, 1)) * 2
            conf = int(_clamp(base_conf, 0, 99))
            insights.append(Insight(
                id=f"OB_CONC_{hash(secretaria+empresa) & 0xffffffff}",
                tipo="CONCENTRACAO", severidade=_severity_from(conf, exposicao, min_exposicao),
                confianca=conf, exposicao=exposicao, titulo=f"Concentração: {empresa}",
                descricao=f"Na secretaria **{secretaria}**, a empresa **{empresa}** detém **{int(share*100)}%** do volume monitorado.",
                pattern="SECRETARIA → TOP FORNECEDOR → CONCENTRAÇÃO",
                fontes=["Portal Transparência (Obras)"],
                evidencias=evid.head(max_evidencias).to_dict('records'),
                n_amostra=total_obras_sec, total_unidade=float(row["total_sec"])
            ))
    return sorted(insights, key=lambda x: (x.severidade == "CRITICO", x.confianca), reverse=True)

def generate_insights_for_servidores(df: pd.DataFrame) -> List[Insight]:
    if df is None or df.empty: return []
    
    # Mapeamento para as colunas normalizadas do CSV de Rio Branco
    group_cols = ['cargo', 'ch', 'vinculo']
    target_col = 'salario_liquido'
    
    # Verifica se as colunas necessárias existem
    for col in group_cols + [target_col, 'servidor']:
        if col not in df.columns: return []
        
    df_stats = df.copy()
    
    # Filtra grupos com amostra estatística mínima
    counts = df_stats.groupby(group_cols)[target_col].transform('count')
    df_stats = df_stats[counts >= 10]
    
    if df_stats.empty: return []
    
    def compute_z(group):
        if len(group) < 2 or group.std() == 0 or pd.isna(group.std()): 
            return pd.Series(0.0, index=group.index)
        return (group - group.mean()) / group.std()
        
    df_stats['z_score'] = df_stats.groupby(group_cols)[target_col].transform(compute_z)
    
    # Outliers acima de 2.5 desvios padrão
    outliers = df_stats[df_stats['z_score'] > 2.5].sort_values('z_score', ascending=False)
    
    insights = []
    for idx, row in outliers.head(50).iterrows():
        z = float(row['z_score'])
        val = float(row[target_col])
        conf = int(_clamp(45 + (z * 8), 0, 99))
        
        # Análise de Imposto de Renda (se disponível)
        ir = float(row.get('imposto_de_renda', 0))
        ir_flag = ""
        if ir > 0:
            ir_flag = f" | ⚠️ IR descontado: {_fmt_brl(ir)} (Requer checagem de rubricas e base legal)"

        # Extrair matrícula e nome (Formato: 600141/1-ABDEL BARBOSA DERZE)
        serv_str = str(row['servidor'])
        if '-' in serv_str:
            matricula, nome = serv_str.split('-', 1)
        else:
            matricula, nome = "N/A", serv_str

        insights.append(Insight(
            id=f"SAL_{matricula}_{idx}",
            tipo="ANOMALIA ESTATÍSTICA",
            severidade="CRITICO" if z > 4.5 else ("ALTO" if z > 3.5 else "MEDIO"),
            confianca=conf, exposicao=val,
            titulo=f"Indício: {nome}",
            descricao=f"Registro aponta valor líquido de **{_fmt_brl(val)}**, que representa **{z:.1f}x** o desvio padrão da média do grupo: **{row['cargo']}** ({row['ch']}h, {row['vinculo']}).{ir_flag}",
            pattern="CARGO+CH+VINCULO → Z-SCORE → ANOMALIA",
            fontes=["Portal da Transparência (Rio Branco)"],
            evidencias=[row.to_dict()]
        ))
    return insights


def generate_insights_for_diarias(df: pd.DataFrame) -> List[Insight]:
    if df is None or df.empty: return []
    
    # Colunas: servidor_nome, destino, data_saida, data_retorno, valor, motivo, secretaria
    group_cols = ['destino', 'data_saida', 'data_retorno', 'motivo']
    for col in group_cols:
        if col not in df.columns: return []
        
    # Agrupar para detectar viagens em bloco
    agrupado = df.groupby(group_cols).agg({
        'servidor_nome': 'count',
        'valor': 'sum'
    }).reset_index()
    
    agrupado.columns = group_cols + ['qtd_servidores', 'total_gasto']
    
    # Filtrar blocos com 3 ou mais pessoas
    blocos = agrupado[agrupado['qtd_servidores'] >= 3].sort_values('total_gasto', ascending=False)
    
    insights = []
    for idx, row in blocos.head(20).iterrows():
        n = int(row['qtd_servidores'])
        val = float(row['total_gasto'])
        
        # Confiança baseada no número de pessoas e valor
        conf = int(_clamp(50 + (n * 5) + (val / 5000), 0, 99))
        
        insights.append(Insight(
            id=f"DIA_AGRUP_{idx}",
            tipo="CONCENTRAÇÃO DE DIÁRIAS",
            severidade="CRITICO" if n >= 8 or val > 20000 else ("ALTO" if n >= 5 else "MEDIO"),
            confianca=conf, exposicao=val,
            titulo=f"Indício: {n} registros → {row['destino']}",
            descricao=f"Identificado registro de **{n} diárias** para **{row['destino']}** com data de saída em **{row['data_saida'].strftime('%d/%m/%Y')}**. Custo agregado: **{_fmt_brl(val)}**. Risco potencial de baixa economicidade; demanda justificativa administrativa.",
            pattern="DESTINO+DATA → AGRUPAMENTO DE REGISTROS",
            fontes=["Portal da Transparência (Diárias)"],
            evidencias=df[(df['destino'] == row['destino']) & (df['data_saida'] == row['data_saida'])].head(10).to_dict('records')
        ))
        
    return insights

def checklist_validacao(insight: Insight) -> List[str]:
    base = ["Confirme o vínculo e cargo no portal oficial.", "Verifique se o valor líquido inclui parcelas retroativas ou 13º."]
    if insight.tipo == "CONCENTRACAO": base.append("Cheque se a secretaria possui outros fornecedores ativos.")
    return base
