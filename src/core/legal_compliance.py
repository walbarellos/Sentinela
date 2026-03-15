"""
SENTINELA // LEGAL COMPLIANCE & INTEGRITY
Centraliza regras da Lei 14.133/2021 e validação de integridade documental.
"""

from __future__ import annotations
import re

from datetime import date, datetime

# --- LEI 14.133/2021 (Nova Lei de Licitações) ---
# ... (constantes existentes) ...

def validate_company_seniority(creation_date: date | str, contract_date: date | str) -> dict[str, Any]:
    """
    Valida se a empresa tinha senioridade mínima ao assinar o contrato.
    Indicador de risco: 'Shelf Company' (Empresa de prateleira).
    """
    if isinstance(creation_date, str):
        creation_date = datetime.strptime(creation_date[:10], "%Y-%m-%d").date()
    if isinstance(contract_date, str):
        contract_date = datetime.strptime(contract_date[:10], "%Y-%m-%d").date()
    
    delta = (contract_date - creation_date).days
    
    risk = "BAIXO"
    if delta < 0:
        risk = "CRÍTICO" # Contrato assinado antes da empresa existir!
    elif delta < 180:
        risk = "ALTO"    # Menos de 6 meses
    elif delta < 365:
        risk = "MÉDIO"   # Menos de 1 ano
        
    return {
        "days_old": delta,
        "risk_level": risk,
        "is_shelf_company": delta < 180
    }

def validate_financial_capacity(contract_value: float, capital_social: float) -> dict[str, Any]:
    """
    Valida a capacidade econômico-financeira da empresa frente ao valor do contrato.
    Base Legal: Lei 14.133/2021, Art. 69, § 4º (O capital mínimo exigido não pode exceder 10% do valor estimado).
    Logo, se o contrato for maior que 10x o capital social, há risco severo de incapacidade financeira ou empresa de fachada.
    """
    if capital_social <= 0:
        return {
            "ratio": float('inf'),
            "risk_level": "CRÍTICO",
            "is_front_company_risk": True,
            "legal_basis": "Lei 14.133/2021, Art. 69, § 4º"
        }
        
    ratio = contract_value / capital_social
    
    risk = "BAIXO"
    if ratio > 10.0:
        risk = "CRÍTICO" # Contrato supera o limite legal de 10x o capital
    elif ratio > 5.0:
        risk = "ALTO"    # Contrato é 5x maior que o capital (Sinal de alerta)
    elif ratio > 2.0:
        risk = "MÉDIO"
        
    return {
        "ratio": ratio,
        "risk_level": risk,
        "is_front_company_risk": ratio > 10.0,
        "legal_basis": "Lei 14.133/2021, Art. 69, § 4º"
    }

# --- CLASSIFICAÇÃO SETORIAL (CNAE) ---
CNAE_GROUPS = {
    "saude": {
        "prefixes": ["86", "87", "88", "4644", "4645", "4646", "4649"], # Hospitais, Medicamentos, Equipamentos Médicos
        "label": "Atividades de Saúde e Produtos Farmacêuticos"
    },
    "construcao": {
        "prefixes": ["41", "42", "43"],
        "label": "Construção Civil e Engenharia"
    },
    "ti": {
        "prefixes": ["62", "63"],
        "label": "Tecnologia da Informação"
    }
}

def validate_cnae_compatibility(company_cnaes: list[str], target_sector: str) -> dict[str, Any]:
    """
    Valida se os CNAEs da empresa são compatíveis com o setor do contrato.
    Base Legal: Lei 14.133/2021, Art. 67 (Comprovação de aptidão para a atividade).
    """
    if target_sector not in CNAE_GROUPS:
        return {"compatible": True, "risk_level": "BAIXO", "details": "Setor não monitorado para CNAE"}
    
    group = CNAE_GROUPS[target_sector]
    prefixes = group["prefixes"]
    
    # Normaliza e limpa CNAEs da empresa
    cleaned_cnaes = [re.sub(r'[^0-9]', '', str(c)) for v in company_cnaes for c in (v if isinstance(v, list) else [v])]
    
    is_compatible = False
    for cnae in cleaned_cnaes:
        if any(cnae.startswith(prefix) for prefix in prefixes):
            is_compatible = True
            break
            
    risk = "BAIXO"
    if not is_compatible:
        risk = "CRÍTICO" # Nenhum CNAE da empresa bate com o setor do contrato!
        
    return {
        "compatible": is_compatible,
        "risk_level": risk,
        "expected_group": group["label"],
        "found_cnaes": cleaned_cnaes,
        "legal_basis": "Lei 14.133/2021, Art. 67"
    }

def validate_cpf(cpf: str | None) -> bool:
    """Valida formato e dígitos verificadores de CPF."""
    if not cpf:
        return False
    cpf = re.sub(r'[^0-9]', '', str(cpf))
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False
    
    def calculate_digit(numbers: str, weights: list[int]) -> int:
        s = sum(int(n) * w for n, w in zip(numbers, weights))
        r = s % 11
        return 0 if r < 2 else 11 - r

    weights1 = list(range(10, 1, -1))
    weights2 = list(range(11, 1, -1))
    
    d1 = calculate_digit(cpf[:9], weights1)
    d2 = calculate_digit(cpf[:9] + str(d1), weights2)
    
    return cpf[-2:] == f"{d1}{d2}"

def calculate_risk_score(metrics: dict[str, Any]) -> dict[str, Any]:
    """
    Calcula a 'Temperatura de Risco' (0-100).
    Baseado em indicadores de integridade e legalidade.
    """
    score = 0
    flags = []
    
    # 1. Risco de Senioridade (Shelf Company)
    if metrics.get("days_old", 999) < 30:
        score += 40
        flags.append("EMPRESA_RECEM_CRIADA_CRITICO")
    elif metrics.get("days_old", 999) < 180:
        score += 20
        flags.append("EMPRESA_RECENTE")
        
    # 2. Risco de Capital Social (Fachada)
    if metrics.get("front_company_risk"):
        score += 40
        flags.append("INCAPACIDADE_FINANCEIRA_PROVAVEL")
    elif metrics.get("financial_ratio", 0) > 5:
        score += 20
        flags.append("CAPITAL_SOCIAL_BAIXO_X_CONTRATO")
        
    # 3. Risco de CNAE (Desvio de Finalidade)
    if not metrics.get("cnae_compatible", True):
        score += 40
        flags.append("CNAE_INCOMPATIVEL")
        
    # 4. Integridade Documental (Erro fatal)
    if not metrics.get("document_valid", True):
        score = 100
        flags.append("DOCUMENTO_INVALIDO_OU_FRAUDULENTO")
        
    normalized_score = min(100, score)
    
    risk_label = "BAIXO"
    if normalized_score >= 80: risk_label = "CRÍTICO"
    elif normalized_score >= 50: risk_label = "ALTO"
    elif normalized_score >= 25: risk_label = "MÉDIO"
    
    return {
        "score": normalized_score,
        "risk_label": risk_label,
        "flags": flags
    }

def validate_shared_control_cluster(cnpj: str, partners: list[str], con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """
    Detecta se a empresa faz parte de um cluster de controle compartilhado (Cartel ou ORCRIM).
    Cruza sócios da empresa com outros CNPJs na base.
    """
    if not partners:
        return {"in_cluster": False, "cluster_size": 0, "risk_level": "BAIXO"}
    
    # Busca outros CNPJs que compartilham os mesmos sócios
    query = """
        SELECT DISTINCT cnpj
        FROM empresa_socios
        WHERE socio_nome IN (SELECT unnest(?))
          AND cnpj <> ?
    """
    other_cnpjs = con.execute(query, [partners, cnpj]).fetchdf()
    
    cluster_size = len(other_cnpjs)
    risk = "BAIXO"
    if cluster_size >= 5:
        risk = "CRÍTICO" # Rede vasta de empresas sob o mesmo comando
    elif cluster_size >= 2:
        risk = "ALTO"    # Grupo de empresas interligadas
        
    return {
        "in_cluster": cluster_size > 0,
        "cluster_size": cluster_size,
        "linked_cnpjs": other_cnpjs['cnpj'].tolist(),
        "risk_level": risk,
        "details": f"Empresa interligada a {cluster_size} outra(s) entidade(s) via QSA."
    }

def validate_partner_economic_disparity(declared_wealth: float, contract_value: float) -> dict[str, Any]:
    """
    Detecta risco de 'Sócio Laranja' baseado na disparidade entre bens e contratos.
    """
    risk_score = 0
    flags = []
    ratio = None
    
    if declared_wealth > 0:
        ratio = contract_value / declared_wealth
        if ratio > 100:
            risk_score += 60
            flags.append("DISPARIDADE_PATRIMONIAL_EXTREMA")
        elif ratio > 10:
            risk_score += 30
            flags.append("DISPARIDADE_PATRIMONIAL_SUSPEITA")
    elif contract_value > 50000:
        risk_score += 40
        flags.append("SOCIO_SEM_PATRIMONIO_DECLARADO")
        
    risk_label = "BAIXO"
    if risk_score >= 70: risk_label = "CRÍTICO"
    elif risk_score >= 40: risk_label = "ALTO"
    
    return {
        "risk_score": risk_score,
        "risk_label": risk_label,
        "flags": flags,
        "ratio": ratio
    }

def validate_cnpj(cnpj: str | None) -> bool:
    """Valida formato e dígitos verificadores de CNPJ."""
    if not cnpj:
        return False
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False
    
    def calculate_digit(numbers: str, weights: list[int]) -> int:
        s = sum(int(n) * w for n, w in zip(numbers, weights))
        r = s % 11
        return 0 if r < 2 else 11 - r

    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    
    d1 = calculate_digit(cnpj[:12], weights1)
    d2 = calculate_digit(cnpj[:12] + str(d1), weights2)
    
    return cnpj[-2:] == f"{d1}{d2}"

def get_risk_level_by_threshold(value: float, category: str = "servicos") -> str:
    """
    Classifica o risco de fracionamento ou dispensa indevida.
    Base Legal: Art. 75, I e II da Lei 14.133/2021.
    """
    limit = THRESHOLD_DISPENSA_OBRAS_ENGENHARIA if category == "obras" else THRESHOLD_DISPENSA_BENS_SERVICOS
    
    # Próximo ao limite (90%+) = Atenção Operacional
    if value > limit:
        return "ACIMA_LIMITE_DISPENSA"
    if value > (limit * 0.9):
        return "RISCO_FRACIONAMENTO_ALTO"
    return "DENTRO_DO_LIMITE"
