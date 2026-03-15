"""
SENTINELA // LEGAL COMPLIANCE & INTEGRITY
Centraliza regras da Lei 14.133/2021 e validação de integridade documental.
"""

from __future__ import annotations
import re

# --- LEI 14.133/2021 (Nova Lei de Licitações) ---
# Valores atualizados conforme Decreto Federal 11.871/2023 para o exercício de 2024+
THRESHOLD_DISPENSA_OBRAS_ENGENHARIA = 119_812.02
THRESHOLD_DISPENSA_BENS_SERVICOS = 59_906.02

# --- INTEGRIDADE DOCUMENTAL ---

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
