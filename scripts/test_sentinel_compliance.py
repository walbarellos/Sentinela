from src.core.legal_compliance import validate_cnpj, get_risk_level_by_threshold

def test_compliance_integrity():
    # Teste CNPJ (Ex: CNPJ da Apple no Brasil - formatado e desformatado)
    assert validate_cnpj("00.623.904/0001-73") == True
    assert validate_cnpj("00623904000173") == True
    assert validate_cnpj("00000000000000") == False
    assert validate_cnpj("123") == False
    
    # Teste Thresholds Lei 14.133 (Serviços: R$ 59.906,02)
    assert get_risk_level_by_threshold(10000) == "DENTRO_DO_LIMITE"
    assert get_risk_level_by_threshold(55000) == "RISCO_FRACIONAMENTO_ALTO" # > 90%
    assert get_risk_level_by_threshold(65000) == "ACIMA_LIMITE_DISPENSA"
    
    print("🛡️ Sentinel Verification: PASS")

if __name__ == "__main__":
    test_compliance_integrity()
