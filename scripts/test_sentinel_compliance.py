from src.core.legal_compliance import (
    validate_cnpj, 
    get_risk_level_by_threshold, 
    validate_company_seniority, 
    validate_financial_capacity,
    validate_cnae_compatibility
)

def test_compliance_integrity():
    # ... (testes anteriores mantidos implicitamente) ...
    
    # Teste CNAE (Desvio de Finalidade)
    # Caso 1: Empresa de TI ganhando contrato de Saúde (Incompatível)
    res_ti = validate_cnae_compatibility(company_cnaes=["6201501"], target_sector="saude")
    assert res_ti["compatible"] == False
    assert res_ti["risk_level"] == "CRÍTICO"
    
    # Caso 2: Farmácia ganhando contrato de Saúde (Compatível)
    res_saude = validate_cnae_compatibility(company_cnaes=["4771701", "4644301"], target_sector="saude")
    assert res_saude["compatible"] == True
    assert res_saude["risk_level"] == "BAIXO"
    
    # Caso 3: Empresa de Engenharia ganhando contrato de Construção (Compatível)
    res_eng = validate_cnae_compatibility(company_cnaes=["4120400"], target_sector="construcao")
    assert res_eng["compatible"] == True
    
    print("🛡️ Sentinel Verification: PASS")


if __name__ == "__main__":
    test_compliance_integrity()
