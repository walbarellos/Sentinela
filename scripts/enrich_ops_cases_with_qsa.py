from __future__ import annotations

import json
import duckdb
from pathlib import Path
from src.core.legal_compliance import (
    validate_cnpj,
    validate_company_seniority,
    validate_financial_capacity,
    validate_cnae_compatibility,
    calculate_risk_score
)

DB_PATH = 'data/sentinela_analytics.duckdb'

# Mapeamento de Família do Caso -> Setor Sentinel
FAMILY_SECTOR_MAP = {
    "rb_sus_contrato": "saude",
    "saude_societario": "saude",
    "sesacre_sancao": "saude"
}

def main():
    con = duckdb.connect(DB_PATH)
    try:
        # 1. Puxar casos que possuem CNPJ
        cases = con.execute("""
            SELECT case_id, subject_doc, valor_referencia_brl, family 
            FROM ops_case_registry 
            WHERE subject_doc IS NOT NULL AND subject_doc <> ''
        """).fetchdf()
        
        for _, row in cases.iterrows():
            case_id = row['case_id']
            cnpj = row['subject_doc']
            valor = row['valor_referencia_brl']
            family = row['family']
            
            # 2. Buscar dados da empresa no QSA/Receita
            qsa = con.execute("""
                SELECT capital_social, data_abertura, cnae_principal
                FROM empresas_cnpj
                WHERE cnpj = ?
                LIMIT 1
            """, [cnpj]).fetchdf()
            
            if qsa.empty:
                continue
                
            empresa = qsa.iloc[0]
            capital = float(empresa['capital_social'] or 0)
            data_abertura = str(empresa['data_abertura'] or "")
            cnae_principal = str(empresa['cnae_principal'] or "")
            
            # 3. Recalcular métricas Sentinel
            fin_res = validate_financial_capacity(valor, capital)
            seniority = validate_company_seniority(data_abertura, "2026-03-14")
            
            # Validação de CNAE por Setor
            target_sector = FAMILY_SECTOR_MAP.get(family)
            cnae_res = validate_cnae_compatibility([cnae_principal], target_sector) if target_sector else {"compatible": True}
            
            metrics = {
                "document_valid": validate_cnpj(cnpj),
                "financial_ratio": fin_res["ratio"],
                "front_company_risk": fin_res["is_front_company_risk"],
                "days_old": seniority["days_old"],
                "cnae_compatible": cnae_res["compatible"]
            }
            
            risk = calculate_risk_score(metrics)
            
            # 4. Atualizar o registro do caso
            con.execute("""
                UPDATE ops_case_registry
                SET risk_score = ?, risk_label = ?, risk_flags = ?
                WHERE case_id = ?
            """, [risk['score'], risk['risk_label'], risk['flags'], case_id])
            
            print(f"Enriched {case_id}: Score {risk['score']} ({risk['risk_label']}) - CNAE Compatible: {cnae_res['compatible']}")
            
    finally:
        con.close()

if __name__ == "__main__":
    main()
