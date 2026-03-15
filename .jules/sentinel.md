# Sentinel 🛡️ - Auditor Journal

## 2026-03-14 - Inicialização do Protocolo de Integridade
Learning:
A transição do sistema para a V5 priorizou a materialização de evidências, mas as constantes jurídicas da Lei 14.133/2021 estavam fragmentadas. Identifiquei que cruzamentos de CPFs e CNPJs sem validação de dígito verificador podem gerar "ghost matches" (falsos positivos por erro de digitação nos portais).

Action:
Implementado `src/core/legal_compliance.py` com validação de CNPJ e thresholds atualizados (Decreto 11.871/2023).

## 2026-03-14 - Investigação de Empresas Recém-Criadas
Learning:
Um dos sinais mais fortes de direcionamento em contratos de dispensa é a vitória de empresas criadas poucas semanas antes do empenho. O sistema atual captura a data do contrato, mas não valida sistematicamente a idade da empresa frente ao histórico de empenhos.

Action:
Selecionado para implementação: Regra de detecção de "Shelf Companies" (Empresas de Prateleira).

## 2026-03-14 - Blueprint de Rastreio Financeiro (Follow the Money)
Learning:
Para encontrar casos difíceis, não basta cruzar nomes. É preciso cruzar a capacidade estrutural da empresa com o volume de dinheiro público injetado nela. A Lei 14.133 estabelece métricas de qualificação econômico-financeira.

Action:
Implementando a regra de Incompatibilidade de Capital Social (Lei 14.133, Art. 69). Contratos que superam 10x o capital social da empresa disparam um alerta crítico de possível empresa de fachada/laranja.

## 2026-03-14 - Detecção de Desvio de Finalidade (CNAE)
Learning:
Empresas sem "Aptidão Técnica" (Art. 67 da Lei 14.133) são frequentemente usadas em esquemas de direcionamento. O CNAE (Classificação Nacional de Atividades Econômicas) é o rastro oficial da especialidade da empresa. Contratos de saúde dados a empresas sem CNAE de saúde são irregularidades graves e objetivas.

Action:
Implementando o mapeamento de CNAEs críticos por setor para detecção automática de desvio de finalidade.
