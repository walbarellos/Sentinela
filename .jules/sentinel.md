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

## 2026-03-14 - Correção de Visibilidade de Cargos (N/D)
Learning:
Identifiquei que a UI estava tentando extrair o cargo da coluna 'servidor' usando uma lógica de split baseada em um padrão inexistente (' - '), resultando em 'N/D' sistemático. Entretanto, o banco de dados já possui uma coluna 'cargo' nativa e limpa. A coluna 'servidor' na verdade contém a matrícula acoplada ao nome (ex: '1234/1-NOME').

## 2026-03-14 - Descoberta de Incompatibilidades Setoriais (CNAE)
Learning:
A auditoria automatizada revelou que múltiplos fornecedores sancionados da SESACRE operam com CNAEs principais totalmente alheios à área da saúde. Isso sugere que o Estado do Acre pode estar contratando empresas generalistas para fornecer itens técnicos ou de saúde, o que viola o Art. 67 da Lei 14.133/2021.

Action:
Casos 32595581000148 e 13200879000167 promovidos para Score 80 (Crítico). Próximo passo recomendado é verificar o objeto social completo no QSA para ver se a atividade de saúde consta ao menos como secundária.
