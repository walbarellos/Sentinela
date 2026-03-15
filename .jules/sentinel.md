# Sentinel 🛡️ - Auditor Journal

## 2026-03-14 - Protocolo de Integridade Documental
Learning:
Cruzamentos sem validação de dígito verificador geram ruído. Implementada validação de CPF/CNPJ e thresholds da Lei 14.133 para garantir que o sistema não emita alertas baseados em erros de digitação dos portais.

## 2026-03-14 - Rastreio de "Shelf Companies"
Learning:
Empresas recém-criadas que ganham contratos relevantes são indicadores de risco de direcionamento. Implementada regra de senioridade (< 180 dias).

## 2026-03-15 - Auditoria de Redes de Fundo Partidário
Learning:
A detecção de desvios de fundo eleitoral deve focar na figura do 'Operador de Rede' (Contador/Advogado). O critério de anomalia é definido pela disparidade estatística entre o repasse de verba e a performance de votos.

Action:
Implementada a busca por clusters contábeis. O sistema agora agrupa candidatos anômalos por prestador de serviço para identificar operadores de fundos laranjas de forma autônoma.
