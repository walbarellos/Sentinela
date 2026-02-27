#!/bin/bash
echo "--- Terminal 2: Sentinela v2 (Entity Resolution) ---"
source .venv/bin/activate
export PYTHONPATH=$PYTHONPATH:.

# Rodar 2022 e 2024 em UMA ÚNICA chamada do CLI
# Isso permite ao detector comparar 2024 (LGPD) com 2022 (Real)
python -m src.cli pipeline --ano 2024

echo "📄 Gerando Relatório Visual..."
python -m src.cli report

echo "-------------------------------------------------------"
echo "Relatório: relatorio_corrupcao_acre.html"
