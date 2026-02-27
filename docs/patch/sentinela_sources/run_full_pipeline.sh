#!/usr/bin/env bash
# SENTINELA // PIPELINE COMPLETO
# Coleta → Enriquece → Cruza → Alerta
# Cron sugerido: rodar diariamente à 2h da manhã
#
# USAGE:
#   ./run_full_pipeline.sh           # tudo
#   ./run_full_pipeline.sh --fast    # só prioridade 1 (< 30min)
#   ./run_full_pipeline.sh --cross   # só cruzamentos (dados já coletados)

set -e
export PYTHONPATH=$PYTHONPATH:.

FAST=${1:-""}
LOG_DIR="logs"
mkdir -p "$LOG_DIR" data

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG="$LOG_DIR/pipeline_$TIMESTAMP.log"

echo "════════════════════════════════════════"
echo "  SENTINELA // PIPELINE — $TIMESTAMP"
echo "════════════════════════════════════════"

# ─── 1. FONTES LOCAIS (Portal JSF — Rio Branco) ───────────────────────────────
echo ""
echo "▶ [1/5] PORTAL RIO BRANCO (JSF)"
for endpoint in diaria obra servidor despesa contratacao; do
    echo "  → $endpoint..."
    python -m src.ingest.riobranco_jsf_v2 --endpoint "$endpoint" 2>>"$LOG" || true
    sleep 2
done

if [ "$FAST" != "--fast" ]; then
    # ─── 2. FONTES REST / CSV (Nacionais) ─────────────────────────────────────
    echo ""
    echo "▶ [2/5] FONTES NACIONAIS (REST/CSV)"

    # PNCP — contratos federais de Rio Branco
    python -m src.ingest.engine --source pncp_contratos 2>>"$LOG" || true
    sleep 1

    # TSE — doações eleitorais (arquivo ~50MB, baixar 1x por eleição)
    python -m src.ingest.engine --source tse_doacoes 2>>"$LOG" || true
    sleep 1

    # CGU — CEIS/CNEP (empresas inidôneas)
    python -m src.ingest.engine --source cgu_ceis 2>>"$LOG" || true
    python -m src.ingest.engine --source cgu_cnep 2>>"$LOG" || true
    sleep 1

    # Querido Diário — D.O. Rio Branco
    python -m src.ingest.engine --source qd_diario_rb 2>>"$LOG" || true
    sleep 1

    # SICONFI — RCL e Despesa Pessoal
    python -m src.ingest.engine --source siconfi_rreo 2>>"$LOG" || true
    python -m src.ingest.engine --source siconfi_rgf 2>>"$LOG" || true
fi

# ─── 3. ENRIQUECIMENTO CNPJ ───────────────────────────────────────────────────
echo ""
echo "▶ [3/5] ENRIQUECIMENTO CNPJ (BrasilAPI)"
python -m src.ingest.cnpj_enricher 2>>"$LOG" || true

# ─── 4. CRUZAMENTOS / DETECTORES ─────────────────────────────────────────────
echo ""
echo "▶ [4/5] CRUZAMENTOS E DETECÇÃO DE ANOMALIAS"
python -m src.core.cross_reference_engine --export-csv 2>>"$LOG" || true

# ─── 5. SUMÁRIO ───────────────────────────────────────────────────────────────
echo ""
echo "▶ [5/5] SUMÁRIO"
python -c "
import duckdb, json
conn = duckdb.connect('data/sentinela_analytics.duckdb')
tables = conn.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchdf()
for t in tables['table_name']:
    try:
        n = conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        print(f'  {t}: {n:,} registros')
    except:
        pass

try:
    alerts = conn.execute(\"SELECT severity, COUNT(*) as n FROM alerts GROUP BY severity ORDER BY n DESC\").fetchdf()
    print()
    print('ALERTAS:')
    print(alerts.to_string(index=False))
except:
    pass
conn.close()
" 2>>"$LOG"

echo ""
echo "✓ Pipeline concluído — log: $LOG"
echo "  Dashboard: streamlit run app.py"
