from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"

DETECTOR_STATUS_BY_ID = {
    "FRAC": "APOSENTADO",
    "SAL": "APOSENTADO",
    "DIA": "APOSENTADO",
    "OB": "APOSENTADO",
    "CEIS": "COBERTO_OPS",
    "TSE": "APOSENTADO",
    "FDS": "APOSENTADO",
    "NEP": "APOSENTADO",
}


def main() -> int:
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(
            """
            UPDATE alerts
            SET
                uso_externo = 'REVISAO_INTERNA',
                status = 'QUARENTENA',
                classe_achado = COALESCE(classe_achado, 'HIPOTESE_INVESTIGATIVA'),
                grau_probatorio = COALESCE(grau_probatorio, 'EXPLORATORIO'),
                fonte_primaria = COALESCE(fonte_primaria, 'LEGADO_HISTORICO'),
                inferencia_permitida = COALESCE(
                    inferencia_permitida,
                    'Triagem historica apenas para revisao interna.'
                ),
                limite_conclusao = COALESCE(
                    limite_conclusao,
                    'Acervo legado em quarentena. Nao usar como saida externa.'
                )
            """
        )

        status_case = "CASE detector_id " + " ".join(
            f"WHEN '{detector_id}' THEN '{status}'"
            for detector_id, status in DETECTOR_STATUS_BY_ID.items()
        ) + " ELSE 'LEGADO_SEM_CLASSIFICACAO' END"

        con.execute(
            f"""
            CREATE OR REPLACE VIEW v_alerts_legacy_quarantine AS
            SELECT
                dossie_id,
                detector_id,
                {status_case} AS detector_status,
                severity,
                entity_type,
                entity_name,
                description,
                exposure_brl,
                base_legal,
                evidence,
                detected_at,
                'QUARENTENA' AS status,
                COALESCE(classe_achado, 'HIPOTESE_INVESTIGATIVA') AS classe_achado,
                COALESCE(grau_probatorio, 'EXPLORATORIO') AS grau_probatorio,
                COALESCE(fonte_primaria, 'LEGADO_HISTORICO') AS fonte_primaria,
                'REVISAO_INTERNA' AS uso_externo,
                COALESCE(inferencia_permitida, 'Triagem historica apenas para revisao interna.') AS inferencia_permitida,
                COALESCE(limite_conclusao, 'Acervo legado em quarentena. Nao usar como saida externa.') AS limite_conclusao
            FROM alerts
            """
        )

        rows = con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        external = con.execute(
            "SELECT COUNT(*) FROM alerts WHERE COALESCE(uso_externo, '') <> 'REVISAO_INTERNA'"
        ).fetchone()[0]
        quarantined = con.execute(
            "SELECT COUNT(*) FROM alerts WHERE COALESCE(status, '') = 'QUARENTENA'"
        ).fetchone()[0]
        print(f"alerts_rows={rows}")
        print(f"alerts_external_rows={external}")
        print(f"alerts_quarantined_rows={quarantined}")
        print("legacy_alerts_quarantine=PASS")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
