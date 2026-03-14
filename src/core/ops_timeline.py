from __future__ import annotations

import duckdb


TIMELINE_VIEW = """
CREATE OR REPLACE VIEW v_ops_case_timeline_event AS
SELECT
    case_id,
    updated_at AS event_at,
    'CASE_REFRESH' AS event_type,
    family AS event_group,
    'materializacao_caso' AS phase_key,
    '1. Materializacao do caso' AS phase_label,
    10 AS phase_order,
    title AS title,
    resumo_curto AS detail,
    source_table AS source_ref,
    NULL::VARCHAR AS path_ref,
    NULL::JSON AS payload_json
FROM ops_case_registry

UNION ALL

SELECT
    case_id,
    updated_at AS event_at,
    'ARTIFACT_INDEXED' AS event_type,
    kind AS event_group,
    CASE
        WHEN kind IN ('dossie', 'nota', 'bundle', 'generated_export') THEN 'consolidacao_externa'
        ELSE 'evidencia_local'
    END AS phase_key,
    CASE
        WHEN kind IN ('dossie', 'nota', 'bundle', 'generated_export') THEN '5. Consolidacao externa'
        ELSE '2. Evidencia local'
    END AS phase_label,
    CASE
        WHEN kind IN ('dossie', 'nota', 'bundle', 'generated_export') THEN 50
        ELSE 20
    END AS phase_order,
    label AS title,
    COALESCE(path, 'artefato sem caminho') AS detail,
    sha256 AS source_ref,
    path AS path_ref,
    metadata_json AS payload_json
FROM ops_case_artifact

UNION ALL

SELECT
    case_id,
    created_at AS event_at,
    'EXPORT_FROZEN' AS event_type,
    export_mode AS event_group,
    'consolidacao_externa' AS phase_key,
    '5. Consolidacao externa' AS phase_label,
    55 AS phase_order,
    label AS title,
    CONCAT('Exportacao controlada congelada em ', path) AS detail,
    sha256 AS source_ref,
    path AS path_ref,
    NULL::JSON AS payload_json
FROM ops_case_generated_export

UNION ALL

SELECT
    case_id,
    updated_at AS event_at,
    'INBOX_DOCUMENT' AS event_type,
    COALESCE(destino, 'SEM_DESTINO') AS event_group,
    CASE
        WHEN status_documento IN ('RECEBIDO', 'ANALISADO') THEN 'resposta_oficial'
        ELSE 'diligencia_documental'
    END AS phase_key,
    CASE
        WHEN status_documento IN ('RECEBIDO', 'ANALISADO') THEN '4. Resposta oficial'
        ELSE '3. Diligencia documental'
    END AS phase_label,
    CASE
        WHEN status_documento IN ('RECEBIDO', 'ANALISADO') THEN 40
        ELSE 30
    END AS phase_order,
    documento_chave AS title,
    CONCAT(
        COALESCE(status_documento, 'SEM_STATUS'),
        CASE WHEN protocolo IS NOT NULL AND protocolo != '' THEN CONCAT(' / protocolo ', protocolo) ELSE '' END,
        CASE WHEN recebido_em IS NOT NULL THEN CONCAT(' / recebido em ', CAST(recebido_em AS VARCHAR)) ELSE '' END
    ) AS detail,
    categoria_documental AS source_ref,
    file_path AS path_ref,
    NULL::JSON AS payload_json
FROM ops_case_inbox_document

UNION ALL

SELECT
    REPLACE(pipeline, 'ops_case_workflow:', '') AS case_id,
    started_at AS event_at,
    'WORKFLOW_RUN' AS event_type,
    status AS event_group,
    'processamento_analitico' AS phase_key,
    '3. Processamento analitico' AS phase_label,
    35 AS phase_order,
    pipeline AS title,
    COALESCE(error_text, CONCAT('Workflow do caso com status ', status)) AS detail,
    actor AS source_ref,
    NULL::VARCHAR AS path_ref,
    details_json AS payload_json
FROM ops_pipeline_run
WHERE pipeline LIKE 'ops_case_workflow:%'
"""


def ensure_ops_timeline(con: duckdb.DuckDBPyConnection) -> None:
    from src.core.ops_export import ensure_ops_export_gate

    ensure_ops_export_gate(con)
    con.execute(TIMELINE_VIEW)
