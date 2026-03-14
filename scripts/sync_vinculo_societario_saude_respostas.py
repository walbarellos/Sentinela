from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = (
    ROOT
    / "docs"
    / "Claude-march"
    / "patch_claude"
    / "claude_update"
    / "patch"
    / "entrega_denuncia_atual"
)
RESP_DIR = OUT_DIR / "cedimp_respostas"
INDEX_PATH = RESP_DIR / "cedimp_respostas_index.csv"

CSV_FIELDS = [
    "case_key",
    "cnpj",
    "razao_social",
    "destino",
    "eixo",
    "documento_chave",
    "categoria_documental",
    "descricao_documento",
    "status_documento",
    "protocolo",
    "recebido_em",
    "file_relpath",
    "notas",
]

TEMPLATE_ROWS = [
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "compatibilidade_horarios",
        "documento_chave": "ficha_funcional_maira",
        "categoria_documental": "FICHA_FUNCIONAL",
        "descricao_documento": "Ficha funcional completa de MAIRA SANTIAGO PIRES PARENTE, com historico de lotacoes e cargas.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "compatibilidade_horarios",
        "documento_chave": "ficha_funcional_marcos",
        "categoria_documental": "FICHA_FUNCIONAL",
        "descricao_documento": "Ficha funcional completa de MARCOS PAULO PARENTE ARAUJO, com historico de lotacoes e cargas.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "acumulacao_ilegal",
        "documento_chave": "declaracao_acumulacao_maira",
        "categoria_documental": "DECLARACAO_ACUMULACAO",
        "descricao_documento": "Declaracao de acumulacao ou ausencia de acumulacao de MAIRA.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "acumulacao_ilegal",
        "documento_chave": "declaracao_acumulacao_marcos",
        "categoria_documental": "DECLARACAO_ACUMULACAO",
        "descricao_documento": "Declaracao de acumulacao ou ausencia de acumulacao de MARCOS.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "compatibilidade_horarios",
        "documento_chave": "escalas_ponto_maira",
        "categoria_documental": "ESCALA_PONTO",
        "descricao_documento": "Escalas, plantao ou ponto de MAIRA nas competencias de maior carga.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "compatibilidade_horarios",
        "documento_chave": "escalas_ponto_marcos",
        "categoria_documental": "ESCALA_PONTO",
        "descricao_documento": "Escalas, plantao ou ponto de MARCOS nas competencias de maior carga.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "compatibilidade_horarios",
        "documento_chave": "parecer_compatibilidade",
        "categoria_documental": "PARECER_COMPATIBILIDADE",
        "descricao_documento": "Parecer ou despacho formal sobre compatibilidade de horarios/atividades.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SEMSA_RH",
        "eixo": "vedacao_art_107_x",
        "documento_chave": "autorizacao_gerencia_societaria",
        "categoria_documental": "AUTORIZACAO_GERENCIA",
        "descricao_documento": "Eventual autorizacao, despacho ou documento sobre gerencia/administracao societaria.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SESACRE",
        "eixo": "contrato_estadual",
        "documento_chave": "processo_integral_contrato_779_2023",
        "categoria_documental": "PROCESSO_CONTRATO",
        "descricao_documento": "Processo integral do contrato 779/2023, com termo, anexos, fiscais e justificativas.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SESACRE",
        "eixo": "contrato_estadual",
        "documento_chave": "habilitacao_empresa_779_2023",
        "categoria_documental": "HABILITACAO_EMPRESA",
        "descricao_documento": "Documentos de habilitacao e qualificacao apresentados pela empresa no contrato 779/2023.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SESACRE",
        "eixo": "contrato_estadual",
        "documento_chave": "execucao_medicao_glosa_779_2023",
        "categoria_documental": "EXECUCAO_MEDICAO_GLOSA",
        "descricao_documento": "Registros de execucao, medicao, glosas e fiscalizacao do contrato 779/2023.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
    {
        "case_key": "CEDIMP",
        "cnpj": "13325100000130",
        "razao_social": "CEDIMP-CENTRO DE DIAGNOSTICO POR IMAGEM DRS. MAIRA E MARCOS PARENTE LTDA",
        "destino": "SESACRE",
        "eixo": "contrato_estadual",
        "documento_chave": "relacao_profissionais_execucao_779_2023",
        "categoria_documental": "RELACAO_PROFISSIONAIS_EXECUCAO",
        "descricao_documento": "Documentos que identifiquem os profissionais vinculados a execucao do contrato e sua carga declarada.",
        "status_documento": "PENDENTE",
        "protocolo": "",
        "recebido_em": "",
        "file_relpath": "",
        "notas": "",
    },
]

DDL = """
CREATE TABLE IF NOT EXISTS vinculo_societario_saude_respostas (
    row_id VARCHAR PRIMARY KEY,
    case_key VARCHAR,
    cnpj VARCHAR,
    razao_social VARCHAR,
    destino VARCHAR,
    eixo VARCHAR,
    documento_chave VARCHAR,
    categoria_documental VARCHAR,
    descricao_documento VARCHAR,
    status_documento VARCHAR,
    protocolo VARCHAR,
    recebido_em TIMESTAMP,
    file_relpath VARCHAR,
    file_exists BOOLEAN,
    file_sha256 VARCHAR,
    file_size_bytes BIGINT,
    notas VARCHAR,
    evidence_json JSON,
    capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_template_files() -> None:
    RESP_DIR.mkdir(parents=True, exist_ok=True)
    (RESP_DIR / "anexos" / "semsa").mkdir(parents=True, exist_ok=True)
    (RESP_DIR / "anexos" / "sesacre").mkdir(parents=True, exist_ok=True)
    (RESP_DIR / "anexos" / "controle").mkdir(parents=True, exist_ok=True)
    readme = RESP_DIR / "README.md"
    if not readme.exists():
        readme.write_text(
            "\n".join(
                [
                    "# Cedimp - Caixa de Respostas Oficiais",
                    "",
                    "Use esta pasta para anexar documentos recebidos de `SEMSA/RH`, `SESACRE` ou controle interno.",
                    "",
                    "## Como usar",
                    "",
                    "- Preencha `cedimp_respostas_index.csv`.",
                    "- Se houver arquivo, grave em `cedimp_respostas/anexos/...` e informe `file_relpath` relativo a `entrega_denuncia_atual`.",
                    "- Depois rode `scripts/sync_vinculo_societario_saude_respostas.py`.",
                    "- Em seguida rerode `scripts/sync_vinculo_societario_saude_maturidade.py` e os exports do caso.",
                    "",
                    "## Regras",
                    "",
                    "- Nao sobrescreva o `documento_chave`; ele ancora a trilha probatoria.",
                    "- So marque `ANALISADO` quando o arquivo estiver localmente presente.",
                    "- `ARQUIVO_NAO_LOCALIZADO` e gerado automaticamente se o caminho apontado nao existir.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
    if not INDEX_PATH.exists():
        with INDEX_PATH.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(TEMPLATE_ROWS)


def load_csv_rows() -> list[dict]:
    ensure_template_files()
    with INDEX_PATH.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [{field: (row.get(field) or "").strip() for field in CSV_FIELDS} for row in rows]


def normalize_status(file_relpath: str, file_exists: bool, requested_status: str) -> str:
    status = (requested_status or "").strip().upper()
    if file_relpath and not file_exists:
        return "ARQUIVO_NAO_LOCALIZADO"
    if file_exists and status in {"ANALISADO", "RECEBIDO", "VALIDADO"}:
        return status
    if file_exists:
        return "RECEBIDO"
    return status or "PENDENTE"


def resolve_file(file_relpath: str) -> tuple[Path | None, bool]:
    if not file_relpath:
        return None, False
    path = Path(file_relpath)
    if not path.is_absolute():
        path = OUT_DIR / path
    return path, path.exists()


def parse_received_at(value: str) -> str | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).isoformat(sep=" ")
    except ValueError:
        return None


def to_insert_row(row: dict) -> dict:
    path, exists = resolve_file(row["file_relpath"])
    status = normalize_status(row["file_relpath"], exists, row["status_documento"])
    return {
        "row_id": f"vps_resp:{row['case_key']}:{row['documento_chave']}",
        "case_key": row["case_key"],
        "cnpj": row["cnpj"],
        "razao_social": row["razao_social"],
        "destino": row["destino"],
        "eixo": row["eixo"],
        "documento_chave": row["documento_chave"],
        "categoria_documental": row["categoria_documental"],
        "descricao_documento": row["descricao_documento"],
        "status_documento": status,
        "protocolo": row["protocolo"],
        "recebido_em": parse_received_at(row["recebido_em"]),
        "file_relpath": row["file_relpath"],
        "file_exists": exists,
        "file_sha256": sha256_file(path) if exists and path else None,
        "file_size_bytes": path.stat().st_size if exists and path else None,
        "notas": row["notas"],
        "evidence_json": json.dumps(
            {
                "csv_row": row,
                "resolved_path": str(path) if path else "",
                "generated_status": status,
            },
            ensure_ascii=False,
        ),
    }


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_respostas AS
        SELECT *
        FROM vinculo_societario_saude_respostas
        ORDER BY destino, eixo, documento_chave
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_vinculo_societario_saude_respostas_cobertura AS
        WITH base AS (
            SELECT *
            FROM vinculo_societario_saude_respostas
        )
        SELECT
            cnpj,
            razao_social,
            eixo,
            COUNT(*) AS docs_esperados,
            SUM(CASE WHEN status_documento IN ('RECEBIDO', 'ANALISADO', 'VALIDADO') THEN 1 ELSE 0 END) AS docs_recebidos,
            SUM(CASE WHEN file_exists THEN 1 ELSE 0 END) AS docs_localizados,
            SUM(CASE WHEN status_documento = 'ARQUIVO_NAO_LOCALIZADO' THEN 1 ELSE 0 END) AS docs_com_erro,
            MAX(CASE WHEN categoria_documental = 'FICHA_FUNCIONAL' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_ficha_funcional,
            MAX(CASE WHEN categoria_documental = 'DECLARACAO_ACUMULACAO' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_declaracao_acumulacao,
            MAX(CASE WHEN categoria_documental = 'ESCALA_PONTO' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_escala_ponto,
            MAX(CASE WHEN categoria_documental = 'PARECER_COMPATIBILIDADE' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_parecer_compatibilidade,
            MAX(CASE WHEN categoria_documental = 'AUTORIZACAO_GERENCIA' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_autorizacao_gerencia,
            MAX(CASE WHEN categoria_documental = 'PROCESSO_CONTRATO' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_processo_contrato,
            MAX(CASE WHEN categoria_documental = 'EXECUCAO_MEDICAO_GLOSA' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_execucao_medicao,
            MAX(CASE WHEN categoria_documental = 'RELACAO_PROFISSIONAIS_EXECUCAO' AND file_exists THEN 1 ELSE 0 END) = 1 AS has_relacao_profissionais_execucao,
            STRING_AGG(
                CASE WHEN status_documento NOT IN ('RECEBIDO', 'ANALISADO', 'VALIDADO') THEN documento_chave ELSE NULL END,
                ', '
                ORDER BY documento_chave
            ) AS documentos_pendentes
        FROM base
        GROUP BY cnpj, razao_social, eixo
        ORDER BY cnpj, eixo
        """
    )


def main() -> int:
    rows = load_csv_rows()
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    con.execute("DELETE FROM vinculo_societario_saude_respostas")
    inserted = 0
    for row in rows:
        payload = to_insert_row(row)
        con.execute(
            """
            INSERT OR REPLACE INTO vinculo_societario_saude_respostas (
                row_id, case_key, cnpj, razao_social, destino, eixo, documento_chave,
                categoria_documental, descricao_documento, status_documento, protocolo,
                recebido_em, file_relpath, file_exists, file_sha256, file_size_bytes,
                notas, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                payload["row_id"],
                payload["case_key"],
                payload["cnpj"],
                payload["razao_social"],
                payload["destino"],
                payload["eixo"],
                payload["documento_chave"],
                payload["categoria_documental"],
                payload["descricao_documento"],
                payload["status_documento"],
                payload["protocolo"],
                payload["recebido_em"],
                payload["file_relpath"],
                payload["file_exists"],
                payload["file_sha256"],
                payload["file_size_bytes"],
                payload["notas"],
                payload["evidence_json"],
            ],
        )
        inserted += 1
    create_views(con)
    missing = con.execute(
        "SELECT COUNT(*) FROM vinculo_societario_saude_respostas WHERE status_documento = 'ARQUIVO_NAO_LOCALIZADO'"
    ).fetchone()[0]
    con.close()
    print(f"rows={inserted}")
    print(f"missing_files={missing}")
    print(f"index={INDEX_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
