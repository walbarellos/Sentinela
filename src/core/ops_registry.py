from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[2]
PATCH_DIR = (
    ROOT
    / "docs"
    / "Claude-march"
    / "patch_claude"
    / "claude_update"
    / "patch"
)
ENTREGA_DIR = PATCH_DIR / "entrega_denuncia_atual"
SESACRE_DIR = PATCH_DIR / "sesacre_prioritarios"

CASE_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_registry (
    case_id VARCHAR PRIMARY KEY,
    family VARCHAR,
    title VARCHAR,
    subtitle VARCHAR,
    subject_name VARCHAR,
    subject_doc VARCHAR,
    esfera VARCHAR,
    ente VARCHAR,
    orgao VARCHAR,
    municipio VARCHAR,
    uf VARCHAR,
    area_tematica VARCHAR,
    severity VARCHAR,
    classe_achado VARCHAR,
    uso_externo VARCHAR,
    estagio_operacional VARCHAR,
    status_operacional VARCHAR,
    prioridade INTEGER,
    valor_referencia_brl DOUBLE,
    source_table VARCHAR,
    source_row_ref VARCHAR,
    resumo_curto VARCHAR,
    proximo_passo VARCHAR,
    bundle_path VARCHAR,
    bundle_sha256 VARCHAR,
    artifact_count INTEGER DEFAULT 0,
    evidence_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

ARTIFACT_DDL = """
CREATE TABLE IF NOT EXISTS ops_case_artifact (
    artifact_id VARCHAR PRIMARY KEY,
    case_id VARCHAR NOT NULL,
    label VARCHAR,
    kind VARCHAR,
    path VARCHAR,
    exists BOOLEAN,
    sha256 VARCHAR,
    size_bytes BIGINT,
    metadata_json JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_path(relpath: str | None) -> Path | None:
    if not relpath:
        return None
    path = Path(relpath)
    if path.is_absolute():
        return path
    return ROOT / relpath


def make_artifact(case_id: str, label: str, kind: str, relpath: str | None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    path = resolve_path(relpath)
    exists = bool(path and path.exists())
    return {
        "artifact_id": f"{case_id}:{kind}:{label}".replace(" ", "_").replace("/", "_"),
        "case_id": case_id,
        "label": label,
        "kind": kind,
        "path": str(path.relative_to(ROOT)) if path else None,
        "exists": exists,
        "sha256": sha256_file(path) if exists and path else None,
        "size_bytes": path.stat().st_size if exists and path else None,
        "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
    }


def existing_bundle_sha(path: Path, manifest_path: Path | None = None) -> str | None:
    if manifest_path and manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            if payload.get("bundle_sha256"):
                return payload["bundle_sha256"]
        except Exception:
            pass
    if path.exists():
        return sha256_file(path)
    return None


def ensure_ops_registry(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(CASE_DDL)
    con.execute(ARTIFACT_DDL)


def build_cedimp_case(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        row = con.execute(
            """
            SELECT *
            FROM v_vinculo_societario_saude_gate
            ORDER BY score_triagem DESC, contrato_valor_brl DESC
            LIMIT 1
            """
        ).fetchdf()
    except duckdb.Error:
        return [], []
    if row.empty:
        return [], []

    case = json.loads(row.to_json(orient="records", force_ascii=False))[0]
    bundle_path = ENTREGA_DIR / "cedimp_case_bundle_20260313.tar.gz"
    bundle_manifest = ENTREGA_DIR / "CEDIMP_CASE_BUNDLE_MANIFEST.json"
    case_id = "cedimp:saude_societario:13325100000130"
    artifacts = [
        make_artifact(case_id, "dossie_followup", "dossie", "docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md"),
        make_artifact(case_id, "dossie_gate", "dossie", "docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_DOSSIE.md"),
        make_artifact(case_id, "nota_operacional", "nota", "docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/nota_operacional_cedimp.txt"),
        make_artifact(case_id, "diligencias", "dossie", "docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS.md"),
        make_artifact(case_id, "bundle", "bundle", "docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/cedimp_case_bundle_20260313.tar.gz"),
    ]
    case_row = {
        "case_id": case_id,
        "family": "saude_societario",
        "title": "Vinculo societario em saude com sobreposicao publico-privada documentada",
        "subtitle": f"Contrato {case['contrato_numero']} / {case['contrato_orgao']}",
        "subject_name": case["razao_social"],
        "subject_doc": case["cnpj"],
        "esfera": "estadual",
        "ente": "Acre",
        "orgao": "SESACRE",
        "municipio": "Rio Branco",
        "uf": "AC",
        "area_tematica": "saude",
        "severity": "MEDIO",
        "classe_achado": "HIPOTESE_INVESTIGATIVA",
        "uso_externo": case["uso_recomendado"],
        "estagio_operacional": case["estagio_operacional"],
        "status_operacional": "aberto",
        "prioridade": int(case.get("score_triagem") or 0),
        "valor_referencia_brl": float(case.get("contrato_valor_brl") or 0),
        "source_table": "v_vinculo_societario_saude_gate",
        "source_row_ref": case["row_id"],
        "resumo_curto": case["resumo_decisao"],
        "proximo_passo": "Protocolar pedidos objetivos para SEMSA/RH e SESACRE e alimentar a caixa de respostas do caso.",
        "bundle_path": str(bundle_path.relative_to(ROOT)) if bundle_path.exists() else None,
        "bundle_sha256": existing_bundle_sha(bundle_path, bundle_manifest),
        "artifact_count": len(artifacts),
        "evidence_json": json.dumps(
            {
                "gate": case,
                "artifacts": [item["path"] for item in artifacts],
            },
            ensure_ascii=False,
        ),
    }
    return [case_row], artifacts


def build_rb_cases(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        rows = con.execute(
            """
            SELECT *
            FROM v_rb_contratos_prioritarios
            ORDER BY prioridade_final DESC, valor_referencia_brl DESC, numero_contrato
            """
        ).fetchdf()
    except duckdb.Error:
        return [], []
    if rows.empty:
        return [], []

    cases: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for item in json.loads(rows.to_json(orient="records", force_ascii=False)):
        numero = str(item["numero_contrato"])
        case_id = f"rb:contrato:{numero}"
        sancionado = bool(item.get("sancao_ativa"))
        classes = "CRUZAMENTO_SANCIONATORIO" if sancionado else "DIVERGENCIA_DOCUMENTAL"
        title = (
            "Contrato SUS municipal com fornecedor sancionado"
            if sancionado
            else "Contrato SUS municipal com divergencia documental de licitacao"
        )
        summary = (
            f"Contrato {numero}, processo {item['numero_processo']}, {item['secretaria']}, valor {item['valor_referencia_brl']:.2f}."
        )
        next_step = (
            "Representacao preliminar com foco em cruzamento sancionatorio e cadeia documental do contrato."
            if sancionado
            else "Representacao preliminar com foco em item fora do edital e das propostas da licitacao-mae."
        )
        cases.append(
            {
                "case_id": case_id,
                "family": "rb_sus_contrato",
                "title": title,
                "subtitle": f"Processo {item['numero_processo']} / contrato {numero}",
                "subject_name": item.get("fornecedor") or f"Contrato {numero}",
                "subject_doc": item.get("cnpj"),
                "esfera": "municipal",
                "ente": "Prefeitura de Rio Branco",
                "orgao": "SEMSA",
                "municipio": "Rio Branco",
                "uf": "AC",
                "area_tematica": "saude",
                "severity": "CRITICO" if sancionado else "ALTO",
                "classe_achado": classes,
                "uso_externo": "APTO_APURACAO",
                "estagio_operacional": "APTO_REPRESENTACAO_PRELIMINAR",
                "status_operacional": "aberto",
                "prioridade": int(item.get("prioridade_final") or 0),
                "valor_referencia_brl": float(item.get("valor_referencia_brl") or 0),
                "source_table": "v_rb_contratos_prioritarios",
                "source_row_ref": item["row_id"],
                "resumo_curto": summary,
                "proximo_passo": next_step,
                "bundle_path": None,
                "bundle_sha256": None,
                "artifact_count": 0,
                "evidence_json": json.dumps(item, ensure_ascii=False),
            }
        )
        denuncia_name = f"docs/Claude-march/patch_claude/claude_update/patch/entrega_denuncia_atual/denuncia_preliminar_{numero}.txt"
        artifacts.extend(
            [
                make_artifact(case_id, f"denuncia_preliminar_{numero}", "nota", denuncia_name),
                make_artifact(case_id, "dossie_rb_sus", "dossie", "docs/Claude-march/patch_claude/claude_update/patch/dossie_rb_sus_prioritarios.md"),
            ]
        )
        if numero == "3895":
            artifacts.extend(
                [
                    make_artifact(case_id, "contrato_detail", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3895/contrato_3895_detail.html"),
                    make_artifact(case_id, "contrato_anexo", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3895/contrato_3895_anexo_2247658.pdf"),
                ]
            )
        if numero == "3898":
            artifacts.extend(
                [
                    make_artifact(case_id, "contrato_detail", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/contrato_3898_detail.html"),
                    make_artifact(case_id, "licitacao_detail", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/licitacao_2274334_detail.html"),
                    make_artifact(case_id, "cpl_publicacao_1554_pdf", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/cpl_publicacao_1554_3.pdf"),
                ]
            )

    artifact_counts = {}
    for artifact in artifacts:
        artifact_counts[artifact["case_id"]] = artifact_counts.get(artifact["case_id"], 0) + 1
    for case in cases:
        case["artifact_count"] = artifact_counts.get(case["case_id"], 0)
    return cases, artifacts


def build_sesacre_cases(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        rows = con.execute(
            """
            SELECT cnpj_cpf, nome_sancionado, orgao_ac, valor_contratado_ac, n_sancoes_ativas, n_contratos_ac, fonte
            FROM sancoes_collapsed
            WHERE orgao_ac = 'SESACRE' AND ativa
            ORDER BY valor_contratado_ac DESC, nome_sancionado
            LIMIT 10
            """
        ).fetchdf()
    except duckdb.Error:
        return [], []
    if rows.empty:
        return [], []

    cases: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    dossie = "docs/Claude-march/patch_claude/claude_update/patch/sesacre_prioritarios/dossie_sesacre_sancoes_prioritarias.md"
    repr_path = "docs/Claude-march/patch_claude/claude_update/patch/sesacre_prioritarios/representacao_preliminar_sesacre_top10.txt"
    for item in json.loads(rows.to_json(orient="records", force_ascii=False)):
        cnpj = str(item["cnpj_cpf"])
        case_id = f"sesacre:sancao:{cnpj}"
        cases.append(
            {
                "case_id": case_id,
                "family": "sesacre_sancao",
                "title": "Fornecedor com sancao ativa contratado pela SESACRE",
                "subtitle": f"{item['n_sancoes_ativas']} sancao(oes) ativa(s) / {item['n_contratos_ac']} contrato(s)",
                "subject_name": item["nome_sancionado"],
                "subject_doc": cnpj,
                "esfera": "estadual",
                "ente": "Acre",
                "orgao": "SESACRE",
                "municipio": "Rio Branco",
                "uf": "AC",
                "area_tematica": "saude",
                "severity": "CRITICO",
                "classe_achado": "CRUZAMENTO_SANCIONATORIO",
                "uso_externo": "APTO_APURACAO",
                "estagio_operacional": "APTO_REPRESENTACAO_PRELIMINAR",
                "status_operacional": "aberto",
                "prioridade": min(100, int(item.get("n_sancoes_ativas") or 0) * 10 + 70),
                "valor_referencia_brl": float(item.get("valor_contratado_ac") or 0),
                "source_table": "sancoes_collapsed",
                "source_row_ref": cnpj,
                "resumo_curto": (
                    f"{item['nome_sancionado']} com {item['n_sancoes_ativas']} sancao(oes) ativa(s), "
                    f"{item['n_contratos_ac']} contrato(s) e R$ {float(item['valor_contratado_ac'] or 0):,.2f} no recorte SESACRE."
                ),
                "proximo_passo": "Usar o dossie estadual e a representacao preliminar top10 para apuracao externa focada em sancoes ativas.",
                "bundle_path": None,
                "bundle_sha256": None,
                "artifact_count": 2,
                "evidence_json": json.dumps(item, ensure_ascii=False),
            }
        )
        artifacts.extend(
            [
                make_artifact(case_id, "dossie_sesacre_top10", "dossie", dossie),
                make_artifact(case_id, "representacao_top10", "nota", repr_path),
            ]
        )
    return cases, artifacts


def sync_ops_case_registry(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_registry(con)
    con.execute("DELETE FROM ops_case_artifact")
    con.execute("DELETE FROM ops_case_registry")

    builders = [build_cedimp_case, build_rb_cases, build_sesacre_cases]
    all_cases: list[dict[str, Any]] = []
    all_artifacts: list[dict[str, Any]] = []
    for builder in builders:
        cases, artifacts = builder(con)
        all_cases.extend(cases)
        all_artifacts.extend(artifacts)

    for case in all_cases:
        con.execute(
            """
            INSERT INTO ops_case_registry (
                case_id, family, title, subtitle, subject_name, subject_doc,
                esfera, ente, orgao, municipio, uf, area_tematica,
                severity, classe_achado, uso_externo, estagio_operacional, status_operacional,
                prioridade, valor_referencia_brl, source_table, source_row_ref,
                resumo_curto, proximo_passo, bundle_path, bundle_sha256, artifact_count, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                case["case_id"],
                case["family"],
                case["title"],
                case["subtitle"],
                case["subject_name"],
                case["subject_doc"],
                case["esfera"],
                case["ente"],
                case["orgao"],
                case["municipio"],
                case["uf"],
                case["area_tematica"],
                case["severity"],
                case["classe_achado"],
                case["uso_externo"],
                case["estagio_operacional"],
                case["status_operacional"],
                case["prioridade"],
                case["valor_referencia_brl"],
                case["source_table"],
                case["source_row_ref"],
                case["resumo_curto"],
                case["proximo_passo"],
                case["bundle_path"],
                case["bundle_sha256"],
                case["artifact_count"],
                case["evidence_json"],
            ],
        )

    for artifact in all_artifacts:
        con.execute(
            """
            INSERT INTO ops_case_artifact (
                artifact_id, case_id, label, kind, path, exists, sha256, size_bytes, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                artifact["artifact_id"],
                artifact["case_id"],
                artifact["label"],
                artifact["kind"],
                artifact["path"],
                artifact["exists"],
                artifact["sha256"],
                artifact["size_bytes"],
                artifact["metadata_json"],
            ],
        )

    con.execute(
        """
        CREATE OR REPLACE VIEW v_ops_case_registry AS
        SELECT *
        FROM ops_case_registry
        ORDER BY prioridade DESC, valor_referencia_brl DESC, title, case_id
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW v_ops_case_artifact AS
        SELECT *
        FROM ops_case_artifact
        ORDER BY case_id, kind, label
        """
    )

    return {"cases": len(all_cases), "artifacts": len(all_artifacts)}
