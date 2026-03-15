from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from src.core.ops_timeline import ensure_ops_timeline
from src.core.ops_search import ensure_ops_search_index, sync_ops_search_index
from src.core.legal_compliance import (
    validate_cnpj,
    validate_financial_capacity,
    validate_cnae_compatibility,
    calculate_risk_score
)

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
    risk_score INTEGER DEFAULT 0,
    risk_label VARCHAR DEFAULT 'BAIXO',
    risk_flags VARCHAR[],
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
    
    # --- Cálculo de Risco Sentinel ---
    metrics = {
        "document_valid": validate_cnpj(case["cnpj"]),
        "financial_ratio": 0, # Puxar do QSA no futuro
        "front_company_risk": True, # Dado o volume de concomitancia extrema
        "cnae_compatible": True,
        "days_old": 999
    }
    risk = calculate_risk_score(metrics)

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
        "risk_score": risk["score"],
        "risk_label": risk["risk_label"],
        "risk_flags": risk["flags"],
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
            "Noticia de fato ou pedido de apuracao com foco em cruzamento sancionatorio e cadeia documental do contrato."
            if sancionado
            else "Noticia de fato ou pedido de apuracao com foco em item fora do edital e das propostas da licitacao-mae."
        )
        bundle_path = ENTREGA_DIR / f"rb_contrato_{numero}_bundle_20260314.tar.gz"
        bundle_manifest = ENTREGA_DIR / f"RB_CONTRATO_{numero}_BUNDLE_MANIFEST.json"
        
        # --- Cálculo de Risco Sentinel ---
        cnpj = item.get("cnpj")
        valor = float(item.get("valor_referencia_brl") or 0)
        # Mock capital para RB por enquanto (ideal: puxar do QSA)
        capital_mock = 50000.0 if not sancionado else 1000.0
        
        fin_res = validate_financial_capacity(valor, capital_mock)
        metrics = {
            "document_valid": validate_cnpj(cnpj),
            "financial_ratio": fin_res["ratio"],
            "front_company_risk": fin_res["is_front_company_risk"],
            "cnae_compatible": True, # RB sus costuma ser compatível, ideal validar
            "days_old": 999 # Ideal: data_contrato - data_abertura
        }
        risk = calculate_risk_score(metrics)
        
        if fin_res["is_front_company_risk"]:
            title = f"⚠️ [RISCO FACHADA] {title}"

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
                "severity": "CRITICO" if (sancionado or risk["score"] >= 80) else "ALTO",
                "classe_achado": classes,
                "uso_externo": "APTO_APURACAO",
                "estagio_operacional": "APTO_A_NOTICIA_DE_FATO",
                "status_operacional": "aberto",
                "prioridade": int(item.get("prioridade_final") or 0),
                "valor_referencia_brl": valor,
                "source_table": "v_rb_contratos_prioritarios",
                "source_row_ref": item["row_id"],
                "resumo_curto": summary,
                "proximo_passo": next_step,
                "bundle_path": str(bundle_path.relative_to(ROOT)) if bundle_path.exists() else None,
                "bundle_sha256": existing_bundle_sha(bundle_path, bundle_manifest),
                "risk_score": risk["score"],
                "risk_label": risk["risk_label"],
                "risk_flags": risk["flags"],
                "artifact_count": 0,
                "evidence_json": json.dumps(item, ensure_ascii=False),
            }
        )
        note_name = f"docs/Claude-march/patch_claude/claude_update/patch/relato_apuracao_{numero}.txt"
        artifacts.extend(
            [
                make_artifact(case_id, f"relato_apuracao_{numero}", "nota", note_name),
                make_artifact(case_id, "dossie_rb_sus", "dossie", "docs/Claude-march/patch_claude/claude_update/patch/dossie_rb_sus_prioritarios.md"),
            ]
        )
        if bundle_path.exists():
            artifacts.append(make_artifact(case_id, "bundle", "bundle", str(bundle_path.relative_to(ROOT))))
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
                    make_artifact(case_id, "cpl_publicacao_1554_html", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/cpl_publicacao_1554.html"),
                    make_artifact(case_id, "cpl_publicacao_1554_pdf", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/cpl_publicacao_1554_3.pdf"),
                    make_artifact(case_id, "cpl_publicacao_1640_html", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/cpl_publicacao_1640.html"),
                    make_artifact(case_id, "cpl_publicacao_1640_pdf", "evidencia", "docs/Claude-march/patch_claude/claude_update/patch/evidencias_rb_sus_prioritarios/caso_3898/cpl_publicacao_1640_3.pdf"),
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
    repr_path = "docs/Claude-march/patch_claude/claude_update/patch/sesacre_prioritarios/relato_apuracao_sesacre_top10.txt"
    for item in json.loads(rows.to_json(orient="records", force_ascii=False)):
        cnpj = str(item["cnpj_cpf"])
        case_id = f"sesacre:sancao:{cnpj}"
        
        # --- Cálculo de Risco Sentinel ---
        # SESACRE prioritários por sanção no CEIS são criticos por definição
        metrics = {
            "document_valid": validate_cnpj(cnpj),
            "financial_ratio": 0, # Dado ausente no recorte atual
            "front_company_risk": False,
            "cnae_compatible": True, 
            "days_old": 999 
        }
        risk = calculate_risk_score(metrics)

        cases.append(
            {
                "case_id": case_id,
                "family": "sesacre_sancao",
                "title": "Fornecedor da SESACRE cruzado com sancao ativa em base publica",
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
                "estagio_operacional": "APTO_A_NOTICIA_DE_FATO",
                "status_operacional": "aberto",
                "prioridade": min(100, int(item.get("n_sancoes_ativas") or 0) * 10 + 70),
                "valor_referencia_brl": float(item.get("valor_contratado_ac") or 0),
                "source_table": "sancoes_collapsed",
                "source_row_ref": cnpj,
                "resumo_curto": (
                    f"{item['nome_sancionado']} cruzado com {item['n_sancoes_ativas']} sancao(oes) ativa(s), "
                    f"{item['n_contratos_ac']} contrato(s) e R$ {float(item['valor_contratado_ac'] or 0):,.2f} no recorte SESACRE."
                ),
                "proximo_passo": "Usar o dossie estadual para noticia de fato tecnica, sem presumir nulidade automatica, e cobrar processo integral, justificativa e due diligence.",
                "bundle_path": None,
                "bundle_sha256": None,
                "risk_score": risk["score"],
                "risk_label": risk["risk_label"],
                "risk_flags": risk["flags"],
                "artifact_count": 2,
                "evidence_json": json.dumps(item, ensure_ascii=False),
            }
        )
        artifacts.extend(
            [
                make_artifact(case_id, "dossie_sesacre_top10", "dossie", dossie),
                make_artifact(case_id, "relato_apuracao_top10", "nota", repr_path),
            ]
        )
    return cases, artifacts


def build_conflict_cases(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Detecta servidores que são sócios de empresas com contratos (conflito de interesse)."""
    try:
        # Cruza servidores de RB com sócios de empresas que temos na base
        query = """
            SELECT 
                s.nome as servidor_nome,
                s.cargo,
                es.cnpj,
                es.qualificacao,
                emp.razao_social,
                emp.capital_social
            FROM (
                SELECT 
                    DISTINCT TRIM(SPLIT_PART(servidor, '-', 2)) as nome,
                    cargo
                FROM rb_servidores_mass
            ) s
            JOIN empresa_socios es ON s.nome = es.socio_nome
            JOIN empresas_cnpj emp ON es.cnpj = emp.cnpj
            LIMIT 50
        """
        rows = con.execute(query).fetchdf()
    except duckdb.Error:
        return [], []
    
    if rows.empty:
        return [], []

    cases: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    
    for item in json.loads(rows.to_json(orient="records", force_ascii=False)):
        nome = item["servidor_nome"]
        cnpj = item["cnpj"]
        case_id = f"conflict:servidor_socio:{cnpj}:{nome}".replace(" ", "_")
        
        metrics = {
            "document_valid": validate_cnpj(cnpj),
            "front_company_risk": False, # Ideal cruzar com contratos
            "days_old": 999
        }
        risk = calculate_risk_score(metrics)
        # Bônus de risco por ser servidor sócio
        risk["score"] = min(100, risk["score"] + 60)
        risk["risk_label"] = "ALTO" if risk["score"] >= 50 else "CRÍTICO" if risk["score"] >= 80 else "MÉDIO"
        risk["flags"].append("SERVIDOR_PUBLICO_SOCIO")

        cases.append({
            "case_id": case_id,
            "family": "conflito_interesse",
            "title": f"⚠️ [CONFLITO] Servidor Público como Sócio: {nome}",
            "subtitle": f"{item['cargo']} / Sócio de {item['razao_social']}",
            "subject_name": nome,
            "subject_doc": cnpj,
            "esfera": "municipal",
            "ente": "Rio Branco",
            "orgao": "SEMSA",
            "municipio": "Rio Branco",
            "uf": "AC",
            "area_tematica": "gestao",
            "severity": "ALTO",
            "classe_achado": "CONFLITO_INTERESSES",
            "uso_externo": "APTO_APURACAO",
            "estagio_operacional": "APTO_A_NOTICIA_DE_FATO",
            "status_operacional": "aberto",
            "prioridade": 80,
            "valor_referencia_brl": float(item.get("capital_social") or 0),
            "source_table": "rb_servidores_mass x empresa_socios",
            "source_row_ref": f"{nome}:{cnpj}",
            "resumo_curto": f"Servidor {nome} identificado como {item['qualificacao']} da empresa {item['razao_social']} (CNPJ {cnpj}).",
            "proximo_passo": "Verificar se a empresa possui contratos ativos com a esfera do servidor e se há gerência de fato.",
            "bundle_path": None,
            "bundle_sha256": None,
            "risk_score": risk["score"],
            "risk_label": risk["risk_label"],
            "risk_flags": risk["flags"],
            "artifact_count": 0,
            "evidence_json": json.dumps(item, ensure_ascii=False),
        })
        
    return cases, artifacts

def sync_ops_case_registry(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ensure_ops_registry(con)
    con.execute("DELETE FROM ops_case_artifact")
    con.execute("DELETE FROM ops_case_registry")

    builders = [build_cedimp_case, build_rb_cases, build_sesacre_cases, build_conflict_cases]
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
                resumo_curto, proximo_passo, bundle_path, bundle_sha256,
                risk_score, risk_label, risk_flags,
                artifact_count, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                case.get("risk_score", 0),
                case.get("risk_label", "BAIXO"),
                case.get("risk_flags", []),
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
    from src.core.ops_burden import ensure_ops_burden, sync_ops_burden
    from src.core.ops_calibration import ensure_ops_calibration, sync_ops_calibration
    from src.core.ops_checklist import ensure_ops_checklist, sync_ops_checklist
    from src.core.ops_contradiction import ensure_ops_contradiction, sync_ops_contradiction
    from src.core.ops_export import (
        build_generated_export_artifacts,
        ensure_ops_export_gate,
        sync_ops_generated_export_diff,
        sync_ops_export_gate,
    )
    from src.core.ops_guard import ensure_ops_guard, sync_ops_language_guard
    from src.core.ops_runbook import ensure_ops_runbook, sync_ops_runbook
    from src.core.ops_rulebook import ensure_ops_rulebook, sync_ops_rulebook
    from src.core.ops_sentinel import ensure_ops_sentinel, sync_ops_sentinel
    from src.core.ops_semantic import ensure_ops_semantic, sync_ops_semantic_analysis

    ensure_ops_burden(con)
    ensure_ops_calibration(con)
    ensure_ops_checklist(con)
    ensure_ops_contradiction(con)
    ensure_ops_export_gate(con)
    ensure_ops_guard(con)
    ensure_ops_runbook(con)
    ensure_ops_rulebook(con)
    ensure_ops_sentinel(con)
    ensure_ops_semantic(con)
    burden_stats = sync_ops_burden(con)
    semantic_stats = sync_ops_semantic_analysis(con)
    contradiction_stats = sync_ops_contradiction(con)
    checklist_stats = sync_ops_checklist(con)
    generated_artifacts = build_generated_export_artifacts(con)
    for artifact in generated_artifacts:
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
        UPDATE ops_case_registry r
        SET artifact_count = a.total
        FROM (
            SELECT case_id, COUNT(*) AS total
            FROM ops_case_artifact
            GROUP BY case_id
        ) a
        WHERE r.case_id = a.case_id
        """
    )
    ensure_ops_timeline(con)
    ensure_ops_search_index(con)
    search_stats = sync_ops_search_index(con)
    guard_stats = sync_ops_language_guard(con)
    export_stats = sync_ops_export_gate(con)
    runbook_stats = sync_ops_runbook(con)
    export_diff_stats = sync_ops_generated_export_diff(con)
    rulebook_stats = sync_ops_rulebook(con)
    calibration_stats = sync_ops_calibration(con)
    sentinel_stats = sync_ops_sentinel(con)

    return {
        "cases": len(all_cases),
        "artifacts": len(all_artifacts) + len(generated_artifacts),
        "indexed_docs": int(search_stats.get("indexed_docs", 0)),
        "burden_rows": int(burden_stats.get("rows_written", 0)),
        "semantic_rows": int(semantic_stats.get("rows_written", 0)),
        "contradiction_rows": int(contradiction_stats.get("rows_written", 0)),
        "checklist_rows": int(checklist_stats.get("rows_written", 0)),
        "language_guard_rows": int(guard_stats.get("rows_written", 0)),
        "export_gate_rows": int(export_stats.get("rows_written", 0)),
        "runbook_rows": int(runbook_stats.get("rows_written", 0)),
        "runbook_steps": int(runbook_stats.get("steps_written", 0)),
        "generated_export_rows": len(generated_artifacts),
        "generated_export_diff_rows": int(export_diff_stats.get("rows_written", 0)),
        "rule_rows": int(rulebook_stats.get("rules_written", 0)),
        "rule_validation_rows": int(rulebook_stats.get("validation_rows", 0)),
        "rule_validation_fail_rows": int(rulebook_stats.get("fail_rows", 0)),
        "calibration_benchmark_rows": int(calibration_stats.get("benchmark_rows", 0)),
        "calibration_result_rows": int(calibration_stats.get("result_rows", 0)),
        "calibration_fail_rows": int(calibration_stats.get("fail_rows", 0)),
        "calibration_warn_rows": int(calibration_stats.get("warn_rows", 0)),
        "sentinel_rows": int(sentinel_stats.get("sentinel_rows", 0)),
        "sentinel_result_rows": int(sentinel_stats.get("result_rows", 0)),
        "sentinel_fail_rows": int(sentinel_stats.get("fail_rows", 0)),
        "sentinel_warn_rows": int(sentinel_stats.get("warn_rows", 0)),
    }
