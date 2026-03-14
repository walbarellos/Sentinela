from __future__ import annotations

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


def check(condition: bool, code: str, ok_msg: str, fail_msg: str) -> dict:
    return {
        "code": code,
        "status": "PASS" if condition else "FAIL",
        "message": ok_msg if condition else fail_msg,
    }


def main() -> int:
    con = duckdb.connect(str(DB_PATH), read_only=True)

    checks: list[dict] = []

    count_followup = con.execute("SELECT COUNT(*) FROM vinculo_societario_saude_followup").fetchone()[0]
    checks.append(
        check(
            count_followup == 1,
            "Q01",
            "Existe exatamente 1 caso no follow-up de saude.",
            f"Esperado 1 caso no follow-up de saude, encontrado {count_followup}.",
        )
    )

    count_juridico = con.execute("SELECT COUNT(*) FROM vinculo_societario_saude_juridico").fetchone()[0]
    checks.append(
        check(
            count_juridico == 1,
            "Q02",
            "Existe exatamente 1 caso na matriz juridico-funcional.",
            f"Esperado 1 caso na matriz juridico-funcional, encontrado {count_juridico}.",
        )
    )

    count_func = con.execute("SELECT COUNT(*) FROM vinculo_societario_saude_apuracao_funcional").fetchone()[0]
    checks.append(
        check(
            count_func == 1,
            "Q03",
            "Existe exatamente 1 caso na triagem funcional.",
            f"Esperado 1 caso na triagem funcional, encontrado {count_func}.",
        )
    )

    count_maturidade = con.execute("SELECT COUNT(*) FROM vinculo_societario_saude_maturidade").fetchone()[0]
    checks.append(
        check(
            count_maturidade >= 10,
            "Q04",
            "A matriz de maturidade tem cobertura minima esperada.",
            f"Esperado ao menos 10 linhas na matriz de maturidade, encontrado {count_maturidade}.",
        )
    )

    count_respostas = con.execute("SELECT COUNT(*) FROM vinculo_societario_saude_respostas").fetchone()[0]
    checks.append(
        check(
            count_respostas >= 12,
            "Q04B",
            "Existe base minima de documentos esperados na camada de respostas oficiais.",
            f"Esperado ao menos 12 itens na camada de respostas, encontrado {count_respostas}.",
        )
    )

    count_gate = con.execute("SELECT COUNT(*) FROM vinculo_societario_saude_gate").fetchone()[0]
    checks.append(
        check(
            count_gate == 1,
            "Q04C",
            "Existe exatamente 1 linha no gate operacional do caso.",
            f"Esperado 1 linha no gate operacional, encontrado {count_gate}.",
        )
    )

    insight_rows = con.execute(
        """
        SELECT kind, classe_achado, grau_probatorio, uso_externo
        FROM insight
        WHERE id IN (
            'vps_saude:13325100000130',
            'vps_saude_prof:13325100000130',
            'vps_saude_hist:13325100000130',
            'vps_saude_carga:13325100000130',
            'vps_func:13325100000130'
        )
        """
    ).fetchall()
    insight_map = {row[0]: row[1:] for row in insight_rows}

    checks.append(
        check(
            insight_map.get("QSA_VINCULO_SOCIETARIO_SAUDE_EXATO") == ("HIPOTESE_INVESTIGATIVA", "INDICIARIO", "REVISAO_INTERNA"),
            "Q05",
            "Insight societario basico permanece em revisao interna.",
            f"Insight societario basico inconsistente: {insight_map.get('QSA_VINCULO_SOCIETARIO_SAUDE_EXATO')}.",
        )
    )
    checks.append(
        check(
            insight_map.get("VINCULO_EXATO_CNES_PROFISSIONAL_SAUDE") == ("FATO_DOCUMENTAL", "DOCUMENTAL_CORROBORADO", "APTO_APURACAO"),
            "Q06",
            "Insight de profissional no CNES esta classificado como fato documental corroborado.",
            f"Insight profissional CNES inconsistente: {insight_map.get('VINCULO_EXATO_CNES_PROFISSIONAL_SAUDE')}.",
        )
    )
    checks.append(
        check(
            insight_map.get("VINCULO_EXATO_CNES_HISTORICO_PUBLICO_PRIVADO_SAUDE") == ("FATO_DOCUMENTAL", "DOCUMENTAL_PRIMARIO", "APTO_APURACAO"),
            "Q07",
            "Insight de historico CNES esta classificado corretamente.",
            f"Insight historico CNES inconsistente: {insight_map.get('VINCULO_EXATO_CNES_HISTORICO_PUBLICO_PRIVADO_SAUDE')}.",
        )
    )
    checks.append(
        check(
            insight_map.get("VINCULO_EXATO_CNES_CARGA_CONCOMITANTE_SAUDE") == ("FATO_DOCUMENTAL", "DOCUMENTAL_PRIMARIO", "APTO_APURACAO"),
            "Q08",
            "Insight de carga concomitante esta classificado corretamente.",
            f"Insight carga concomitante inconsistente: {insight_map.get('VINCULO_EXATO_CNES_CARGA_CONCOMITANTE_SAUDE')}.",
        )
    )
    checks.append(
        check(
            insight_map.get("APURACAO_FUNCIONAL_SAUDE_PRIORITARIA") == ("HIPOTESE_INVESTIGATIVA", "INDICIARIO", "REVISAO_INTERNA"),
            "Q09",
            "Insight de apuracao funcional permanece interno.",
            f"Insight de apuracao funcional inconsistente: {insight_map.get('APURACAO_FUNCIONAL_SAUDE_PRIORITARIA')}.",
        )
    )

    maturity = {
        row[0]: (row[1], row[2])
        for row in con.execute(
            "SELECT eixo, status_probatorio, uso_externo FROM vinculo_societario_saude_maturidade"
        ).fetchall()
    }
    checks.append(
        check(
            maturity.get("nepotismo") == ("SEM_BASE_ATUAL", "NAO_USAR_EXTERNAMENTE"),
            "Q10",
            "Eixo nepotismo esta corretamente bloqueado para uso externo.",
            f"Eixo nepotismo inconsistente: {maturity.get('nepotismo')}.",
        )
    )
    checks.append(
        check(
            maturity.get("fraude_penal") == ("SEM_BASE_ATUAL", "NAO_USAR_EXTERNAMENTE"),
            "Q11",
            "Eixo fraude penal esta corretamente bloqueado para uso externo.",
            f"Eixo fraude penal inconsistente: {maturity.get('fraude_penal')}.",
        )
    )
    checks.append(
        check(
            maturity.get("compatibilidade_horarios") in {
                ("PENDENTE_DOCUMENTO", "REVISAO_INTERNA"),
                ("DOCUMENTO_RECEBIDO_PENDENTE_ANALISE", "REVISAO_INTERNA"),
                ("COMPROVADO_DOCUMENTAL", "APTO_APURACAO"),
            },
            "Q12",
            "Compatibilidade de horarios esta em estado probatorio valido.",
            f"Eixo compatibilidade_horarios inconsistente: {maturity.get('compatibilidade_horarios')}.",
        )
    )

    socio_admin_count = con.execute(
        "SELECT json_array_length(socios_administradores_publicos_json) FROM vinculo_societario_saude_juridico"
    ).fetchone()[0]
    checks.append(
        check(
            socio_admin_count == 1,
            "Q13",
            "Existe 1 socio-administrador materializado na camada juridica.",
            f"Esperado 1 socio-administrador materializado, encontrado {socio_admin_count}.",
        )
    )

    carga = con.execute(
        """
        SELECT n_competencias_concomitantes_total, n_competencias_ge_60h, n_competencias_ge_80h, max_ch_total_concomitante
        FROM vinculo_societario_saude_followup
        """
    ).fetchone()
    checks.append(
        check(
            carga == (262, 234, 28, 100),
            "Q14",
            "Metricas de carga permanecem estaveis.",
            f"Metricas de carga divergentes: {carga}.",
        )
    )

    missing_response_files = con.execute(
        "SELECT COUNT(*) FROM vinculo_societario_saude_respostas WHERE status_documento = 'ARQUIVO_NAO_LOCALIZADO'"
    ).fetchone()[0]
    checks.append(
        check(
            missing_response_files == 0,
            "Q15",
            "Nao ha referencias quebradas na camada de respostas oficiais.",
            f"Ha {missing_response_files} referencia(s) quebrada(s) na camada de respostas oficiais.",
        )
    )

    gate = con.execute(
        """
        SELECT estagio_operacional, uso_recomendado, pode_uso_externo
        FROM vinculo_societario_saude_gate
        """
    ).fetchone()
    checks.append(
        check(
            gate == ("APTO_OFICIO_DOCUMENTAL", "PEDIDO_DOCUMENTAL", True),
            "Q16",
            "Gate operacional do caso esta em pedido documental, sem salto indevido para representacao.",
            f"Gate operacional inconsistente: {gate}.",
        )
    )

    con.close()

    expected_files = [
        "TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md",
        "TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_DOSSIE.md",
        "TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_DOSSIE.md",
        "TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS.md",
        "TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_DOSSIE.md",
        "TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_DOSSIE.md",
        "TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_DOSSIE.md",
        "nota_operacional_cedimp.txt",
        "cedimp_case_bundle_20260313.tar.gz",
    ]
    for idx, name in enumerate(expected_files, start=17):
        path = OUT_DIR / name
        checks.append(
            check(
                path.exists(),
                f"Q{idx:02d}",
                f"Arquivo esperado presente: {name}.",
                f"Arquivo esperado ausente: {name}.",
            )
        )

    report_lines = [
        "# Validação de Qualidade - Caso CEDIMP",
        "",
        "Relatório de consistência para evitar regressão probatória e extrapolação indevida.",
        "",
    ]
    for item in checks:
        report_lines.append(f"- `{item['code']}` / `{item['status']}` / {item['message']}")
    report = "\n".join(report_lines) + "\n"

    report_path = OUT_DIR / "VALIDACAO_QUALIDADE_CEDIMP.md"
    json_path = OUT_DIR / "VALIDACAO_QUALIDADE_CEDIMP.json"
    report_path.write_text(report, encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(),
                "checks": checks,
                "all_pass": all(item["status"] == "PASS" for item in checks),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"checks={len(checks)}")
    print(f"all_pass={all(item['status'] == 'PASS' for item in checks)}")
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
