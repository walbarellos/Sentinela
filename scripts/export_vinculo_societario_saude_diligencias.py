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


def fmt_brl(value: object) -> str:
    number = float(value or 0)
    text = f"{number:,.2f}"
    return "R$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_one(con: duckdb.DuckDBPyConnection, query: str) -> dict:
    df = con.execute(query).fetchdf()
    if df.empty:
        return {}
    return json.loads(df.to_json(orient="records", force_ascii=False))[0]


def build_requests(case: dict, juridico: dict, funcional: dict) -> dict[str, list[str]]:
    resumo = json.loads(funcional.get("resumo_profissionais_json") or "[]")

    semsa = [
        "Ficha funcional completa dos servidores identificados, com historico de lotacoes e unidades.",
        "Declaracoes de acumulacao entregues pelos servidores e eventuais atualizacoes.",
        "Processos, despachos ou pareceres de compatibilidade de horarios/atividades.",
        "Escalas, plantões, folhas de ponto ou controles equivalentes nas competencias de maior carga documental no CNES.",
        "Informacao formal sobre eventual autorizacao para exercicio de gerencia/administracao societaria, se existente.",
    ]
    sesacre = [
        f"Processo integral do contrato {case.get('contrato_numero')}, com termo, anexos, fiscais e justificativas.",
        "Documentos de habilitacao e qualificacao da empresa no contrato.",
        "Registros de execucao, medicao e glosas do contrato.",
        "Eventuais documentos que identifiquem os profissionais vinculados a execucao do contrato e sua carga declarada.",
    ]
    controle = [
        "Confrontar historico oficial do CNES com a ficha funcional municipal e com a execucao do contrato estadual.",
        "Verificar se a condicao de socio-administrador se enquadra nas restricoes do art. 107, X, da Lei Municipal 1.794/2009.",
        "Verificar se a diferenca entre base local e CNES indica outro vinculo publico, cadastro incompleto ou incompatibilidade funcional.",
    ]

    for item in resumo:
        semsa.append(
            f"Para {item['nome']}: esclarecer a divergencia entre carga local `{item['ch_publica_local']}h` e carga publica maxima no CNES `{item['max_ch_publica_cnes']}h`."
        )
        controle.append(
            f"Para {item['nome']}: revisar a competencia pico `{item['competencia_pico']}` com total documental `{item['max_ch_total_concomitante']}h`."
        )

    return {
        "SEMSA_RH": semsa,
        "SESACRE": sesacre,
        "CONTROLE": controle,
    }


def build_md(case: dict, juridico: dict, funcional: dict, requests_by_target: dict[str, list[str]]) -> str:
    resumo = json.loads(funcional.get("resumo_profissionais_json") or "[]")
    lines: list[str] = []
    lines.append("# Trace Vinculo Societario em Saude - Diligencias Dirigidas")
    lines.append("")
    lines.append("Pacote de requisicoes objetivas para aprofundar o caso sem extrapolar a prova atual.")
    lines.append("")
    lines.append("## Caso")
    lines.append("")
    lines.append(f"- empresa: `{case['razao_social']}`")
    lines.append(f"- CNPJ: `{case['cnpj']}`")
    lines.append(f"- contrato: `{case['contrato_numero']}` / `{case['contrato_orgao']}` / `{fmt_brl(case['contrato_valor_brl'])}`")
    lines.append(f"- score funcional interno: `{funcional['score_triagem']}` / prioridade `{funcional['prioridade']}`")
    lines.append("")
    lines.append("## Ponto ja documentado")
    lines.append("")
    lines.append(f"- `262` competencias concomitantes no historico oficial do `CNES`.")
    lines.append(f"- `234` competencias com `>=60h` e `28` com `>=80h`.")
    lines.append(f"- pico documental de `100h`.")
    for item in resumo:
        lines.append(
            f"- `{item['nome']}`: base local `{item['ch_publica_local']}h` x CNES publico max `{item['max_ch_publica_cnes']}h` x empresa max `{item['max_ch_empresa_cnes']}h`."
        )
    lines.append("")
    lines.append("## Requisicoes por destino")
    lines.append("")
    for target, items in requests_by_target.items():
        lines.append(f"### {target}")
        lines.append("")
        for item in items:
            lines.append(f"- {item}")
        lines.append("")
    lines.append("## Limite")
    lines.append("")
    lines.append("- O pacote e voltado a apuracao funcional e documental.")
    lines.append("- Nao afirma ilegalidade, nepotismo ou conflito vedado por si so.")
    return "\n".join(lines)


def build_txt(case: dict, target: str, items: list[str]) -> str:
    lines: list[str] = []
    lines.append(f"Assunto: solicitacao objetiva de documentos para apuracao funcional - {case['razao_social']} ({case['cnpj']})")
    lines.append("")
    lines.append(
        f"No contexto de verificacao funcional e documental relacionada ao contrato {case['contrato_numero']} "
        f"da {case['contrato_orgao']}, solicita-se a disponibilizacao dos seguintes itens:"
    )
    lines.append("")
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. {item}")
    lines.append("")
    lines.append("Observacao: a solicitacao tem natureza de apuracao preliminar e nao pressupoe conclusao previa de ilegalidade.")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    case = load_one(con, "SELECT * FROM v_vinculo_societario_saude_followup LIMIT 1")
    juridico = load_one(con, "SELECT * FROM v_vinculo_societario_saude_juridico LIMIT 1")
    funcional = load_one(con, "SELECT * FROM v_vinculo_societario_saude_apuracao_funcional LIMIT 1")
    con.close()

    if not case or not juridico or not funcional:
        print("rows=0")
        return 0

    requests_by_target = build_requests(case, juridico, funcional)

    csv_rows = [
        {"destino": destino, "ordem": idx, "item": item}
        for destino, items in requests_by_target.items()
        for idx, item in enumerate(items, start=1)
    ]

    csv_path = OUT_DIR / "trace_vinculo_societario_saude_diligencias.csv"
    dossier_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS.md"
    manifest_path = OUT_DIR / "TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS_MANIFEST.json"
    semsa_txt = OUT_DIR / "pedido_preliminar_semsa_cedimp.txt"
    sesacre_txt = OUT_DIR / "pedido_preliminar_sesacre_cedimp.txt"

    write_csv(csv_path, csv_rows)
    dossier_path.write_text(build_md(case, juridico, funcional, requests_by_target), encoding="utf-8")
    semsa_txt.write_text(build_txt(case, "SEMSA_RH", requests_by_target["SEMSA_RH"]), encoding="utf-8")
    sesacre_txt.write_text(build_txt(case, "SESACRE", requests_by_target["SESACRE"]), encoding="utf-8")

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "rows": len(csv_rows),
        "files": {
            csv_path.name: sha256_file(csv_path),
            dossier_path.name: sha256_file(dossier_path),
            semsa_txt.name: sha256_file(semsa_txt),
            sesacre_txt.name: sha256_file(sesacre_txt),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"rows={len(csv_rows)}")
    print(f"dossier={dossier_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
