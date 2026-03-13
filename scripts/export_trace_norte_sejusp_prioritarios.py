from __future__ import annotations

import csv
import hashlib
import json
import math
import shutil
import tarfile
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
CASE_DIR = OUT_DIR / "trace_norte_sejusp"
RAW_DIR = CASE_DIR / "raw"
OUT_MD = OUT_DIR / "TRACE_NORTE_SEJUSP_PRIORITARIOS.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_SEJUSP_PRIORITARIOS_MANIFEST.json"
OUT_BLOCOS_CSV = OUT_DIR / "trace_norte_sejusp_blocos.csv"
OUT_DOCS_CSV = OUT_DIR / "trace_norte_sejusp_docs.csv"
OUT_AUDIT_CSV = OUT_DIR / "trace_norte_sejusp_audit.csv"
OUT_BUNDLE = OUT_DIR / "trace_norte_sejusp_bundle_20260313.tar.gz"


def brl(value: object) -> str:
    if value is None:
        return "n/d"
    number = float(value)
    if math.isnan(number):
        return "n/d"
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CASE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    blocos_df = con.execute("SELECT * FROM v_trace_norte_sejusp_blocos ORDER BY total_brl DESC, fornecedor_nome").fetchdf()
    docs_df = con.execute("SELECT * FROM v_trace_norte_sejusp_docs ORDER BY ano, doc_key").fetchdf()
    audit_df = con.execute("SELECT * FROM v_trace_norte_sejusp_audit ORDER BY numero_contrato").fetchdf()
    raw_current = con.execute(
        """
        SELECT
            fornecedor_nome, cnpj, unidade_gestora, numero_contrato, id_contrato,
            valor_brl, vigencia_inicial, vigencia_final, modalidade_contrato, raw_contrato_json
        FROM trace_norte_rede_vinculo_exato
        WHERE orgao = 'SEJUSP'
          AND (
                (cnpj = '21813150000194' AND numero_contrato = '076/2024')
             OR (cnpj = '04582979000104' AND unidade_gestora = 'SECRETARIA DE ESTADO DE JUSTIÇA E SEGURANÇA PÚBLICA – SEJUSP')
          )
        ORDER BY valor_brl DESC, numero_contrato
        """
    ).fetchall()
    con.close()

    blocos = blocos_df.to_dict(orient="records")
    docs = docs_df.to_dict(orient="records")
    audit_rows = audit_df.to_dict(orient="records")
    write_csv(OUT_BLOCOS_CSV, blocos)
    write_csv(OUT_DOCS_CSV, docs)
    write_csv(OUT_AUDIT_CSV, audit_rows)
    write_json(RAW_DIR / "sejusp_current_contracts.json", raw_current)

    manifest_files = []
    for doc in docs:
        for key in ["local_pdf", "local_txt"]:
            rel = str(doc.get(key) or "").strip()
            if not rel:
                continue
            src = ROOT / rel
            if src.exists():
                dst = CASE_DIR / Path(rel).name
                shutil.copy2(src, dst)
                manifest_files.append(dst)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Trace NORTE - SEJUSP Prioritários",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este dossiê isola o recorte `SEJUSP` dentro da trilha `NORTE`, separando dois eixos que não devem ser misturados:",
        "",
        "- `NORTE-CENTRO`: serviços terceirizados/apoio operacional com mão de obra.",
        "- `AGRO NORTE`: aquisições de viaturas e contratos de manutenção veicular.",
        "",
        "## Síntese atual do portal",
        "",
    ]
    for row in blocos:
        contratos = json.loads(str(row["contratos_json"]))
        parts.extend(
            [
                f"- `{row['fornecedor_nome']}` (`{row['cnpj']}`) | categoria `{row['categoria']}` | total `{brl(row['total_brl'])}` | contratos `{int(row['n_contratos'] or 0)}`",
                f"  unidade `{row['unidade_gestora']}`",
                f"  numeros: {', '.join(contratos)}",
            ]
        )

    norte_row = next((row for row in blocos if str(row["cnpj"]) == "21813150000194"), None)
    agro_row = next((row for row in blocos if str(row["cnpj"]) == "04582979000104"), None)

    if norte_row:
        parts.extend(
            [
                "",
                "## Bloco NORTE-CENTRO / SEJUSP",
                "",
                f"- Total atual no portal: `{brl(norte_row['total_brl'])}`",
                "- Contrato atual relevante: `076/2024`",
                "- Objeto atual no portal: `prestação de serviço terceirizado e continuado de apoio operacional e administrativo, com disponibilização de mão de obra em regime de dedicação exclusiva`",
                "- Situação do portal: `origem = C`, `modalidade = NÃO INFORMADO`, sem `id_licitacao` exposto",
                "- Base formal já fechada no DOE: `26/12/2024` conecta o `076/2024` à `ARP 04/2024`, ao `PP 053/2023` e ao processo `0819.014451.00277/2024-18`",
                "",
                "### Linha do tempo pública já congelada",
                "",
            ]
        )
        for doc in docs:
            if doc["categoria"] not in {"servico_terceirizado", "servicos_limpeza"}:
                continue
            if doc["fornecedor_nome"] and "AGRO" in str(doc["fornecedor_nome"]).upper():
                continue
            label = doc["tipo_documento"]
            if doc["numero_adesao"]:
                label += f" {doc['numero_adesao']}"
            if doc["numero_contrato"]:
                label += f" / contrato {doc['numero_contrato']}"
            line = f"- `{doc['ano']}` | `{label}` | fornecedor `{doc['fornecedor_nome']}`"
            if doc["lic_numero"]:
                line += f" | licitação `{doc['lic_numero']}`"
            if doc["valor_brl"] is not None:
                line += f" | valor `{brl(doc['valor_brl'])}`"
            parts.append(line)
            parts.append(f"  resumo: {doc['objeto_resumo']}")

    if agro_row:
        parts.extend(
            [
                "",
                "## Bloco AGRO NORTE / SEJUSP",
                "",
                f"- Total atual no portal: `{brl(agro_row['total_brl'])}`",
                f"- Contratos atuais: `{int(agro_row['n_contratos'] or 0)}`",
                "- Natureza do bloco: viaturas e caminhonetes, não terceirização de pessoal",
                "- Situação do portal: contratos publicados com `origem = C` e sem `id_licitacao` exposto",
                "",
                "### Atos formais já fechados",
                "",
                "- `004/2023` | processo `0853.013719.00030/2022-18` | `ARP 008/2022` | `PE 318/2022 - SEPA` | `R$ 250.000,00` | 1 caminhonete para o `NASP`",
                "- `151/2023` | processo `0819.012805.00066/2022-40` | `ARP 008/2022` | `PE 318/2022 - SEPA` | `R$ 250.000,00` | 1 caminhonete para o `CIEPS`",
                "- `082/2024` | processo `0819.016417.00053/2024-94` | extrato `R$ 733.491,00` | 3 caminhonetes para patrulhamento velado",
                "- `33/2024` | termo de adesão do mesmo bloco | `ARP 49/2023` | `PE 206/2023` | `R$ 733.491,00`",
                "- `147/2024` | processo `0064.014914.00006/2024-48` | `ARP 49/2023` | `PE 206/2023` | `R$ 977.988,00`",
                "- `69/2024` | termo de adesão do mesmo processo | `ARP 49/2023` | `PE 206/2023` | `R$ 2.200.473,00`",
                "- `149/2024` | processo `0819.012834.00145/2024-93` | portaria de gestão/fiscalização | recursos do `FNSP`",
                "",
                "### Auditoria portal x DOE",
                "",
            ]
        )
        for row in audit_rows:
            qty_note = ""
            if row.get("portal_qty") is not None and row.get("doc_qty") is not None:
                qty_note = f" | portal `{int(row['portal_qty'])}` x DOE `{int(row['doc_qty'])}`"
            parts.append(f"- `{row['numero_contrato']}` | status `{row['status']}`{qty_note}")
            parts.append(f"  observação: {row['observacao']}")

        parts.extend(
            [
                "",
                "### Contratos do bloco",
                "",
            ]
        )
        for item in raw_current:
            fornecedor_nome, cnpj, unidade, numero_contrato, id_contrato, valor_brl, vig_ini, vig_fim, modalidade_contrato, raw_json = item
            if str(cnpj) != "04582979000104":
                continue
            raw = json.loads(str(raw_json))
            objeto = " ".join(str(raw.get("objeto") or "").split())
            parts.extend(
                [
                    f"- `{numero_contrato}` | `{brl(valor_brl)}` | vigência `{vig_ini}` a `{vig_fim}`",
                    f"  objeto: {objeto}",
                ]
            )

    parts.extend(
        [
            "",
            "## Leitura técnica",
            "",
            "Este recorte fecha quatro conclusões objetivas.",
            "",
            "1. O eixo `NORTE-CENTRO / SEJUSP` é de serviços continuados com mão de obra e apoio operacional. O portal não expõe `id_licitacao` no card do `076/2024`, mas o DOE de `26/12/2024` já fecha o vínculo formal com `ARP 04/2024` e `PP 053/2023`.",
            "2. O eixo `AGRO NORTE / SEJUSP` ganhou fechamento documental adicional em `2023`: `004/2023` e `151/2023` agora estão amarrados a `ARP 008/2022` e `PE 318/2022 - SEPA`, além dos atos de `2024` já fechados.",
            "3. O contrato `151/2023` tem divergência objetiva entre o portal e o DOE: o portal publica objeto compatível com `10` caminhonetes, enquanto o extrato oficial materializa `1` caminhonete para o `CIEPS`, no mesmo valor de `R$ 250.000,00`.",
            "4. O recorte `SEJUSP` continua separado em dois problemas diferentes: terceirização/apoio (`NORTE-CENTRO`) e viaturas (`AGRO NORTE`). Misturar os dois reduziria a força probatória.",
            "",
            "## Próximo passo técnico",
            "",
            "- fechar o contrato `170/2023`, que ainda segue sem trilha formal local neste recorte;",
            "- expandir o mesmo método documental para `DETRAN` e `IAPEN`, que já aparecem com blocos altos do mesmo CNPJ;",
            "- consolidar depois o recorte `SEJUSP` em linha do tempo única por processo, ARP e fonte de recurso, incluindo a divergência documental do `151/2023`.",
            "",
            "## Arquivos congelados",
            "",
        ]
    )

    for path in sorted(CASE_DIR.iterdir()):
        if path.name == "raw":
            continue
        parts.append(f"- `trace_norte_sejusp/{path.name}`")
    parts.extend(
        [
            "- `trace_norte_sejusp/raw/sejusp_current_contracts.json`",
            "",
        ]
    )

    OUT_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")

    manifest_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inputs": [
            "trace_norte_sejusp_blocos",
            "trace_norte_sejusp_docs",
            "trace_norte_rede_vinculo_exato",
        ],
        "outputs": [
            str(OUT_MD.relative_to(ROOT)),
            str(OUT_JSON.relative_to(ROOT)),
            str(OUT_BLOCOS_CSV.relative_to(ROOT)),
            str(OUT_DOCS_CSV.relative_to(ROOT)),
            str(OUT_AUDIT_CSV.relative_to(ROOT)),
            str(CASE_DIR.relative_to(ROOT)),
            str(OUT_BUNDLE.relative_to(ROOT)),
        ],
    }

    if OUT_BUNDLE.exists():
        OUT_BUNDLE.unlink()
    with tarfile.open(OUT_BUNDLE, "w:gz") as tar:
        for path in [OUT_MD, OUT_BLOCOS_CSV, OUT_DOCS_CSV, OUT_AUDIT_CSV]:
            if path.exists():
                tar.add(path, arcname=path.relative_to(OUT_DIR.parent))
        tar.add(CASE_DIR, arcname=CASE_DIR.relative_to(OUT_DIR.parent))

    manifest_payload["bundle_sha256"] = sha256_file(OUT_BUNDLE)
    OUT_JSON.write_text(
        json.dumps(
            manifest_payload,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
