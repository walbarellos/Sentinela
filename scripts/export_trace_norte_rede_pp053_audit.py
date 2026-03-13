from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
CASE_DIR = OUT_DIR / "trace_norte_pp053"
OUT_MD = OUT_DIR / "TRACE_NORTE_PP053_DOSSIE.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_PP053_MANIFEST.json"
OUT_CSV = OUT_DIR / "trace_norte_pp053_audit.csv"
RAW_DIR = CASE_DIR / "raw"
LOCAL_DOE_PDF = ROOT / "data" / "tmp" / "pp053" / "doe_homologacao_pp053_2023.pdf"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CASE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    row = con.execute("SELECT * FROM trace_norte_rede_pp053_audit").fetchone()
    cols = [desc[0] for desc in con.description]
    if not row:
        raise SystemExit("trace_norte_rede_pp053_audit vazio")
    data = dict(zip(cols, row))
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM v_trace_norte_rede_pp053_audit
        ) TO '{OUT_CSV.as_posix()}' (HEADER, DELIMITER ',')
        """
    )
    con.close()

    shutil.copy2(LOCAL_DOE_PDF, CASE_DIR / "doe_homologacao_pp053_2023.pdf")
    (CASE_DIR / "doe_homologacao_pp053_2023_excerpt.txt").write_text(
        str(data["doe_excerpt"]) + "\n",
        encoding="utf-8",
    )
    write_json(RAW_DIR / "pp053_audit_row.json", data)
    write_json(RAW_DIR / "pp053_contracts_ids.json", json.loads(str(data["contracts_ids_json"])))
    write_json(RAW_DIR / "pp053_aditivos.json", json.loads(str(data["aditivos_json"])))
    write_json(RAW_DIR / "pp053_evidence.json", json.loads(str(data["evidence_json"])))

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Trace NORTE - PP 053/2023 (SEPLAN)",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este dossiê consolida a divergência documental entre o vencedor homologado do `PP 053/2023` e o fornecedor que aparece nos contratos de 2024 ligados ao mesmo `id_licitacao` no portal estadual.",
        "",
        "## Síntese",
        "",
        f"- Licitação: `PP {data['lic_numero']}/{data['lic_ano']}`",
        f"- Processo administrativo: `{data['lic_processo_adm']}`",
        f"- Modalidade: `{data['lic_modalidade']}`",
        f"- Vencedor homologado no DOE: `{data['vencedor_doe_nome']}` (`{data['vencedor_doe_cnpj']}`)",
        f"- Valor homologado: `{brl(data['valor_homologado_brl'])}`",
        f"- Fornecedor que aparece nos contratos: `{data['contracts_fornecedor']}` (`{data['contracts_cnpj']}`)",
        f"- Contratos ligados no portal: `{int(data['contracts_n'] or 0)}`",
        f"- Valor total dos contratos ligados: `{brl(data['contracts_total_brl'])}`",
        f"- Delta entre contratos atuais e homologação: `{brl(data['delta_brl'])}`",
        f"- Status materializado: `{data['status']}`",
        "",
        "## Trecho do DOE",
        "",
        data["doe_excerpt"],
        "",
        "## Leitura técnica",
        "",
        "O DOE oficial homologa o certame para `NORTE COMÉRCIO E SERVIÇOS`, CNPJ `21.813.150/0001-94`. O portal de contratos, para o mesmo `id_licitacao = 27462`, materializa 5 contratos de 2024 em nome de `CENTRAL NORTE`, CNPJ `36.990.588/0001-15`.",
        "",
        "Isso não prova, por si só, irregularidade. Mas prova uma divergência documental objetiva entre `vencedor homologado` e `fornecedor contratual` que precisa ser explicada por cessão, sucessão, subcontratação admitida, erro de cadastro ou outro ato formal equivalente.",
        "",
        "## Aditivos",
        "",
    ]

    aditivos = json.loads(str(data["aditivos_json"]))
    total_aditivos = sum(len(v or []) for v in aditivos.values())
    parts.append(f"- Total de aditivos capturados pelo portal para este bloco: `{total_aditivos}`")
    for contract_id, items in aditivos.items():
        for item in items:
            parts.extend(
                [
                    f"- contrato portal id `{contract_id}` | aditivo `{item.get('numero_aditivo')}` | publicação `{item.get('data_publicacao')}`",
                    f"  processo `{item.get('numero_processo_administrativo')}`",
                ]
            )
    parts.extend(
        [
            "",
            "## Arquivos congelados",
            "",
            "- `trace_norte_pp053/doe_homologacao_pp053_2023.pdf`",
            "- `trace_norte_pp053/doe_homologacao_pp053_2023_excerpt.txt`",
            "- `trace_norte_pp053/raw/pp053_audit_row.json`",
            "- `trace_norte_pp053/raw/pp053_contracts_ids.json`",
            "- `trace_norte_pp053/raw/pp053_aditivos.json`",
            "- `trace_norte_pp053/raw/pp053_evidence.json`",
            "",
        ]
    )

    OUT_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "inputs": [
                    "trace_norte_rede_pp053_audit",
                    "trace_norte_rede_vinculo_exato",
                ],
                "outputs": [
                    str(OUT_MD.relative_to(ROOT)),
                    str(OUT_JSON.relative_to(ROOT)),
                    str(OUT_CSV.relative_to(ROOT)),
                    str(CASE_DIR.relative_to(ROOT)),
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
