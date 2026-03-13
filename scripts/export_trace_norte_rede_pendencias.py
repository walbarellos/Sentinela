from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
OUT_MD = OUT_DIR / "TRACE_NORTE_REDE_PENDENCIAS.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_REDE_PENDENCIAS_MANIFEST.json"
OUT_CSV = OUT_DIR / "trace_norte_rede_pendencias.csv"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute(
        """
        SELECT
            cnpj,
            fornecedor_nome,
            orgao,
            unidade_gestora,
            COUNT(*) AS n_contratos,
            SUM(valor_brl) AS total_brl,
            LIST(numero_contrato ORDER BY valor_brl DESC, numero_contrato) AS contratos,
            LIST(id_contrato ORDER BY valor_brl DESC, numero_contrato) AS ids_contrato
        FROM trace_norte_rede_vinculo_exato
        WHERE COALESCE(lic_numero, '') = ''
        GROUP BY ALL
        ORDER BY total_brl DESC, fornecedor_nome
        """
    ).fetchall()
    con.execute(
        f"""
        COPY (
            SELECT
                cnpj,
                fornecedor_nome,
                orgao,
                unidade_gestora,
                COUNT(*) AS n_contratos,
                SUM(valor_brl) AS total_brl,
                LIST(numero_contrato ORDER BY valor_brl DESC, numero_contrato) AS contratos,
                LIST(id_contrato ORDER BY valor_brl DESC, numero_contrato) AS ids_contrato
            FROM trace_norte_rede_vinculo_exato
            WHERE COALESCE(lic_numero, '') = ''
            GROUP BY ALL
            ORDER BY total_brl DESC, fornecedor_nome
        ) TO '{OUT_CSV.as_posix()}' (HEADER, DELIMITER ',')
        """
    )
    con.close()

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Trace NORTE - Pendências de Vínculo Exato",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este arquivo lista os blocos contratuais da rede TRACE_NORTE em que o portal de contratos já devolveu o registro bruto, mas o detalhe de licitação ainda não foi resolvido em `lic_ano/processo`.",
        "",
        f"- Blocos pendentes: `{len(rows)}`",
        "",
    ]
    for row in rows[:15]:
        parts.extend(
            [
                f"- `{row[1]}` (`{row[0]}`) | orgao `{row[2]}` | unidade `{row[3]}`",
                f"  contratos `{int(row[4] or 0)}` | total `{brl(row[5])}`",
                f"  numeros: {', '.join(str(x) for x in (row[6] or []))}",
            ]
        )
    OUT_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "inputs": ["trace_norte_rede_vinculo_exato"],
                "outputs": [
                    str(OUT_MD.relative_to(ROOT)),
                    str(OUT_JSON.relative_to(ROOT)),
                    str(OUT_CSV.relative_to(ROOT)),
                ],
                "pending_blocks": len(rows),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
