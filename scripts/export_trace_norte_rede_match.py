from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
OUT_MD = OUT_DIR / "TRACE_NORTE_REDE_MATCH_DOSSIE.md"
OUT_JSON = OUT_DIR / "TRACE_NORTE_REDE_MATCH_MANIFEST.json"
OUT_CSV = OUT_DIR / "trace_norte_rede_match_best.csv"


def brl(value: object) -> str:
    number = float(value or 0)
    integer, decimal = f"{number:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"R$ {integer},{decimal}"


def confidence_label(score: float) -> str:
    if score >= 0.60:
        return "forte"
    if score >= 0.45:
        return "medio"
    return "fraco"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute(
        """
        SELECT
            contrato_fornecedor, contrato_cnpj, contrato_orgao, contrato_unidade_gestora,
            contrato_numero, contrato_valor_brl, lic_numero_processo, lic_modalidade,
            lic_orgao, lic_unidade_gestora, lic_data_abertura, score_total, score_objeto, lic_objeto
        FROM v_trace_norte_rede_match_best
        ORDER BY score_total DESC, contrato_valor_brl DESC
        """
    ).fetchall()
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM v_trace_norte_rede_match_best
            ORDER BY score_total DESC, contrato_valor_brl DESC
        ) TO '{OUT_CSV.as_posix()}' (HEADER, DELIMITER ',')
        """
    )
    con.close()

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Trace NORTE - Match Contrato x Licitacao",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este arquivo lista a melhor licitacao candidata para cada contrato dos dois leads prioritarios de terceirizacao.",
        "",
        f"- Contratos com melhor match materializado: `{len(rows)}`",
        "",
    ]
    for row in rows:
        parts.extend(
            [
                f"- `{row[0]}` (`{row[1]}`) | contrato `{row[4]}` | orgao `{row[2]}` | valor `{brl(row[5])}`",
                f"  licitacao candidata processo `{row[6]}` | modalidade `{row[7]}` | unidade `{row[9]}` | abertura `{row[10]}` | score `{row[11]:.3f}` | forca `{confidence_label(float(row[11] or 0))}`",
                f"  score_objeto `{row[12]:.3f}`",
                f"  objeto licitacao: {row[13]}",
            ]
        )
    OUT_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "inputs": ["trace_norte_rede_contratos", "estado_ac_licitacoes", "trace_norte_rede_match", "v_trace_norte_rede_match_best"],
                "outputs": [
                    str(OUT_MD.relative_to(ROOT)),
                    str(OUT_JSON.relative_to(ROOT)),
                    str(OUT_CSV.relative_to(ROOT)),
                ],
                "matches": len(rows),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
