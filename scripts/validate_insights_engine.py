from __future__ import annotations

import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from insights_engine import (
    generate_insights_for_diarias,
    generate_insights_for_obras,
    generate_insights_for_servidores,
)


def _sample_servidores() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for i in range(30):
        rows.append(
            {
                "servidor": f"{1000 + i}-SERVIDOR {i}",
                "cargo": "MEDICO",
                "ch": 40,
                "vinculo": "CONCURSADO",
                "salario_liquido": 10_000.0,
                "imposto_de_renda": 0.0,
            }
        )
    rows.append(
        {
            "servidor": "9999-SERVIDOR OUTLIER",
            "cargo": "MEDICO",
            "ch": 40,
            "vinculo": "CONCURSADO",
            "salario_liquido": 40_000.0,
            "imposto_de_renda": 3_000.0,
        }
    )
    return pd.DataFrame(rows)


def _sample_diarias() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": i,
                "servidor_nome": f"Servidor {i}",
                "destino": "Brasilia",
                "data_saida": pd.Timestamp("2026-03-01"),
                "data_retorno": pd.Timestamp("2026-03-03"),
                "motivo": "Reuniao institucional",
                "valor": 1500.0,
            }
            for i in range(4)
        ]
    )


def _sample_obras() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for i in range(8):
        rows.append(
            {
                "id": i,
                "secretaria": "SEINFRA",
                "empresa_nome": "EMPRESA DOMINANTE",
                "valor_total": 100_000.0,
            }
        )
    for i in range(8, 12):
        rows.append(
            {
                "id": i,
                "secretaria": "SEINFRA",
                "empresa_nome": "EMPRESA SECUNDARIA",
                "valor_total": 10_000.0,
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    serv = _sample_servidores()
    dia = _sample_diarias()
    obras = _sample_obras()

    default_rows = {
        "servidores": len(generate_insights_for_servidores(serv)),
        "diarias": len(generate_insights_for_diarias(dia)),
        "obras": len(generate_insights_for_obras(obras)),
    }
    internal_rows = {
        "servidores": len(generate_insights_for_servidores(serv, allow_internal=True)),
        "diarias": len(generate_insights_for_diarias(dia, allow_internal=True)),
        "obras": len(generate_insights_for_obras(obras, allow_internal=True)),
    }

    print(f"default_guard={default_rows}")
    print(f"internal_opt_in={internal_rows}")

    assert default_rows == {"servidores": 0, "diarias": 0, "obras": 0}
    assert internal_rows["servidores"] >= 1
    assert internal_rows["diarias"] >= 1
    assert internal_rows["obras"] >= 1

    print("insights_engine_guard=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
