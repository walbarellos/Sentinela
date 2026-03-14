from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import shutil
import tarfile
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sentinela_analytics.duckdb"
OUT_DIR = ROOT / "docs" / "Claude-march" / "patch_claude" / "claude_update" / "patch" / "entrega_denuncia_atual"
CASE_DIR = OUT_DIR / "trace_agro_unidades"
OUT_MD = OUT_DIR / "TRACE_AGRO_UNIDADES_FOLLOWUP.md"
OUT_JSON = OUT_DIR / "TRACE_AGRO_UNIDADES_FOLLOWUP_MANIFEST.json"
OUT_RESUMO_CSV = OUT_DIR / "trace_agro_unidades_resumo.csv"
OUT_CONTRATOS_CSV = OUT_DIR / "trace_agro_unidades_followup.csv"
OUT_DOCS_CSV = OUT_DIR / "trace_agro_unidades_docs.csv"
OUT_AUDIT_CSV = OUT_DIR / "trace_agro_unidades_audit.csv"
OUT_BUNDLE = OUT_DIR / "trace_agro_unidades_bundle_20260313.tar.gz"


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
    con = duckdb.connect(str(DB_PATH), read_only=True)
    resumo_df = con.execute(
        """
        SELECT *
        FROM v_trace_agro_unidades_resumo
        ORDER BY total_brl DESC, cluster_key
        """
    ).fetchdf()
    contratos_df = con.execute(
        """
        SELECT *
        FROM v_trace_agro_unidades_followup
        ORDER BY cluster_key, valor_brl DESC, ano DESC, numero_contrato
        """
    ).fetchdf()
    docs_df = con.execute(
        """
        SELECT *
        FROM v_trace_agro_unidades_docs
        ORDER BY cluster_key, numero_contrato, doc_key
        """
    ).fetchdf()
    audit_df = con.execute(
        """
        SELECT *
        FROM v_trace_agro_unidades_audit
        ORDER BY cluster_key, numero_contrato, audit_kind
        """
    ).fetchdf()
    con.close()

    resumo = resumo_df.to_dict(orient="records")
    contratos = contratos_df.to_dict(orient="records")
    docs = docs_df.to_dict(orient="records")
    audits = audit_df.to_dict(orient="records")
    write_csv(OUT_RESUMO_CSV, resumo)
    write_csv(OUT_CONTRATOS_CSV, contratos)
    write_csv(OUT_DOCS_CSV, docs)
    write_csv(OUT_AUDIT_CSV, audits)

    copied = set()
    for doc in docs:
        for key in ["local_txt", "local_pdf"]:
            rel = str(doc.get(key) or "").strip()
            if not rel:
                continue
            src = ROOT / rel
            if src.exists():
                dst = CASE_DIR / Path(rel).name
                if dst.name not in copied:
                    shutil.copy2(src, dst)
                    copied.add(dst.name)
        if not str(doc.get("local_txt") or "").strip() and not str(doc.get("local_pdf") or "").strip():
            snapshot = CASE_DIR / f"{doc['doc_key']}.json"
            write_json(snapshot, doc)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [
        "# Trace AGRO - unidades prioritárias",
        "",
        f"Gerado em: `{generated_at}`",
        "",
        "Este pacote isola a expansão da `AGRO NORTE` para o eixo de frota pública fora do bloco principal já trabalhado da `SEJUSP`, com foco em `DETRAN` e `execução penal/socioeducativa`.",
        "",
        "## Resumo dos blocos",
        "",
    ]
    for row in resumo:
        unidades = ", ".join(json.loads(str(row["unidades_json"])))
        contratos_json = ", ".join(json.loads(str(row["contratos_json"])))
        sem_id_refs = json.loads(str(row["sem_id_licitacao_json"]))
        sem_id_text = ", ".join(ref["numero_contrato"] for ref in sem_id_refs) if sem_id_refs else "nenhum"
        parts.extend(
            [
                f"- `{row['cluster_label']}` | total `{brl(row['total_brl'])}` | contratos `{int(row['n_contratos'] or 0)}` | aquisição `{brl(row['aquisicao_brl'])}` | manutenção `{brl(row['manutencao_brl'])}`",
                f"  unidades: {unidades}",
                f"  contratos: {contratos_json}",
                f"  sem `id_licitacao` exposto no portal: {sem_id_text}",
            ]
        )

    detran = next((row for row in resumo if row["cluster_key"] == "DETRAN_FROTA"), None)
    exec_penal = next((row for row in resumo if row["cluster_key"] == "EXECUCAO_PENAL_FROTA"), None)

    if detran:
        parts.extend(
            [
                "",
                "## Bloco DETRAN",
                "",
                f"- Total materializado: `{brl(detran['total_brl'])}`",
                f"- Aquisição principal sem `id_licitacao` exposto: `022/2023` (`{brl(2296950.0)}`)",
                f"- Manutenção/revisão posterior no mesmo fornecedor: `{brl(detran['manutencao_brl'])}`",
                "- Ato formal local já fechado: DOE de `20/04/2023` com extrato do contrato `022/2023`, processo `0068.008553.00042/2023-71`, `6` unidades e valor unitário `R$ 382.825,00`.",
                "- Leitura operacional: o portal expõe a compra principal sem origem licitatória útil no card, mas a cadeia posterior de revisão/manutenção permanece com o mesmo CNPJ e já referencia `6` veículos L200 no contrato `071/2023`.",
                "",
            ]
        )
        for row in [item for item in contratos if item["cluster_key"] == "DETRAN_FROTA"]:
            parts.extend(
                [
                    f"- `{row['numero_contrato']}` | `{brl(row['valor_brl'])}` | categoria `{row['categoria_contrato']}` | vigência `{row['data_inicio_vigencia']}` a `{row['data_fim_vigencia']}`",
                    f"  objeto: {row['objeto']}",
                ]
            )

    if exec_penal:
        parts.extend(
            [
                "",
                "## Bloco execução penal / socioeducativo",
                "",
                f"- Total materializado: `{brl(exec_penal['total_brl'])}`",
                "- Contratos centrais sem `id_licitacao` exposto no portal: `038/2023`, `073/2023`, `072/2024`",
                "- Atos formais locais já fechados:",
                "  `FUNPENACRE/IAPEN`: termo de adesão `5/2023/IAPEN` e contrato exato `038/2023`, ambos no processo `4005.014135.00006/2023-90`, `PE SRP 74/2022`, `ARP TJ/AC 304/2022`, total `R$ 1.330.000,00`, com `5` caminhonetes a `R$ 266.000,00` por unidade.",
                "  `IAPEN`: contrato exato `073/2023` no DOE de `04/08/2023`, termo de adesão `26/2023/IAPEN`, processo `4005.014141.00047/2023-70`, `PE SRP 258/2022`, `ARP 010/2022-SEPA`, `1` caminhonete e `R$ 254.000,00`; o card estadual (`id_contrato 82564`) corrobora a mesma CIAP / Convênio `905916/2020 MJ/DEPEN`.",
                "  `ISE`: extrato do contrato `072/2024`, termo de adesão `4/2024/ISE`, `PE SRP 504/2023`, `ARP 01/2024-SECOM`, processo `4025.013665.00067/2024-11`, `10` unidades, `R$ 248.400,00` por unidade e `R$ 2.480.000,00` no DOE.",
                "- Leitura operacional: a AGRO aparece em sequência alta de aquisições de caminhonete no eixo `FUNPENACRE / IAPEN / ISE`. O `038/2023` e o `073/2023` já passaram de rastro parcial para contrato exato com origem formal publicada; o `072/2024` também está fechado no DOE, mas mantém divergência nominal entre portal e extrato oficial.",
                "",
            ]
        )
        for row in [item for item in contratos if item["cluster_key"] == "EXECUCAO_PENAL_FROTA"]:
            parts.extend(
                [
                    f"- `{row['numero_contrato']}` | unidade `{row['unidade_gestora']}` | `{brl(row['valor_brl'])}` | categoria `{row['categoria_contrato']}`",
                    f"  objeto: {row['objeto']}",
                ]
            )

    parts.extend(
        [
            "",
            "## Atos formais já fechados",
            "",
        ]
    )
    for doc in docs:
        numero = doc["numero_contrato"] or doc["numero_adesao"]
        valor_total = doc.get("valor_total_brl")
        valor = f" | valor `{brl(valor_total)}`" if valor_total is not None else ""
        lic = f" | licitação `{doc['licitacao']}`" if doc["licitacao"] else ""
        arp = f" | ARP `{doc['ata_registro_precos']}`" if doc["ata_registro_precos"] else ""
        qtd = f" | qtd `{int(doc['quantidade'])}`" if doc.get("quantidade") is not None else ""
        unit = f" | unitário `{brl(doc['valor_unitario_brl'])}`" if doc.get("valor_unitario_brl") is not None else ""
        parts.extend(
            [
                f"- `{doc['doc_key']}` | referência `{numero}` | tipo `{doc['tipo_documento']}`{lic}{arp}{qtd}{unit}{valor}",
                f"  processo: {doc['processo']}",
                f"  resumo: {doc['objeto_resumo']}",
            ]
        )

    parts.extend(
        [
            "",
            "## Auditoria formal",
            "",
        ]
    )
    for row in audits:
        parts.extend(
            [
                f"- `{row['numero_contrato']}` | auditoria `{row['audit_kind']}` | status `{row['status']}`",
                f"  observação: {row['observacao']}",
            ]
        )

    parts.extend(
        [
            "",
            "## Leitura técnica",
            "",
            "1. `DETRAN` já permite rastreio mais forte de cadeia contratual: o DOE do `022/2023` materializa `6` viaturas, e o `071/2023` já leva para manutenção `6` veículos L200 no mesmo fornecedor. Isso não fecha placas, mas fecha compatibilidade quantitativa relevante.",
            "2. `FUNPENACRE / IAPEN / ISE` formam um segundo eixo objetivo de expansão da AGRO em veículos utilitários ligados à execução penal e ao socioeducativo. O `038/2023` e o `073/2023` já estão fechados como contratos exatos publicados; o `072/2024` também já está fechado no DOE.",
            "3. O contrato `072/2024` agora tem divergência nominal objetiva entre portal e DOE: `R$ 2.484.000,00` no portal contra `R$ 2.480.000,00` no extrato oficial, com `10` unidades no DOE.",
            "4. O valor analítico deste pacote é separar contratos de frota/viatura dos casos de terceirização; são problemas diferentes e pedem linhas de auditoria diferentes.",
            "",
            "## Próximo passo técnico",
            "",
            "- manter `038/2023` e `073/2023` como blocos formalmente resolvidos e usar esse fechamento como referência de consistência para o eixo `FUNPENACRE / IAPEN`;",
            "- manter `022/2023` do `DETRAN` e `072/2024` do `ISE` como blocos suficientemente fechados para o pacote atual;",
            "- voltar ao `170/2023` da SEJUSP/PMAC, porque o buraco restante relevante agora ficou isolado na origem `ARP/pregão/extrato` desse contrato.",
            "",
        ]
    )

    OUT_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")

    manifest_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inputs": [
            "estado_ac_contratos",
            "v_trace_norte_rede_sem_licitacao",
            "trace_agro_unidades_followup",
            "trace_agro_unidades_resumo",
        ],
        "outputs": [
            str(OUT_MD.relative_to(ROOT)),
            str(OUT_JSON.relative_to(ROOT)),
            str(OUT_RESUMO_CSV.relative_to(ROOT)),
            str(OUT_CONTRATOS_CSV.relative_to(ROOT)),
            str(OUT_DOCS_CSV.relative_to(ROOT)),
            str(OUT_AUDIT_CSV.relative_to(ROOT)),
            str(CASE_DIR.relative_to(ROOT)),
            str(OUT_BUNDLE.relative_to(ROOT)),
        ],
    }

    if OUT_BUNDLE.exists():
        OUT_BUNDLE.unlink()
    with tarfile.open(OUT_BUNDLE, "w:gz") as tar:
        for path in [OUT_MD, OUT_RESUMO_CSV, OUT_CONTRATOS_CSV, OUT_DOCS_CSV, OUT_AUDIT_CSV]:
            if path.exists():
                tar.add(path, arcname=path.relative_to(OUT_DIR.parent))
        tar.add(CASE_DIR, arcname=CASE_DIR.relative_to(OUT_DIR.parent))

    manifest_payload["bundle_sha256"] = sha256_file(OUT_BUNDLE)
    write_json(OUT_JSON, manifest_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
