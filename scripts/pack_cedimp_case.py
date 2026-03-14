from __future__ import annotations

import hashlib
import json
import tarfile
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = (
    ROOT
    / "docs"
    / "Claude-march"
    / "patch_claude"
    / "claude_update"
    / "patch"
    / "entrega_denuncia_atual"
)


FILES = [
    "TRACE_VINCULO_SOCIETARIO_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_MANIFEST.json",
    "trace_vinculo_societario_resumo.csv",
    "trace_vinculo_societario_matches.csv",
    "trace_vinculo_societario_contratos.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_MANIFEST.json",
    "trace_vinculo_societario_saude_followup.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_JURIDICO_MANIFEST.json",
    "trace_vinculo_societario_saude_juridico.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_APURACAO_FUNCIONAL_MANIFEST.json",
    "trace_vinculo_societario_saude_apuracao_funcional.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_DILIGENCIAS_MANIFEST.json",
    "trace_vinculo_societario_saude_diligencias.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_MATURIDADE_MANIFEST.json",
    "trace_vinculo_societario_saude_maturidade.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_RESPOSTAS_MANIFEST.json",
    "trace_vinculo_societario_saude_respostas.csv",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_DOSSIE.md",
    "TRACE_VINCULO_SOCIETARIO_SAUDE_GATE_MANIFEST.json",
    "trace_vinculo_societario_saude_gate.csv",
    "nota_operacional_cedimp.txt",
    "pedido_preliminar_semsa_cedimp.txt",
    "pedido_preliminar_sesacre_cedimp.txt",
    "cedimp_respostas/README.md",
    "cedimp_respostas/cedimp_respostas_index.csv",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    bundle_path = OUT_DIR / "cedimp_case_bundle_20260313.tar.gz"
    manifest_path = OUT_DIR / "CEDIMP_CASE_BUNDLE_MANIFEST.json"

    existing = [OUT_DIR / name for name in FILES if (OUT_DIR / name).exists()]
    with tarfile.open(bundle_path, "w:gz") as tar:
        for path in existing:
            tar.add(path, arcname=str(path.relative_to(OUT_DIR)))

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "bundle": bundle_path.name,
        "bundle_sha256": sha256_file(bundle_path),
        "files": {str(path.relative_to(OUT_DIR)): sha256_file(path) for path in existing},
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"files={len(existing)}")
    print(f"bundle={bundle_path}")
    print(f"sha256={manifest['bundle_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
