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
    "relato_apuracao_3898.txt",
    "relatorio_final_acre_rio_branco_sus.md",
    "rb_sus_respostas/contrato_3898/README.md",
    "rb_sus_respostas/contrato_3898/rb_sus_respostas_index.csv",
    "ops_exports/rb_contrato_3898/20260314T021803__noticia_fato.txt",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    bundle_path = OUT_DIR / "rb_contrato_3898_bundle_20260314.tar.gz"
    manifest_path = OUT_DIR / "RB_CONTRATO_3898_BUNDLE_MANIFEST.json"

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
