from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.cross_reference_engine import (  # noqa: E402
    DETECTORS,
    INTERNAL_ONLY_DEFAULT,
    LEGACY_INTERNAL_USAGE,
)


def main() -> int:
    detector_set = set(DETECTORS)
    internal_set = set(INTERNAL_ONLY_DEFAULT)
    missing = sorted(detector_set - internal_set)
    extra_unknown = sorted(set(INTERNAL_ONLY_DEFAULT) - set(DETECTORS))
    print(f"legacy_detectors={len(DETECTORS)}")
    print(f"internal_only_default={len(INTERNAL_ONLY_DEFAULT)}")
    print(f"legacy_usage={LEGACY_INTERNAL_USAGE}")
    if missing:
        print(f"detectors_not_internal_only={missing}")
        return 2
    if extra_unknown:
        print(f"unknown_internal_only={extra_unknown}")
        return 2
    if LEGACY_INTERNAL_USAGE != "REVISAO_INTERNA":
        print("legacy_usage_must_be_revisao_interna")
        return 2
    print("cross_reference_engine_guard=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
