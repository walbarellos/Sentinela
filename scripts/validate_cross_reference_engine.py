from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.cross_reference_engine import (  # noqa: E402
    DETECTORS,
    DETECTOR_STATUS,
    INTERNAL_ONLY_DEFAULT,
    LEGACY_INTERNAL_USAGE,
    OUTLIER_MIN_DELTA_BRL,
    OUTLIER_MIN_GROUP_N,
    OUTLIER_Z_THRESHOLD,
    RETIRED_DEFAULT,
)


def main() -> int:
    detector_set = set(DETECTORS)
    internal_set = set(INTERNAL_ONLY_DEFAULT)
    retired_set = set(RETIRED_DEFAULT)
    status_keys = set(DETECTOR_STATUS)
    missing = sorted(detector_set - internal_set)
    extra_unknown = sorted(set(INTERNAL_ONLY_DEFAULT) - set(DETECTORS))
    missing_status = sorted(detector_set - status_keys)
    extra_status = sorted(status_keys - detector_set)
    retired_invalid = sorted(
        k for k in retired_set if DETECTOR_STATUS.get(k) not in {"APOSENTADO", "COBERTO_OPS"}
    )
    print(f"legacy_detectors={len(DETECTORS)}")
    print(f"internal_only_default={len(INTERNAL_ONLY_DEFAULT)}")
    print(f"retired_default={len(RETIRED_DEFAULT)}")
    print(f"legacy_usage={LEGACY_INTERNAL_USAGE}")
    print(f"outlier_min_group_n={OUTLIER_MIN_GROUP_N}")
    print(f"outlier_z_threshold={OUTLIER_Z_THRESHOLD}")
    print(f"outlier_min_delta_brl={OUTLIER_MIN_DELTA_BRL}")
    if missing:
        print(f"detectors_not_internal_only={missing}")
        return 2
    if extra_unknown:
        print(f"unknown_internal_only={extra_unknown}")
        return 2
    if missing_status:
        print(f"detectors_without_status={missing_status}")
        return 2
    if extra_status:
        print(f"unknown_status_entries={extra_status}")
        return 2
    if retired_invalid:
        print(f"retired_status_mismatch={retired_invalid}")
        return 2
    if LEGACY_INTERNAL_USAGE != "REVISAO_INTERNA":
        print("legacy_usage_must_be_revisao_interna")
        return 2
    if OUTLIER_MIN_GROUP_N < 30:
        print("outlier_min_group_n_too_low")
        return 2
    if OUTLIER_Z_THRESHOLD < 4.0:
        print("outlier_z_threshold_too_low")
        return 2
    if OUTLIER_MIN_DELTA_BRL < 5000.0:
        print("outlier_min_delta_brl_too_low")
        return 2
    print("cross_reference_engine_guard=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
