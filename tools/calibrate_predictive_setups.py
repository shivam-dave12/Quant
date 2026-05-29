#!/usr/bin/env python3
"""Generate approved predictive live-model records from chronological shadow labels.

Usage:
    python tools/calibrate_predictive_setups.py \
        --labels research_output/predictive_barrier_labels.jsonl \
        --output research_output/predictive_calibration_report.json

The tool never fabricates approval: each model key is evaluated only on the final
chronological holdout slice.  A registry row is marked approved only when sample,
Brier score and Wilson lower-bound requirements pass on that holdout.
"""
from __future__ import annotations
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

MODEL_VERSION = "pre_move_orderflow_hazard_v1+predictive_bracket_viability_v3"


def wilson_lower(wins: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = wins / total
    denom = 1.0 + z * z / total
    centre = p + z * z / (2.0 * total)
    radius = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return max(0.0, (centre - radius) / denom)


def run(labels_path: Path, min_obs: int, max_brier: float, min_lower: float, holdout_fraction: float) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    with labels_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get("model_key") or "UNSPECIFIED")].append(row)
    report: dict[str, Any] = {"model_version": MODEL_VERSION, "labels_path": str(labels_path), "prediction_authority": "analytic_bracket_score_calibrated_against_independent_tp_sl_outcomes", "approved_registry": {}, "groups": {}}
    for key, group in sorted(groups.items()):
        group.sort(key=lambda r: int(r.get("observation_ts_ns", 0)))
        cut = max(1, int(len(group) * (1.0 - holdout_fraction)))
        holdout = group[cut:] if len(group) > 1 else []
        resolved = [r for r in holdout if str(r.get("outcome")) in {"TP_FIRST", "SL_FIRST", "TIMEOUT"}]
        n = len(resolved)
        wins = sum(1 for r in resolved if r.get("outcome") == "TP_FIRST")
        probs = [max(0.0, min(1.0, float(r.get("predicted_target_before_stop_probability", 0.0) or 0.0))) for r in resolved]
        targets = [1.0 if r.get("outcome") == "TP_FIRST" else 0.0 for r in resolved]
        brier = sum((p - y) ** 2 for p, y in zip(probs, targets)) / n if n else None
        lower = wilson_lower(wins, n)
        approved = bool(n >= min_obs and brier is not None and brier <= max_brier and lower >= min_lower)
        row = {"model_version": MODEL_VERSION, "prediction_authority": "analytic_bracket_score_calibrated_against_independent_tp_sl_outcomes", "out_of_sample_observations": n, "walk_forward_validated": approved, "brier_score": brier, "lower_confidence_tp_before_sl": lower, "observed_tp_before_sl": wins / n if n else None, "approved": approved}
        report["groups"][key] = row
        if approved:
            report["approved_registry"][key] = row
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--labels", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--min-observations", type=int, default=250)
    parser.add_argument("--max-brier", type=float, default=0.20)
    parser.add_argument("--min-lower-confidence", type=float, default=0.60)
    parser.add_argument("--holdout-fraction", type=float, default=0.40)
    args = parser.parse_args()
    report = run(args.labels, args.min_observations, args.max_brier, args.min_lower_confidence, args.holdout_fraction)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report["approved_registry"], indent=2, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
