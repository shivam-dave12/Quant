"""Setup selection agent.

Consumes cached strategy diagnostics after the strategy has processed ticks.
It avoids calling private entry methods, so it cannot create orders as a side
effect. The current strategy remains the alpha engine.
"""

from __future__ import annotations

from typing import Any, Iterable, List

from fund.mandate import FundMandate
from fund.types import SetupCandidate, clamp, safe_float


class SetupSelectionAgent:
    def __init__(self, mandate: FundMandate | None = None) -> None:
        self.mandate = mandate or FundMandate.from_config()

    def evaluate_many(self, contexts: Iterable[Any]) -> List[SetupCandidate]:
        cands = [self.evaluate_context(ctx) for ctx in contexts]
        cands.sort(key=lambda c: c.score, reverse=True)
        return cands

    def evaluate_context(self, ctx: Any) -> SetupCandidate:
        inst = getattr(ctx, "instrument", None)
        strategy = getattr(ctx, "strategy", None)
        asset_id = str(getattr(inst, "asset_id", "UNKNOWN") or "UNKNOWN")
        if strategy is None:
            return SetupCandidate(asset_id=asset_id, score=0.0, reasons=("strategy unavailable",))

        entry_signal = getattr(strategy, "_last_entry_signal", None)
        readiness = getattr(strategy, "_last_entry_readiness", None)
        inst_decision = getattr(strategy, "_last_institutional_decision", None)
        entry_engine = getattr(strategy, "_entry_engine", None)
        sweep = getattr(entry_engine, "_last_sweep_analysis", None) if entry_engine is not None else None

        side = str(getattr(entry_signal, "side", "") or "")
        setup_type = str(getattr(getattr(entry_signal, "entry_type", ""), "value", "") or "SCANNING")
        probability = self._probability(entry_signal, sweep)
        rr = safe_float(getattr(inst_decision, "rr", 0.0), 0.0)
        readiness_score = safe_float(getattr(readiness, "score", 0.0), 0.0)
        decision_score = safe_float(getattr(inst_decision, "score", 0.0), 0.0)
        ev = safe_float(getattr(entry_signal, "expected_value_r", 0.0), 0.0)
        if ev <= 0 and probability > 0 and rr > 0:
            ev = probability * rr - (1.0 - probability)

        score = (
            0.36 * clamp(probability)
            + 0.28 * clamp(decision_score)
            + 0.20 * clamp(readiness_score)
            + 0.16 * clamp(max(0.0, ev) / 1.25)
        )

        reasons: list[str] = []
        phase = str(getattr(ctx, "phase_name", "UNKNOWN") or "UNKNOWN")
        if phase != "FLAT":
            reasons.append(f"phase={phase}")
        if setup_type != "SCANNING":
            reasons.append(f"setup={setup_type}")
        if readiness_score:
            reasons.append(f"readiness={readiness_score:.2f}")
        if decision_score:
            reasons.append(f"audit={decision_score:.2f}")
        if probability:
            reasons.append(f"p={probability:.2f}")
        if ev:
            reasons.append(f"ev={ev:.2f}R")
        if not reasons:
            reasons.append("no active cached setup")

        return SetupCandidate(
            asset_id=asset_id,
            score=clamp(score),
            side=side,
            setup_type=setup_type,
            probability=clamp(probability),
            expected_value_r=ev,
            rr=rr,
            entry=safe_float(getattr(entry_signal, "entry_price", 0.0), 0.0),
            stop=safe_float(getattr(entry_signal, "sl_price", 0.0), 0.0),
            target=safe_float(getattr(entry_signal, "tp_price", 0.0), 0.0),
            size_mult=safe_float(getattr(inst_decision, "size_mult", 0.0), 0.0),
            source="quant_strategy_cache",
            reasons=tuple(reasons),
        )

    @staticmethod
    def _probability(entry_signal: Any, sweep: Any) -> float:
        for obj, names in (
            (entry_signal, ("posterior_prob", "posterior", "probability")),
            (sweep or {}, ("posterior", "quant_posterior", "probability")),
        ):
            for name in names:
                try:
                    val = obj.get(name) if isinstance(obj, dict) else getattr(obj, name)
                    f = safe_float(val, 0.0)
                    if f > 0:
                        return clamp(f)
                except Exception:
                    continue
        return 0.0
