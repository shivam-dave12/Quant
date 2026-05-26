"""After-cost executable edge and probability-weighted trade-score calculations."""
from __future__ import annotations
from dataclasses import dataclass
from core.identifiers import CostEstimate

@dataclass(frozen=True)
class EdgeResult:
    predicted_gross_return_bps: float
    cost: CostEstimate
    net_edge_bps: float
    trade_score_bps: float

def executable_edge(predicted_gross_return_bps: float, cost: CostEstimate, *, tp_probability: float | None = None,
                    reward_bps: float | None = None, risk_bps: float | None = None) -> EdgeResult:
    net = predicted_gross_return_bps - cost.total_bps
    if tp_probability is None or reward_bps is None or risk_bps is None:
        score = net
    else:
        p = max(0.0, min(1.0, tp_probability))
        score = p * reward_bps - (1.0 - p) * risk_bps - cost.total_bps
    return EdgeResult(predicted_gross_return_bps, cost, net, score)
