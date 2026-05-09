"""Ticker selection agent.

Ranks desks by institutional tradability and opportunity quality. It does not
call strategy entry logic and it cannot place orders.
"""

from __future__ import annotations

import math
from typing import Iterable, List

from fund.mandate import FundMandate
from fund.types import AgentScore, MarketDiagnostics, TickerSelection, clamp, weighted_score


class TickerSelectionAgent:
    def __init__(self, mandate: FundMandate | None = None) -> None:
        self.mandate = mandate or FundMandate.from_config()

    def rank(self, diagnostics: Iterable[MarketDiagnostics]) -> List[TickerSelection]:
        rows: list[TickerSelection] = []
        for d in diagnostics:
            components = self._components(d)
            score = weighted_score(components)
            hard_reason = self._hard_reason(d)
            rows.append(TickerSelection(
                asset_id=d.asset_id,
                score=0.0 if hard_reason else score,
                rank=0,
                diagnostics=d,
                components=tuple(components),
                selected=False,
                reason=hard_reason or self._reason(d, score),
            ))
        rows.sort(key=lambda x: x.score, reverse=True)
        ranked: list[TickerSelection] = []
        for i, row in enumerate(rows, 1):
            ranked.append(TickerSelection(
                asset_id=row.asset_id,
                score=row.score,
                rank=i,
                diagnostics=row.diagnostics,
                components=row.components,
                selected=row.score >= self.mandate.min_ticker_score,
                reason=row.reason,
            ))
        return ranked

    def _components(self, d: MarketDiagnostics) -> tuple[AgentScore, ...]:
        max_spread = max(1.0, self.mandate.max_spread_for_class(d.asset_class))
        spread_score = clamp(1.0 - (d.spread_bps / max_spread))
        if d.spread_bps <= max_spread * 0.35:
            spread_score = max(spread_score, 0.88)

        warmup = min(d.warmup_1m, d.warmup_5m)
        freshness = clamp(1.0 - d.data_age_sec / max(1.0, self.mandate.max_data_age_sec))
        depth = clamp(math.log10(max(1.0, d.book_depth_usd)) / 7.0)
        atr_sweet = 1.0 - abs(clamp(d.atr_pctile) - 0.55) / 0.55
        atr_sweet = clamp(atr_sweet)
        activity = clamp(abs(d.book_imbalance) * 1.6)
        position_room = 1.0 if not d.has_position else 0.65
        readiness = 1.0 if d.ready and d.tradable else 0.0

        return (
            AgentScore("readiness", readiness, 0.18, "desk ready and instrument confirmed"),
            AgentScore("freshness", freshness, 0.14, "recent price/book update"),
            AgentScore("warmup", warmup, 0.13, "sufficient 1m/5m candle history"),
            AgentScore("spread", spread_score, 0.18, "transaction cost discipline"),
            AgentScore("depth", depth, 0.12, "top-book executable liquidity"),
            AgentScore("atr_regime", atr_sweet, 0.13, "volatility in tradable band"),
            AgentScore("activity", activity, 0.06, "book imbalance/activity proxy"),
            AgentScore("position_room", position_room, 0.06, "portfolio slot preference"),
        )

    def _hard_reason(self, d: MarketDiagnostics) -> str:
        if not d.tradable:
            return "no confirmed tradable instrument"
        if not d.ready:
            return "data desk not ready"
        if d.price <= 0:
            return "no valid price"
        if min(d.warmup_1m, d.warmup_5m) < self.mandate.min_warmup_ratio:
            return "warmup incomplete"
        if d.data_age_sec > self.mandate.max_data_age_sec:
            return f"stale data {d.data_age_sec:.0f}s"
        if d.spread_bps > self.mandate.max_spread_for_class(d.asset_class):
            return f"spread {d.spread_bps:.1f}bps above mandate"
        return ""

    @staticmethod
    def _reason(d: MarketDiagnostics, score: float) -> str:
        if score >= 0.75:
            return "front-desk candidate"
        if score >= 0.58:
            return "eligible depth-scan candidate"
        return "parked by CIO score"
