"""Daily and rolling realised drawdown hard controls."""
from __future__ import annotations
from collections import deque
class DrawdownController:
    def __init__(self, portfolio_daily_loss_cap: float, desk_daily_loss_caps: dict[str, float], rolling_loss_cap: float | None = None) -> None:
        self.portfolio_cap = float(portfolio_daily_loss_cap); self.desk_caps = dict(desk_daily_loss_caps)
        self.rolling_cap = float(rolling_loss_cap or portfolio_daily_loss_cap * 1.5); self._pnl: dict[str, float] = {}; self._rolling: deque[float] = deque(maxlen=100)
    def record_realised(self, desk: str, pnl: float) -> None: self._pnl[desk] = self._pnl.get(desk, 0.0) + float(pnl); self._rolling.append(float(pnl))
    def permit_new_risk(self, desk: str, incremental_risk: float) -> tuple[bool, str]:
        if sum(max(0.0, -x) for x in self._pnl.values()) + incremental_risk > self.portfolio_cap: return False, "PORTFOLIO_DAILY_DRAWDOWN_BUDGET"
        if max(0.0, -self._pnl.get(desk, 0.0)) + incremental_risk > self.desk_caps.get(desk, self.portfolio_cap): return False, "DESK_DAILY_DRAWDOWN_BUDGET"
        if max(0.0, -sum(self._rolling)) + incremental_risk > self.rolling_cap: return False, "ROLLING_DRAWDOWN_BUDGET"
        return True, "OK"
