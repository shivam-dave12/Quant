"""
orchestration/portfolio_manager.py — central portfolio authority
=================================================================

Central account/portfolio layer for the multi-asset bot. Strategy instances are
independent alpha engines. This manager owns shared account state, budget slices,
position-slot rules, asset-class caps, balance caching and per-contract runtime
cadence. It deliberately does not create alpha or override a positive posterior;
it controls portfolio mechanics only.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Callable

import config
from core.instruments import AssetClass, TradableInstrument
from core.market_policy import active_policy
from risk.risk_manager import RiskManager

logger = logging.getLogger(__name__)


class PortfolioManager:
    def __init__(self) -> None:
        self.max_open_positions = max(1, int(getattr(config, 'PORTFOLIO_MAX_OPEN_POSITIONS', 4)))
        self.max_same_class = max(1, int(getattr(config, 'PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS', self.max_open_positions)))
        self.max_per_contract = max(1, int(getattr(config, 'PORTFOLIO_MAX_OPEN_PER_CONTRACT', 1)))
        self.budget_mode = str(getattr(config, 'PORTFOLIO_BUDGET_MODE', 'equal_slots')).lower()
        self.risk_budget_mode = str(getattr(config, 'PORTFOLIO_RISK_BUDGET_MODE', 'portfolio_equity')).lower()
        self._lock = threading.RLock()
        self._balance_cache: Dict[str, Dict[str, Any]] = {}
        self.balance_ttl_sec = float(getattr(config, 'PORTFOLIO_BALANCE_CACHE_TTL_SEC', 2.0))

    # ---------- portfolio state ----------
    def reserved_contexts(self, contexts: List[Any]) -> List[Any]:
        return [c for c in contexts if getattr(c, 'has_position', False)]

    def count_open(self, contexts: List[Any]) -> int:
        return len(self.reserved_contexts(contexts or []))

    def can_evaluate_entry(self, ctx: Any, contexts: List[Any]) -> tuple[bool, str]:
        """Entry-slot decision. Position management is always allowed."""
        with self._lock:
            reserved_ctx = self.reserved_contexts(contexts or [])
            if getattr(ctx, 'has_position', False):
                return True, f"{getattr(ctx, 'phase_name', 'position').lower()} position management"
            inst = ctx.instrument
            same_contract = [c for c in reserved_ctx if c.instrument.asset_id == inst.asset_id]
            if len(same_contract) >= self.max_per_contract:
                return False, f"contract slot occupied {inst.asset_id} {len(same_contract)}/{self.max_per_contract}"
            if len(reserved_ctx) >= self.max_open_positions:
                return False, f"portfolio exposure cap {len(reserved_ctx)}/{self.max_open_positions}"
            same_class = [c for c in reserved_ctx if c.instrument.asset_class == inst.asset_class]
            if len(same_class) >= self.max_same_class:
                return False, f"asset-class exposure cap {inst.asset_class.value} {len(same_class)}/{self.max_same_class}"
            return True, 'portfolio slot available'

    def evaluation_interval(self, ctx: Any) -> float:
        """Per-contract loop cadence. Positions are stepped fastest for exits and TP-ladder reconciliation."""
        if getattr(ctx, 'has_position', False):
            return max(0.05, float(getattr(config, 'SCANNER_POSITION_TICK_SEC', 0.25)))
        try:
            return max(0.05, float(active_policy(ctx.instrument).loop_interval_sec))
        except Exception:
            return max(0.05, float(getattr(config, 'SCANNER_TICK_SLEEP_SEC', 0.25)))

    # ---------- balance cache / allocation ----------
    def _cache_key(self, ctx: Optional[Any], api: Any = None) -> str:
        try:
            ex = ctx.instrument.primary_exchange.value if ctx is not None else 'default'
        except Exception:
            ex = 'default'
        return ex

    def get_cached_raw_balance(self, ctx: Optional[Any], api: Any, fetcher: Callable[[], Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
        key = self._cache_key(ctx, api)
        now = time.time()
        with self._lock:
            row = self._balance_cache.get(key)
            if row and now - float(row.get('ts', 0.0)) <= self.balance_ttl_sec:
                data = dict(row.get('data') or {})
                data['portfolio_balance_cached'] = True
                return data
        raw = fetcher()
        if isinstance(raw, dict):
            with self._lock:
                self._balance_cache[key] = {'ts': now, 'data': dict(raw)}
        return raw

    def allocate_balance(self, ctx: Optional[Any], contexts: List[Any], raw_balance: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if raw_balance is None or not isinstance(raw_balance, dict):
            return raw_balance
        try:
            raw_available = max(0.0, float(raw_balance.get('available', 0.0) or 0.0))
            raw_total = max(0.0, float(raw_balance.get('total', raw_available) or raw_available))
        except Exception:
            return raw_balance

        configured_slots = max(1, self.max_open_positions)
        active_universe = max(1, len(contexts or []))
        slot_count = min(configured_slots, active_universe) if self.budget_mode in {'active_equal_slots', 'active_slots'} else configured_slots
        slot_equity = raw_total / float(slot_count) if slot_count > 0 else raw_total
        slot_available = min(slot_equity, raw_available)

        pol = active_policy(getattr(ctx, 'instrument', None) if ctx is not None else None)
        # Margin/cash budget is slot-scoped and per-policy.  Risk-dollar base is
        # portfolio equity by default, then multiplied by the instrument policy.
        adjusted = dict(raw_balance)
        adjusted['available_raw'] = raw_available
        adjusted['total_raw'] = raw_total
        adjusted['available'] = max(0.0, slot_available)
        adjusted['total'] = max(0.0, slot_equity)
        # Risk sizing must be based on portfolio equity, not exchange free cash.
        # Free cash shrinks as earlier brackets reserve margin; using it as the
        # risk base makes the 2nd/3rd trade mechanically smaller even when the
        # portfolio equity is unchanged.  Margin feasibility remains protected by
        # the slot-scoped `available` field above, so we do not fabricate spendable
        # cash; we only keep dollar-risk sizing anchored to the account equity.
        adjusted['risk_available'] = raw_total if self.risk_budget_mode == 'portfolio_equity' else slot_available
        adjusted['risk_total'] = raw_total if self.risk_budget_mode == 'portfolio_equity' else slot_equity
        adjusted['portfolio_free_cash_after_reserves'] = raw_available
        adjusted['portfolio_scoped'] = True
        adjusted['portfolio_budget_mode'] = self.budget_mode
        adjusted['portfolio_risk_budget_mode'] = self.risk_budget_mode
        adjusted['portfolio_slot_count'] = slot_count
        adjusted['portfolio_slot_available'] = slot_available
        adjusted['portfolio_slot_equity'] = slot_equity
        adjusted['portfolio_reserved_slots'] = self.count_open(contexts or [])
        adjusted['portfolio_max_slots'] = self.max_open_positions
        adjusted['instrument_policy'] = pol.asdict()
        adjusted['instrument_risk_multiplier'] = float(pol.risk_multiplier)
        adjusted['instrument_margin_pct'] = float(pol.margin_pct)
        if ctx is not None:
            adjusted['portfolio_asset_id'] = ctx.instrument.asset_id
        return adjusted

    def report_line(self, ctx: Any) -> str:
        pol = active_policy(ctx.instrument)
        return (f"policy={pol.asset_class} lev={pol.leverage}x margin={pol.margin_pct:.0%} "
                f"risk_mult={pol.risk_multiplier:.2f} loop={pol.loop_interval_sec:.2f}s")


class PortfolioRiskManager(RiskManager):
    """RiskManager wrapper that asks the central PortfolioManager for account view."""
    def __init__(self, shared_api=None, *, allocator: Callable, context_getter: Callable[[], Optional[Any]], contexts_getter: Callable[[], List[Any]], manager: Optional[PortfolioManager] = None):
        super().__init__(shared_api=shared_api)
        self._portfolio_allocator = allocator
        self._portfolio_context_getter = context_getter
        self._portfolio_contexts_getter = contexts_getter
        self._portfolio_manager = manager

    def get_available_balance(self) -> Optional[Dict]:
        try:
            ctx = self._portfolio_context_getter()
            contexts = self._portfolio_contexts_getter()
        except Exception:
            ctx, contexts = None, []

        def _fetch():
            return super(PortfolioRiskManager, self).get_available_balance()

        try:
            if self._portfolio_manager is not None:
                raw = self._portfolio_manager.get_cached_raw_balance(ctx, self.api, _fetch)
            else:
                raw = _fetch()
            return self._portfolio_allocator(ctx, contexts, raw)
        except Exception:
            logger.exception('Portfolio balance allocation failed; using raw exchange balance')
            return _fetch()
