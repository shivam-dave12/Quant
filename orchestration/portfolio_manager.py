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
        """Per-contract loop cadence. Positions are stepped fastest for exits/trailing."""
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

    def _instrument_policy(self, ctx: Optional[Any]):
        try:
            return active_policy(getattr(ctx, 'instrument', None) if ctx is not None else None)
        except Exception:
            return active_policy(None)

    @staticmethod
    def _position(ctx: Any) -> Any:
        return getattr(ctx, 'position', None) or getattr(ctx, '_pos', None) or getattr(getattr(ctx, 'strategy', None), '_pos', None)

    def _open_margin_used(self, ctx: Any) -> float:
        try:
            pos = self._position(ctx)
            if pos is None:
                return 0.0
            qty = max(0.0, float(getattr(pos, 'quantity', 0.0) or 0.0))
            entry = max(0.0, float(getattr(pos, 'entry_price', 0.0) or 0.0))
            if qty <= 0.0 or entry <= 0.0:
                return 0.0
            pol = self._instrument_policy(ctx)
            lev = max(1.0, float(getattr(pol, 'leverage', 1) or 1.0))
            return qty * entry / lev
        except Exception:
            return 0.0

    def _open_initial_risk(self, ctx: Any) -> float:
        try:
            pos = self._position(ctx)
            if pos is None:
                return 0.0
            ir = float(getattr(pos, 'initial_risk', 0.0) or 0.0)
            if ir > 0.0:
                return ir
            qty = max(0.0, float(getattr(pos, 'quantity', 0.0) or 0.0))
            entry = float(getattr(pos, 'entry_price', 0.0) or 0.0)
            sl = float(getattr(pos, 'sl_price', 0.0) or 0.0)
            return abs(entry - sl) * qty if qty > 0.0 and entry > 0.0 and sl > 0.0 else 0.0
        except Exception:
            return 0.0

    def _allocation_universe(self, ctx: Optional[Any], contexts: List[Any]) -> List[Any]:
        """Capital is normalised over the most relevant tradable sleeve set.

        If 14 products are being watched but only 6 can be open, normalising
        over all 14 would underfund every ticker.  We therefore normalise over
        the highest-weight instruments up to the slot cap, always including the
        candidate being sized.
        """
        pool = [c for c in (contexts or []) if getattr(c, 'instrument', None) is not None]
        if ctx is not None and getattr(ctx, 'instrument', None) is not None and ctx not in pool:
            pool.append(ctx)
        if not pool:
            return []
        ranked = sorted(pool, key=lambda c: float(getattr(self._instrument_policy(c), 'portfolio_weight', 1.0) or 1.0), reverse=True)
        selected = ranked[:max(1, self.max_open_positions)]
        if ctx is not None and ctx not in selected:
            selected = selected[:-1] + [ctx] if selected else [ctx]
        # Deduplicate by asset id but keep deterministic order.
        out, seen = [], set()
        for c in selected:
            aid = getattr(getattr(c, 'instrument', None), 'asset_id', id(c))
            if aid in seen:
                continue
            seen.add(aid)
            out.append(c)
        return out

    def _allocation_share(self, ctx: Optional[Any], contexts: List[Any]) -> tuple[float, float, float, int]:
        universe = self._allocation_universe(ctx, contexts)
        if not universe:
            return 1.0, 1.0, 1.0, 1
        weights = []
        for c in universe:
            try:
                weights.append(max(0.05, float(getattr(self._instrument_policy(c), 'portfolio_weight', 1.0) or 1.0)))
            except Exception:
                weights.append(1.0)
        total_w = max(1e-9, sum(weights))
        if ctx is None:
            w = weights[0]
        else:
            try:
                w = max(0.05, float(getattr(self._instrument_policy(ctx), 'portfolio_weight', 1.0) or 1.0))
            except Exception:
                w = 1.0
        return max(0.0, min(1.0, w / total_w)), w, total_w, len(universe)

    def allocate_balance(self, ctx: Optional[Any], contexts: List[Any], raw_balance: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if raw_balance is None or not isinstance(raw_balance, dict):
            return raw_balance
        try:
            raw_available = max(0.0, float(raw_balance.get('available', 0.0) or 0.0))
            raw_total = max(0.0, float(raw_balance.get('total', raw_available) or raw_available))
        except Exception:
            return raw_balance

        pol = self._instrument_policy(ctx)
        configured_slots = max(1, self.max_open_positions)
        active_universe = max(1, len(contexts or []))
        institutional_modes = {'institutional', 'institutional_risk_parity', 'risk_parity', 'policy_weighted'}

        if self.budget_mode in institutional_modes:
            cash_share, alloc_weight, total_weight, sleeve_count = self._allocation_share(ctx, contexts or [])
            slot_count = sleeve_count
            sleeve_equity = raw_total * cash_share
            sleeve_available = raw_available * cash_share
            allocation_mode = 'institutional_risk_parity'
        else:
            slot_count = min(configured_slots, active_universe) if self.budget_mode in {'active_equal_slots', 'active_slots'} else configured_slots
            sleeve_equity = raw_total / float(slot_count) if slot_count > 0 else raw_total
            sleeve_available = min(sleeve_equity, raw_available)
            cash_share = sleeve_equity / raw_total if raw_total > 0 else 1.0
            alloc_weight = 1.0
            total_weight = float(slot_count)
            allocation_mode = self.budget_mode

        reserved_ctx = self.reserved_contexts(contexts or [])
        open_risk = sum(self._open_initial_risk(c) for c in reserved_ctx)
        open_margin = sum(self._open_margin_used(c) for c in reserved_ctx)
        max_risk_budget = raw_total * max(0.0, float(getattr(config, 'PORTFOLIO_MAX_CONCURRENT_RISK_PCT', 0.030)))
        max_margin_budget = raw_available * max(0.0, min(1.0, float(getattr(config, 'PORTFOLIO_MAX_TOTAL_MARGIN_USAGE_PCT', 0.75))))
        remaining_risk = max(0.0, max_risk_budget - open_risk)
        remaining_margin = max(0.0, max_margin_budget - open_margin)

        normal_cash_budget = sleeve_available * max(0.01, min(1.0, float(getattr(pol, 'margin_pct', 0.20))))
        elastic_pct = max(0.0, min(1.0, float(getattr(pol, 'min_lot_elastic_margin_pct', 0.0) or 0.0)))
        elastic_cash = 0.0
        if bool(getattr(config, 'PORTFOLIO_MIN_LOT_ELASTIC_ENABLED', True)):
            elastic_cash = min(remaining_margin, raw_available * elastic_pct)
        # Do not let elastic borrowing exceed the ticker's own maximum margin cap.
        ticker_margin_cap = min(remaining_margin, raw_available * max(0.01, min(1.0, float(getattr(pol, 'max_position_margin_pct', getattr(pol, 'margin_pct', 0.20))))))
        executable_cash_budget = max(normal_cash_budget, min(elastic_cash, ticker_margin_cap))

        risk_available = raw_available if self.risk_budget_mode == 'portfolio_equity' else sleeve_available
        risk_total = raw_total if self.risk_budget_mode == 'portfolio_equity' else sleeve_equity
        adjusted = dict(raw_balance)
        adjusted['available_raw'] = raw_available
        adjusted['total_raw'] = raw_total
        adjusted['available'] = max(0.0, sleeve_available)
        adjusted['total'] = max(0.0, sleeve_equity)
        adjusted['risk_available'] = risk_available
        adjusted['risk_total'] = risk_total
        adjusted['portfolio_scoped'] = True
        adjusted['portfolio_budget_mode'] = allocation_mode
        adjusted['portfolio_risk_budget_mode'] = self.risk_budget_mode
        adjusted['portfolio_slot_count'] = slot_count
        adjusted['portfolio_slot_available'] = sleeve_available
        adjusted['portfolio_slot_equity'] = sleeve_equity
        adjusted['portfolio_cash_share'] = cash_share
        adjusted['portfolio_allocation_weight'] = alloc_weight
        adjusted['portfolio_total_weight'] = total_weight
        adjusted['portfolio_reserved_slots'] = self.count_open(contexts or [])
        adjusted['portfolio_max_slots'] = self.max_open_positions
        adjusted['portfolio_open_risk_usd'] = open_risk
        adjusted['portfolio_open_margin_usd'] = open_margin
        adjusted['portfolio_max_risk_budget_usd'] = max_risk_budget
        adjusted['portfolio_remaining_risk_budget_usd'] = remaining_risk
        adjusted['portfolio_max_margin_budget_usd'] = max_margin_budget
        adjusted['portfolio_remaining_margin_budget_usd'] = remaining_margin
        adjusted['portfolio_normal_cash_budget_usd'] = normal_cash_budget
        adjusted['portfolio_elastic_cash_budget_usd'] = executable_cash_budget
        adjusted['portfolio_ticker_max_risk_usd'] = raw_total * max(0.0, float(getattr(pol, 'max_trade_risk_pct', 0.005)))
        adjusted['portfolio_ticker_margin_cap_usd'] = ticker_margin_cap
        adjusted['instrument_policy'] = pol.asdict()
        adjusted['instrument_risk_multiplier'] = float(pol.risk_multiplier)
        adjusted['instrument_margin_pct'] = float(pol.margin_pct)
        adjusted['instrument_portfolio_weight'] = float(getattr(pol, 'portfolio_weight', 1.0))
        adjusted['instrument_max_position_margin_pct'] = float(getattr(pol, 'max_position_margin_pct', pol.margin_pct))
        adjusted['instrument_max_trade_risk_pct'] = float(getattr(pol, 'max_trade_risk_pct', 0.005))
        if ctx is not None:
            adjusted['portfolio_asset_id'] = ctx.instrument.asset_id
        return adjusted

    def report_line(self, ctx: Any) -> str:
        pol = active_policy(ctx.instrument)
        return (f"policy={pol.asset_class} lev={pol.leverage}x margin={pol.margin_pct:.1%} "
                f"risk_mult={pol.risk_multiplier:.2f} weight={getattr(pol, 'portfolio_weight', 1.0):.2f} "
                f"risk_cap={getattr(pol, 'max_trade_risk_pct', 0.005):.2%} loop={pol.loop_interval_sec:.2f}s")


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
