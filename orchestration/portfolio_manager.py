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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Callable

import config
from core.market_policy import active_policy
from risk.risk_manager import RiskManager
try:
    from agents.desk_router import InstitutionalDeskRouter
except Exception:  # pragma: no cover
    InstitutionalDeskRouter = None  # type: ignore

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if out == out and abs(out) != float("inf") else default
    except Exception:
        return default


def _parse_num_map(raw: Any, *, value_type=float) -> Dict[str, float]:
    """Parse config maps supplied either as dicts or 'DESK:VALUE,DESK:VALUE'."""
    out: Dict[str, float] = {}
    if isinstance(raw, dict):
        items = raw.items()
    else:
        items = []
        for part in str(raw or "").replace(";", ",").split(","):
            if ":" not in part:
                continue
            key, value = part.split(":", 1)
            items.append((key, value))
    for key, value in items:
        desk = str(key or "").strip().upper()
        if not desk:
            continue
        try:
            if value_type is int:
                out[desk] = float(max(0, int(float(value))))
            else:
                out[desk] = max(0.0, float(value))
        except Exception:
            continue
    return out


def _risk_fraction() -> float:
    raw = _as_float(_cfg("RISK_PER_TRADE", 0.005), 0.005)
    if raw > 0.05 and raw <= 5.0:
        raw /= 100.0
    return min(0.05, max(1e-9, raw))


@dataclass(frozen=True)
class DeskBook:
    desk_id: str
    max_open: int
    capital_weight: float
    risk_weight: float
    selector_quota: int = 0


class PortfolioManager:
    def __init__(self) -> None:
        self.max_open_positions = max(1, int(getattr(config, 'PORTFOLIO_MAX_OPEN_POSITIONS', 4)))
        self.max_same_class = max(1, int(getattr(config, 'PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS', self.max_open_positions)))
        self.max_per_contract = max(1, int(getattr(config, 'PORTFOLIO_MAX_OPEN_PER_CONTRACT', 1)))
        self.budget_mode = str(getattr(config, 'PORTFOLIO_BUDGET_MODE', 'equal_slots')).lower()
        self.risk_budget_mode = str(getattr(config, 'PORTFOLIO_RISK_BUDGET_MODE', 'portfolio_equity')).lower()
        self.desk_budget_mode = str(getattr(config, 'PORTFOLIO_DESK_BUDGET_MODE', self.budget_mode)).lower()
        self.aggregate_risk_pct = max(0.0, _as_float(getattr(config, 'PORTFOLIO_MAX_AGGREGATE_RISK_PCT', 0.0), 0.0)) / 100.0
        self._router = InstitutionalDeskRouter() if InstitutionalDeskRouter is not None else None
        self.desk_position_limits = self._build_desk_limits()
        self.desk_capital_weights = self._normalised_weights(
            _parse_num_map(getattr(config, "DESK_CAPITAL_WEIGHT_BY_ID", "")),
            self.desk_position_limits,
        )
        self.desk_risk_weights = self._normalised_weights(
            _parse_num_map(getattr(config, "DESK_RISK_WEIGHT_BY_ID", "")),
            self.desk_position_limits,
        )
        self._lock = threading.RLock()
        self._balance_cache: Dict[str, Dict[str, Any]] = {}
        self.balance_ttl_sec = float(getattr(config, 'PORTFOLIO_BALANCE_CACHE_TTL_SEC', 2.0))

    # ---------- portfolio state ----------
    def _build_desk_limits(self) -> Dict[str, int]:
        defaults = {
            "BTC_GLOBAL": int(_cfg("DESK_BTC_GLOBAL_MAX_OPEN", 1)),
            "CRYPTO_ALTS": int(_cfg("DESK_CRYPTO_ALTS_MAX_OPEN", 2)),
            "US_STOCK_DERIVATIVES": int(_cfg("DESK_US_STOCK_DERIVATIVES_MAX_OPEN", 2)),
            "COMMODITIES_GLOBAL": int(_cfg("DESK_COMMODITIES_GLOBAL_MAX_OPEN", 1)),
            "ICICI_INDEX_OPTIONS": int(_cfg("DESK_ICICI_INDEX_OPTIONS_MAX_OPEN", 1)),
            "ICICI_STOCK_OPTIONS": int(_cfg("DESK_ICICI_STOCK_OPTIONS_MAX_OPEN", 1)),
        }
        configured = _parse_num_map(_cfg("DESK_POSITION_LIMITS_BY_ID", ""), value_type=int)
        defaults.update({k: int(v) for k, v in configured.items()})
        return {k: max(0, min(self.max_open_positions, int(v))) for k, v in defaults.items()}

    @staticmethod
    def _normalised_weights(raw: Dict[str, float], limits: Dict[str, int]) -> Dict[str, float]:
        if raw:
            base = {str(k).upper(): max(0.0, float(v)) for k, v in raw.items()}
        else:
            base = {desk: float(max(0, limit)) for desk, limit in limits.items()}
        for desk in limits:
            base.setdefault(desk, 0.0)
        total = sum(v for v in base.values() if v > 0.0)
        if total <= 0.0:
            n = max(1, len(limits))
            return {desk: 1.0 / n for desk in limits}
        return {desk: (max(0.0, val) / total) for desk, val in base.items()}

    def desk_id_for(self, ctx: Any) -> str:
        try:
            if self._router is not None and ctx is not None:
                return str(self._router.desk_id_for(ctx.instrument)).upper()
        except Exception:
            pass
        try:
            return str(getattr(getattr(ctx, "instrument", None), "asset_class", "UNKNOWN")).upper()
        except Exception:
            return "UNKNOWN"

    def book_for_context(self, ctx: Any) -> DeskBook:
        desk_id = self.desk_id_for(ctx)
        selector_quota = 0
        if self._router is not None:
            try:
                selector_quota = int(self._router.profile_for(desk_id).quota)
            except Exception:
                selector_quota = 0
        fallback_weight = 1.0 / max(1, len(self.desk_position_limits))
        return DeskBook(
            desk_id=desk_id,
            max_open=max(1, int(self.desk_position_limits.get(desk_id, 1))),
            capital_weight=float(self.desk_capital_weights.get(desk_id, fallback_weight)),
            risk_weight=float(self.desk_risk_weights.get(desk_id, fallback_weight)),
            selector_quota=max(0, selector_quota),
        )

    def reserved_contexts(self, contexts: List[Any]) -> List[Any]:
        return [c for c in contexts if getattr(c, 'has_position', False)]

    def count_open(self, contexts: List[Any]) -> int:
        return len(self.reserved_contexts(contexts or []))

    def reserved_contexts_for_desk(self, contexts: List[Any], desk_id: str) -> List[Any]:
        target = str(desk_id or "").upper()
        return [c for c in self.reserved_contexts(contexts or []) if self.desk_id_for(c) == target]

    def count_open_for_desk(self, ctx_or_desk: Any, contexts: List[Any]) -> int:
        desk = ctx_or_desk if isinstance(ctx_or_desk, str) else self.desk_id_for(ctx_or_desk)
        return len(self.reserved_contexts_for_desk(contexts or [], desk))

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
            book = self.book_for_context(ctx)
            same_desk = self.reserved_contexts_for_desk(contexts or [], book.desk_id)
            if len(same_desk) >= book.max_open:
                return False, f"desk book exposure cap {book.desk_id} {len(same_desk)}/{book.max_open}"
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
        book = self.book_for_context(ctx)
        desk_open = self.count_open_for_desk(book.desk_id, contexts or [])
        desk_modes = {'desk', 'desk_book', 'desk_weight', 'desk_weighted', 'desk_books'}
        if self.desk_budget_mode in desk_modes or self.budget_mode in desk_modes:
            slot_count = max(1, int(book.max_open))
            desk_total = raw_total * book.capital_weight
            desk_available = raw_available * book.capital_weight
            slot_equity = desk_total / float(slot_count)
            slot_available = min(slot_equity, desk_available)
            slot_scope = "desk"
        else:
            slot_count = min(configured_slots, active_universe) if self.budget_mode in {'active_equal_slots', 'active_slots'} else configured_slots
            slot_equity = raw_total / float(slot_count) if slot_count > 0 else raw_total
            slot_available = min(slot_equity, raw_available)
            desk_total = raw_total * book.capital_weight
            desk_available = raw_available * book.capital_weight
            slot_scope = "portfolio"

        pol = active_policy(getattr(ctx, 'instrument', None) if ctx is not None else None)
        base_risk_fraction = _risk_fraction()
        risk_modes = desk_modes | {'desk_risk', 'desk_risk_weighted'}
        if (self.risk_budget_mode in risk_modes or self.desk_budget_mode in desk_modes) and self.aggregate_risk_pct > 0:
            desk_risk_budget = raw_total * self.aggregate_risk_pct * book.risk_weight
            desk_risk_per_slot = desk_risk_budget / float(max(1, book.max_open))
            risk_base = desk_risk_per_slot / base_risk_fraction
            liquidity_ratio = min(1.0, raw_available / raw_total) if raw_total > 0 else 1.0
            risk_available = max(0.0, risk_base * liquidity_ratio)
            risk_total = max(0.0, risk_base)
        elif self.risk_budget_mode == 'portfolio_equity':
            desk_risk_budget = raw_total * self.aggregate_risk_pct * book.risk_weight if self.aggregate_risk_pct > 0 else 0.0
            desk_risk_per_slot = desk_risk_budget / float(max(1, book.max_open)) if desk_risk_budget > 0 else 0.0
            risk_available = raw_available
            risk_total = raw_total
        else:
            desk_risk_budget = raw_total * self.aggregate_risk_pct * book.risk_weight if self.aggregate_risk_pct > 0 else 0.0
            desk_risk_per_slot = desk_risk_budget / float(max(1, book.max_open)) if desk_risk_budget > 0 else 0.0
            risk_available = slot_available
            risk_total = slot_equity

        # Margin/cash is desk-slot scoped; dollar risk is owned by the desk book
        # or the legacy portfolio/slot mode above.
        adjusted = dict(raw_balance)
        adjusted['available_raw'] = raw_available
        adjusted['total_raw'] = raw_total
        adjusted['available'] = max(0.0, slot_available)
        adjusted['total'] = max(0.0, slot_equity)
        adjusted['risk_available'] = risk_available
        adjusted['risk_total'] = risk_total
        adjusted['portfolio_scoped'] = True
        adjusted['portfolio_budget_mode'] = self.budget_mode
        adjusted['portfolio_risk_budget_mode'] = self.risk_budget_mode
        adjusted['portfolio_desk_budget_mode'] = self.desk_budget_mode
        adjusted['portfolio_slot_scope'] = slot_scope
        adjusted['portfolio_slot_count'] = slot_count
        adjusted['portfolio_slot_available'] = slot_available
        adjusted['portfolio_slot_equity'] = slot_equity
        adjusted['portfolio_reserved_slots'] = self.count_open(contexts or [])
        adjusted['portfolio_max_slots'] = self.max_open_positions
        adjusted['portfolio_desk_id'] = book.desk_id
        adjusted['portfolio_desk_open'] = desk_open
        adjusted['portfolio_desk_max_open'] = book.max_open
        adjusted['portfolio_desk_selector_quota'] = book.selector_quota
        adjusted['portfolio_desk_capital_weight'] = book.capital_weight
        adjusted['portfolio_desk_risk_weight'] = book.risk_weight
        adjusted['portfolio_desk_capital_total'] = desk_total
        adjusted['portfolio_desk_available'] = desk_available
        adjusted['portfolio_desk_risk_budget'] = desk_risk_budget
        adjusted['portfolio_desk_risk_per_slot'] = desk_risk_per_slot
        adjusted['instrument_policy'] = pol.asdict()
        adjusted['instrument_risk_multiplier'] = float(pol.risk_multiplier)
        adjusted['instrument_margin_pct'] = float(pol.margin_pct)
        if ctx is not None:
            adjusted['portfolio_asset_id'] = ctx.instrument.asset_id
        return adjusted

    def report_line(self, ctx: Any) -> str:
        pol = active_policy(ctx.instrument)
        book = self.book_for_context(ctx)
        return (f"desk={book.desk_id} book={book.max_open} cap_w={book.capital_weight:.2f} "
                f"risk_w={book.risk_weight:.2f} policy={pol.asset_class} lev={pol.leverage}x "
                f"margin={pol.margin_pct:.0%} risk_mult={pol.risk_multiplier:.2f} loop={pol.loop_interval_sec:.2f}s")


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
