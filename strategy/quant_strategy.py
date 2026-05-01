"""
strategy/quant_strategy.py

Full institutional orchestrator.

This class wires:
LiquidityMap -> EntryEngine -> RiskManager -> InstitutionalLifecycle -> OrderManager -> LiquidityTrail

No synthetic data.
No fallback liquidity.
No raw EntrySignal execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import logging

try:
    from strategy.entry_engine import EntryEngine, EntryEngineConfig, MarketState, EntrySignal
    from strategy.institutional_lifecycle import InstitutionalLifecycle
    from strategy.liquidity_trail import LiquidityTrail, PositionState, ExitContext
    from risk.risk_manager import RiskManager, RiskConfig, AccountState, ExchangeConstraints
    from execution.order_manager import OrderManager, OrderResult
except Exception:
    from .entry_engine import EntryEngine, EntryEngineConfig, MarketState, EntrySignal
    from .institutional_lifecycle import InstitutionalLifecycle
    from .liquidity_trail import LiquidityTrail, PositionState, ExitContext
    from risk.risk_manager import RiskManager, RiskConfig, AccountState, ExchangeConstraints
    from execution.order_manager import OrderManager, OrderResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategyConfig:
    symbol: str = "BTCUSD"
    tick_size: float = 0.1
    min_rr: float = 1.8
    min_target_probability: float = 0.18
    min_net_ev_r: float = 0.05
    min_route_utility: float = 0.0
    min_risk_atr: float = 0.45
    max_risk_atr: float = 3.50
    round_trip_fee_rate: float = 0.00110
    slippage_atr: float = 0.03


class QuantStrategy:
    def __init__(
        self,
        *,
        exchange_adapter: Any,
        account_provider: Any,
        config: Optional[StrategyConfig] = None,
        risk_manager: Optional[RiskManager] = None,
        entry_engine: Optional[EntryEngine] = None,
        order_manager: Optional[OrderManager] = None,
        trail: Optional[LiquidityTrail] = None,
    ) -> None:
        self.cfg = config or StrategyConfig()
        self.account_provider = account_provider
        self.entry_engine = entry_engine or EntryEngine(EntryEngineConfig(
            tick_size=self.cfg.tick_size,
            min_rr=self.cfg.min_rr,
            min_target_probability=self.cfg.min_target_probability,
            min_net_ev_r=self.cfg.min_net_ev_r,
            min_route_utility=self.cfg.min_route_utility,
            min_risk_atr=self.cfg.min_risk_atr,
            max_risk_atr=self.cfg.max_risk_atr,
            round_trip_fee_rate=self.cfg.round_trip_fee_rate,
            slippage_atr=self.cfg.slippage_atr,
        ))
        self.risk_manager = risk_manager or RiskManager()
        self.order_manager = order_manager or OrderManager(exchange_adapter, symbol=self.cfg.symbol)
        self.lifecycle = InstitutionalLifecycle(
            tick_size=self.cfg.tick_size,
            min_rr=self.cfg.min_rr,
            min_target_probability=self.cfg.min_target_probability,
            min_net_ev_r=self.cfg.min_net_ev_r,
            min_route_utility=self.cfg.min_route_utility,
            min_risk_atr=self.cfg.min_risk_atr,
            max_risk_atr=self.cfg.max_risk_atr,
            round_trip_fee_rate=self.cfg.round_trip_fee_rate,
            slippage_atr=self.cfg.slippage_atr,
        )
        self.trail = trail or LiquidityTrail()
        self.active_position: Optional[PositionState] = None
        self.last_reject_reason = ""

    def on_market(self, market: MarketState) -> Optional[OrderResult]:
        if self.active_position is not None:
            self.manage_position(market)
            return None

        signal = self.entry_engine.evaluate(market)
        if signal is None:
            self.last_reject_reason = self.entry_engine.last_reject_reason
            return None

        account = self._get_account_state()
        size = self.risk_manager.calculate_position_size(
            account=account,
            entry_price=signal.entry_price,
            sl_price=signal.sl_price,
            side=signal.side,
            conviction=signal.conviction,
            size_multiplier=1.0,
        )
        if not size.accepted:
            self.last_reject_reason = size.reason
            self.entry_engine.arm_refined_watch(signal, reason=f"size rejected: {size.reason}")
            logger.info("ENTRY_SIZE_REJECTED thesis=%s reason=%s", signal.thesis_id, size.reason)
            return None

        atr = 0.0
        if self.entry_engine.last_snapshot is not None:
            atr = self.entry_engine.last_snapshot.atr

        noise_floor_sl = self.entry_engine._noise_floor_sl(signal.side, signal.entry_price, atr)
        plan = self.lifecycle.build_plan(
            side=signal.side,
            entry=signal.entry_price,
            atr=atr,
            structural_sl=signal.sl_price,
            noise_floor_sl=noise_floor_sl,
            raw_tp=signal.tp_price,
            raw_tp_label=signal.raw_tp_label,
            target_probability=signal.route_probability,
            expected_value_r=signal.route_ev_r,
            route_utility=signal.route_utility,
            conviction=signal.conviction,
            qty=size.qty,
            thesis_id=signal.thesis_id,
            diagnostics=signal.diagnostics,
        )

        if not plan.executable:
            self.last_reject_reason = plan.reason
            self.entry_engine.arm_refined_watch(signal, reason=plan.reason)
            logger.info("ENTRY_PLAN_REJECTED thesis=%s reason=%s raw_tp=%.1f", signal.thesis_id, plan.reason, signal.tp_price)
            return None

        result = self.order_manager.place_bracket_order(**plan.as_order_payload())
        if result.accepted:
            self.active_position = PositionState(
                side=plan.side,
                entry_price=plan.entry,
                sl_price=plan.sl,
                tp_price=plan.tp,
                qty=plan.qty,
                initial_sl_price=plan.sl,
                thesis_id=plan.thesis_id,
            )
            logger.info(
                "ENTRY_EXECUTED thesis=%s side=%s qty=%.8f entry=%.1f sl=%.1f tp=%.1f rr=%.2f",
                plan.thesis_id, plan.side, plan.qty, plan.entry, plan.sl, plan.tp, plan.rr,
            )
        else:
            logger.info("ORDER_REJECTED thesis=%s reason=%s", plan.thesis_id, result.reason)

        return result

    # Compatibility aliases.
    on_tick = on_market
    process_market = on_market
    evaluate = on_market

    def manage_position(self, market: MarketState) -> None:
        pos = self.active_position
        if pos is None:
            return

        snapshot = self.entry_engine.last_snapshot
        atr = snapshot.atr if snapshot is not None else 0.0
        if atr <= 0:
            logger.info("TRAIL_SKIP invalid ATR")
            return

        # Structure trail should be supplied by real market structure only.
        structure_trail_price = 0.0
        if snapshot is not None:
            if pos.side == "long":
                ssl = snapshot.nearest_ssl_below(market.price)
                structure_trail_price = ssl.price if ssl else 0.0
            else:
                bsl = snapshot.nearest_bsl_above(market.price)
                structure_trail_price = bsl.price if bsl else 0.0

        ctx = ExitContext(
            mark_price=market.price,
            atr=atr,
            structure_trail_price=structure_trail_price,
            cisd_followthrough=market.cisd_confirmed,
            opposing_pool_progress=self._opposing_pool_progress(pos, market),
            inside_noise_zone=False,
            reached_route_midpoint=self._reached_midpoint(pos, market.price),
        )
        decision = self.trail.evaluate(pos, ctx)
        if not decision.should_update:
            logger.debug("TRAIL_HOLD thesis=%s reason=%s", pos.thesis_id, decision.reason)
            return

        modify = getattr(self.order_manager.exchange, "modify_stop_loss", None)
        if not callable(modify):
            logger.warning("TRAIL_SKIP exchange missing modify_stop_loss")
            return

        ok = modify(symbol=self.cfg.symbol, side=pos.side, quantity=pos.qty, stop_price=decision.new_sl, reduce_only=True)
        if ok:
            logger.info("TRAIL_UPDATE thesis=%s old=%.1f new=%.1f reason=%s", pos.thesis_id, pos.sl_price, decision.new_sl, decision.reason)
            pos.sl_price = decision.new_sl

    def _get_account_state(self) -> AccountState:
        provider = self.account_provider
        if provider is None:
            raise RuntimeError("account_provider is required; no fake balance fallback allowed")

        if callable(provider):
            raw = provider()
        elif hasattr(provider, "get_account_state"):
            raw = provider.get_account_state()
        elif hasattr(provider, "get_balance"):
            raw = provider.get_balance()
        else:
            raise RuntimeError("account_provider must be callable or implement get_account_state/get_balance")

        if isinstance(raw, AccountState):
            return raw
        if isinstance(raw, dict):
            return AccountState(
                available_balance=float(raw.get("available_balance", raw.get("available", 0.0))),
                total_balance=float(raw.get("total_balance", raw.get("total", 0.0))),
                currency=str(raw.get("currency", "USD")),
            )
        return AccountState(
            available_balance=float(getattr(raw, "available_balance", getattr(raw, "available", 0.0))),
            total_balance=float(getattr(raw, "total_balance", getattr(raw, "total", 0.0))),
            currency=str(getattr(raw, "currency", "USD")),
        )

    def _reached_midpoint(self, pos: PositionState, price: float) -> bool:
        if pos.side == "long":
            return price >= pos.entry_price + (pos.tp_price - pos.entry_price) * 0.50
        return price <= pos.entry_price - (pos.entry_price - pos.tp_price) * 0.50

    def _opposing_pool_progress(self, pos: PositionState, market: MarketState) -> bool:
        return self._reached_midpoint(pos, market.price)
