from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import numpy as np

from .config import Settings, load_delta_credentials
from .costs import CostModel, parse_delta_fill
from .execution import DeltaRestClient, PaperExecutor
from .features import L2FeatureBuilder
from .models import LiveLearningModelStack
from .orderbook import DeltaOrderBook
from .recorder import AsyncJsonlRecorder
from .risk import AccountState, RiskEngine
from .types import AlphaDecision, BookSnapshot, Side, TradeTick

log = logging.getLogger(__name__)


def _ns_from_delta_ts(v: Any) -> int:
    try:
        x = int(float(v))
        if x < 10**12:
            return x * 1_000_000_000
        if x < 10**15:
            return x * 1_000_000
        if x < 10**18:
            return x * 1000
        return x
    except Exception:
        return time.time_ns()


class FullBTCStrategyEngine:
    """Complete live-learning BTC strategy stack.

    - captures raw trades/books/fills
    - reconstructs L2 book with sequence/checksum controls
    - trains online return and event models from future-matured real observations
    - updates exact fee/slippage cost model from REST fills
    - learns dynamic TP/SL from post-signal paths
    - can submit only atomic bracket orders when governance gates permit
    """

    def __init__(self, settings: Settings) -> None:
        settings.validate()
        self.s = settings
        for p in [self.s.raw_event_journal, self.s.feature_journal, self.s.decision_journal, self.s.model_dir, self.s.execution_ledger, self.s.state_path, self.s.telemetry_snapshot]:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
        self.raw_recorder = AsyncJsonlRecorder(self.s.raw_event_journal, {
            "source": "delta_live_websocket_raw_capture", "venue": "DELTA", "symbol": self.s.delta_symbol,
            "is_synthetic": False, "schema_version": 5,
        })
        self.feature_recorder = AsyncJsonlRecorder(self.s.feature_journal, {
            "source": "causal_live_features_from_delta_l2_trades", "venue": "DELTA", "symbol": self.s.delta_symbol,
            "is_synthetic": False, "schema_version": 5,
        })
        self.decision_recorder = AsyncJsonlRecorder(self.s.decision_journal, {
            "source": "model_signal_and_gate_audit", "venue": "DELTA", "symbol": self.s.delta_symbol,
            "is_synthetic": False, "schema_version": 5,
        })
        self.book = DeltaOrderBook(self.s.delta_symbol)
        self.features = L2FeatureBuilder()
        self.models = LiveLearningModelStack(
            self.s.label_horizons_ms,
            min_labels_to_score=self.s.min_labels_to_score,
            model_dir=self.s.model_dir,
            auto_promote=self.s.auto_promote_model,
        )
        self.models.maybe_load_bootstrap(self.s.bootstrap_tradeflow_model, self.s.bootstrap_tradeflow_manifest)
        self.costs = CostModel(
            taker_fee_bps_pre_gst=self.s.taker_fee_bps_pre_gst,
            maker_fee_bps_pre_gst=self.s.maker_fee_bps_pre_gst,
            gst_rate=self.s.gst_rate,
            impact_floor_bps=self.s.impact_floor_bps,
            min_real_fills=self.s.min_real_fill_count_for_cost_model,
            ledger_path=self.s.execution_ledger,
        )
        self.risk = RiskEngine(
            max_risk_per_trade=self.s.max_risk_per_trade,
            max_gross_leverage=self.s.max_gross_leverage,
            daily_drawdown_halt=self.s.daily_drawdown_halt,
            min_net_edge_bps=self.s.min_net_edge_bps,
            max_open_contracts=self.s.max_open_position_contracts,
        )
        self.account = AccountState(self.s.starting_equity_usd, self.s.starting_equity_usd)
        self.last_book: BookSnapshot | None = None
        self.product_id = 27
        self.contract_value_btc = 0.001
        self.private_fill_seq: int | None = None
        self.sticky_halt_reason: str | None = None
        self.started_ns = time.time_ns()
        self.last_public_event_ns: int | None = None
        self.last_book_event_ns: int | None = None
        self.last_ob_update_ns: int | None = None
        self.last_trade_event_ns: int | None = None
        self.public_event_count = 0
        self.book_event_count = 0
        self.trade_event_count = 0
        if self.s.trading_mode == "PAPER" or self.s.trading_mode == "SHADOW":
            self.executor = PaperExecutor()
        else:
            api_key, secret_key = load_delta_credentials()
            self.executor = DeltaRestClient(api_key, secret_key, testnet=self.s.delta_testnet)

    def configure_product(self, raw: dict[str, Any]) -> None:
        self.product_id = int(raw.get("id", raw.get("product_id", self.product_id)))
        self.contract_value_btc = float(raw.get("contract_value", self.contract_value_btc))
        self.risk.contract_value_btc = self.contract_value_btc
        self.costs.configure_from_product(raw)
        log.info("Product configured: product_id=%s contract_value_btc=%s", self.product_id, self.contract_value_btc)

    def on_public_message(self, msg: dict[str, Any]) -> None:
        receive_ns = time.time_ns()
        self.last_public_event_ns = receive_ns
        self.public_event_count += 1
        self.raw_recorder.write({"stream": "public", "raw": msg})
        typ = msg.get("type")
        if typ == "ob_updates":
            if str(msg.get("action", "")).lower() == "update":
                self.last_ob_update_ns = receive_ns
            book = self.book.apply(msg, receive_ns)
            if self.book.state.halted:
                self._halt(f"book_integrity:{self.book.state.halt_reason}")
            if book is not None:
                self.last_book_event_ns = receive_ns
                self.book_event_count += 1
                self.on_book(book)
        elif typ == "trades":
            self.last_trade_event_ns = receive_ns
            self.trade_event_count += 1
            self.on_trade_message(msg, receive_ns)
        elif typ in {"funding_rate", "mark_price"}:
            # Stored raw. Funding/mark are intentionally not guessed into labels until modelled explicitly.
            pass

    def on_trade_message(self, msg: dict[str, Any], receive_ns: int | None = None) -> None:
        receive_ns = int(receive_ns or time.time_ns())
        rows = msg.get("trades") if isinstance(msg.get("trades"), list) else msg.get("data") if isinstance(msg.get("data"), list) else None
        if rows is None:
            rows = [msg]
        for row in rows:
            try:
                trade = TradeTick(
                    venue="DELTA", symbol=str(row.get("symbol", row.get("sy", self.s.delta_symbol))).upper(),
                    price=float(row.get("price", row.get("p"))),
                    size_contracts=float(row.get("size", row.get("s", row.get("size_contracts", 0)))),
                    buyer_role=str(row.get("buyer_role", row.get("r", row.get("buyer_role", "")))).lower() or None,
                    exchange_ts_ns=_ns_from_delta_ts(row.get("timestamp", row.get("t", msg.get("ts", 0)))),
                    receive_ts_ns=receive_ns,
                    raw=row,
                )
            except Exception:
                continue
            if trade.symbol == self.s.delta_symbol:
                self.features.on_trade(trade)

    def on_private_message(self, msg: dict[str, Any]) -> None:
        receive_ns = time.time_ns()
        self.raw_recorder.write({"stream": "private", "raw": msg})
        typ = msg.get("type")
        if typ == "v2/user_trades":
            seq = msg.get("se")
            if seq is not None:
                seq = int(seq)
                if self.private_fill_seq is not None and seq not in {1, self.private_fill_seq + 1}:
                    self._halt(f"private_fill_sequence_gap expected={self.private_fill_seq + 1} got={seq}")
                self.private_fill_seq = seq
            # private channel is fast but lacks commission; actual cost comes from REST fills.
        elif typ and "position" in typ:
            size = msg.get("size") or (msg.get("result") or {}).get("size") if isinstance(msg.get("result"), dict) else None
            try:
                self.account.open_contracts = int(float(size))
            except Exception:
                pass

    def on_rest_fill(self, raw_fill: dict[str, Any], decision_mid: float | None = None) -> None:
        fill = parse_delta_fill(raw_fill, contract_value_btc=self.contract_value_btc, decision_mid=decision_mid)
        if fill is not None:
            self.costs.observe_fill(fill)

    def on_book(self, book: BookSnapshot) -> dict[str, Any] | None:
        start = time.perf_counter_ns()
        self.last_book = book
        spread_cost = self.costs.round_trip_bps(book.spread_bps)
        feature_row = self.features.vector(book)
        self.models.learn_matured(book.receive_ts_ns, book.mid)
        pred = self.models.observe_decision(book.receive_ts_ns, book.mid, feature_row, spread_cost)
        signal = pred.get("signal")
        costs_snapshot = self.costs.snapshot(book.spread_bps)
        model_status = self.models.status()
        feature_payload = {"book_seq": book.seq, "mid": book.mid, "spread_bps": book.spread_bps, "features": feature_row, "prediction": pred, "costs": costs_snapshot}
        self.feature_recorder.write(feature_payload)

        decision_audit: dict[str, Any] = {
            "book_seq": book.seq,
            "mid": book.mid,
            "spread_bps": book.spread_bps,
            "model_tests": self.models.test_matrix(cost_bps=spread_cost),
            "prediction": pred,
            "chosen_signal": signal,
            "gate_reason": None,
            "plan": None,
            "action": "NO_SIGNAL",
        }
        if signal is None:
            decision_audit["gate_reason"] = self.models.no_signal_reason(cost_bps=spread_cost)
            self.decision_recorder.write(decision_audit)
            return None
        decision = AlphaDecision(
            ts_ns=book.receive_ts_ns,
            side=Side(signal["side"]),
            expected_net_edge_bps=float(signal["expected_net_edge_bps"]),
            confidence=float(signal["confidence"]),
            source="live_learning_l2_tradeflow_stack_v5",
            features=feature_row,
            diagnostics={"model_signal": signal, "costs": costs_snapshot, "models": model_status},
        )
        live_gate = self._live_gate_reason()
        if live_gate is not None:
            decision_audit["action"] = "SIGNAL_BLOCKED_BY_LIVE_GATE"
            decision_audit["gate_reason"] = live_gate
            self.decision_recorder.write(decision_audit)
            return None
        allow_cold_bracket = self.s.trading_mode != "LIVE" or self.s.allow_unvalidated_bootstrap_live or self.models.promoted
        plan = self.risk.build_bracket_plan(decision, book, self.account, spread_cost, allow_cold_start_bracket=allow_cold_bracket)
        if plan is None:
            decision_audit["action"] = "SIGNAL_BLOCKED_BY_RISK_OR_BRACKET"
            decision_audit["gate_reason"] = "risk_sizing_bracket_or_min_edge_rejected"
            self.decision_recorder.write(decision_audit)
            return None
        decision_audit["plan"] = {
            "side": plan.side.value,
            "entry_price": plan.entry_price,
            "stop_price": plan.stop_price,
            "take_profit_price": plan.take_profit_price,
            "quantity_contracts": plan.quantity_contracts,
            "risk_usd": plan.risk_usd,
            "expected_net_edge_bps": plan.expected_net_edge_bps,
            "confidence": plan.confidence,
            "rationale": plan.rationale,
        }
        latency_ms = (time.perf_counter_ns() - start) / 1e6
        if latency_ms > self.s.max_decision_latency_ms:
            decision_audit["action"] = "SIGNAL_BLOCKED_BY_LATENCY"
            decision_audit["gate_reason"] = f"decision_latency_ms={latency_ms:.3f}"
            self.decision_recorder.write(decision_audit)
            return None
        if self.s.trading_mode == "SHADOW":
            decision_audit["action"] = "SHADOW_SIGNAL_TESTED"
            decision_audit["latency_ms"] = latency_ms
            self.decision_recorder.write(decision_audit)
            return {"mode": "SHADOW_SIGNAL_ONLY", "decision": decision, "plan": plan, "latency_ms": latency_ms}
        client_order_id = f"hftv5{int(time.time()*1000)%10**20}"
        result = self.executor.place_atomic_bracket_market(self.s.delta_symbol, self.product_id, plan, client_order_id)
        decision_audit["action"] = "ORDER_SUBMITTED"
        decision_audit["execution"] = result
        decision_audit["latency_ms"] = latency_ms
        self.decision_recorder.write(decision_audit)
        if result.get("success"):
            self.account.open_contracts += plan.quantity_contracts if plan.side is Side.LONG else -plan.quantity_contracts
            self.account.gross_notional_usd += plan.quantity_contracts * self.contract_value_btc * book.mid
        return {"mode": self.s.trading_mode, "decision": decision, "plan": plan, "execution": result, "latency_ms": latency_ms}

    def _live_gate_reason(self) -> str | None:
        if self.sticky_halt_reason:
            return self.sticky_halt_reason
        feed_reason = self._feed_gate_reason()
        if feed_reason and self.s.trading_mode == "LIVE":
            return feed_reason
        if self.s.trading_mode != "LIVE":
            return None
        if not self.models.promoted and not self.s.allow_unvalidated_bootstrap_live:
            return "model_not_promoted_from_real_live_labels"
        if self.models.matured_labels < self.s.min_labels_to_trade and not self.s.allow_unvalidated_bootstrap_live:
            return "insufficient_matured_live_labels"
        if self.costs.snapshot()["real_fill_count"] < self.s.min_real_fills_to_live_trade and not self.s.allow_unvalidated_bootstrap_live:
            return "insufficient_rest_reconciled_real_fills"
        return None

    def _halt(self, reason: str) -> None:
        if not self.sticky_halt_reason:
            self.sticky_halt_reason = reason
            self.account.halted_reason = reason
            log.critical("STICKY HALT: %s", reason)

    def _age_seconds(self, ts_ns: int | None) -> float | None:
        if ts_ns is None:
            return None
        return max(0.0, (time.time_ns() - ts_ns) / 1e9)

    def _feed_gate_reason(self) -> str | None:
        # Once a snapshot is received, BTCUSD should normally produce continuous update/trade flow.
        # A stalled feed means live labels cannot mature and signals/cost estimates are unsafe.
        if self.book.state.snapshots > 0:
            age_update = self._age_seconds(self.last_ob_update_ns)
            age_book = self._age_seconds(self.last_book_event_ns)
            if self.book.state.updates == 0 and self._age_seconds(self.last_book_event_ns) is not None:
                if self._age_seconds(self.last_book_event_ns) > self.s.feed_stall_seconds:
                    return "orderbook_feed_stalled_no_incremental_updates"
            if age_update is not None and age_update > self.s.feed_stall_seconds:
                return "orderbook_incremental_updates_stale"
            if age_book is not None and age_book > self.s.feed_stall_seconds:
                return "book_feature_stream_stale"
        elif self._age_seconds(self.started_ns) is not None and self._age_seconds(self.started_ns) > self.s.feed_stall_seconds:
            return "no_orderbook_snapshot_received"
        return None

    def feed_health(self) -> dict[str, Any]:
        reason = self._feed_gate_reason()
        return {
            "public_event_count": self.public_event_count,
            "book_event_count": self.book_event_count,
            "trade_event_count": self.trade_event_count,
            "seconds_since_public_event": self._age_seconds(self.last_public_event_ns),
            "seconds_since_book_event": self._age_seconds(self.last_book_event_ns),
            "seconds_since_incremental_update": self._age_seconds(self.last_ob_update_ns),
            "seconds_since_trade_event": self._age_seconds(self.last_trade_event_ns),
            "stalled": reason is not None,
            "stall_reason": reason,
        }

    def status(self) -> dict[str, Any]:
        return {
            "mode": self.s.trading_mode,
            "symbol": self.s.delta_symbol,
            "sticky_halt_reason": self.sticky_halt_reason,
            "account": self.account.__dict__.copy(),
            "book_integrity": self.book.integrity(),
            "feed_health": self.feed_health(),
            "models": self.models.status(cost_bps=self.costs.round_trip_bps(self.last_book.spread_bps if self.last_book else 0.0)),
            "costs": self.costs.snapshot(self.last_book.spread_bps if self.last_book else 0.0),
            "recorders": {"raw": self.raw_recorder.stats(), "features": self.feature_recorder.stats(), "decisions": self.decision_recorder.stats()},
            "live_gate_reason": self._live_gate_reason(),
        }

    def close(self) -> None:
        self.models.checkpoint()
        self.s.state_path.write_text(json.dumps(self.status(), indent=2, default=str), encoding="utf-8")
        self.s.telemetry_snapshot.write_text(json.dumps(self.status(), indent=2, default=str), encoding="utf-8")
        self.raw_recorder.close(); self.feature_recorder.close(); self.decision_recorder.close()
