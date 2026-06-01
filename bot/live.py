from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from .collector import flatten_option_chain, flatten_quote
from .config import BotConfig
from .groww_adapter import GrowwAdapter
from .nse_contracts import load_nifty_options_contracts
from .option_models import (
    OptionModelSuite,
    build_option_model_frame,
    load_option_model_raw_data,
    train_model_suite_from_store,
)
from .risk import limit_price_from_quote, option_buy_quantity, quote_is_executable
from .storage import Store

log = logging.getLogger(__name__)


class LiveOptionBot:
    """Live option-only loop.

    The live loop is strict by design:
    - it records Groww option-chain/quote data first;
    - it trains only from rows tagged by the live Groww collector;
    - it refuses to trade if no real trained model suite exists;
    - it refuses live orders unless the model passes live gates and the two-key live switch is enabled.
    """

    def __init__(self, cfg: BotConfig, store: Store, adapter: GrowwAdapter):
        self.cfg = cfg
        self.store = store
        self.adapter = adapter
        self.suite: OptionModelSuite | None = self._load_suite_if_exists()
        self.last_train_status: dict[str, Any] = {}
        self.contract_meta = None
        if cfg.nse_contract_file.exists():
            try:
                self.contract_meta = load_nifty_options_contracts(cfg.nse_contract_file, cfg.underlying).set_index("trading_symbol")
            except Exception:
                self.contract_meta = None

    def _load_suite_if_exists(self) -> OptionModelSuite | None:
        if not self.cfg.model_suite_path.exists():
            return None
        try:
            return OptionModelSuite.load(self.cfg.model_suite_path)
        except Exception:
            log.exception("model suite load failed | path=%s", self.cfg.model_suite_path)
            return None

    def model_gate_passed(self) -> bool:
        return bool(self.suite and self.suite.metrics.get("passed_live_gate", False))

    def _maybe_train_live_model(self, force: bool = False) -> dict[str, Any]:
        if not self.cfg.auto_train_enabled and not force:
            return {"trained": False, "reason": "auto_train_disabled"}
        if self.suite is not None and self.model_gate_passed() and not force:
            return {"trained": False, "reason": "existing_model_gate_passed"}
        try:
            result = train_model_suite_from_store(self.cfg, self.store)
            self.suite = OptionModelSuite.load(result.model_path)
            status = {
                "trained": True,
                "model_path": str(result.model_path),
                "passed_live_gate": bool(result.metrics.get("passed_live_gate", False)),
                "top1_win_rate": result.metrics.get("top1_per_snapshot_win_rate"),
                "top1_sharpe": result.metrics.get("top1_per_snapshot_sharpe"),
                "top1_alpha": result.metrics.get("top1_per_snapshot_alpha_vs_universe"),
            }
            log.info("🧠 live model trained | %s", json.dumps(status, default=str))
            self.last_train_status = status
            return status
        except ValueError as exc:
            status = {"trained": False, "reason": str(exc)}
            self.last_train_status = status
            log.info("model not ready | %s", exc)
            return status
        except Exception as exc:
            status = {"trained": False, "reason": f"training_error: {exc}"}
            self.last_train_status = status
            log.exception("live model training failed")
            return status

    def _collect_quotes_for_snapshot(self, snap: pd.DataFrame) -> int:
        if snap.empty:
            return 0
        candidates = snap[
            snap["ltp"].between(self.cfg.min_ltp, self.cfg.max_ltp)
            & (snap["volume"].fillna(0) >= self.cfg.min_volume)
            & (snap["open_interest"].fillna(0) >= self.cfg.min_oi)
        ].copy()
        if candidates.empty:
            return 0
        candidates["liq_score"] = candidates["volume"].fillna(0) + candidates["open_interest"].fillna(0) * 0.05
        symbols = (
            candidates.sort_values("liq_score", ascending=False)["trading_symbol"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .head(self.cfg.max_quote_symbols)
            .tolist()
        )
        total = 0
        for sym in symbols:
            try:
                q = self.adapter.get_quote(sym)
                total += self.store.append_df("quote_snapshots", flatten_quote(sym, q))
            except Exception:
                log.exception("quote snapshot failed | symbol=%s", sym)
        return total

    def collect_and_score(self, expiry: str) -> pd.DataFrame:
        payload = self.adapter.get_option_chain(self.cfg.underlying, expiry)
        snap = flatten_option_chain(payload, expiry)
        rows = self.store.append_df("option_chain_snapshots", snap)
        quote_rows = self._collect_quotes_for_snapshot(snap)
        log.info("📥 live snapshot saved | chain_rows=%s quote_rows=%s", rows, quote_rows)

        if self.suite is None or not self.model_gate_passed():
            self._maybe_train_live_model()
        if self.suite is None:
            return pd.DataFrame()

        raw = load_option_model_raw_data(self.cfg, self.store)
        if raw.empty:
            return pd.DataFrame()
        frame = build_option_model_frame(raw, horizon_rows=self.cfg.label_horizon_rows, cost_bps=self.cfg.estimated_round_trip_cost_bps)
        if frame.empty:
            return pd.DataFrame()
        latest_ts = pd.to_datetime(frame["ts"]).max()
        latest = frame[pd.to_datetime(frame["ts"]).eq(latest_ts)].copy()
        eligible = latest[
            latest["ltp"].between(self.cfg.min_ltp, self.cfg.max_ltp)
            & (latest["volume"].fillna(0) >= self.cfg.min_volume)
            & (latest["open_interest"].fillna(0) >= self.cfg.min_oi)
        ].copy()
        if eligible.empty:
            return eligible

        scored = self.suite.predict(eligible)
        for col in [
            "open_interest", "volume", "mid_price", "quoted_entry_price", "quoted_exit_price",
            "spread", "spread_pct", "bid_price", "offer_price", "bid_quantity", "ask_quantity",
        ]:
            if col in eligible.columns:
                scored[col] = eligible.loc[scored.index, col]
        scored["edge_score"] = scored["predicted_return"] - self.cfg.uncertainty_buffer
        return scored.sort_values(["model_score", "edge_score"], ascending=False).reset_index(drop=True)

    def decide_once(self, expiry: str) -> dict[str, Any]:
        scored = self.collect_and_score(expiry)
        if self.suite is None:
            return {"decision": "NO_TRADE", "reason": "no_real_groww_trained_model", "train_status": self.last_train_status}
        if scored.empty:
            return {"decision": "NO_TRADE", "reason": "no_eligible_options", "train_status": self.last_train_status}
        top = scored.iloc[0].to_dict()
        if float(top.get("edge_score", 0.0)) < self.cfg.min_edge_return:
            self._save_signal(top, "NO_TRADE", "edge_below_threshold")
            return {"decision": "NO_TRADE", "reason": "edge_below_threshold", "top": top, "train_status": self.last_train_status}
        self._save_signal(top, "TRADE_CANDIDATE", "edge_positive")
        return {"decision": "TRADE_CANDIDATE", "top": top, "train_status": self.last_train_status}

    def _save_signal(self, row: dict[str, Any], decision: str, reason: str) -> None:
        feature_cols = self.suite.feature_cols if self.suite is not None else []
        df = pd.DataFrame([{
            "ts": datetime.utcnow(),
            "trading_symbol": row.get("trading_symbol"),
            "expiry": pd.to_datetime(row.get("expiry")).date() if row.get("expiry") is not None else None,
            "strike": row.get("strike"),
            "option_type": row.get("option_type"),
            "ltp": row.get("ltp"),
            "predicted_return": row.get("predicted_return"),
            "edge_score": row.get("edge_score"),
            "decision": decision,
            "reason": reason,
            "features_json": json.dumps({k: row.get(k) for k in feature_cols if k in row}, default=str),
        }])
        self.store.append_df("model_signals", df)

    def trade_once(self, expiry: str) -> dict[str, Any]:
        decision = self.decide_once(expiry)
        if decision.get("decision") != "TRADE_CANDIDATE":
            log.info("⏸ no trade | %s", decision.get("reason"))
            return decision

        if not self.model_gate_passed():
            metrics = self.suite.metrics if self.suite is not None else {}
            log.warning("🛑 live blocked: model has not passed real-data live gates | metrics=%s", json.dumps(metrics, default=str))
            decision["decision"] = "PAPER_ONLY_MODEL_GATE_FAILED"
            return decision

        top = decision["top"]
        sym = str(top["trading_symbol"])
        lot_size = 50
        tick_size = 0.05
        if self.contract_meta is not None and sym in self.contract_meta.index:
            meta = self.contract_meta.loc[sym]
            lot_size = int(float(meta.get("lot_size") or lot_size))
            tick_size = float(meta.get("tick_size") or tick_size)

        quote = self.adapter.get_quote(sym)
        ok, reason = quote_is_executable(quote)
        if not ok:
            log.warning("🛑 execution blocked | symbol=%s reason=%s", sym, reason)
            decision["decision"] = "NO_TRADE"
            decision["reason"] = reason
            return decision

        price = limit_price_from_quote(quote, tick_size=tick_size)
        if price is None:
            decision["decision"] = "NO_TRADE"
            decision["reason"] = "no_limit_price"
            return decision

        qty = option_buy_quantity(price, lot_size, self.cfg.account_capital, self.cfg.risk_per_trade_pct, self.cfg.max_premium_value_per_trade)
        if qty <= 0:
            decision["decision"] = "NO_TRADE"
            decision["reason"] = "qty_zero_risk_budget"
            return decision

        if self.cfg.paper_trading or not self.cfg.live_trading_enabled:
            log.info("🧪 PAPER BUY | %s qty=%s limit=%.2f edge=%.4f", sym, qty, price, float(top["edge_score"]))
            self._save_order("PAPER", sym, "BUY", qty, price, None, None, "PAPER", {"signal": top, "quote": quote})
            decision["decision"] = "PAPER_BUY"
            decision["qty"] = qty
            decision["limit_price"] = price
            return decision

        log.warning("🚨 LIVE BUY | %s qty=%s limit=%.2f", sym, qty, price)
        order = self.adapter.place_buy_option_limit(sym, qty, price, product=self.cfg.product)
        oid = order.get("groww_order_id")
        self._save_order("LIVE", sym, "BUY", qty, price, oid, order.get("order_reference_id"), order.get("order_status"), order)
        if not oid:
            decision["decision"] = "ORDER_SUBMIT_FAILED"
            decision["order"] = order
            return decision

        fill = self.adapter.wait_for_fill(oid, timeout_seconds=10)
        avg_fill = fill.get("average_fill_price") or fill.get("avgFillPrice") or price
        filled_qty = int(float(fill.get("filled_quantity") or fill.get("filledQty") or 0))
        if filled_qty <= 0:
            decision["decision"] = "ENTRY_NOT_FILLED"
            decision["order"] = fill
            return decision

        oco = self.adapter.create_exit_oco(sym, filled_qty, float(avg_fill), self.cfg.tp_pct, self.cfg.sl_pct, self.cfg.product)
        decision["decision"] = "LIVE_BUY_WITH_OCO"
        decision["entry_order"] = fill
        decision["oco"] = oco
        return decision

    def _save_order(self, mode, sym, side, qty, price, oid, ref, status, raw) -> None:
        df = pd.DataFrame([{
            "ts": datetime.utcnow(),
            "mode": mode,
            "trading_symbol": sym,
            "transaction_type": side,
            "quantity": int(qty),
            "price": float(price),
            "groww_order_id": oid,
            "order_reference_id": ref,
            "order_status": status,
            "raw_json": json.dumps(raw, default=str),
        }])
        self.store.append_df("orders", df)
