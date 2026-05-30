from __future__ import annotations

from collections import deque
from pathlib import Path
import json
import threading
import time
from typing import Any

import numpy as np

from .types import FillRecord


class CostModel:
    """Fee/slippage model: scheduled taker+GST floor, upgraded by REST-reconciled fills."""

    def __init__(self, *, taker_fee_bps_pre_gst: float, maker_fee_bps_pre_gst: float, gst_rate: float, impact_floor_bps: float, min_real_fills: int, ledger_path: str | Path) -> None:
        self.taker_fee_bps_pre_gst = float(taker_fee_bps_pre_gst)
        self.maker_fee_bps_pre_gst = float(maker_fee_bps_pre_gst)
        self.gst_rate = float(gst_rate)
        self.impact_floor_bps = float(impact_floor_bps)
        self.min_real_fills = int(min_real_fills)
        self.fees: deque[float] = deque(maxlen=5000)
        self.slippages: deque[float] = deque(maxlen=5000)
        self.seen_ids: set[str] = set()
        self.lock = threading.RLock()
        self.ledger_path = Path(ledger_path)
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.ledger_path.exists() or self.ledger_path.stat().st_size == 0:
            with self.ledger_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({
                    "type": "execution_feedback_metadata",
                    "source": "delta_rest_v2_fills_reconciled_by_runtime",
                    "venue": "DELTA", "symbol": "BTCUSD", "is_synthetic": False,
                    "schema_version": 2,
                }, separators=(",", ":")) + "\n")

    @property
    def scheduled_one_way_taker_fee_bps_with_gst(self) -> float:
        return self.taker_fee_bps_pre_gst * (1.0 + self.gst_rate)

    def configure_from_product(self, product: dict[str, Any]) -> None:
        def rate(key: str, cur: float) -> float:
            try:
                x = float(product.get(key))
                return x * 1e4 if 0 < x < 1 else cur
            except Exception:
                return cur
        with self.lock:
            self.taker_fee_bps_pre_gst = rate("taker_commission_rate", self.taker_fee_bps_pre_gst)
            self.maker_fee_bps_pre_gst = rate("maker_commission_rate", self.maker_fee_bps_pre_gst)

    def observe_fill(self, fill: FillRecord) -> bool:
        with self.lock:
            if fill.fill_id in self.seen_ids:
                return False
            self.seen_ids.add(fill.fill_id)
            self.fees.append(max(0.0, fill.fee_bps))
            slip = fill.slippage_bps
            if slip is not None and np.isfinite(slip):
                self.slippages.append(max(0.0, float(slip)))
        with self.ledger_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"type": "rest_reconciled_fill", "recorded_at_ns": time.time_ns(), **fill.to_json()}, separators=(",", ":"), default=str) + "\n")
        return True

    def one_way_fee_bps(self) -> float:
        with self.lock:
            if len(self.fees) >= self.min_real_fills:
                return max(self.scheduled_one_way_taker_fee_bps_with_gst, float(np.quantile(np.asarray(self.fees), 0.95)))
            return self.scheduled_one_way_taker_fee_bps_with_gst

    def one_way_impact_bps(self, spread_bps: float = 0.0) -> float:
        floor = max(self.impact_floor_bps, max(0.0, spread_bps) / 2.0)
        with self.lock:
            if len(self.slippages) >= self.min_real_fills:
                return max(floor, float(np.quantile(np.asarray(self.slippages), 0.95)))
            return floor

    def round_trip_bps(self, spread_bps: float = 0.0) -> float:
        return 2.0 * (self.one_way_fee_bps() + self.one_way_impact_bps(spread_bps))

    def snapshot(self, spread_bps: float = 0.0) -> dict[str, Any]:
        with self.lock:
            return {
                "real_fill_count": len(self.fees),
                "scheduled_one_way_taker_fee_bps_with_gst": self.scheduled_one_way_taker_fee_bps_with_gst,
                "one_way_fee_bps": self.one_way_fee_bps(),
                "one_way_impact_bps": self.one_way_impact_bps(spread_bps),
                "round_trip_bps": self.round_trip_bps(spread_bps),
                "fee_basis": "rest_fill_p95_floor" if len(self.fees) >= self.min_real_fills else "scheduled_taker_plus_gst",
                "impact_basis": "real_slippage_p95" if len(self.slippages) >= self.min_real_fills else "spread_half_plus_floor",
            }


def parse_delta_fill(raw: dict[str, Any], *, contract_value_btc: float, decision_mid: float | None = None) -> FillRecord | None:
    try:
        fid = str(raw.get("id") or raw.get("fill_id") or raw.get("f") or "")
        if not fid:
            return None
        size = int(float(raw.get("size", raw.get("s"))))
        price = float(raw.get("price", raw.get("p")))
        commission = raw.get("commission")
        if commission is None:
            commission = (raw.get("meta_data") or {}).get("total_commission_in_settling_asset")
        if commission is None:
            return None
        return FillRecord(
            fill_id=fid,
            order_id=str(raw.get("order_id", raw.get("o", ""))),
            symbol=str(raw.get("product_symbol", raw.get("sy", "BTCUSD"))).upper(),
            side=str(raw.get("side", raw.get("S", ""))).lower(),
            role=str(raw.get("role", raw.get("r", "unknown"))).lower(),
            size_contracts=abs(size),
            price=price,
            commission_settling=float(commission),
            receive_ts_ns=time.time_ns(),
            exchange_created_at=str(raw.get("created_at", raw.get("t", ""))) or None,
            decision_mid=decision_mid,
            contract_value_btc=float(contract_value_btc),
        )
    except Exception:
        return None
