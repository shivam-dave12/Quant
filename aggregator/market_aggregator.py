"""
aggregator/market_aggregator.py — Dual-Exchange Market Data Aggregator
=======================================================================
Fuses microstructure signals (orderbook depth, CVD, tick flow) from
BOTH exchanges while sourcing candles exclusively from the primary.

Architecture
------------
                ┌──────────────────┐     ┌──────────────────┐
                │  CoinSwitchDM    │     │    DeltaDM       │
                │  (data manager)  │     │  (data manager)  │
                └───────┬──────────┘     └────────┬─────────┘
                        │  orderbook                │  orderbook
                        │  trades                   │  trades
                        │  candles (primary only)   │
                        └──────────┬────────────────┘
                                   ▼
                         MarketAggregator
                         ─────────────────
                         get_candles()       → primary DM
                         get_last_price()    → weighted average
                         get_orderbook()     → fused OB snapshot
                         get_recent_trades() → merged + deduplicated
                         ─────────────────
                                   ▼
                            QuantStrategy

What is fused (non-duplicate signals only)
------------------------------------------
  Orderbook depth:  Both exchanges' bid/ask walls merged then re-sorted.
                    Depth imbalance calculated on the merged book (2× data).

  Cumulative Volume Delta (CVD):
                    Trades from both feeds contribute to the rolling CVD.
                    One exchange's spoofed cancel does not fool both.

  Tick Flow:        Both trade streams feed the strategy's real-time handler.
                    Tick flow score is computed on the combined tick stream.

  Order Flow Imbalance:
                    Computed from the merged orderbook; far more reliable
                    than a single-exchange snapshot.

What is NOT fused
-----------------
  Candles:          Only from the primary exchange.  Mixing OHLCV from two
                    exchanges creates synthetic candles — invalid for ATR,
                    VWAP, and ICT structure work.

  Price (mid):      Weighted average of both mids (see AGG_PRIMARY_WEIGHT).
                    Used only for heartbeat/display; strategy uses candle close
                    for all structural decisions.

Fallback behaviour
------------------
  If the secondary exchange is unavailable, weights auto-shift to 1.0/0.0
  and the aggregator operates transparently as a single-exchange wrapper.
  No bot restart required.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Dict, List, Optional

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)


def _norm_levels(raw_levels: list) -> list:
    """
    Normalise orderbook levels to [[price, qty], ...] format.

    Handles both the canonical list format [[price, qty], ...]
    and the Delta Exchange dict format [{'limit_price': p, 'size': q}, ...].
    Returns an empty list for any level that cannot be parsed.
    """
    result = []
    for lvl in (raw_levels or []):
        try:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                result.append([float(lvl[0]), float(lvl[1])])
            elif isinstance(lvl, dict):
                px  = float(lvl.get("limit_price") or lvl.get("price") or 0)
                qty = float(lvl.get("size") or lvl.get("quantity") or
                            lvl.get("depth") or 0)
                if px > 0:
                    result.append([px, qty])
        except Exception:
            continue
    return result


class MarketAggregator:
    """
    Unified data manager facade consumed by QuantStrategy.
    Implements the same interface as CoinSwitchDataManager / DeltaDataManager.
    """

    def __init__(
        self,
        primary_dm,    # CoinSwitchDataManager | DeltaDataManager
        secondary_dm,  # DeltaDataManager | CoinSwitchDataManager | None
    ) -> None:
        self._primary   = primary_dm
        self._secondary = secondary_dm

        self._lock = threading.RLock()

        # Merged trade deque — both feeds write here
        self._merged_trades: deque = deque(maxlen=1000)

        # Strategy ref forwarded to primary DM for real-time candle events
        self._strategy_ref = None

        # Config weights
        self._w_pri = float(getattr(config, "AGG_PRIMARY_WEIGHT",   0.55))
        self._w_sec = float(getattr(config, "AGG_SECONDARY_WEIGHT", 0.45))
        self._ob_depth = int(getattr(config, "AGG_OB_DEPTH_LEVELS", 10))

        # Secondary availability flag — auto-detected
        self._secondary_alive = False

        # Install a tap on the secondary trade stream to merge into our deque
        if self._secondary is not None:
            self._install_secondary_trade_tap()

        logger.info(
            f"MarketAggregator initialised "
            f"(primary={type(primary_dm).__name__} "
            f"secondary={'none' if secondary_dm is None else type(secondary_dm).__name__})"
        )

    # ── Internal: secondary trade tap ────────────────────────────────────────

    def _install_secondary_trade_tap(self) -> None:
        """
        Inject a callback into the secondary data manager's trade stream so
        every trade tick it receives is also appended to our merged deque.
        We do this by monkey-patching its _on_trade method to call our tap
        AFTER its own handler runs.  Thread-safe.
        """
        secondary = self._secondary
        # BUG-AGG-1 FIX: guard against secondary DM that doesn't expose _on_trade.
        # Without this, a secondary DM that lacks the method crashes the entire
        # aggregator at construction time with AttributeError.
        original_on_trade = getattr(secondary, '_on_trade', None)
        if original_on_trade is None:
            logger.warning(
                f"Secondary DM {type(secondary).__name__} has no _on_trade — "
                "trade tap not installed; secondary trades will not flow to aggregator"
            )
            return

        agg_ref = self   # closure capture

        def tapped_on_trade(data: Dict) -> None:
            # Call original first
            original_on_trade(data)
            # Then tap into our merged stream
            try:
                price = float(data.get("price") or data.get("p") or 0)
                # BUG-TAP FIX: Delta WS trade messages use "size" for quantity,
                # not "quantity". The old code read data.get("quantity") which
                # always returned None for Delta → qty=0 for all secondary
                # trades → CVD magnitude from secondary permanently zeroed.
                # Fix: mirror the key-priority order used in DeltaDataManager._on_trade.
                qty   = float(data.get("size") or data.get("quantity") or
                              data.get("q") or 0)
                side_raw = data.get("side") or ""
                side = "buy" if str(side_raw).lower() == "buy" else "sell"
                if not side_raw:
                    # Fallback: "m"=True means buyer was maker = sell aggressor
                    side = "sell" if data.get("m") else "buy"
                if price > 0:
                    with agg_ref._lock:
                        agg_ref._merged_trades.append({
                            "price":     price,
                            "quantity":  qty,
                            "side":      side,
                            "timestamp": time.time(),
                            "source":    "secondary",
                        })
                        agg_ref._secondary_alive = True
            except Exception:
                pass

        secondary._on_trade = tapped_on_trade

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        """Start both data managers concurrently for faster boot."""
        import threading

        primary_ok = [False]
        secondary_ok = [False]

        def start_primary():
            primary_ok[0] = self._primary.start()

        def start_secondary():
            if self._secondary is None:
                return
            try:
                secondary_ok[0] = self._secondary.start()
            except Exception as e:
                logger.warning(f"Secondary DM start failed (non-fatal): {e}")
                secondary_ok[0] = False

        t1 = threading.Thread(target=start_primary,   daemon=True)
        t2 = threading.Thread(target=start_secondary, daemon=True)
        t1.start(); t2.start()
        t1.join(); t2.join()

        if not primary_ok[0]:
            logger.error("❌ Primary DM failed to start — cannot trade")
            return False

        if self._secondary and not secondary_ok[0]:
            logger.warning(
                "⚠️  Secondary DM failed to start — running on primary only. "
                "CVD/OB signals will be single-exchange."
            )
            self._secondary_alive = False
        elif self._secondary:
            self._secondary_alive = True
            logger.info("✅ Both exchanges live — dual-feed aggregation active")

        return True

    def stop(self) -> None:
        self._primary.stop()
        if self._secondary:
            try:
                self._secondary.stop()
            except Exception:
                pass

    def restart_streams(self) -> bool:
        ok = self._primary.restart_streams()
        if self._secondary:
            try:
                self._secondary.restart_streams()
            except Exception:
                pass
        return ok

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        """
        Wait for the primary DM to be ready.
        If primary fails within timeout AND secondary is available and ready,
        transparently swap them so the bot can still trade.
        """
        import time as _time

        # Fast path — primary is already ready
        if self._primary.is_ready:
            return True

        # Wait for primary
        ready = self._primary.wait_until_ready(timeout_sec)
        if ready:
            return True

        # Primary timed out — check if secondary can take over
        if self._secondary is not None and getattr(self._secondary, 'is_ready', False):
            logger.warning(
                "⚠️  Primary DM not ready within timeout — "
                "promoting secondary to primary for candle data."
            )
            # Swap: secondary becomes primary for candle reads
            # (microstructure tap from old secondary still flows; we just
            # redirect get_candles / get_last_price to the new primary)
            self._primary, self._secondary = self._secondary, self._primary
            self._secondary_alive = True
            # BUG-AGG-2 FIX: after promoting secondary to primary, re-register
            # the strategy on the new primary so its _on_trade fires callbacks.
            # Without this, the new primary's _strategy_ref is None and all
            # real-time trade callbacks (tick-flow, CVD) are permanently severed.
            if self._strategy_ref is not None:
                try:
                    self._primary.register_strategy(self._strategy_ref)
                    logger.info(
                        "✅ Strategy re-registered on new primary after failover"
                    )
                except Exception as _reg_e:
                    logger.error(
                        f"Failed to re-register strategy on new primary: {_reg_e}"
                    )
            logger.info(
                f"✅ Swapped: new primary={type(self._primary).__name__} "
                f"new secondary={type(self._secondary).__name__}"
            )
            return True

        logger.error("❌ Both data managers not ready — bot cannot trade safely")
        return False

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy
        self._primary.register_strategy(strategy)
        # Secondary does NOT register strategy — we don't want double
        # on_realtime_trade calls.  The tap above handles secondary trades.

    # ── Candles — primary exchange only ──────────────────────────────────────

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        return self._primary.get_candles(timeframe, limit)

    # ── Price — weighted average (display only) ────────────────────────────

    def get_last_price(self) -> float:
        p_price = self._primary.get_last_price()
        if self._secondary is None or not self._secondary_alive:
            return p_price
        try:
            s_price = self._secondary.get_last_price()
            if s_price > 0 and p_price > 0:
                return p_price * self._w_pri + s_price * self._w_sec
        except Exception:
            pass
        return p_price

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        return self._primary.is_price_fresh(max_stale_seconds)

    # ── Orderbook — fused from both exchanges ─────────────────────────────────

    def get_orderbook(self) -> Dict:
        """
        Merge orderbooks from both exchanges.

        Strategy:
          1. Collect bids and asks from both feeds up to AGG_OB_DEPTH_LEVELS each.
          2. Combine price levels — at the same price point, sum quantities.
          3. Re-sort: bids descending, asks ascending.
          4. Return the top AGG_OB_DEPTH_LEVELS levels of each side.

        This creates a 'virtual' aggregated book that shows true depth
        across both venues.  CVD and OFI signals computed on this book
        are substantially more reliable than single-exchange snapshots.
        """
        p_ob = self._primary.get_orderbook()

        if self._secondary is None or not self._secondary_alive:
            # Normalise even single-exchange OBs so consumers get [[p,q]] always
            return {
                "bids": _norm_levels(p_ob.get("bids", []) or p_ob.get("buy", [])),
                "asks": _norm_levels(p_ob.get("asks", []) or p_ob.get("sell", [])),
                "timestamp": p_ob.get("timestamp", time.time()),
            }

        try:
            s_ob = self._secondary.get_orderbook()
            if not s_ob:
                return p_ob
        except Exception:
            return p_ob

        def merge_side(p_levels: list, s_levels: list, ascending: bool) -> list:
            combined: Dict[float, float] = {}
            all_levels = (
                _norm_levels(p_levels)[:self._ob_depth] +
                _norm_levels(s_levels)[:self._ob_depth]
            )
            for lvl in all_levels:
                try:
                    px  = round(lvl[0], 2)
                    qty = lvl[1]
                    combined[px] = combined.get(px, 0.0) + qty
                except Exception:
                    continue
            sorted_levels = sorted(combined.items(),
                                   key=lambda x: x[0],
                                   reverse=not ascending)
            return [[px, qty] for px, qty in sorted_levels[:self._ob_depth]]

        merged_bids = merge_side(p_ob.get("bids", []) or p_ob.get("buy", []),
                                 s_ob.get("bids", []) or s_ob.get("buy", []),
                                 ascending=False)
        merged_asks = merge_side(p_ob.get("asks", []) or p_ob.get("sell", []),
                                 s_ob.get("asks", []) or s_ob.get("sell", []),
                                 ascending=True)

        return {
            "bids":      merged_bids,
            "asks":      merged_asks,
            "timestamp": time.time(),
            "_sources":  2,
        }

    # ── Trades — merged from both exchanges ──────────────────────────────────

    def get_recent_trades_raw(self) -> List[Dict]:
        """
        Return the merged, time-ordered trade stream from both exchanges.

        Primary trades + secondary tap = ~2× the tick data for CVD/tick-flow.
        Deduplication is by source tag so cross-exchange fills of the same
        institutional order show up twice (they ARE two separate fills).
        Trades are sorted newest-last for compatibility with strategy code.
        """
        p_trades = self._primary.get_recent_trades_raw()

        if self._secondary is None or not self._secondary_alive:
            return p_trades

        with self._lock:
            s_trades = list(self._merged_trades)

        # Merge and sort by timestamp, keep last 400
        all_trades = p_trades + [t for t in s_trades if t.get("source") == "secondary"]
        all_trades.sort(key=lambda t: t.get("timestamp", 0))
        return all_trades[-400:]

    # ── Supplementary helpers (forwarded to primary) ──────────────────────────

    def get_volume_delta(self, lookback_seconds: float = 60.0) -> Dict:
        """CVD from the merged trade stream."""
        merged = self.get_recent_trades_raw()
        cutoff = time.time() - lookback_seconds
        buy_vol  = sum(t["quantity"] for t in merged
                       if t.get("timestamp", 0) >= cutoff and t.get("side") == "buy")
        sell_vol = sum(t["quantity"] for t in merged
                       if t.get("timestamp", 0) >= cutoff and t.get("side") == "sell")
        total = buy_vol + sell_vol
        return {
            "buy_volume":  buy_vol,
            "sell_volume": sell_vol,
            "delta":       buy_vol - sell_vol,
            "delta_pct":   (buy_vol - sell_vol) / total if total > 0 else 0.0,
        }

    @property
    def ws(self):
        """Expose primary WS for health supervisor."""
        return getattr(self._primary, "ws", None)

    def get_secondary_status(self) -> Dict:
        return {
            "alive":       self._secondary_alive,
            "has_secondary": self._secondary is not None,
            "primary":     type(self._primary).__name__,
            "secondary":   type(self._secondary).__name__ if self._secondary else "none",
        }
