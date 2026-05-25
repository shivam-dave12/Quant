"""
aggregator/market_aggregator.py — Dual-Exchange Market Data Aggregator
=======================================================================
Aggregates raw venue market data (candles, trades and executable quotes) from
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
                         get_orderbook()     → executable primary OB + secondary side-channel
                         get_recent_trades() → merged + deduplicated
                         ─────────────────
                                   ▼
                            QuantStrategy

What is fused (non-duplicate signals only)
------------------------------------------
  Orderbook depth:  Both exchanges' bid/ask walls merged then re-sorted.
                    Depth imbalance calculated on the merged book (2× data).

  Trade-stream continuity:
                    Trades from both feeds contribute to the rolling trade stream.
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
        instrument=None,
        analysis_dm=None,
    ) -> None:
        self.instrument = instrument
        self._primary   = primary_dm
        self._secondary = secondary_dm
        self._analysis  = analysis_dm

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
            f"[{getattr(instrument, 'asset_id', 'legacy')}] "
            f"(primary={type(primary_dm).__name__} "
            f"secondary={'none' if secondary_dm is None else type(secondary_dm).__name__} "
            f"analysis={'none' if analysis_dm is None else type(analysis_dm).__name__})"
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
                # trades → trade-stream completeness from secondary permanently zeroed.
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

        analysis_ok = [True]

        def start_analysis():
            if self._analysis is None:
                return
            try:
                analysis_ok[0] = self._analysis.start()
            except Exception as e:
                logger.warning(f"Analysis DM start failed (non-fatal): {e}")
                analysis_ok[0] = False

        t1 = threading.Thread(target=start_primary,   daemon=True)
        t2 = threading.Thread(target=start_secondary, daemon=True)
        t3 = threading.Thread(target=start_analysis,  daemon=True)
        t1.start(); t2.start(); t3.start()
        t1.join(); t2.join(); t3.join()

        if not primary_ok[0]:
            logger.error("❌ Primary DM failed to start — cannot trade")
            return False

        if self._secondary and not secondary_ok[0]:
            logger.warning(
                "⚠️  Secondary DM failed to start — running on primary only. "
                "raw trade/quote telemetry will be single-exchange."
            )
            self._secondary_alive = False
        elif self._secondary:
            self._secondary_alive = True
            logger.info("✅ Both exchanges live — dual-feed aggregation active")

        if self._analysis and not analysis_ok[0]:
            logger.warning("Analysis DM unavailable; primary candles will be used for structure")

        return True

    def stop(self) -> None:
        self._primary.stop()
        if self._analysis:
            try:
                self._analysis.stop()
            except Exception:
                pass
        if self._secondary:
            try:
                self._secondary.stop()
            except Exception:
                pass

    def restart_streams(self) -> bool:
        ok = self._primary.restart_streams()
        if self._analysis:
            try:
                self._analysis.restart_streams()
            except Exception:
                pass
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
            if self._analysis is not None:
                try:
                    self._analysis.wait_until_ready(min(timeout_sec, 30.0))
                except Exception:
                    pass
            return True

        # Wait for primary
        ready = self._primary.wait_until_ready(timeout_sec)
        if ready:
            if self._analysis is not None:
                try:
                    self._analysis.wait_until_ready(min(timeout_sec, 30.0))
                except Exception:
                    pass
            return True

        # Primary timed out — check if secondary can take over
        if self._secondary is not None and getattr(self._secondary, 'is_ready', False):
            logger.warning(
                "⚠️  Primary DM not ready within timeout — "
                "promoting secondary to primary for candle data."
            )
            # Swap: secondary becomes primary for candle reads
            self._primary, self._secondary = self._secondary, self._primary
            self._secondary_alive = True
            # BUG-AGG-2 FIX: after promoting secondary to primary, re-register
            # the strategy on the new primary so its _on_trade fires callbacks.
            # Without this, the new primary's _strategy_ref is None and all
            # real-time trade callbacks (trade and quote callbacks) are permanently severed.
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

            # BUG-AGG-3 FIX: the OLD primary is now the NEW secondary, but its
            # _on_trade is the untapped original. After the swap, secondary
            # trades no longer flow into _merged_trades. Re-install the tap
            # on the new secondary so dual-feed trade monitoring resumes.
            if self._secondary is not None:
                try:
                    self._install_secondary_trade_tap()
                    logger.info(
                        "✅ Trade tap re-installed on new secondary after failover"
                    )
                except Exception as _tap_e:
                    logger.error(
                        f"Failed to re-install trade tap on new secondary: {_tap_e}"
                    )

            logger.info(
                f"✅ Swapped: new primary={type(self._primary).__name__} "
                f"new secondary={type(self._secondary).__name__}"
            )
            return True

        logger.error("❌ Both data managers not ready — bot cannot trade safely")
        return False

    def prepare_icici_session_contract_book(self, available_funds: float) -> bool:
        """Preselect CE/PE execution vehicles after underlying warmup.

        This delegates only for the ICICI option-primary manager.  Structural
        analysis remains on the underlying feed; no direction is forecast here.
        """
        preparer = getattr(self._primary, "prepare_session_contract_book", None)
        if not callable(preparer):
            return True
        spot = 0.0
        if self._analysis is not None:
            try:
                spot = float(self._analysis.get_last_price() or 0.0)
            except Exception:
                spot = 0.0
        if spot <= 0:
            try:
                spot = float(self._primary.get_last_price() or 0.0)
            except Exception:
                spot = 0.0
        return bool(preparer(spot, float(available_funds or 0.0), reason="session_start"))

    def release_icici_execution_vehicle(self) -> None:
        releaser = getattr(self._primary, "release_execution_vehicle", None)
        if callable(releaser):
            releaser()

    def get_session_contract_book_status(self) -> Dict:
        status_fn = getattr(self._primary, "session_contract_book_status", None)
        if callable(status_fn):
            try:
                return dict(status_fn() or {})
            except Exception:
                return {"status": "ERROR"}
        return {"status": "UNSUPPORTED"}

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy
        self._primary.register_strategy(strategy)
        if self._analysis is not None:
            try:
                self._analysis.register_strategy(strategy)
            except Exception:
                pass
        # Secondary does NOT register strategy — we don't want double
        # on_realtime_trade calls.  The tap above handles secondary trades.

    # ── Candles — primary exchange only ──────────────────────────────────────

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        if self._analysis is not None:
            try:
                rows = self._analysis.get_candles(timeframe, limit)
                if rows:
                    return rows
            except Exception:
                pass
        return self._primary.get_candles(timeframe, limit)

    def get_execution_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        return self._primary.get_candles(timeframe, limit)

    # ── Price — weighted average (display only) ────────────────────────────

    def get_last_price(self) -> float:
        """Executable instrument mark price (option premium once ICICI vehicle is activated)."""
        price = self._primary.get_last_price()
        if price > 0:
            return price
        if self._analysis is not None:
            try:
                return float(self._analysis.get_last_price() or 0.0)
            except Exception:
                pass
        return price

    def get_analysis_price(self) -> float:
        """Price in the structural-analysis domain.

        For ICICI long-premium execution, ``get_last_price`` changes to the
        selected option premium after contract activation, while liquidity/ICT
        state remains in underlying-index units.  Exposing this separately
        prevents premium prices being compared with NIFTY liquidity pools.
        """
        if self._analysis is not None:
            try:
                price = float(self._analysis.get_last_price() or 0.0)
                if price > 0:
                    return price
            except Exception:
                pass
        return float(self.get_last_price() or 0.0)

    def get_consensus_price(self) -> float:
        """Weighted cross-venue price for display/diagnostics only."""
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

    def get_feed_reliability(self) -> Dict:
        """
        Return data-quality metadata consumed by operator UI and strategy.

        Reliability is not an alpha signal. It is a confidence multiplier for
        raw monitoring features. When the secondary feed is unavailable, raw trade/quote telemetry
        information remains usable but must be treated as single-venue evidence.
        """
        primary_ready = bool(getattr(self._primary, "is_ready", False))
        analysis_ready = bool(self._analysis is not None and getattr(self._analysis, "is_ready", False))
        secondary_configured = self._secondary is not None
        secondary_ready = bool(
            secondary_configured and self._secondary_alive
            and getattr(self._secondary, "is_ready", True)
        )
        sources = 1 + int(secondary_ready)
        microstructure_weight = 1.0 if secondary_ready else 0.62
        return {
            "primary_ready": primary_ready,
            "analysis_ready": analysis_ready,
            "secondary_configured": secondary_configured,
            "secondary_alive": secondary_ready,
            "sources": sources,
            "microstructure_weight": microstructure_weight,
            "mode": "analysis_underlying" if analysis_ready else ("dual" if secondary_ready else "single"),
            "note": (
                "underlying-analysis + executable option premium" if analysis_ready else
                "dual-feed microstructure" if secondary_ready
                else "single-feed microstructure; structural engine does not consume raw trade/quote telemetry"
            ),
        }

    def get_data_quality(self) -> Dict:
        """Backward-compatible alias for dashboards/controllers."""
        return self.get_feed_reliability()

    def get_data_lineage(self) -> Dict:
        """State-domain lineage: analysis candles and executable pricing must not be mixed."""
        return {
            "analysis_source": type(self._analysis if self._analysis is not None else self._primary).__name__,
            "execution_source": type(self._primary).__name__,
            "analysis_domain": "UNDERLYING" if self._analysis is not None else "EXECUTION_INSTRUMENT",
            "execution_domain": "OPTION_PREMIUM" if self._analysis is not None else "EXECUTION_INSTRUMENT",
            "secondary_source": type(self._secondary).__name__ if self._secondary is not None else "none",
        }

    def is_analysis_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        """Freshness of the price/candles used for the structural decision domain."""
        dm = self._analysis if self._analysis is not None else self._primary
        try:
            return bool(dm.is_price_fresh(max_stale_seconds))
        except Exception:
            return False

    def is_execution_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        """Freshness of the instrument that will actually be ordered/fill-reconciled."""
        try:
            return bool(self._primary.is_price_fresh(max_stale_seconds))
        except Exception:
            return False

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        # Backward-compatible health view; entry authority must call
        # is_analysis_price_fresh so option-premium freshness cannot conceal a
        # stale NIFTY-underlying structural feed.
        return self.is_analysis_price_fresh(max_stale_seconds)

    @staticmethod
    def _update_timestamp(dm) -> float:
        if dm is None:
            return 0.0
        try:
            value = dm.get_last_update()
            if hasattr(value, "timestamp"):
                value = value.timestamp()
            return float(value or 0.0)
        except Exception:
            return 0.0

    def get_analysis_last_update(self) -> float:
        return self._update_timestamp(self._analysis if self._analysis is not None else self._primary)

    def get_execution_last_update(self) -> float:
        return self._update_timestamp(self._primary)

    def get_last_update(self) -> float:
        vals = []
        for dm in (self._primary, self._secondary, self._analysis):
            if dm is None:
                continue
            value = self._update_timestamp(dm)
            if value > 0:
                vals.append(value)
        return max(vals) if vals else 0.0

    # ── Orderbook — fused from both exchanges ─────────────────────────────────

    def get_orderbook(self) -> Dict:
        """
        Return the executable primary venue book.

        Cross-venue synthetic depth is useful for diagnostics, but it is not an
        executable book: merging CoinSwitch and Delta levels can invent liquidity
        that cannot be hit by a single Delta order. Consumers that route orders,
        compute spread, or validate stop placement must see the primary book.
        The secondary book is attached under `_secondary_book` as a side-channel
        for analytics that explicitly opt into non-executable context.
        """
        p_ob = self._primary.get_orderbook()
        primary_book = {
            "bids": _norm_levels((p_ob or {}).get("bids", []) or (p_ob or {}).get("buy", []))[:self._ob_depth],
            "asks": _norm_levels((p_ob or {}).get("asks", []) or (p_ob or {}).get("sell", []))[:self._ob_depth],
            "timestamp": (p_ob or {}).get("timestamp", time.time()),
            "_sources": 1,
            "_executable_source": "primary",
            "_feed_reliability": self.get_feed_reliability(),
        }

        if self._secondary is None or not self._secondary_alive:
            return primary_book

        try:
            s_ob = self._secondary.get_orderbook()
            if s_ob:
                primary_book["_secondary_book"] = {
                    "bids": _norm_levels(s_ob.get("bids", []) or s_ob.get("buy", []))[:self._ob_depth],
                    "asks": _norm_levels(s_ob.get("asks", []) or s_ob.get("sell", []))[:self._ob_depth],
                    "timestamp": s_ob.get("timestamp", time.time()),
                }
                primary_book["_sources"] = 2
        except Exception:
            pass
        return primary_book

    # ── Trades — merged from both exchanges ──────────────────────────────────

    def get_recent_trades_raw(self) -> List[Dict]:
        """
        Return the merged, time-ordered trade stream from both exchanges.

        Primary trades + secondary tap = ~2× the tick data for trade-stream monitoring.
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
        """Merged raw trade-stream delta for monitoring only; never an entry input."""
        merged = self.get_recent_trades_raw()
        cutoff = time.time() - lookback_seconds
        buy_vol  = sum(t["quantity"] for t in merged
                       if t.get("timestamp", 0) >= cutoff and t.get("side") == "buy")
        sell_vol = sum(t["quantity"] for t in merged
                       if t.get("timestamp", 0) >= cutoff and t.get("side") == "sell")
        total = buy_vol + sell_vol
        reliability = self.get_feed_reliability()
        return {
            "buy_volume":  buy_vol,
            "sell_volume": sell_vol,
            "delta":       buy_vol - sell_vol,
            "delta_pct":   (buy_vol - sell_vol) / total if total > 0 else 0.0,
            "sources":     reliability.get("sources", 1),
            "microstructure_weight": reliability.get("microstructure_weight", 1.0),
            "reliability": reliability,
        }

    @property
    def ws(self):
        """Expose primary WS for health supervisor."""
        return getattr(self._primary, "ws", None)

    def get_secondary_status(self) -> Dict:
        reliability = self.get_feed_reliability()
        return {
            "alive":       reliability.get("secondary_alive", False),
            "has_secondary": self._secondary is not None,
            "primary":     type(self._primary).__name__,
            "secondary":   type(self._secondary).__name__ if self._secondary else "none",
            "mode":        reliability.get("mode", "single"),
            "sources":     reliability.get("sources", 1),
            "microstructure_weight": reliability.get("microstructure_weight", 1.0),
            "note":        reliability.get("note", ""),
        }
