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
                         get_orderbook()     → executable primary orderbook + secondary side-channel
                         get_recent_trades() → merged + deduplicated
                         ─────────────────
                                   ▼
                            InstitutionalStrategy

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
                    VWAP, and Institutional structure work.

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
from datetime import datetime, timezone
from collections import deque
from typing import Dict, List, Optional

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from agents.indian_options_desk import BlackScholesModel

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
    Unified data manager facade consumed by InstitutionalStrategy.
    Implements the same interface as CoinSwitchDataManager / DeltaDataManager.
    """

    def __init__(
        self,
        primary_dm,    # CoinSwitchDataManager | DeltaDataManager
        secondary_dm,  # DeltaDataManager | CoinSwitchDataManager | None
        instrument=None,
        analysis_dm=None,
        reference_dms=None,
    ) -> None:
        self.instrument = instrument
        self._primary   = primary_dm
        self._secondary = secondary_dm
        self._analysis  = analysis_dm
        # Read-only context feeds (e.g. Hyperliquid) are never executable and
        # are kept as independent microstates rather than merged depth.
        self._references = list(reference_dms or [])

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
            f"[{getattr(instrument, 'asset_id', 'compatibility')}] "
            f"(primary={type(primary_dm).__name__} "
            f"secondary={'none' if secondary_dm is None else type(secondary_dm).__name__} "
            f"analysis={'none' if analysis_dm is None else type(analysis_dm).__name__} "
            f"references={[type(x).__name__ for x in self._references]})"
        )

    def _requires_analysis_feed(self) -> bool:
        """GROWW options must never infer NIFTY structure from option premium candles."""
        exchange = getattr(getattr(self.instrument, "primary_exchange", None), "value", None)
        if exchange is None:
            exchange = getattr(self.instrument, "primary_exchange", "")
        return str(exchange or "").lower() == "groww" and bool(getattr(config, "GROWW_REQUIRE_UNDERLYING_ANALYSIS_FEED", True))

    def _market_managers(self):
        return [dm for dm in [self._primary, self._secondary, *self._references] if dm is not None]

    def _promote_ready_market_manager(self, manager) -> None:
        """Promote a live executable feed for wrapper compatibility only.

        Signal origination remains venue-local and independent; promotion only
        prevents a failed first-listed catalogue venue from making the whole
        asset context appear disconnected.
        """
        if manager is self._primary:
            return
        old_primary = self._primary
        if manager is self._secondary:
            self._primary, self._secondary = self._secondary, self._primary
            self._secondary_alive = bool(getattr(self._secondary, "is_ready", False))
        else:
            self._references = [dm for dm in self._references if dm is not manager]
            self._primary = manager
            if old_primary is not None and old_primary not in self._references and old_primary is not self._secondary:
                self._references.append(old_primary)
        if self._strategy_ref is not None:
            try:
                self._primary.register_strategy(self._strategy_ref)
            except Exception:
                pass
        logger.warning("⚖ Context compatibility feed promoted to %s after bootstrap feed unavailability; venue-local alpha/routing remains independent", type(self._primary).__name__)

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
        """Start every configured venue concurrently; no catalogue-first venue is mandatory.

        For non-option assets, any live venue can keep the context alive because
        strategy and routing are venue-local. Groww remains fail-closed on its
        mandatory underlying/option feed relationship.
        """
        import threading

        managers = self._market_managers()
        start_ok: dict[int, bool] = {id(dm): False for dm in managers}
        analysis_ok = [True]

        def start_dm(dm):
            try:
                start_ok[id(dm)] = bool(dm.start())
            except Exception as exc:
                logger.warning("Market data manager %s start failed: %s", type(dm).__name__, exc)
                start_ok[id(dm)] = False

        threads = [threading.Thread(target=start_dm, args=(dm,), daemon=True) for dm in managers]
        for thread in threads:
            thread.start()
        if self._analysis is not None:
            def start_analysis():
                try:
                    analysis_ok[0] = bool(self._analysis.start())
                except Exception as exc:
                    logger.warning("Analysis DM start failed: %s", exc)
                    analysis_ok[0] = False
            analysis_thread = threading.Thread(target=start_analysis, daemon=True)
            analysis_thread.start()
            threads.append(analysis_thread)
        for thread in threads:
            thread.join()

        if self._requires_analysis_feed():
            if not start_ok.get(id(self._primary), False):
                logger.error("GROWW execution feed failed to start — desk remains fail-closed")
                return False
            if not analysis_ok[0]:
                logger.error("GROWW analysis websocket unavailable; underlying structural feed is mandatory")
                return False
            return True

        if not any(start_ok.values()):
            logger.error("❌ No venue data manager started — asset context cannot trade safely")
            return False
        if not start_ok.get(id(self._primary), False):
            logger.warning("Bootstrap catalogue feed failed to start; awaiting any live executable venue")
        if self._secondary is not None:
            self._secondary_alive = bool(start_ok.get(id(self._secondary), False))
        logger.info("✅ Venue-isolated data feeds started: %s", ", ".join(type(dm).__name__ for dm in managers if start_ok.get(id(dm), False)))
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
        for ref in self._references:
            try:
                ref.stop()
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
        for ref in self._references:
            try:
                ref.stop(); ref.start()
            except Exception:
                pass
        return ok

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        """Become ready when any eligible venue has a verified live state.

        A first-listed discovery venue is never a gate for crypto/commodity
        contexts. Groww options retain their mandatory dual-domain readiness.
        """
        deadline = time.time() + float(timeout_sec)
        if self._requires_analysis_feed():
            primary_ready = bool(self._primary.wait_until_ready(timeout_sec))
            analysis_ready = bool(self._analysis is not None and self._analysis.wait_until_ready(min(timeout_sec, 30.0)))
            return bool(primary_ready and analysis_ready)
        while time.time() < deadline:
            ready = [dm for dm in self._market_managers() if bool(getattr(dm, "is_ready", False))]
            if ready:
                if self._primary not in ready:
                    self._promote_ready_market_manager(ready[0])
                return True
            time.sleep(0.05)
        logger.error("❌ No independently configured venue became ready — asset context cannot trade safely")
        return False

    def prepare_groww_session_contract_book(self, available_funds: float) -> bool:
        """Preselect CE/PE execution vehicles after underlying warmup.

        This delegates only for the GROWW option-primary manager.  Structural
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

    def release_groww_execution_vehicle(self) -> None:
        releaser = getattr(self._primary, "release_execution_vehicle", None)
        if callable(releaser):
            releaser()

    def get_groww_option_volatility_context(self) -> Dict:
        """Build live long-premium valuation context from official Groww inputs.

        The primary contributes official chain IV/OI data; the analysis manager
        contributes NIFTY underlying candles.  The method deliberately returns
        not-ready when either side is incomplete.
        """
        getter = getattr(self._primary, "get_verified_option_chain_snapshot", None)
        if not callable(getter):
            return {"ready_for_long_premium_decision": False, "reasons": ["official_option_chain_unavailable"]}
        try:
            chain = list(getter() or [])
        except Exception as exc:
            return {"ready_for_long_premium_decision": False, "reasons": [f"official_option_chain_error:{exc}"]}
        try:
            from agents.indian_options_desk import build_option_volatility_context
            candles = self.get_candles("1d", 60)
            spot = self.get_analysis_price()
            lot_getter = getattr(self._primary, "get_session_book_lot_size", None)
            lot = int(lot_getter() or 0) if callable(lot_getter) else 0
            if lot <= 0:
                ensure = getattr(self._primary, "ensure_session_contract_book", None)
                if callable(ensure):
                    ensure(float(spot or 0.0))
                    lot = int(lot_getter() or 0) if callable(lot_getter) else 0
            if lot <= 0:
                status_getter = getattr(self._primary, "session_contract_book_status", None)
                status = status_getter() if callable(status_getter) else {"status": "MISSING"}
                return {"ready_for_long_premium_decision": False, "reasons": ["verified_option_lot_unavailable_execution_universe_monitoring"], "execution_book_status": status}
            return build_option_volatility_context(
                chain=chain, underlying_candles=candles, spot=spot, lot_size=lot
            ).as_dict()
        except Exception as exc:
            return {"ready_for_long_premium_decision": False, "reasons": [f"option_volatility_context_error:{exc}"]}

    def activate_groww_execution_vehicle(self, thesis_side: str, available_funds: float):
        """Activate the already-preselected CE or PE only after a valid thesis.

        Selection is delegated to the official Groww option manager and remains
        gated by fresh LTP plus market-depth state for that exact contract.
        """
        selector = getattr(self._primary, "select_contract_for_thesis", None)
        if not callable(selector):
            return None
        return selector(
            str(thesis_side), underlying_spot=float(self.get_analysis_price() or 0.0),
            available_funds=float(available_funds or 0.0),
        )

    def get_session_contract_book_status(self) -> Dict:
        status_fn = getattr(self._primary, "session_contract_book_status", None)
        if callable(status_fn):
            try:
                return dict(status_fn() or {})
            except Exception:
                return {"status": "ERROR"}
        return {"status": "UNSUPPORTED"}

    def get_active_option_risk_state(self) -> Dict:
        """Calculate current long-option Greeks from live premium and NIFTY spot.

        This method is only meaningful for the Groww option desk: the premium
        comes from the official execution feed and spot comes from the official
        index-value analysis feed.
        """
        getter = getattr(self._primary, "get_active_option_contract_state", None)
        if not callable(getter):
            return {}
        state = dict(getter() or {})
        premium = float(state.get("premium", 0.0) or 0.0)
        strike = float(state.get("strike", 0.0) or 0.0)
        spot = float(self.get_analysis_price() or 0.0)
        expiry = str(state.get("expiry") or "")
        try:
            # NSE option expiry/session close is 15:30 IST, i.e. 10:00 UTC.
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d").replace(hour=10, minute=0, tzinfo=timezone.utc)
            dte = max((expiry_dt - datetime.now(timezone.utc)).total_seconds() / 86400.0, 1e-6)
        except Exception:
            dte = 0.0
        right = str(state.get("right") or "").lower()
        option_type = "put" if right in {"put", "pe", "p"} else "call"
        rate = float(getattr(config, "INDIA_RISK_FREE_RATE", 0.065))
        iv = BlackScholesModel.implied_volatility(option_type, spot, strike, dte, rate, premium) if min(spot, strike, premium, dte) > 0 else None
        greeks = BlackScholesModel.greeks(option_type, spot, strike, dte, rate, iv, premium=premium) if iv else None
        state.update({"underlying_spot": spot, "dte": dte, "current_iv": iv})
        if greeks is not None:
            state.update({
                "current_delta": greeks.delta,
                "current_theta_to_premium": greeks.theta_to_premium,
                "current_vega_per_vol_point": greeks.vega_per_vol_point,
            })
        return state

    def get_execution_feed_status(self) -> Dict:
        """Expose the orderable instrument feed separately from analysis feed.

        For GROWW, analysis remains NIFTY underlying while execution is CE/PE
        premium/depth.  This prevents a healthy underlying tick from masking an
        unarmed or stale option vehicle.
        """
        status_fn = getattr(self._primary, "execution_feed_status", None)
        if callable(status_fn):
            try:
                return dict(status_fn() or {})
            except Exception as exc:
                return {"status": "ERROR", "session_vehicle_stream_ready": False, "error": str(exc)}
        return {
            "status": "DIRECT_EXECUTION_FEED",
            "session_vehicle_stream_ready": bool(self.is_execution_price_fresh(float(getattr(config, "PRICE_STALE_SECONDS", 90.0)))),
        }

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
        """Executable instrument mark price (option premium once GROWW vehicle is activated)."""
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

        For GROWW long-premium execution, ``get_last_price`` changes to the
        selected option premium after contract activation, while liquidity/Institutional
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
        """Execution-venue health, enriched with independent context-feed status."""
        getter = getattr(self._primary, "get_feed_reliability", None)
        if callable(getter):
            try:
                out = dict(getter() or {})
            except Exception:
                out = {}
        else:
            ready = bool(getattr(self._primary, "is_ready", False))
            out = {
                "connected": ready, "heartbeat_ok": ready, "sequence_valid": True,
                "snapshot_ready": ready, "exchange_timestamp_available": False,
                "latency_vs_baseline_z": None, "no_change_heartbeat_valid": ready,
            }
        secondary_ready = bool(self._secondary is not None and self._secondary_alive and getattr(self._secondary, "is_ready", True))
        reference_ready = sum(1 for dm in self._references if bool(getattr(dm, "is_ready", False)))
        out.update({
            "primary_ready": bool(getattr(self._primary, "is_ready", False)),
            "analysis_ready": bool(self._analysis is not None and getattr(self._analysis, "is_ready", False)),
            "secondary_configured": self._secondary is not None,
            "secondary_alive": secondary_ready,
            "reference_feeds_ready": reference_ready,
            "sources": 1 + int(secondary_ready) + reference_ready,
            "mode": "analysis_underlying" if self._analysis is not None else ("cross_venue" if secondary_ready or reference_ready else "single"),
        })
        return out

    def _manager_for_venue(self, venue: str):
        """Resolve an independent venue manager without merging price domains."""
        key = str(venue or "").strip().lower()
        for dm in [self._primary, self._secondary, *self._references]:
            if dm is None:
                continue
            declared = str(getattr(dm, "venue", "") or "").lower()
            cls = type(dm).__name__.lower()
            inferred = (
                "delta" if "delta" in cls else
                "coinswitch" if "coinswitch" in cls else
                "hyperliquid" if "hyperliquid" in cls else
                "groww" if "groww" in cls else declared
            )
            if key in {declared, inferred}:
                return dm
        return None

    def get_venue_candles(self, venue: str, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        """Return candles from the exact candidate venue only.

        Venue-local structural alpha must never be generated from the primary
        venue's candles when an alternate broker is being scored.
        """
        dm = self._manager_for_venue(venue)
        getter = getattr(dm, "get_candles", None) if dm is not None else None
        if not callable(getter):
            return []
        try:
            return list(getter(timeframe, limit) or [])
        except Exception:
            return []

    def get_venue_microstructure_research_state(self, venue: str) -> Dict:
        """Protection-calibration event stream for the selected execution venue."""
        dm = self._manager_for_venue(venue)
        getter = getattr(dm, "get_microstructure_research_state", None) if dm is not None else None
        if callable(getter):
            try:
                return dict(getter() or {})
            except Exception:
                return {}
        return {}

    def get_venue_microstates(self) -> Dict[str, object]:
        """Return separate venue states; no cross-venue depth summation is permitted."""
        out: Dict[str, object] = {}
        for dm in [self._primary, self._secondary, *self._references]:
            if dm is None:
                continue
            getter = getattr(dm, "get_venue_microstate", None)
            if not callable(getter):
                continue
            try:
                state = getter()
            except Exception:
                state = None
            if state is not None:
                out[str(getattr(state, "venue", type(dm).__name__)).lower()] = state
        return out

    def get_microstructure_research_state(self) -> Dict:
        """Exit-model input from the executable primary venue only.

        Kyle impact and VPIN must be calibrated against the venue where the
        liquidation order would execute; secondary/reference depth is context,
        not executable liquidity.
        """
        getter = getattr(self._primary, "get_microstructure_research_state", None)
        if callable(getter):
            try:
                return dict(getter() or {})
            except Exception:
                return {}
        return {}

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

