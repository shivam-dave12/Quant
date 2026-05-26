"""
INSTITUTIONAL AUCTION STRATEGY — UNIFIED STRUCTURAL AUTHORITY
============================================================

Entry authority represents several auditable auction archetypes under one
execution/risk lifecycle: stop-run reversal, displacement continuation and
liquidity expansion retest.  All routes require protected structure, an
imbalance/reprice entry and an actual opposing liquidity destination.

Multi-level order-book/micro-price and aggressive trade-flow data are consumed
as execution-horizon evidence and latency triggers.  They do not become a
standalone directional overlay and they are never represented as calibrated
probability without labelled replay calibration.
"""

from __future__ import annotations
import logging, math, time, threading
from collections import deque
import dataclasses
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import config
from core.pnl import gross_pnl_usd
from core.instruments import current_instrument, instrument_scope
from core.market_policy import policy_value, active_policy
from telegram.notifier import send_telegram_message, format_entry_alert, format_exit_alert, format_partial_exit_alert
from execution.order_manager import CancelResult
try:
    from strategy.tp_ladder import build_tp_ladder, TPLadderPlan
except Exception:  # pragma: no cover
    build_tp_ladder = None  # type: ignore
    TPLadderPlan = None  # type: ignore
try:
    from strategy.fee_engine import ExecutionCostEngine
except ImportError:
    ExecutionCostEngine = None   # fee_engine.py not yet present — graceful fallback


# ── Institutional Auction Trade Engine — unified structural authority ────────

logger = logging.getLogger(__name__)

# -- Unified structural auction entry authority -------------------------------
try:
    from strategy.liquidity_map import LiquidityMap, _last_closed_candle_idx
    _LIQ_MAP_AVAILABLE = True
except ImportError:
    try:
        from liquidity_map import LiquidityMap, _last_closed_candle_idx  # type: ignore
        _LIQ_MAP_AVAILABLE = True
    except ImportError:
        _LIQ_MAP_AVAILABLE = False

try:
    from strategy.entry_engine import EntryEngine, EntryType
    from strategy.auction_state import build_microstructure_state
    _ENTRY_ENGINE_AVAILABLE = True
except ImportError:
    try:
        from entry_engine import EntryEngine, EntryType  # type: ignore
        from auction_state import build_microstructure_state  # type: ignore
        _ENTRY_ENGINE_AVAILABLE = True
    except ImportError:
        _ENTRY_ENGINE_AVAILABLE = False

# ── Fixed-SL TP ladder exit model ──────────────────────────────────────────
# The previous SL-migration engine is not loaded in this strategy build.
# Exits are handled by the original exchange-side SL plus dynamic reduce-only
# TP ladder legs and the selected final TP.
DisabledSLMigrationEngine = None   # type: ignore
LiquidityTrailResult = None   # type: ignore


# ═══════════════════════════════════════════════════════════════
# CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════
def _cfg(name: str, default):
    val = getattr(config, name, None)
    return default if val is None else val


class QCfg:
    """Active venue/risk accessors used by the ICT/Liquidity lifecycle only."""
    @staticmethod
    def SYMBOL() -> str:
        inst = current_instrument()
        return str(inst.display_symbol if inst is not None else config.SYMBOL)

    @staticmethod
    def EXCHANGE() -> str:
        inst = current_instrument()
        return str(inst.primary_exchange.value if inst is not None else getattr(config, "EXCHANGE", getattr(config, "EXECUTION_EXCHANGE", "delta")))

    @staticmethod
    def LEVERAGE() -> int:
        try:
            return max(1, int(policy_value("leverage", _cfg("LEVERAGE", 30))))
        except Exception:
            return max(1, int(_cfg("LEVERAGE", 30)))

    @staticmethod
    def MARGIN_PCT() -> float: return float(policy_value("margin_pct", _cfg("QUANT_MARGIN_PCT", 0.20)))

    @staticmethod
    def LOT_STEP() -> float:
        inst = current_instrument()
        if inst is not None and inst.lot_step > 0: return float(inst.lot_step)
        return float(_cfg("LOT_STEP_SIZE", 0.001))

    @staticmethod
    def MIN_QTY() -> float:
        inst = current_instrument()
        if inst is not None and inst.min_qty > 0: return float(inst.min_qty)
        return float(_cfg("MIN_POSITION_SIZE", 0.001))

    @staticmethod
    def MAX_QTY() -> float:
        inst = current_instrument()
        if inst is not None and inst.max_qty > 0: return float(inst.max_qty)
        return float(_cfg("MAX_POSITION_SIZE", 1.0))

    @staticmethod
    def MIN_MARGIN_USDT() -> float: return float(policy_value("min_margin_usd", _cfg("MIN_MARGIN_PER_TRADE", 1.0)))

    @staticmethod
    def COMMISSION_RATE() -> float: return float(_cfg("COMMISSION_RATE", 0.00055))

    @staticmethod
    def TICK_SIZE() -> float:
        inst = current_instrument()
        if inst is not None and inst.tick_size > 0: return float(inst.tick_size)
        getter = getattr(config, "get_tick_size", None)
        if callable(getter): return float(getter())
        return float(_cfg("TICK_SIZE", 0.1))

    @staticmethod
    def SLIPPAGE_TOL() -> float: return float(policy_value("slippage_tolerance", _cfg("QUANT_SLIPPAGE_TOLERANCE", 0.0005)))

    @staticmethod
    def ATR_PERIOD() -> int: return int(_cfg("SL_ATR_PERIOD", 14))

    @staticmethod
    def MIN_5M_BARS() -> int: return int(policy_value("min_5m_bars", _cfg("MIN_CANDLES_5M", 60)))

    @staticmethod
    def ATR_PCTILE_WINDOW() -> int: return int(_cfg("QUANT_ATR_PCTILE_WINDOW", 100))

    @staticmethod
    def ATR_MIN_PCTILE() -> float: return float(policy_value("atr_min_pctile", _cfg("QUANT_ATR_MIN_PCTILE", 0.05)))

    @staticmethod
    def ATR_MAX_PCTILE() -> float: return float(policy_value("atr_max_pctile", _cfg("QUANT_ATR_MAX_PCTILE", 0.97)))

    @staticmethod
    def COOLDOWN_SEC() -> int: return int(policy_value("cooldown_sec", _cfg("MIN_TIME_BETWEEN_TRADES_SEC", 300)))

    @staticmethod
    def LOSS_LOCKOUT_SEC() -> int: return int(policy_value("loss_lockout_sec", _cfg("QUANT_LOSS_LOCKOUT_SEC", 5400)))

    @staticmethod
    def TICK_EVAL_SEC() -> float: return float(policy_value("tick_eval_sec", _cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 1)))

    @staticmethod
    def POS_SYNC_SEC() -> float: return float(_cfg("QUANT_POS_SYNC_SEC", 30))

    @staticmethod
    def MAX_DAILY_TRADES() -> int: return int(_cfg("MAX_DAILY_TRADES", 10))

    @staticmethod
    def MAX_CONSEC_LOSSES() -> int: return int(_cfg("MAX_CONSECUTIVE_LOSSES", 2))

    @staticmethod
    def MAX_DAILY_LOSS_PCT() -> float: return float(_cfg("MAX_DAILY_LOSS_PCT", 3.0))


def _round_to_tick(price: float) -> float:
    tick = QCfg.TICK_SIZE()
    return round(round(price / tick) * tick, 10) if tick > 0 else price


def _round_structural_levels(pos_side: str, sl_price: float, tp_price: float) -> tuple[float, float]:
    """Round execution levels conservatively without weakening the ICT geometry.

    A long position has a stop below price and a target above price: both are
    floored so the stop cannot move inside the raided wick and the TP cannot be
    moved beyond the front-run liquidity objective.  A short position uses the
    mirror-image ceiling rule.
    """
    tick = max(float(QCfg.TICK_SIZE() or 0.0), 1e-12)
    if str(pos_side or "").lower() == "long":
        return (round(math.floor(sl_price / tick) * tick, 10),
                round(math.floor(tp_price / tick) * tick, 10))
    return (round(math.ceil(sl_price / tick) * tick, 10),
            round(math.ceil(tp_price / tick) * tick, 10))


def _icici_primary_raw(instrument: Any = None) -> Dict[str, Any]:
    inst = instrument if instrument is not None else current_instrument()
    try:
        raw = getattr(getattr(inst, "primary", None), "raw", {}) or {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _icici_exchange_name(instrument: Any = None) -> str:
    inst = instrument if instrument is not None else current_instrument()
    try:
        return str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", ""))).lower()
    except Exception:
        return ""


def _is_icici_underlying_chain_instrument(instrument: Any = None) -> bool:
    raw = _icici_primary_raw(instrument)
    return bool(
        _icici_exchange_name(instrument) == "icici"
        and raw.get("icici_underlying_desk")
        and str(raw.get("contract_selector_mode") or "").lower() == "session_preselected_execution"
        and not raw.get("selected_option_contract")
    )


def _is_icici_option_instrument(instrument: Any = None) -> bool:
    raw = _icici_primary_raw(instrument)
    if _icici_exchange_name(instrument) != "icici":
        return False
    if raw.get("selected_option_contract"):
        return True
    txt = " ".join(str(raw.get(k, "")) for k in ("product_type", "product", "contract_type", "right", "option_type", "OptionType"))
    return "option" in txt.lower() or _icici_option_right(raw) in ("call", "put")


def _icici_selected_contract_dict(instrument: Any = None) -> Dict[str, Any]:
    raw = _icici_primary_raw(instrument)
    selected = raw.get("selected_option_contract")
    if isinstance(selected, dict):
        merged = dict(raw)
        selected_raw = selected.get("raw")
        if isinstance(selected_raw, dict):
            merged.update(selected_raw)
        merged.update({k: v for k, v in selected.items() if k != "raw"})
        return merged
    return raw


def _icici_option_right(raw_or_instrument: Any = None) -> str:
    raw = raw_or_instrument if isinstance(raw_or_instrument, dict) else _icici_selected_contract_dict(raw_or_instrument)
    value = str(
        raw.get("right")
        or raw.get("option_type")
        or raw.get("OptionType")
        or raw.get("Right")
        or raw.get("CallPut")
        or ""
    ).strip().lower()
    if value in ("c", "ce", "call"):
        return "call"
    if value in ("p", "pe", "put"):
        return "put"
    return ""


def _icici_allowed_thesis_side(instrument: Any, thesis_side: str) -> bool:
    side = str(thesis_side or "").lower()
    right = _icici_option_right(instrument)
    if right == "call":
        return side == "long"
    if right == "put":
        return side == "short"
    return side in ("long", "short")


def _icici_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if not value:
                return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _icici_selected_delta(instrument: Any = None) -> float:
    raw = _icici_selected_contract_dict(instrument)
    for key in ("delta", "Delta", "bs_delta", "runtime_delta"):
        v = _icici_float(raw.get(key), 0.0)
        if abs(v) > 0.01:
            return abs(v)
    target = _icici_float(_cfg("ICICI_INDEX_OPTION_TARGET_ABS_DELTA", 0.45), 0.45)
    return max(0.05, min(0.95, abs(target)))


def _icici_runtime_lot_size(instrument: Any = None) -> float:
    raw = _icici_selected_contract_dict(instrument)
    for key in ("runtime_lot_size", "LotSize", "lot_size", "lotSize", "MinimumLotQty", "min_qty"):
        lot = _icici_float(raw.get(key), 0.0)
        if lot > 0:
            return lot
    return max(0.0, _icici_float(_cfg("ICICI_OPTION_DEFAULT_LOT_SIZE", 0.0), 0.0))


def _icici_selected_premium(instrument: Any = None, fallback: float = 0.0) -> float:
    raw = _icici_selected_contract_dict(instrument)
    for key in ("selected_entry_premium", "ltp", "last_price", "lastPrice", "close", "price", "settlement_price"):
        px = _icici_float(raw.get(key), 0.0)
        if px > 0:
            return px
    return float(fallback or 0.0)


def _icici_contract_identity_ok(raw: Dict[str, Any]) -> bool:
    if not isinstance(raw, dict):
        return False
    strike = _icici_float(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("strike"), 0.0)
    expiry = str(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or "").strip()
    right = _icici_option_right(raw)
    return bool(strike > 0 and expiry and right in ("call", "put"))


def _icici_contract_from_position(ex_pos: Any) -> Dict[str, Any]:
    if not isinstance(ex_pos, dict):
        return {}
    merged: Dict[str, Any] = {}
    raw = ex_pos.get("raw")
    if isinstance(raw, dict):
        merged.update(raw)
    merged.update({k: v for k, v in ex_pos.items() if k != "raw"})
    return merged


def _icici_ensure_adoptable_contract(instrument: Any, ex_pos: Any) -> bool:
    """Ensure ICICI recovery has an exact option vehicle before adoption."""
    if _icici_contract_identity_ok(_icici_selected_contract_dict(instrument)):
        return True
    pos_contract = _icici_contract_from_position(ex_pos)
    if not _icici_contract_identity_ok(pos_contract):
        return False
    raw = _icici_primary_raw(instrument)
    if not isinstance(raw, dict):
        return False
    entry_px = _icici_float(
        pos_contract.get("entry_price")
        or pos_contract.get("average_price")
        or pos_contract.get("avg_price")
        or pos_contract.get("ltp")
        or pos_contract.get("last_price"),
        0.0,
    )
    selected = dict(pos_contract)
    selected.setdefault("raw", dict(pos_contract))
    if entry_px > 0:
        selected.setdefault("selected_entry_premium", entry_px)
    right = _icici_option_right(pos_contract)
    raw["selected_option_contract"] = selected
    raw["stock_code"] = pos_contract.get("stock_code") or pos_contract.get("StockCode") or raw.get("stock_code") or raw.get("underlying")
    raw["exchange_code"] = pos_contract.get("exchange_code") or pos_contract.get("ExchangeCode") or raw.get("exchange_code") or "NFO"
    raw["product_type"] = "options"
    raw["right"] = "Call" if right == "call" else "Put"
    raw["option_type"] = raw["right"]
    raw["strike_price"] = str(pos_contract.get("strike_price") or pos_contract.get("StrikePrice") or pos_contract.get("strike") or "")
    raw["expiry_date"] = str(pos_contract.get("expiry_date") or pos_contract.get("ExpiryDate") or pos_contract.get("expiry") or "")
    raw["TradingSymbol"] = (
        pos_contract.get("TradingSymbol")
        or pos_contract.get("trading_symbol")
        or pos_contract.get("selected_symbol")
        or pos_contract.get("symbol")
        or raw.get("TradingSymbol")
    )
    lot = _icici_float(
        pos_contract.get("runtime_lot_size")
        or pos_contract.get("LotSize")
        or pos_contract.get("lot_size")
        or raw.get("runtime_lot_size"),
        0.0,
    )
    if lot > 0:
        raw["runtime_lot_size"] = lot
    if entry_px > 0:
        raw["selected_entry_premium"] = entry_px
    # Recovery may identify a real broker option position, but automated order
    # management cannot safely route until its exact NFO lot is known.
    return _icici_contract_identity_ok(raw) and _icici_runtime_lot_size(instrument) > 0


def _icici_select_contract_for_thesis(
    instrument: Any,
    data_manager: Any,
    thesis_side: str,
    *,
    underlying_spot: float = 0.0,
    available_funds: float = 0.0,
):
    primary = getattr(data_manager, "_primary", data_manager)
    selector = getattr(primary, "select_contract_for_thesis", None)
    if not callable(selector):
        return None
    return selector(
        thesis_side,
        underlying_spot=float(underlying_spot or 0.0),
        available_funds=float(available_funds or 0.0),
    )


def _icici_execution_atr(data_manager: Any, entry_premium: float = 0.0, period: int = 14) -> float:
    try:
        getter = getattr(data_manager, "get_execution_candles", None) or getattr(data_manager, "get_candles", None)
        candles = getter("5m", max(period + 2, 60)) if callable(getter) else []
        rows = list(candles or [])
        rows = rows[:-1] if len(rows) > 1 else []  # the option feed includes a forming bar
        trs: List[float] = []
        for i in range(1, len(rows)):
            h = _icici_float(rows[i].get("h", rows[i].get("high")), 0.0)
            l = _icici_float(rows[i].get("l", rows[i].get("low")), 0.0)
            prev_close = _icici_float(rows[i - 1].get("c", rows[i - 1].get("close")), 0.0)
            if h <= 0 or l <= 0 or prev_close <= 0 or h < l:
                continue
            trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
        if len(trs) >= period:
            atr = sum(trs[:period]) / period
            for value in trs[period:]:
                atr = (atr * (period - 1) + value) / period
            return atr
    except Exception:
        pass
    prem = float(entry_premium or 0.0)
    return prem * max(0.02, _icici_float(_cfg("ICICI_OPTION_MIN_PREMIUM_RISK_PCT", 0.14), 0.14) * 0.50) if prem > 0 else 0.0


def _icici_market_session_open() -> tuple[bool, str]:
    try:
        from exchanges.icici.market_session import icici_market_session_state
        state = icici_market_session_state()
        return bool(state.is_open), str(state.reason)
    except Exception as exc:
        return False, f"ICICI market session unavailable: {exc}"


def _icici_option_premium_levels(
    *,
    thesis_side: str,
    premium_entry: float,
    underlying_entry: float,
    underlying_sl: float,
    underlying_tp: float,
    instrument: Any = None,
) -> tuple[Optional[float], Optional[float], str]:
    side = str(thesis_side or "").lower()
    prem = float(premium_entry or 0.0)
    u_entry = float(underlying_entry or 0.0)
    u_sl = float(underlying_sl or 0.0)
    u_tp = float(underlying_tp or 0.0)
    if side not in ("long", "short") or prem <= 0 or u_entry <= 0 or u_sl <= 0 or u_tp <= 0:
        return None, None, "missing_underlying_or_premium_level"
    if side == "long" and not (u_sl < u_entry < u_tp):
        return None, None, "bullish_underlying_levels_not_protective"
    if side == "short" and not (u_tp < u_entry < u_sl):
        return None, None, "bearish_underlying_levels_not_protective"

    delta_abs = _icici_selected_delta(instrument)
    mult = max(0.25, _icici_float(_cfg("ICICI_OPTION_SLTP_DELTA_MULT", 1.0), 1.0))
    convex_bonus = max(0.0, _icici_float(_cfg("ICICI_OPTION_PREMIUM_TP_CONVEXITY_BONUS", 0.08), 0.08))
    min_risk_pct = max(0.01, _icici_float(_cfg("ICICI_OPTION_MIN_PREMIUM_RISK_PCT", 0.14), 0.14))
    max_risk_pct = max(min_risk_pct, _icici_float(_cfg("ICICI_OPTION_MAX_PREMIUM_RISK_PCT", 0.58), 0.58))
    min_tp_pct = max(0.01, _icici_float(_cfg("ICICI_OPTION_MIN_TP_PREMIUM_PCT", 0.18), 0.18))
    min_rr = max(1.0, float(policy_value("min_rr", _cfg("MIN_RISK_REWARD_RATIO", 1.6), instrument)))
    max_rr = max(min_rr, float(policy_value("max_rr", _cfg("QUANT_TP_MAX_RR", 4.0), instrument)))

    u_sl_dist = abs(u_entry - u_sl)
    u_tp_dist = abs(u_tp - u_entry)
    sl_dist = u_sl_dist * delta_abs * mult
    tp_dist = u_tp_dist * delta_abs * mult * (1.0 + convex_bonus)
    sl_dist = max(prem * min_risk_pct, min(sl_dist, prem * max_risk_pct))
    tp_dist = max(tp_dist, prem * min_tp_pct, sl_dist * min_rr)
    tp_dist = min(tp_dist, sl_dist * max_rr)
    tick = max(QCfg.TICK_SIZE(), _icici_float(_cfg("ICICI_OPTION_TICK_SIZE", 0.05), 0.05), 1e-9)
    sl = prem - sl_dist
    if sl <= tick:
        return None, None, "option_premium_stop_would_be_near_zero"
    tp = prem + tp_dist
    reason = (
        f"delta={delta_abs:.2f} uSL={u_sl_dist:.1f} uTP={u_tp_dist:.1f} "
        f"premium_risk={sl_dist:.2f} premium_tp={tp_dist:.2f} rr={tp_dist / max(sl_dist, 1e-9):.2f}"
    )
    return sl, tp, reason


def _round_to_tick_protective(pos_side: str, price: float) -> float:
    """Directionally round a stop/BE level so it remains truly protective.

    LONG BE/profit-lock floors must round UP; SHORT floors must round DOWN.
    Nearest-tick rounding can leave a tiny net-loss after fees.
    """
    tick = max(QCfg.TICK_SIZE(), 1e-10)
    if pos_side == "long":
        return round(math.ceil(price / tick) * tick, 10)
    return round(math.floor(price / tick) * tick, 10)


def _stop_exit_fee_rate() -> float:
    """Estimated future fee rate for SL/stop exits.

    Entry can be maker, but a stop exit is treated as taker/risk-exit.
    This prevents false breakeven when the entry fee was cheaper than the stop.
    """
    candidates = [
        getattr(config, 'STOP_EXIT_COMMISSION_RATE', None),
        getattr(config, 'DELTA_TAKER_COMMISSION_RATE', None),
        getattr(config, 'TAKER_COMMISSION_RATE', None),
        getattr(config, 'COMMISSION_RATE', None),
    ]
    rates = []
    for v in candidates:
        if v is None:
            continue
        try:
            fv = float(v)
            if math.isfinite(fv) and fv >= 0:
                rates.append(fv)
        except Exception:
            pass
    return max(rates) if rates else 0.00055






# ATR ENGINE
# ═══════════════════════════════════════════════════════════════
class ATREngine:
    def __init__(self):
        self._atr = 0.0; self._atr_hist: deque = deque(maxlen=QCfg.ATR_PCTILE_WINDOW())
        self._last_ts = -1; self._seeded = False

    def reset_state(self):
        """Force full re-seed from next candle batch after stream restart."""
        self._seeded = False
        self._last_ts = -1
        self._atr_hist.clear()
        self._atr = 0.0

    def soft_reset(self):
        """
        Issue 1 fix: Use this instead of reset_state() after stream restart.

        Resets the seeding flag so the ATR will be fully recomputed from the
        next candle batch, but PRESERVES the last computed ATR value and history.

        Why: reset_state() sets self._atr = 0.0, which causes _compute_signals
        to return None every tick for up to 75 minutes (the 5m re-seed time).
        During this window all entry gates return None with zero logging, so the
        bot appears dead. soft_reset() keeps the last valid ATR so signals
        continue to work immediately after reconnect, while still triggering a
        proper full re-seed from the fresh candle batch.
        """
        self._seeded = False
        self._last_ts = -1
        # _atr and _atr_hist intentionally preserved

    @staticmethod
    def _pctile_rank_window() -> int:
        return int(_cfg("ATR_PCTILE_RANK_WINDOW", 30))

    def compute(self, candles: List[Dict]) -> float:
        if not candles: return self._atr
        period = QCfg.ATR_PERIOD()

        # Resolve the latest fully closed bar exactly as LiquidityMap and the
        # entry engine do.  The previous unconditional candles[-2] rule lagged
        # one full bar whenever the stream already contained closed bars only.
        def _ts(c) -> int:
            try:
                return int(c['t'])
            except (KeyError, TypeError):
                pass
            try:
                return int(getattr(c, 'timestamp', 0) * 1000)
            except Exception:
                return 0

        closed_idx = _last_closed_candle_idx(candles, "5m", time.time())
        if closed_idx < 0:
            return self._atr
        closed = list(candles[:closed_idx + 1])
        if not closed:
            return self._atr
        last_ts = _ts(closed[-1])
        if last_ts == self._last_ts and self._seeded:
            return self._atr
        if len(closed) < period + 1:
            return self._atr

        if not self._seeded:
            trs = [max(float(closed[i]['h'])-float(closed[i]['l']),
                       abs(float(closed[i]['h'])-float(closed[i-1]['c'])),
                       abs(float(closed[i]['l'])-float(closed[i-1]['c'])))
                   for i in range(1, len(closed))]
            if len(trs) < period:
                return self._atr
            atr = sum(trs[:period]) / period
            for tr in trs[period:]:
                atr = (atr * (period - 1) + tr) / period
            # Only keep the final seeded ATR — prevents warmup-era volatility
            # from poisoning live percentile ranking.
            self._atr_hist.clear()
            self._atr_hist.append(atr)
            self._atr = atr
            self._seeded = True
            self._last_ts = last_ts
            return self._atr
        else:
            # Incremental: closed[-1] has just completed; closed[-2] supplies
            # the prior close for its true-range update.
            if len(closed) < 2:
                return self._atr
            hi  = float(closed[-1]['h'])
            lo  = float(closed[-1]['l'])
            prc = float(closed[-2]['c'])
            self._atr = (self._atr*(period-1)+max(hi-lo,abs(hi-prc),abs(lo-prc)))/period
        self._atr_hist.append(self._atr); self._last_ts = last_ts
        return self._atr

    @property
    def atr(self) -> float: return self._atr

    def get_percentile(self) -> float:
        hist = list(self._atr_hist)
        n = len(hist)
        # v4.3 FIX: Need at least half a rank window of LIVE data before
        # departing from neutral. This prevents warmup data from locking
        # the percentile at extreme values during the first ~75 min.
        min_samples = max(5, self._pctile_rank_window() // 2)
        if n < min_samples: return 0.5
        window = hist[max(0, n - self._pctile_rank_window()):]
        if len(window) < 2: return 0.5
        cur = window[-1]
        return sum(1 for h in window[:-1] if h <= cur) / (len(window) - 1)

    def regime_valid(self) -> bool:
        p = self.get_percentile()
        return QCfg.ATR_MIN_PCTILE() <= p <= QCfg.ATR_MAX_PCTILE()

    def regime_penalty(self) -> float:
        return 1.0 if self.regime_valid() else 0.0

# ═══════════════════════════════════════════════════════════════
# HTF TREND FILTER — DYNAMIC CONTEXT
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# STRUCTURAL ENTRY AUDIT SUMMARY
# ═══════════════════════════════════════════════════════════════
@dataclass
class StructuralEntrySummary:
    """Execution audit record for one approved structural auction thesis.

    ``delivery_score`` is observed evidence and is not a trade win
    probability.  A probability is populated only after replay calibration.
    """
    atr: float = 0.0
    delivery_score: float = 0.0
    delivery_probability: Optional[float] = None
    probability_calibrated: bool = False
    archetype: str = ""
    structural_validation: str = ""

    def __str__(self) -> str:
        p = f" calibratedP={self.delivery_probability:.3f}" if self.probability_calibrated and self.delivery_probability is not None else " calibratedP=N/A"
        return f"{self.archetype or 'STRUCTURAL_AUCTION'} deliveryScore={self.delivery_score:+.3f}{p} ATR={self.atr:.4f} | {self.structural_validation}"


# ═══════════════════════════════════════════════════════════════
# EXECUTION VIABILITY
@dataclass


@dataclass
class ExecutionViability:
    route: str
    side: str
    price: float
    sl_price: float
    tp_price: float
    current_sl_dist: float
    reward_dist: float
    round_trip_cost_pts: float
    round_trip_cost_bps: float
    fee_to_risk: float
    fee_soft: float
    fee_no_alloc: float
    min_viable_sl_dist: float
    geometry_gap_pts: float
    required_sl_price: float
    required_entry_price: float
    delivery_probability: Optional[float]
    net_win_r: float
    net_loss_r: float
    expected_net_utility_r: float
    utility_known: bool
    allocation_allowed: bool
    reason: str

    def as_refine_context(self) -> Dict[str, object]:
        return {
            "route": self.route,
            "side": self.side,
            "price": self.price,
            "sl_price": self.sl_price,
            "tp_price": self.tp_price,
            "current_sl_dist": self.current_sl_dist,
            "reward_dist": self.reward_dist,
            "round_trip_cost_pts": self.round_trip_cost_pts,
            "round_trip_cost_bps": self.round_trip_cost_bps,
            "fee_to_risk": self.fee_to_risk,
            "fee_no_alloc": self.fee_no_alloc,
            "min_viable_sl_dist": self.min_viable_sl_dist,
            "geometry_gap_pts": self.geometry_gap_pts,
            "required_sl_price": self.required_sl_price,
            "required_entry_price": self.required_entry_price,
            "delivery_probability": self.delivery_probability,
            "expected_net_utility_r": self.expected_net_utility_r,
            "utility_known": float(self.utility_known),
            "allocation_allowed": float(self.allocation_allowed),
            "reason": self.reason,
        }

# POSITION STATE
# ═══════════════════════════════════════════════════════════════
class PositionPhase(Enum):
    FLAT = auto(); ENTERING = auto(); ACTIVE = auto(); EXITING = auto()

@dataclass
class PositionState:
    phase: PositionPhase = PositionPhase.FLAT
    side: str = ""; quantity: float = 0.0; entry_price: float = 0.0
    sl_price: float = 0.0; tp_price: float = 0.0
    sl_order_id: Optional[str] = None; tp_order_id: Optional[str] = None
    # TP ladder: internal liquidity reduce-only targets + original final TP.
    # SL price is fixed at original structural invalidation.
    tp_ladder: List[Dict[str, object]] = field(default_factory=list)
    tp_ladder_order_ids: List[str] = field(default_factory=list)
    tp_ladder_active: bool = False
    tp_ladder_last_sync_qty: float = 0.0
    tp_ladder_initial_qty: float = 0.0
    tp_ladder_realized_pnl: float = 0.0
    tp_ladder_realized_gross: float = 0.0
    tp_ladder_realized_fees: float = 0.0
    tp_ladder_recorded_order_ids: List[str] = field(default_factory=list)
    # Reporting/P&L idempotency for TP-ladder partial fills. Delta fill_qty/paid_commission
    # are cumulative per order, so we store the already-booked cumulative values and only
    # book the delta on later PARTIAL_FILL -> FILLED updates.
    tp_ladder_recorded_fill_qty_by_order: Dict[str, float] = field(default_factory=dict)
    tp_ladder_recorded_exit_fee_by_order: Dict[str, float] = field(default_factory=dict)
    tp_ladder_realized_entry_fees: float = 0.0
    tp_ladder_realized_exit_fees: float = 0.0
    entry_order_id: Optional[str] = None; entry_time: float = 0.0
    initial_risk: float = 0.0; initial_sl_dist: float = 0.0
    entry_signal: Optional[StructuralEntrySummary] = None
    # Execution-time structural ATR retained for protected-position audit and recovery.
    entry_atr: float = 0.0
    peak_profit: float = 0.0
    peak_price_abs: float = 0.0  # realised path extreme for execution analytics
    last_seen_price: float = 0.0
    entry_fill_type: str = "taker"  # v4.3: "maker" | "taker" — for correct PnL fee calc
    entry_leverage: float = 0.0    # actual exchange leverage selected at entry for margin/risk reporting
    entry_fee_paid: float = 0.0    # v8.1: exact paid_commission from Delta entry order
    entry_fee_exact: bool = False  # True when fee came from Delta paid_commission / fill commission
    entry_session: str = ""          # canonical session captured at entry
    # FIX 8: store actual HTF scores at entry time for post-trade attribution.
    # Previously deviation_atr was stored under "htf_15m" key — all HTF analytics were wrong.
    entry_htf_15m: float = 0.0
    entry_htf_4h:  float = 0.0
    delivery_probability: float = 0.0  # non-zero only when probability_calibrated=True
    probability_calibrated: bool = False
    delivery_score: float = 0.0
    archetype: str = ""
    delivery_utility_r: float = 0.0  # non-zero only for calibrated probability
    quant_components: Dict[str, float] = field(default_factory=dict)
    # Exact Maximum Adverse Excursion in price units for execution analytics.
    peak_adverse:  float = 0.0
    # Exact manual close accounting. When the strategy closes
    # with a reduce-only market order (not an exchange SL/TP child), the fired
    # order id must be tracked so PnL is booked from the actual fill instead of
    # falling back to exchange-flat/zero-PnL reconciliation.
    manual_exit_order_id: str = ""
    manual_exit_reason: str = ""
    manual_exit_requested_at: float = 0.0
    manual_exit_reference_price: float = 0.0
    # Venue-scoped accounting metadata. Never infer a live position's payoff
    # model or currency from global startup config in a multi-desk process.
    exchange: str = ""
    execution_symbol: str = ""
    asset_id: str = ""
    currency_code: str = "USD"
    currency_symbol: str = "$"
    pnl_model: str = "linear"          # linear | inverse_btcusd
    quantity_unit: str = "units"
    # Signal-domain state for derivatives whose execution mark is a premium
    # but whose liquidity thesis is formed on an underlying (ICICI NFO options).
    thesis_side: str = ""
    analysis_entry_price: float = 0.0
    analysis_sl_price: float = 0.0
    analysis_tp_price: float = 0.0
    analysis_atr: float = 0.0
    # A broker-flat state without a resolved fill is not realised P&L. Keep the
    # lifecycle pending until an exact closing execution can be recovered.
    unconfirmed_exit_notice_at: float = 0.0
    unconfirmed_exit_attempts: int = 0

    def is_active(self): return self.phase == PositionPhase.ACTIVE
    def is_flat(self): return self.phase == PositionPhase.FLAT
    def to_dict(self):
        return {"side": self.side, "quantity": self.quantity,
                "entry_price": self.entry_price,
                "sl_price": self.sl_price, "tp_price": self.tp_price,
                "tp_ladder": list(self.tp_ladder or []),
                "tp_ladder_active": bool(self.tp_ladder_active),
                "tp_ladder_realized_pnl": float(self.tp_ladder_realized_pnl or 0.0),
                "entry_leverage": float(self.entry_leverage or 0.0),
                "phase": self.phase.name,
                "exchange": self.exchange, "execution_symbol": self.execution_symbol,
                "asset_id": self.asset_id, "currency_code": self.currency_code,
                "currency_symbol": self.currency_symbol, "pnl_model": self.pnl_model,
                "quantity_unit": self.quantity_unit, "thesis_side": self.thesis_side,
                "analysis_entry_price": float(self.analysis_entry_price or 0.0),
                "analysis_sl_price": float(self.analysis_sl_price or 0.0),
                "analysis_tp_price": float(self.analysis_tp_price or 0.0),
                "analysis_atr": float(self.analysis_atr or 0.0),
                "entry_atr": float(self.entry_atr or 0.0)}

# ═══════════════════════════════════════════════════════════════
# DAILY RISK GATE with consecutive loss lockout
# ═══════════════════════════════════════════════════════════════
class DailyRiskGate:
    # BUG-TZ FIX: Trading day boundary must be midnight IST (UTC+5:30), not
    # midnight UTC.  date.today() on a cloud server (UTC) flips at midnight
    # UTC = 05:30 IST.  A trade opening at 05:29 IST has record_trade_start()
    # increment day N; if it closes at 05:31 IST, _reset_if_new_day() fires
    # first (UTC midnight passed), zeroes _daily_pnl, then record_trade_result()
    # adds PnL to day N+1.  Day N: trades=1 pnl=0. Day N+1: trades=0 pnl=+X.
    # The daily loss cap is also corrupted.  Fix: IST-aware date comparison.
    _IST = timezone(timedelta(hours=5, minutes=30))

    @staticmethod
    def _today_ist() -> date:
        return datetime.now(DailyRiskGate._IST).date()

    def __init__(self):
        self._today = self._today_ist(); self._daily_trades = 0; self._consec_losses = 0
        self._daily_pnl = 0.0; self._daily_open_bal = 0.0
        self._loss_lockout_until = 0.0; self._lock = threading.Lock()
        self._loss_lockout_resets_consec = False

    def _reset_if_new_day(self):
        today = self._today_ist()
        if today != self._today:
            self._today = today; self._daily_trades = 0; self._daily_pnl = 0.0
            self._daily_open_bal = 0.0; self._consec_losses = 0; self._loss_lockout_until = 0.0
            self._loss_lockout_resets_consec = False

    def set_opening_balance(self, balance):
        with self._lock:
            self._reset_if_new_day()
            if self._daily_open_bal < 1e-10 and balance > 0: self._daily_open_bal = balance

    def can_trade(self, current_balance) -> Tuple[bool, str]:
        with self._lock:
            self._reset_if_new_day(); now = time.time()
            if now < self._loss_lockout_until:
                return False, f"Loss lockout: {int(self._loss_lockout_until - now)}s remaining"
            # FIX 2: Reset consec_losses when lockout expires so the bot can
            # actually trade again.  Without this the lockout re-arms on every
            # call after expiry because consec_losses is still ≥ MAX — infinite loop.
            elif self._loss_lockout_until > 0 and now >= self._loss_lockout_until:
                if self._loss_lockout_resets_consec:
                    self._consec_losses = 0
                self._loss_lockout_until = 0.0
                self._loss_lockout_resets_consec = False
            if self._daily_trades >= QCfg.MAX_DAILY_TRADES():
                return False, f"Daily cap: {self._daily_trades}/{QCfg.MAX_DAILY_TRADES()}"
            if self._consec_losses >= QCfg.MAX_CONSEC_LOSSES():
                self._loss_lockout_until = now + QCfg.LOSS_LOCKOUT_SEC()
                self._loss_lockout_resets_consec = True
                return False, f"Consec loss cap → {QCfg.LOSS_LOCKOUT_SEC()}s lockout"
            if self._daily_open_bal > 1e-10:
                lp = -self._daily_pnl / self._daily_open_bal * 100.0
                if lp >= QCfg.MAX_DAILY_LOSS_PCT():
                    return False, f"Daily loss cap: {lp:.1f}%"
            return True, ""

    def record_trade_start(self):
        with self._lock: self._reset_if_new_day(); self._daily_trades += 1

    def record_trade_result(self, pnl):
        with self._lock:
            self._daily_pnl += pnl
            if pnl < 0:
                self._consec_losses += 1
                try:
                    loss_lockout = float(getattr(config, "QUANT_LOCKOUT_AFTER_LOSS_SEC", QCfg.LOSS_LOCKOUT_SEC()) or 0.0)
                except Exception:
                    loss_lockout = float(QCfg.LOSS_LOCKOUT_SEC())
                if loss_lockout > 0.0:
                    self._loss_lockout_until = max(self._loss_lockout_until, time.time() + loss_lockout)
                    self._loss_lockout_resets_consec = self._consec_losses >= QCfg.MAX_CONSEC_LOSSES()
            else:
                self._consec_losses = 0
                self._loss_lockout_resets_consec = False

    def force_reset(self, reset_consec: bool = True, reset_daily: bool = False) -> str:
        """
        Manual override reset — callable from Telegram /resetrisk.

        reset_consec: clears consecutive_losses + loss_lockout (default True)
        reset_daily:  also clears daily_pnl + daily_trades counter (opt-in only)

        Returns a human-readable summary of what was cleared.
        """
        with self._lock:
            parts = []
            if reset_consec:
                prev_cl = self._consec_losses
                prev_lo = self._loss_lockout_until
                self._consec_losses       = 0
                self._loss_lockout_until  = 0.0
                self._loss_lockout_resets_consec = False
                parts.append(f"consec_losses {prev_cl}→0")
                if prev_lo > 0:
                    import time as _t
                    remaining = max(0, int(prev_lo - _t.time()))
                    parts.append(f"lockout cleared ({remaining}s was remaining)")
            if reset_daily:
                prev_dt  = self._daily_trades
                prev_dp  = self._daily_pnl
                self._daily_trades = 0
                self._daily_pnl    = 0.0
                parts.append(f"daily_trades {prev_dt}→0")
                parts.append(f"daily_pnl {prev_dp:+.2f}→0.00 account-units")
            return "; ".join(parts) if parts else "nothing to reset"

    @property
    def daily_trades(self):
        with self._lock:
            self._reset_if_new_day()
            return self._daily_trades

    @property
    def daily_pnl(self):
        with self._lock:
            self._reset_if_new_day()
            return self._daily_pnl

    @property
    def consec_losses(self):
        with self._lock:
            self._reset_if_new_day()
            return self._consec_losses

    @property
    def loss_lockout_until(self):
        with self._lock:
            self._reset_if_new_day()
            return self._loss_lockout_until

# ═══════════════════════════════════════════════════════════════
# MAIN STRATEGY CLASS
# ═══════════════════════════════════════════════════════════════
class QuantStrategy:
    def __init__(self, order_manager=None, instrument=None):
        self._instrument = instrument
        self._asset_id = getattr(instrument, "asset_id", getattr(config, "SYMBOL", "BTCUSDT"))
        self._om = order_manager
        self._lock = threading.RLock()
        self._fee_engine = ExecutionCostEngine() if ExecutionCostEngine is not None else None
        self._prev_price_for_urgency = 0.0
        self._atr_5m = ATREngine()
        with instrument_scope(instrument):
            pass
        self._liq_map = LiquidityMap() if _LIQ_MAP_AVAILABLE else None
        self._entry_engine = EntryEngine(on_self_recovery=self._on_entry_engine_self_recovery, instrument=instrument) if _ENTRY_ENGINE_AVAILABLE else None
        self._pos = PositionState()
        self._last_sig = StructuralEntrySummary()
        self._last_entry_signal = None
        self._risk_gate = DailyRiskGate()
        self._last_eval_time = 0.0
        self._last_exit_time = 0.0
        self._last_tp_gate_rejection = 0.0
        self._tp_gate_rejection_mode = ""
        self._last_execution_viability = None
        self._last_pos_sync = 0.0
        self._last_exit_sync = 0.0
        self._exiting_since = 0.0
        self._entering_since = 0.0
        self._entry_order_placed_at = 0.0
        self._pos_sync_in_progress = False
        self._exit_sync_in_progress = False
        self._tp_ladder_sync_in_progress = False
        self._last_exit_side = ""
        # Institutional decision tape: emit on material state/geometry changes and on a
        # slow audit cadence only. This exposes calculations without per-tick spam.
        self._last_decision_log = 0.0
        self._last_decision_fingerprint = None
        self._decision_snapshot_sec = float(getattr(config, "ICT_DECISION_SNAPSHOT_SEC", 60.0) or 60.0)
        self._last_reconcile_time = 0.0
        self._RECONCILE_SEC = 30.0
        self._reconcile_pending = False
        self._reconcile_data = None
        self._total_trades = 0
        self._winning_trades = 0
        self._total_pnl = 0.0
        self._trade_history: deque = deque(maxlen=200)
        self.current_sl_price = 0.0
        self.current_tp_price = 0.0
        self._exit_completed = False
        self._pnl_recorded_for = 0.0
        self._last_known_price = 0.0
        self._last_data_warn = 0.0
        self._last_atr_warn = 0.0
        self._last_price_warn = 0.0
        self._last_exit_policy_log = 0.0
        self._last_structure_fingerprint = None
        self._last_maxhold_check = 0.0
        self._force_sl = None
        self._force_tp = None
        self._risk_manager_ref = None
        self.watchdog_trading_frozen = False
        self._last_watchdog_freeze_log = 0.0
        self._watchdog_freeze_seen = False
        self._watchdog_freeze_active_since = 0.0
        self._active_spread_cost_mult = 1.0
        self._last_spread_gate_context = {}
        self._last_data_integrity_context: Dict[str, Any] = {}
        self._last_data_integrity_log = 0.0
        # Non-blocking market-event bridge: WS callbacks only wake the scanner;
        # all calculations and order routing remain in the controlled strategy loop.
        self._market_event = threading.Event()
        self._market_wakeup_cb = None
        self._last_market_event_time = 0.0
        self._last_event_eval_delay_ms = 0.0
        self._last_event_eval_time = 0.0
        self._last_eval_source = "POLL"
        # This initial value is only a venue cap fallback for pre-position UI.
        # Real positions store the structural-funding leverage selected at entry.
        self._active_effective_leverage = 1.0
        self._active_margin_risk_pct = 0.0
        self._last_closed_side = ""
        self._last_closed_reason = ""
        self._last_closed_exit_price = 0.0
        self._last_closed_entry_price = 0.0
        self._last_closed_mfe_pts = 0.0
        self._last_closed_mae_pts = 0.0
        self._last_closed_atr = 0.0
        self._log_init()

    @staticmethod
    def _execution_session_label(now: Optional[datetime] = None) -> str:
        """Auditable market-session label; never participates in entry alpha."""
        dt = now or datetime.now(DailyRiskGate._IST)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=DailyRiskGate._IST)
        local = dt.astimezone(DailyRiskGate._IST)
        minute = local.hour * 60 + local.minute
        weekday = local.weekday() < 5
        if weekday and (9 * 60 + 15) <= minute < (15 * 60 + 30):
            return "INDIA_FNO_SESSION"
        if (5 * 60 + 30) <= minute < (13 * 60 + 30):
            return "ASIA_GLOBAL_SESSION"
        if (13 * 60 + 30) <= minute < (20 * 60 + 30):
            return "EUROPE_GLOBAL_SESSION"
        if (19 * 60) <= minute or minute < (2 * 60 + 30):
            return "US_GLOBAL_SESSION"
        return "OFF_SESSION"

    def _telegram_context(self) -> dict:
        """Runtime context attached to every asset-specific Telegram alert."""
        try:
            p = getattr(self, "_pos", None)
            phase = getattr(getattr(p, "phase", None), "name", "UNKNOWN")
            price = float(getattr(self, "_last_known_price", 0.0) or 0.0)
            ctx = {
                "state": phase,
                "price": price if price > 0 else None,
                "leverage": float(getattr(p, "entry_leverage", 0.0) or getattr(self, "_active_effective_leverage", 0.0) or QCfg.LEVERAGE()),
            }
            if p is not None and not getattr(p, "is_flat", lambda: True)():
                ctx.update({
                    "position_side": getattr(p, "side", ""),
                    "entry": getattr(p, "entry_price", 0.0),
                    "sl": getattr(p, "sl_price", 0.0),
                    "tp": getattr(p, "tp_price", 0.0),
                    "qty": getattr(p, "quantity", 0.0),
                })
            return ctx
        except Exception:
            return {}

    def _send_telegram(self, message: str, parse_mode: str = "HTML", *, event_type: str = None, **kwargs) -> bool:
        """Asset-aware Telegram send wrapper for this strategy instance."""
        try:
            ctx = self._telegram_context()
            extra_ctx = kwargs.pop("context", None)
            if isinstance(extra_ctx, dict):
                ctx.update(extra_ctx)
            return send_telegram_message(
                message,
                parse_mode=parse_mode,
                instrument=getattr(self, "_instrument", None),
                event_type=event_type,
                context=ctx,
                **kwargs,
            )
        except Exception:
            return send_telegram_message(message, parse_mode=parse_mode)

    def _log_init(self):
        logger.info("=" * 80)
        logger.info("🏛 INSTITUTIONAL AUCTION STRATEGY — UNIFIED STRUCTURAL ORDER AUTHORITY")
        with instrument_scope(getattr(self, "_instrument", None)):
            inst = getattr(self, "_instrument", None)
            asset = getattr(inst, "asset_id", QCfg.SYMBOL())
            venues = ", ".join(f"{ex.value.upper()}:{ei.display_symbol}" for ex, ei in getattr(inst, "by_exchange", {}).items()) if inst is not None else QCfg.EXCHANGE().upper()
            logger.info(f"   {asset} | {QCfg.SYMBOL()} | venues={venues} | leverage_cap={QCfg.LEVERAGE()}x | margin_policy={QCfg.MARGIN_PCT():.0%}")
        _is_nifty_profile = str(getattr(inst, "asset_id", "") or "").upper() in {"NIFTY", "NIFTY50", "CNXNIFTY"} and str(QCfg.EXCHANGE()).lower() == "icici"
        if _is_nifty_profile:
            logger.info(f"   EntryAuthority: {'ACTIVE' if self._entry_engine is not None else 'UNAVAILABLE'} | NIFTY intraday phase → same-direction 1m/5m liquidity-sweep reclaim")
            logger.info(f"   LiquidityMap: {'ACTIVE' if self._liq_map is not None else 'UNAVAILABLE'} | fast cash-out targets=nearest real 5m/15m+ directional pool")
            logger.info("   Context: ACTIVE (15m tempo + 1h auction + 4h location) | Trigger: ACTIVE (trend-direction 1m/5m pullback sweep reclaim; no forced FVG wait)")
            logger.info("   Execution: fresh CE/PE websocket vehicle required at activation + premium-native SL/TP + exact-fill reconciliation")
            logger.info("   ExitModel: compact structural invalidation + nearest liquidity target + NIFTY failed-auction/time stop")
        else:
            logger.info(f"   EntryAuthority: {'ACTIVE' if self._entry_engine is not None else 'UNAVAILABLE'} | 4H/15m DOL → 5m raid/MSS/FVG")
            logger.info(f"   LiquidityMap: {'ACTIVE' if self._liq_map is not None else 'UNAVAILABLE'} | targets=opposing 15m/4H/1D liquidity")
            logger.info("   Context: ACTIVE (market-phase delivery; dominant intraday tempo cannot be faded by weak split-HTF location) | Trigger: ACTIVE (5m raid→MSS→FVG)")
            logger.info("   Execution: venue lot rules + structural SL risk + bracket protection + exact-fill reconciliation")
            logger.info("   ExitModel: structural SL + opposing 15m/4H/1D liquidity targets")
        logger.info("=" * 80)

    @staticmethod
    def _bounded(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, float(value)))

    @staticmethod
    def _tf_rank(tf: str) -> int:
        return {
            "1m": 1, "2m": 1, "3m": 1, "5m": 2,
            "15m": 3, "30m": 3, "1h": 4, "4h": 5, "1d": 6,
        }.get(str(tf or "").lower(), 1)

    @staticmethod
    def _risk_pct_fraction(raw_value=None) -> float:
        """
        Canonical RISK_PER_TRADE interpretation used by sizing, liquidation
        preview, and risk diagnostics.  The value is a fraction, not a percent:
        0.015 = 1.5%.  Percent-style legacy values are normalised once here so
        pre-trade guards cannot disagree with _compute_quantity().
        """
        raw = float(_cfg("RISK_PER_TRADE", 0.006) if raw_value is None else raw_value)
        if not math.isfinite(raw) or raw <= 0.0:
            return 0.0
        if raw > 0.05:
            return (raw / 100.0) if raw <= 5.0 else 0.05
        return raw

    def _margin_risk_leverage_cap(self, price: float, sl_dist: float, risk_pct: float = None) -> float:
        """Max leverage that keeps SL loss <= risk_pct of used margin."""
        price = float(price or 0.0)
        sl_dist = abs(float(sl_dist or 0.0))
        risk_pct = self._risk_pct_fraction() if risk_pct is None else float(risk_pct)
        if price <= 0.0 or sl_dist <= 0.0 or risk_pct <= 0.0:
            return 0.0
        return (risk_pct * price) / max(sl_dist, 1e-12)

    def _liquidation_safe_leverage_cap(self, side: str, entry: float, sl: float,
                                       configured_leverage: float = None) -> float:
        """Highest leverage that leaves the actual SL before the liquidation guard."""
        configured = max(float(QCfg.LEVERAGE() if configured_leverage is None else configured_leverage), 1.0)
        side = str(side or "").lower()
        entry = float(entry or 0.0)
        sl = float(sl or 0.0)
        if side not in ("long", "short") or entry <= 0.0 or sl <= 0.0:
            return 0.0
        ok, _, _, _ = self._sl_liquidation_sanity(side, entry, sl, leverage_override=configured)
        if ok:
            return configured
        ok_min, _, _, _ = self._sl_liquidation_sanity(side, entry, sl, leverage_override=1.0)
        if not ok_min:
            return 0.0
        lo, hi = 1.0, configured
        for _ in range(32):
            mid = (lo + hi) / 2.0
            ok_mid, _, _, _ = self._sl_liquidation_sanity(side, entry, sl, leverage_override=mid)
            if ok_mid:
                lo = mid
            else:
                hi = mid
        return max(1.0, lo)

    def _structural_funding_leverage(self, price: float, sl_dist: float,
                                        configured_leverage: float = None,
                                        side: str = None,
                                        sl_price: float = None,
                                        target_margin_budget: float = None,
                                        risk_capital: float = None) -> float:
        """Select the minimum funded leverage required by structural risk geometry.

        Leverage is not an alpha preference or ROE target.  It is the lowest
        integer leverage that can fund the approved structural-risk quantity
        inside the instrument margin allocation, clipped by venue and
        liquidation-protection limits.  When a preview caller has not yet
        supplied capital terms, return only the safe liquidation cap.
        """
        configured = max(float(QCfg.LEVERAGE() if configured_leverage is None else configured_leverage), 1.0)
        price = float(price or 0.0)
        sl_dist = abs(float(sl_dist or 0.0))
        if price <= 0.0 or sl_dist <= 0.0:
            return 0.0
        side_l = str(side or "").lower()
        if sl_price is None:
            if side_l == "short":
                sl = price + sl_dist
            else:
                side_l = "long"
                sl = price - sl_dist
        else:
            sl = float(sl_price or 0.0)
            if side_l not in ("long", "short"):
                side_l = "long" if sl < price else "short"
        liq_cap = self._liquidation_safe_leverage_cap(side_l, price, sl, configured_leverage=configured)
        safe_cap = math.floor(min(configured, liq_cap)) if liq_cap > 0.0 else 0
        if safe_cap < 1:
            return 0.0
        margin_budget = float(target_margin_budget or 0.0)
        risk_budget = float(risk_capital or 0.0)
        if margin_budget <= 0.0 or risk_budget <= 0.0:
            return float(safe_cap)
        qty_at_risk_budget = risk_budget / max(sl_dist, 1e-12)
        target_notional = qty_at_risk_budget * price
        min_required = max(1.0, math.ceil(target_notional / max(margin_budget, 1e-12) - 1e-12))
        return float(min(safe_cap, int(min_required)))

    def _capital_allocation_scalar(self, calibrated_probability: Optional[float], fee_drag_mult: float = 1.0) -> float:
        """Risk allocation pressure from cost and, only when available, calibrated odds.

        Structural evidence is deliberately excluded from this function because a
        live evidence score is not a statistically calibrated hit probability.
        """
        fee = self._bounded(float(fee_drag_mult or 1.0), 0.50, 1.0)
        if calibrated_probability is None:
            return fee
        p = self._bounded(float(calibrated_probability), 0.10, 1.0)
        return self._bounded((0.70 + 0.30 * p) * fee, 0.50, 1.0)


    def _sl_liquidation_sanity(self, side: str, entry: float, sl: float, leverage_override: float = None):
        entry = float(entry or 0.0)
        sl = float(sl or 0.0)
        if entry <= 0 or sl <= 0:
            return False, 0.0, 0.0, "missing entry/SL"
        side = str(side or "").lower()
        leverage = max(float(leverage_override or QCfg.LEVERAGE()), 1.0)
        maint_margin = float(_cfg("MAINTENANCE_MARGIN_RATE", 0.005))
        liq_buffer = float(_cfg("LIQUIDATION_BUFFER_PCT", 0.005))
        liq_move = max((1.0 / leverage) - maint_margin, 0.001)
        if side == "long":
            liq_price = entry * (1.0 - liq_move)
            guard = liq_price * (1.0 + liq_buffer)
            if sl >= entry:
                return False, liq_price, guard, "long SL is not protective"
            if sl <= guard:
                return False, liq_price, guard, (
                    f"long SL {sl:,.1f} is beyond liquidation guard {guard:,.1f}"
                )
            return True, liq_price, guard, ""
        if side == "short":
            liq_price = entry * (1.0 + liq_move)
            guard = liq_price * (1.0 - liq_buffer)
            if sl <= entry:
                return False, liq_price, guard, "short SL is not protective"
            if sl >= guard:
                return False, liq_price, guard, (
                    f"short SL {sl:,.1f} is beyond liquidation guard {guard:,.1f}"
                )
            return True, liq_price, guard, ""
        return False, 0.0, 0.0, "unknown side"

    @staticmethod
    def _is_nifty_trend_sweep_signal(signal) -> bool:
        return str(getattr(signal, "archetype", "") or getattr(getattr(signal, "entry_type", None), "value", "") or "").upper() == "NIFTY_TREND_SWEEP_SCALP"

    def _structural_rr_floor_for_signal(self, signal, policy=None) -> float:
        pol = policy or active_policy(getattr(self, "_instrument", None))
        floor = float(getattr(pol, "min_rr", 1.0) or 1.0)
        if self._is_nifty_trend_sweep_signal(signal):
            # Fast Indian-index option booking: nearest live liquidity pool is
            # the objective. Premium-side fee/viability validation still runs
            # after the actual CE/PE vehicle is activated.
            floor = max(1.0, float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_MIN_RR", 1.15) or 1.15))
        return floor

    def _target_pool_realism(self, signal, liq_snapshot, side: str,
                             entry: float, tp: float, sl: float, atr: float):
        reasons = []
        rejects = []
        target = getattr(signal, "target_pool", None)
        if target is None or getattr(target, "pool", None) is None:
            return 0.0, reasons, ["TP is not backed by a live liquidity pool"]

        pool = target.pool
        pool_price = float(getattr(pool, "price", 0.0) or 0.0)
        pool_tf = str(getattr(pool, "timeframe", "") or "")
        tf_rank = self._tf_rank(pool_tf)
        significance = float(getattr(target, "significance", 0.0) or 0.0)
        distance_atr = float(getattr(target, "distance_atr", 0.0) or 0.0)
        if distance_atr <= 0:
            distance_atr = abs(tp - entry) / max(float(atr or 0.0), 1e-9)

        direction_ok = (
            (side == "long" and tp > entry and pool_price > entry) or
            (side == "short" and tp < entry and pool_price < entry)
        )
        if not direction_ok:
            rejects.append("TP pool is not in the trade delivery direction")

        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = reward / max(risk, 1e-9)
        sig_score = self._bounded(significance / 8.0)
        tf_score = self._bounded(tf_rank / 4.0)
        rr_score = self._bounded(math.sqrt(max(rr, 0.0) / 4.0))

        max_reach = {1: 3.0, 2: 5.0, 3: 8.0, 4: 12.0, 5: 18.0, 6: 24.0}.get(tf_rank, 8.0)
        if distance_atr <= 0.35:
            reach_score = 0.25
        elif distance_atr <= max_reach:
            reach_score = 1.0 - math.exp(-(distance_atr - 0.35) / 2.0)
            reach_score = max(0.40, reach_score)
        else:
            reach_score = max(0.25, 1.0 - ((distance_atr - max_reach) / max(max_reach, 1.0)))

        opposing = []
        if liq_snapshot is not None:
            opposing = list(getattr(liq_snapshot, "ssl_pools", []) or []) if side == "long" else list(getattr(liq_snapshot, "bsl_pools", []) or [])
        lo, hi = sorted((entry, tp))
        gauntlet = 0
        threshold = max(significance * 0.60, 1.0)
        for opp in opposing:
            opp_pool = getattr(opp, "pool", None)
            opp_price = float(getattr(opp_pool, "price", 0.0) or 0.0)
            if opp_price <= lo or opp_price >= hi:
                continue
            opp_sig = float(getattr(opp, "significance", 0.0) or 0.0)
            if opp_sig >= threshold:
                gauntlet += 1
        gauntlet_score = max(0.35, 1.0 - gauntlet * 0.18)

        realism = (
            0.28 * sig_score
            + 0.22 * tf_score
            + 0.20 * rr_score
            + 0.18 * reach_score
            + 0.12 * gauntlet_score
        )

        if distance_atr > max_reach and realism < 0.72:
            rejects.append(
                f"TP reach {distance_atr:.1f}ATR exceeds {pool_tf or 'low-TF'} delivery envelope"
            )
        if rr >= 3.0 and realism >= 0.65:
            reasons.append(f"high-RR backed by {pool_tf or 'pool'} liquidity")
        if gauntlet:
            reasons.append(f"{gauntlet} opposing pool(s) before TP")
        reasons.append(
            f"target_realism={realism:.2f} tf={pool_tf or '?'} sig={significance:.1f}"
        )
        return realism, reasons, rejects

    def get_position(self) -> Optional[Dict]:
        with self._lock: return None if self._pos.is_flat() else self._pos.to_dict()

    def on_stream_restart(self) -> None:
        """Reset transient ICT/Liquidity state after market-data reconnection."""
        if self._entry_engine is not None:
            self._entry_engine.force_reset("market-data stream restarted")
        self._force_sl = None
        self._force_tp = None
        self._last_entry_signal = None



    def _spread_atr_gate(self, data_manager) -> tuple:
        """
        Live spread/ATR participation model.

        Spread is measured execution cost, not an alpha veto. Fresh executable
        books continue into sizing with a cost haircut and operator telemetry.
        Missing/stale books can still block because that is data integrity, not
        a spread opinion.
        """
        strict_live = callable(getattr(data_manager, "get_data_lineage", None))
        try:
            self._active_spread_cost_mult = 1.0
            self._last_spread_gate_context = {}

            atr = float(getattr(self._atr_5m, "atr", 0.0) or 0.0)
            if atr < 1e-10:
                return True, 0.0

            # ── Live bid/ask from current orderbook ───────────────────────────
            ob    = data_manager.get_orderbook()
            bids  = (ob or {}).get("bids", [])
            asks  = (ob or {}).get("asks", [])
            if not bids or not asks:
                book_status = {}
                status_fn = getattr(data_manager, "get_session_contract_book_status", None)
                if callable(status_fn):
                    try:
                        book_status = dict(status_fn() or {})
                    except Exception:
                        book_status = {}
                session_state = str(book_status.get("status") or "").upper()
                # A day-start CE/PE book is valid context even while option
                # websocket ticks are pending. It is not considered executable:
                # pre-order gating and activation both require routed live ticks.
                if session_state in {"READY", "ARMED_PENDING_WEBSOCKET_TICK"}:
                    call = dict(book_status.get("call") or {})
                    put = dict(book_status.get("put") or {})
                    self._last_spread_gate_context = {
                        "book_status": "SESSION_PRESELECTED",
                        "execution_session_status": session_state,
                        "hard_fail": False,
                        "session_trade_date": book_status.get("trade_date_ist"),
                        "session_call_symbol": call.get("symbol"),
                        "session_call_cost": call.get("cost"),
                        "session_call_delta": call.get("delta"),
                        "session_call_ws_fresh": bool(call.get("ws_fresh", False)),
                        "session_call_ws_age_sec": call.get("ws_age_sec"),
                        "session_put_symbol": put.get("symbol"),
                        "session_put_cost": put.get("cost"),
                        "session_put_delta": put.get("delta"),
                        "session_put_ws_fresh": bool(put.get("ws_fresh", False)),
                        "session_put_ws_age_sec": put.get("ws_age_sec"),
                    }
                else:
                    self._last_spread_gate_context = {"book_status": "NO_EXECUTABLE_BOOK", "hard_fail": False}
                return True, 0.0
            book_ts = float((ob or {}).get("timestamp", 0.0) or 0.0)
            book_age = max(0.0, time.time() - book_ts) if book_ts > 0 else None
            max_book_age = float(getattr(config, "EXECUTION_BOOK_MAX_STALE_SEC", 5.0) or 5.0)
            if strict_live and (book_age is None or book_age > max_book_age):
                def _stale_px(lvl) -> float:
                    if isinstance(lvl, (list, tuple)):
                        return float(lvl[0])
                    if isinstance(lvl, dict):
                        return float(lvl.get("limit_price") or lvl.get("price") or 0)
                    return 0.0

                stale_ctx = {
                    "book_status": "STALE" if book_age is not None else "MISSING_TIMESTAMP",
                    "book_age_sec": book_age, "max_book_age_sec": max_book_age,
                    "hard_fail": True, "hard_fail_reason": "STALE_EXECUTION_BOOK",
                    "size_mult": 0.0,
                }
                try:
                    bid = _stale_px(bids[0])
                    ask = _stale_px(asks[0])
                    if bid > 0.0 and ask > bid:
                        mid = (bid + ask) / 2.0
                        spread_usd = ask - bid
                        stale_ctx.update({
                            "bid": bid,
                            "ask": ask,
                            "mid": mid,
                            "spread": spread_usd,
                            "spread_bps": (spread_usd / mid) * 10_000.0 if mid > 0 else 0.0,
                            "spread_atr": spread_usd / max(atr, 1e-12),
                        })
                except Exception:
                    pass
                self._last_spread_gate_context = stale_ctx
                return False, float("inf")

            def _get_px(lvl) -> float:
                if isinstance(lvl, (list, tuple)):
                    return float(lvl[0])
                if isinstance(lvl, dict):
                    return float(lvl.get("limit_price") or lvl.get("price") or 0)
                return 0.0

            bid = _get_px(bids[0])
            ask = _get_px(asks[0])
            if bid <= 0.0 or ask <= bid:
                return True, 0.0

            mid        = (bid + ask) / 2.0
            spread_usd = ask - bid
            spread_bps = (spread_usd / mid) * 10_000.0 if mid > 0 else 0.0
            ratio      = spread_usd / atr

            inst = getattr(self, "_instrument", None) or current_instrument()
            asset_class = str(getattr(getattr(inst, "asset_class", ""), "value", getattr(inst, "asset_class", "")) or "").lower()
            asset_id = str(getattr(inst, "asset_id", getattr(self, "_asset_id", "")) or "").upper()
            tick_size = 0.0
            try:
                tick_size = float(getattr(inst, "tick_size", 0.0) or QCfg.TICK_SIZE() or 0.0)
            except Exception:
                tick_size = 0.0
            spread_ticks = (spread_usd / tick_size) if tick_size > 0 else 0.0

            # BTC/global defaults remain strict.  Non-crypto contracts get
            # calibrated bps caps so the gate rejects genuinely broken/wide
            # books, not normal tokenised-equity tick geometry.
            if asset_class == "equity":
                soft_ratio = float(getattr(config, "QUANT_SPREAD_SOFT_ATR_RATIO_EQUITY", 0.50))
                hard_ratio = float(getattr(config, "QUANT_MAX_SPREAD_ATR_RATIO_EQUITY", 4.00))
                hard_bps   = float(getattr(config, "QUANT_MAX_SPREAD_BPS_EQUITY", 35.0))
                hard_ticks = float(getattr(config, "QUANT_MAX_SPREAD_TICKS_EQUITY", 8.0))
            elif asset_class in ("commodity", "index"):
                soft_ratio = float(getattr(config, "QUANT_SPREAD_SOFT_ATR_RATIO_COMMODITY", 0.50))
                hard_ratio = float(getattr(config, "QUANT_MAX_SPREAD_ATR_RATIO_COMMODITY", 2.00))
                hard_bps   = float(getattr(config, "QUANT_MAX_SPREAD_BPS_COMMODITY", 45.0))
                hard_ticks = float(getattr(config, "QUANT_MAX_SPREAD_TICKS_COMMODITY", 10.0))
            else:
                soft_ratio = float(getattr(config, "QUANT_SPREAD_SOFT_ATR_RATIO_CRYPTO", 0.30))
                hard_ratio = float(getattr(config, "QUANT_MAX_SPREAD_ATR_RATIO", 0.50))
                hard_bps   = float(getattr(config, "QUANT_MAX_SPREAD_BPS_CRYPTO", 12.0))
                hard_ticks = float(getattr(config, "QUANT_MAX_SPREAD_TICKS_CRYPTO", 10.0))

            # Size haircut above soft ratio.  This is an allocation impairment,
            # not an alpha veto.  The later execution-viability model still
            # checks fee-to-risk and EV using actual SL/TP geometry.
            if ratio > soft_ratio and hard_ratio > soft_ratio:
                severity = min(1.0, max(0.0, (ratio - soft_ratio) / (hard_ratio - soft_ratio)))
                self._active_spread_cost_mult = max(
                    float(getattr(config, "QUANT_SPREAD_MIN_SIZE_MULT", 0.35)),
                    1.0 - severity * float(getattr(config, "QUANT_SPREAD_SIZE_HAIRCUT_MAX", 0.55)),
                )
            else:
                self._active_spread_cost_mult = 1.0

            # Fresh books are never vetoed by spread alone. Wide books are
            # reported as cost alerts and flow into allocation intensity.
            cost_alert = (
                ratio > hard_ratio
                or spread_ticks > hard_ticks
                or spread_bps > hard_bps
            )

            self._last_spread_gate_context = {
                "asset_id": asset_id,
                "asset_class": asset_class or "unknown",
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread": spread_usd,
                "spread_bps": spread_bps,
                "spread_atr": ratio,
                "spread_ticks": spread_ticks,
                "atr": atr,
                "soft_ratio": soft_ratio,
                "hard_ratio": hard_ratio,
                "hard_bps": hard_bps,
                "size_mult": float(getattr(self, "_active_spread_cost_mult", 1.0) or 1.0),
                "hard_fail": False,
                "cost_alert": bool(cost_alert),
                "book_status": "FRESH",
                "book_timestamp": book_ts,
                "book_age_sec": book_age,
                "max_book_age_sec": max_book_age,
            }

            _now = time.time()
            if cost_alert:
                if _now - getattr(self, "_last_spread_gate_warn", 0.0) >= 60.0:
                    self._last_spread_gate_warn = _now
                    logger.info(
                        f"Spread cost alert [{asset_class or 'unknown'}]: "
                        f"ratio={ratio:.3f}/{hard_ratio:.2f} spread={spread_bps:.2f}/{hard_bps:.2f}bps "
                        f"ticks={spread_ticks:.1f}/{hard_ticks:.1f} ATR={atr:.4f} "
                        f"spread={spread_usd:.4f} size_mult={self._active_spread_cost_mult:.2f} "
                        f"- no spread veto")

            if ratio > soft_ratio:
                if _now - getattr(self, "_last_spread_gate_soft_log", 0.0) >= 60.0:
                    self._last_spread_gate_soft_log = _now
                    logger.info(
                        f"⚖ Spread cost impairment [{asset_class or 'unknown'}]: "
                        f"ratio={ratio:.3f}>{soft_ratio:.2f} spread={spread_bps:.2f}bps "
                        f"ticks={spread_ticks:.1f} ATR={atr:.4f} spread={spread_usd:.4f} "
                        f"size_mult={self._active_spread_cost_mult:.2f}")

            return True, ratio
        except Exception as exc:
            self._active_spread_cost_mult = 1.0
            self._last_spread_gate_context = {"book_status": "CALCULATION_ERROR", "hard_fail": bool(strict_live), "error": str(exc)[:120]}
            return (False, float("inf")) if strict_live else (True, 0.0)

    def _on_entry_engine_self_recovery(self, state_name: str, age_sec: float) -> None:
        """Surface EntryEngine self-recovery and clear stale reconcile latches."""
        logger.warning(
            f"EntryEngine self-recovered from {state_name} after {age_sec:.0f}s; "
            "strategy reconcile latches cleared"
        )
        try:
            self._reconcile_pending = False
            self._reconcile_data = None
        except Exception:
            pass
        try:
            self._send_telegram(
                f"⚠️ <b>ENTRY ENGINE SELF-RECOVERY</b>\n"
                f"State: {state_name} for {age_sec:.0f}s\n"
                f"Reconcile latches cleared; next tick will re-evaluate state."
            )
        except Exception:
            pass

    def bind_market_wakeup(self, callback) -> None:
        """Bind a cheap scanner wake-up callback; no exchange I/O occurs here."""
        self._market_wakeup_cb = callback if callable(callback) else None

    def _wake_for_market_event(self, price: float = 0.0) -> None:
        ts = time.time()
        self._last_market_event_time = ts
        if float(price or 0.0) > 0.0:
            self._last_known_price = float(price)
        self._market_event.set()
        wake = self._market_wakeup_cb
        if wake is not None:
            try:
                wake()
            except Exception:
                pass

    def _on_realtime_trade(self, price: float, quantity: float, side: str) -> None:
        """Wake structural monitoring from an observed trade; never route here."""
        del quantity, side
        self._wake_for_market_event(price)

    def _on_realtime_quote(self, price: float) -> None:
        """Wake retest monitoring on book changes even before the next print."""
        self._wake_for_market_event(price)

    def _calibrated_signal_probability(self) -> Optional[float]:
        signal = getattr(self, "_last_entry_signal", None)
        if not bool(getattr(signal, "probability_calibrated", False)):
            return None
        try:
            p = float(getattr(signal, "delivery_probability", 0.0) or 0.0)
            return p if 0.0 < p < 1.0 else None
        except Exception:
            return None

    def consume_market_event(self) -> bool:
        seen = self._market_event.is_set()
        if seen:
            self._market_event.clear()
        return seen

    def has_urgent_structural_monitor(self) -> bool:
        engine = getattr(self, "_entry_engine", None)
        return bool(engine is not None and (engine.tracking_info is not None or engine.get_signal() is not None))

    def on_tick(self, data_manager, order_manager, risk_manager, timestamp_ms: int, event_driven: bool = False) -> None:
        with instrument_scope(getattr(self, "_instrument", None)):
            return self._on_tick_scoped(data_manager, order_manager, risk_manager, timestamp_ms, event_driven=event_driven)

    def _on_tick_scoped(self, data_manager, order_manager, risk_manager, timestamp_ms: int, event_driven: bool = False) -> None:
        # ── Bug 1 fix: locked section is non-blocking — only state reads/writes.
        # All exchange API calls (_sync_position, _evaluate_entry, _manage_active,
        # _finalise_exit) happen AFTER the lock is released so protection
        # replace_stop_loss, bracket fill polls, and reconcile writes can never
        # freeze each other or the health-check thread.
        now = timestamp_ms / 1000.0
        with self._lock:
            self._om = order_manager
            self._dm = data_manager
            # Bug #10 fix: store risk_manager reference so _record_exchange_exit
            # can call risk_manager.record_trade without a parameter chain change.
            self._risk_manager_ref = risk_manager
            urgent = bool(event_driven and self.has_urgent_structural_monitor())
            min_event_gap = 0.025  # coalesces event bursts without adding candle/polling latency
            required_gap = min_event_gap if urgent else QCfg.TICK_EVAL_SEC()
            if now - self._last_eval_time < required_gap:
                return
            self._last_eval_time = now
            self._last_eval_source = "MARKET_EVENT" if urgent else "POLL"
            if urgent and self._last_market_event_time > 0.0:
                self._last_event_eval_time = now
                self._last_event_eval_delay_ms = max(0.0, (now - self._last_market_event_time) * 1000.0)

            # Local data feeds — all in-process reads, no I/O
            try:
                ob = data_manager.get_orderbook()
                price = data_manager.get_last_price()
                if self._fee_engine is not None:
                    self._fee_engine.update_orderbook(ob, price)
            except Exception:
                pass
            try:
                p = data_manager.get_last_price()
                if p > 1.0:
                    self._last_known_price = p
            except Exception:
                pass

            # ─── ICT_LIQUIDITY: tick liveness stamp (used by watchdog TickAgeCheck) ───
            self._last_tick_time = now

            # Apply any pending reconcile result (written by background thread)
            if self._reconcile_data is not None:
                _rdata = self._reconcile_data; self._reconcile_data = None
                self._reconcile_apply(order_manager, _rdata)

            # Spawn reconcile background thread if due (non-blocking)
            if not self._reconcile_pending and now - self._last_reconcile_time >= self._RECONCILE_SEC:
                self._last_reconcile_time = now; self._reconcile_pending = True
                threading.Thread(
                    target=self._reconcile_query_thread,
                    args=(order_manager,), daemon=True,
                ).start()

            # Snapshot all decision-relevant state while locked
            phase             = self._pos.phase
            need_pos_sync     = (phase == PositionPhase.ACTIVE  and now - self._last_pos_sync  > QCfg.POS_SYNC_SEC())
            need_exit_sync    = (phase == PositionPhase.EXITING and now - self._last_exit_sync > QCfg.POS_SYNC_SEC())
            exiting_stuck     = (phase == PositionPhase.EXITING and (now - self._exiting_since) > 120.0)
            cooldown_ok       = (now - self._last_exit_time >= float(QCfg.COOLDOWN_SEC()))

        # ── All blocking exchange I/O below — lock is NOT held ───────────────────

        if phase == PositionPhase.ACTIVE:
            if need_pos_sync and not self._pos_sync_in_progress:
                # Dispatch position sync to a background thread.
                # _sync_position calls get_open_position() → Delta REST with a 30s timeout.
                # Running it in the main thread blocks on_tick, trail management, and the
                # heartbeat for up to 30s every 30s (100% duty cycle = permanently frozen).
                self._pos_sync_in_progress = True
                with self._lock:
                    self._last_pos_sync = now   # stamp immediately so we don't re-trigger

                def _bg_sync_active(om=order_manager):
                    try:
                        self._sync_position(om)
                    except Exception as _e:
                        logger.error("_sync_position (ACTIVE) error: %s", _e, exc_info=True)
                    finally:
                        self._pos_sync_in_progress = False

                threading.Thread(target=_bg_sync_active, daemon=True,
                                 name="pos-sync-active").start()

            # Bug #6 fix: _manage_active reads and modifies self._pos (trail SL,
            # extrema and partial-fill accounting concurrently with the background
            # sync thread that also writes to self._pos via _sync_position.
            # Avoid concurrent broker sync and partial-target reconciliation.
            # Exchange-attached structural protection remains live while one management tick is skipped.
            if not self._pos_sync_in_progress:
                self._manage_active(data_manager, order_manager, now)

        elif phase == PositionPhase.EXITING:
            if need_exit_sync and not self._exit_sync_in_progress:
                self._exit_sync_in_progress = True
                with self._lock:
                    self._last_exit_sync = now

                def _bg_sync_exit(om=order_manager):
                    try:
                        self._sync_position(om)
                    except Exception as _e:
                        logger.error("_sync_position (EXITING) error: %s", _e, exc_info=True)
                    finally:
                        self._exit_sync_in_progress = False

                threading.Thread(target=_bg_sync_exit, daemon=True,
                                 name="pos-sync-exit").start()
            if exiting_stuck:
                # v8.0: check if exit was already completed by sync/reconcile thread
                if self._exit_completed:
                    logger.info("EXITING stuck >120s but exit already completed — finalising")
                    with self._lock:
                        self._finalise_exit()
                else:
                    # A timeout is not a fill. Keep the lifecycle blocked in EXITING
                    # and continue exact broker-order reconciliation; zero/estimated
                    # realised P&L would corrupt risk gates and strategy learning.
                    _pending_pos = self._pos
                    _pending_cur = str(getattr(_pending_pos, "currency_symbol", "$") or "$")
                    _notice_due = (now - float(getattr(_pending_pos, "unconfirmed_exit_notice_at", 0.0) or 0.0)) >= 60.0
                    if _notice_due:
                        with self._lock:
                            _pending_pos.unconfirmed_exit_notice_at = now
                        logger.warning("⚠️ EXITING pending exact broker fill reconciliation for >120s — no realised P&L booked")
                        self._send_telegram(
                            "⚠️ <b>EXITING PENDING RECONCILIATION</b>\n"
                            "No exact closing execution has resolved; realised P&L remains unbooked.\n"
                            f"Entry: {_pending_cur}{_pending_pos.entry_price:,.2f} | Side: {_pending_pos.side.upper()}")

        elif phase == PositionPhase.ENTERING:
            # Bracket fill is being polled by a background thread.
            # This phase blocks re-entry on every tick until fill confirmed (→ACTIVE)
            # or the entry aborts (→FLAT via finally in _launch_entry_async).
            #
            # BUG 2 FIX — two-stage watchdog:
            #   Stage A (pre-order): from phase-onset until the limit order
            #     actually hits the exchange.  Bounded by PRE_ORDER_TOLERANCE
            #     (default 45 s) — covers signing, credential refresh, retries.
            #     If this expires, something is wrong BEFORE any order exists,
            #     so it is safe to force-FLAT.
            #
            #   Stage B (post-order): from order-placed timestamp until
            #     fill-confirmation.  Bounded by LIMIT_ORDER_FILL_TIMEOUT_SEC
            #     + watchdog_buffer (25 % margin, min 30 s).  This runs in
            #     parallel with the background thread's own fill poll.
            #
            # The old single-stage watchdog counted from phase-onset, so a 60 s
            # order-placement delay (bracket child resolution) + 60 s fill poll
            # already exceeded 90 s — fired while the position was still live.
            PRE_ORDER_TOLERANCE = 45.0
            _entry_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 120.0))
            _watchdog_buffer = max(30.0, _entry_timeout * 0.25)

            _order_placed_at = getattr(self, '_entry_order_placed_at', 0.0)
            if _order_placed_at <= 0.0:
                # Stage A: waiting for order to be placed
                _elapsed = now - self._entering_since
                _limit = PRE_ORDER_TOLERANCE
                _stage = "pre-order"
            else:
                # Stage B: order placed — wait for fill
                _elapsed = now - _order_placed_at
                _limit = _entry_timeout + _watchdog_buffer
                _stage = "post-order"

            if _elapsed > _limit:
                with self._lock:
                    if self._pos.phase == PositionPhase.ENTERING:
                        logger.warning(
                            f"⚠️ ENTERING watchdog [{_stage}]: >{int(_limit)}s "
                            f"elapsed={_elapsed:.0f}s without fill — forcing FLAT "
                            f"(check exchange for orphaned position)")
                        self._send_telegram(
                            f"⚠️ <b>ENTERING TIMEOUT</b>\n"
                            f"Stage: {_stage}  elapsed={_elapsed:.0f}s  limit={int(_limit)}s\n"
                            f"State reset to FLAT.\n"
                            f"<b>Check exchange for open position!</b>")
                        self._pos.phase = PositionPhase.FLAT
                        self._last_exit_time = now
                        self._entry_order_placed_at = 0.0
                        if self._entry_engine is not None:
                            self._entry_engine.on_entry_failed()
                            logger.info("🔄 Entry engine reset to SCANNING after ENTERING watchdog")

        elif phase == PositionPhase.FLAT:
            if cooldown_ok:
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)

    def _launch_entry_async(self, data_manager, order_manager, risk_manager,
                             side: str, sig, mode: str,
                             setup_grade: str = "",
                             prefetched_bal_info: dict = None,
                             entry_now: float = 0.0) -> None:
        """
        Non-blocking entry: sets ENTERING phase immediately, then runs
        _enter_trade in a daemon thread so the main on_tick loop is never
        blocked by the bracket fill-polling sleep loop (up to 45s).

        entry_now: the exchange-derived timestamp (timestamp_ms / 1000.0)
        from the calling on_tick.  Threaded into _enter_trade so that
        mark_entry_placed() uses the same clock as _check_session_limits,
        preventing clock-drift pacing errors (Bug #21).

        setup_grade: "S" | "A" | "B" | "" — passed through to _enter_trade so
        structural delivery probability can scale size within account-risk limits.

        prefetched_bal_info: Bug #5 fix — the balance dict already fetched in
        _evaluate_entry (REST call #1) is forwarded here so _enter_trade does
        not make a second identical REST call in the same tick. Between the two
        calls the balance cannot change (no position is open), but the redundancy
        added ~50 ms latency and could produce divergent values on stale exchange
        endpoints.

        The try/finally guarantees any abort path inside _enter_trade
        (TP gate rejection, SL failure, partial-fill abort, etc.) resets phase
        to FLAT so entry evaluation resumes after cooldown.
        """
        with self._lock:
            self._pos.phase      = PositionPhase.ENTERING
            self._entering_since = time.time()
            # BUG 2: reset order-placed timestamp — Stage A (pre-order) begins
            self._entry_order_placed_at = 0.0

        _dm, _om, _rm  = data_manager, order_manager, risk_manager
        _bal           = prefetched_bal_info
        _entry_now     = entry_now   # captured for the thread closure

        def _bg():
            with instrument_scope(getattr(self, "_instrument", None)):
              _gate_reject = False
              try:
                self._enter_trade(_dm, _om, _rm, side, sig, mode=mode,
                                  setup_grade=setup_grade,
                                  prefetched_bal_info=_bal,
                                  entry_now=_entry_now)
              except Exception as _e:
                logger.error(
                    f"_enter_trade background thread error ({mode}/{side}): {_e}",
                    exc_info=True)
              finally:
                with self._lock:
                    if self._pos.phase == PositionPhase.ENTERING:
                        # Distinguish pre-trade gate rejection (no order placed)
                        # from a real order-level failure (order placed but aborted).
                        #
                        # Gate rejections (TP R:R, SL/TP sanity, fee floor) set
                        # _last_tp_gate_rejection right before returning from
                        # _enter_trade.  If that timestamp is within the last 5s
                        # we know no order was ever sent — do NOT engage the
                        # cooldown.  Signals resume immediately on the next tick.
                        #
                        # Real failures (exchange error, partial fill abort, etc.)
                        # do not touch _last_tp_gate_rejection, so the gate will
                        # be more than 5s old → full cooldown applies as before.
                        _gate_reject = (time.time() - self._last_tp_gate_rejection) < 5.0
                        if _gate_reject:
                            logger.info(
                                f"⚪ Pre-order entry rejected (mode={mode} side={side}) "
                                f"— resetting to FLAT, no trade cooldown")
                        else:
                            logger.warning(
                                f"⚠️ Entry thread exited without activation "
                                f"(mode={mode} side={side}) — resetting to FLAT")
                            self._last_exit_time = time.time()
                        self._pos.phase = PositionPhase.FLAT
                    # CRITICAL: Always reset entry engine when thread exits
                    # without opening a position. on_entry_failed() is the
                    # single canonical reset path — it handles state machine
                    # transition and counter cleanup atomically.
                    if (self._entry_engine is not None
                            and self._pos.phase != PositionPhase.ACTIVE):
                        try:
                            if _gate_reject and hasattr(self._entry_engine, 'mark_pre_order_rejected'):
                                self._entry_engine.mark_pre_order_rejected(
                                    getattr(self, '_last_entry_signal', None),
                                    cooldown_sec=float(getattr(config, 'PRE_ORDER_REJECT_SWEEP_COOLDOWN_SEC', 30.0)),
                                    execution_context=getattr(self, '_last_execution_viability', None),
                                )
                        except Exception as _pre_gate_e:
                            logger.debug(f"EntryEngine pre-order rejection mark failed: {_pre_gate_e}")
                        self._entry_engine.on_entry_failed()

        threading.Thread(
            target=_bg, daemon=True, name=f"enter-{mode}-{side}"
        ).start()

    @staticmethod
    def _decision_num(info: Dict[str, Any], key: str, default: float = 0.0) -> float:
        try:
            value = info.get(key, None)
            if value is None:
                return default
            value = float(value)
            return value if math.isfinite(value) else default
        except Exception:
            return default

    @staticmethod
    def _decision_has(info: Dict[str, Any], key: str) -> bool:
        try:
            return key in info and info.get(key) is not None and math.isfinite(float(info.get(key)))
        except Exception:
            return False

    @classmethod
    def _decision_fmt(cls, info: Dict[str, Any], key: str, fmt: str = ".4f", missing: str = "N/A") -> str:
        return format(cls._decision_num(info, key), fmt) if cls._decision_has(info, key) else missing

    def _analysis_unit(self) -> str:
        try:
            ctx = self._position_accounting_context()
            if str(ctx.get("exchange", "")).lower() == "icici":
                return "NIFTYpts"
            return str(ctx.get("currency_symbol", "$"))
        except Exception:
            return "$"

    @staticmethod
    def _bar_timestamp_sec(row: Dict[str, Any]) -> float:
        raw = row.get("t", row.get("timestamp", 0.0)) if isinstance(row, dict) else 0.0
        try:
            val = float(raw or 0.0)
            return val / 1000.0 if val > 1e11 else val
        except Exception:
            return 0.0

    def _audit_structural_inputs(self, data_manager, candles_by_tf: Dict[str, List[Dict]], price: float, now: float) -> Dict[str, Any]:
        """Validate analysis lineage while exposing execution-domain readiness separately.

        ICICI is deliberately dual-domain: NIFTY underlying bars may remain live
        while the selected CE/PE vehicle is not executable. Structural awareness
        continues, but an option order can never inherit a false PASS from the
        underlying feed.
        """
        lineage_getter = getattr(data_manager, "get_data_lineage", None)
        lineage = lineage_getter() if callable(lineage_getter) else {
            "analysis_source": type(data_manager).__name__, "execution_source": type(data_manager).__name__,
            "analysis_domain": "EXECUTION_INSTRUMENT", "execution_domain": "EXECUTION_INSTRUMENT",
        }
        strict_live = callable(lineage_getter)
        stale_sec = float(getattr(config, "PRICE_STALE_SECONDS", 90.0))
        analysis_fn = getattr(data_manager, "is_analysis_price_fresh", None) or getattr(data_manager, "is_price_fresh", None)
        analysis_fresh = bool(analysis_fn(stale_sec)) if callable(analysis_fn) else not strict_live
        execution_fn = getattr(data_manager, "is_execution_price_fresh", None) or getattr(data_manager, "is_price_fresh", None)
        execution_fresh = bool(execution_fn(stale_sec)) if callable(execution_fn) else analysis_fresh
        session_book_status = {}
        status_fn = getattr(data_manager, "get_session_contract_book_status", None)
        if callable(status_fn):
            try:
                session_book_status = dict(status_fn() or {})
            except Exception as exc:
                session_book_status = {"status": "ERROR", "error": str(exc)}
        update_age = None
        execution_update_age = None
        try:
            analysis_update = getattr(data_manager, "get_analysis_last_update", None)
            updated = float((analysis_update() if callable(analysis_update) else data_manager.get_last_update()) or 0.0)
            if updated > 0:
                update_age = max(0.0, now - updated)
        except Exception:
            pass
        try:
            execution_update = getattr(data_manager, "get_execution_last_update", None)
            updated = float((execution_update() if callable(execution_update) else data_manager.get_last_update()) or 0.0)
            if updated > 0:
                execution_update_age = max(0.0, now - updated)
        except Exception:
            pass
        execution_status_fn = getattr(data_manager, "get_execution_feed_status", None)
        execution_status = {}
        if callable(execution_status_fn):
            try:
                execution_status = dict(execution_status_fn() or {})
            except Exception as exc:
                execution_status = {"status": "STATUS_ERROR", "error": str(exc)}
        dual_domain = str(lineage.get("analysis_domain", "")) != str(lineage.get("execution_domain", ""))
        execution_blockers: List[str] = []
        execution_snapshot_fresh = execution_fresh
        preselected_ready = bool(execution_status.get("session_vehicle_stream_ready", False))
        # ICICI is dual-domain: REST/prewarm premiums and the live NIFTY
        # underlying may both be fresh while neither selected option vehicle has
        # emitted an identity-routed websocket tick. For order readiness, a live
        # CE/PE websocket route is authoritative; REST snapshot freshness must
        # never clear this gate.
        if dual_domain:
            execution_fresh = preselected_ready
            if not preselected_ready:
                execution_blockers.append("EXECUTION_VEHICLE_WEBSOCKET_NOT_READY")
                execution_blockers.append(str(execution_status.get("status", "EXECUTION_VEHICLE_NOT_STREAMING")))
        elif not execution_fresh:
            execution_blockers.append("EXECUTION_QUOTE_STALE")
        continuous = str(QCfg.EXCHANGE()).lower() in ("delta", "coinswitch")
        tf_rules = {"5m": (QCfg.MIN_5M_BARS(), 300), "15m": (20, 900), "4h": (20, 14400)}
        frames: Dict[str, Dict[str, Any]] = {}
        blockers: List[str] = []
        for tf, (min_bars, step) in tf_rules.items():
            rows = list(candles_by_tf.get(tf, []) or [])
            ts = [self._bar_timestamp_sec(r) for r in rows if self._bar_timestamp_sec(r) > 0]
            duplicate = len(ts) - len(set(ts))
            nonmono = sum(1 for a, b in zip(ts, ts[1:]) if b <= a)
            gaps = sum(1 for a, b in zip(ts, ts[1:]) if b - a > step * float(getattr(config, "DATA_INTEGRITY_MAX_GAP_MULT_CONTINUOUS", 2.25))) if continuous else 0
            invalid = 0
            for row in rows:
                try:
                    o, h, l, c = (float(row.get(k, 0.0) or 0.0) for k in ("o", "h", "l", "c"))
                    invalid += int(min(o, h, l, c) <= 0 or h < max(o, c, l) or l > min(o, c, h))
                except Exception:
                    invalid += 1
            age = max(0.0, now - ts[-1]) if ts else None
            stale = bool(age is not None and age > step * float(getattr(config, "DATA_INTEGRITY_MAX_CLOSED_BAR_AGE_MULT", 3.25)))
            volume_rows = sum(1 for row in rows if float((row or {}).get("v", (row or {}).get("volume", 0.0)) or 0.0) > 0.0)
            frames[tf] = {"bars": len(rows), "min_bars": min_bars, "last_age_sec": age, "duplicates": duplicate, "nonmonotonic": nonmono, "gaps": gaps, "invalid_ohlc": invalid, "stale": stale, "nonzero_volume_bars": volume_rows, "volume_status": "OBSERVED" if volume_rows else "UNAVAILABLE"}
            if len(rows) < min_bars: blockers.append(f"{tf}_INSUFFICIENT_BARS")
            if duplicate or nonmono: blockers.append(f"{tf}_TIMESTAMP_INTEGRITY")
            if invalid: blockers.append(f"{tf}_INVALID_OHLC")
            if gaps: blockers.append(f"{tf}_GAP")
            if strict_live and (not ts or stale): blockers.append(f"{tf}_STALE_OR_UNTIMESTAMPED")
        if strict_live and bool(getattr(config, "DATA_INTEGRITY_REQUIRE_FRESH_QUOTES", True)) and not analysis_fresh:
            blockers.append("ANALYSIS_QUOTE_STALE")
        return {
            "ok": not blockers and price > 0.0, "blockers": blockers or ["NONE"], "lineage": lineage,
            "analysis_price": price, "analysis_quote_fresh": analysis_fresh, "last_update_age_sec": update_age,
            "execution_quote_fresh": execution_fresh, "execution_snapshot_fresh": execution_snapshot_fresh,
            "execution_last_update_age_sec": execution_update_age,
            "execution_ready_for_order": not execution_blockers, "execution_blockers": execution_blockers or ["NONE"],
            "execution_status": execution_status,
            "execution_session_status": str(session_book_status.get("status") or "DIRECT"),
            "execution_session_book": session_book_status,
            "frames": frames, "strict_live": strict_live,
        }

    def _decision_fingerprint(self, info: Dict[str, Any]) -> tuple:
        return (
            str(info.get("state", "SCANNING")), str(info.get("block_reason", "")),
            str(info.get("context_4h", "")), str(info.get("context_15m", "")),
            str(info.get("context_direction", "")), str(info.get("trigger", "")),
            str(info.get("market_phase", "")), str(info.get("auction_control_side", "")),
            str(info.get("execution_posture", "")), round(self._decision_num(info, "auction_control_score"), 4),
            str(info.get("context_bias_path", "")),
            round(self._decision_num(info, "context_delivery_score"), 4),
            str(info.get("side", info.get("raid_side", ""))),
            round(self._decision_num(info, "raid_price"), 6),
            round(self._decision_num(info, "mss_level"), 6),
            round(self._decision_num(info, "fvg_low"), 6),
            round(self._decision_num(info, "target_pool_price"), 6),
        )

    def _log_ict_decision_snapshot(self, info: Dict[str, Any], price: float, now: float, force: bool = False) -> None:
        """Decision tape: stage-aware numeric provenance on change plus slow snapshot."""
        info = dict(info or {})
        fp = self._decision_fingerprint(info)
        changed = fp != getattr(self, "_last_decision_fingerprint", None)
        interval = max(10.0, float(getattr(config, "ICT_DECISION_SNAPSHOT_SEC", getattr(self, "_decision_snapshot_sec", 60.0)) or 60.0))
        periodic = (now - float(getattr(self, "_last_decision_log", 0.0) or 0.0)) >= interval
        if not force and not changed and not periodic:
            return
        self._last_decision_fingerprint, self._last_decision_log = fp, now
        mode = "TRANSITION" if changed else "SNAPSHOT"
        spread = dict(getattr(self, "_last_spread_gate_context", {}) or {})
        quality = dict(getattr(self, "_last_data_integrity_context", {}) or {})
        raid = "none"
        if info.get("raid_side"):
            raid = (f"{str(info.get('raid_side')).upper()}@{self._decision_fmt(info,'raid_price')} "
                    f"wick={self._decision_fmt(info,'raid_wick')} q={self._decision_fmt(info,'raid_quality','.2f')} "
                    f"age={self._decision_fmt(info,'raid_age_sec','.0f')}s")
        elif info.get("candidate_raid_side"):
            raid = (f"REJECTED:{str(info.get('candidate_raid_side')).upper()}@{self._decision_fmt(info,'candidate_raid_price')} "
                    f"q={self._decision_fmt(info,'candidate_raid_quality','.2f')}")
        parent_raid = "none"
        if int(info.get("parent_htf_raid_count", 0) or 0) > 0:
            parent_raid = (f"{str(info.get('parent_htf_raid_side') or '').upper() or 'UNK'}"
                           f"[{info.get('parent_htf_raid_tf') or '?'}]@{self._decision_fmt(info,'parent_htf_raid_price')} "
                           f"wick={self._decision_fmt(info,'parent_htf_raid_wick')} "
                           f"q={self._decision_fmt(info,'parent_htf_raid_quality','.2f')} "
                           f"age={self._decision_fmt(info,'parent_htf_raid_age_sec','.0f')}s")
        if spread:
            age = spread.get("book_age_sec")
            age_txt = f"{float(age):.2f}s" if age is not None else "N/A"
            spread_txt = (f"book={spread.get('book_status','?')}/{age_txt} spread={self._decision_fmt(spread,'spread_bps','.2f')}bps/"
                          f"{self._decision_fmt(spread,'spread_atr','.3f')}ATR size×{self._decision_fmt(spread,'size_mult','.2f','N/A')} "
                          f"hard={'Y' if spread.get('hard_fail') else 'N'}")
            if spread.get("book_status") == "SESSION_PRESELECTED":
                spread_txt = (
                    f"book=SESSION_PRESELECTED/{spread.get('session_trade_date') or 'today'} "
                    f"CE={spread.get('session_call_symbol') or 'n/a'} cost={self._decision_fmt(spread,'session_call_cost','.0f')} "
                    f"PE={spread.get('session_put_symbol') or 'n/a'} cost={self._decision_fmt(spread,'session_put_cost','.0f')} hard=N"
                )
        else:
            spread_txt = "not-evaluated"
        logger.info(
            "🧭 AUCTION_DECISION %s state=%s block=%s archetype=%s | domain=%s mark=%s ATR5=%s pct=%s%% | "
            "4H=%s score=%s=[slope%s+struct%s] threshold=±%s ATR=%s | "
            "15m=%s score=%s=[slope%s+struct%s] threshold=±%s ATR=%s | "
            "phase=%s control=%s(%s) posture=%s risk×%s nifty=%s/%s(%s) | bias=%s dir=%s score=%s strict=%s raids=fresh5m:%d parent_htf:%d accepted:%d opposed:%d invalid:%d accepted=%s parent=%s | cost=%s",
            mode, info.get("state", "SCANNING"), info.get("block_reason", "UNKNOWN"),
            str(info.get("candidate_archetype", info.get("archetype", "-") or "-")), self._analysis_unit(),
            self._decision_fmt({"v": price}, "v"), self._decision_fmt(info,"entry_5m_atr"),
            self._decision_fmt({"p": 100.0*self._decision_num(info,"atr_percentile",0.5)},"p",".0f"),
            info.get("context_4h", "WAIT"), self._decision_fmt(info,"context_4h_score","+.3f"),
            self._decision_fmt(info,"context_4h_slope_component","+.3f"), self._decision_fmt(info,"context_4h_structure_component","+.3f"),
            self._decision_fmt(info,"context_direction_threshold",".2f"), self._decision_fmt(info,"context_4h_atr"),
            info.get("context_15m", "WAIT"), self._decision_fmt(info,"context_15m_score","+.3f"),
            self._decision_fmt(info,"context_15m_slope_component","+.3f"), self._decision_fmt(info,"context_15m_structure_component","+.3f"),
            self._decision_fmt(info,"context_direction_threshold",".2f"), self._decision_fmt(info,"context_15m_atr"),
            info.get("market_phase", "UNCLASSIFIED"), info.get("auction_control_side", "none"),
            self._decision_fmt(info, "auction_control_score", "+.2f"), info.get("execution_posture", "OBSERVE"),
            self._decision_fmt(info, "auction_risk_scalar", ".2f"),
            info.get("nifty_intraday_phase", "-"), info.get("nifty_intraday_aggression", "-"),
            self._decision_fmt(info, "nifty_intraday_score", "+.2f"),
            info.get("context_bias_path", "AWAITING_5M_DOL"), info.get("context_direction", "none"),
            self._decision_fmt(info, "context_delivery_score", ".2f"),
            "Y" if info.get("context_aligned") else "N",
            int(info.get("fresh_5m_raid_count",0) or 0), int(info.get("parent_htf_raid_count",0) or 0),
            int(info.get("aligned_5m_raid_count",0) or 0), int(info.get("opposed_5m_raid_count",0) or 0),
            int(info.get("invalid_5m_raid_count",0) or 0), raid, parent_raid, spread_txt,
        )
        if (changed or force) and info.get("entry_zone_selection_model"):
            logger.info(
                "🧱 ZONE_AUTHORITY profile=%s model=%s | ENTRY=%s[%s-%s] score=%s OB×=%s raidFuel=%s candidates=%s noise=%s | "
                "SL=%s anchor=%s outerLiq=%s protectedPools=%s mass=%s | TP=%s tf=%s liqMass=%s cluster=%s pathImbalance=%s penalty=%s",
                info.get("execution_profile", "AUCTION_CONTROL_ZONE_GRAPH"), info.get("entry_zone_selection_model", "?"),
                info.get("entry_zone_selected_tf", "?"), self._decision_fmt(info, "entry_zone_low"),
                self._decision_fmt(info, "entry_zone_high"), self._decision_fmt(info, "entry_zone_score", ".2f"),
                self._decision_fmt(info, "entry_zone_order_block_overlap", ".2f"),
                self._decision_fmt(info, "entry_zone_raid_fuel_score", ".2f"),
                int(info.get("entry_zone_candidate_count", 0) or 0), int(info.get("entry_zone_noise_count", 0) or 0),
                self._decision_fmt(info, "structural_stop"), self._decision_fmt(info, "stop_structural_anchor"),
                self._decision_fmt(info, "stop_outer_protected_liquidity"), int(info.get("stop_protected_pool_count", 0) or 0),
                self._decision_fmt(info, "stop_protected_cluster_mass", ".2f"), self._decision_fmt(info, "tp"),
                info.get("target_timeframe", "?"), self._decision_fmt(info, "target_structural_liquidity_mass", ".2f"),
                int(info.get("target_liquidity_cluster_count", 0) or 0), int(info.get("target_path_imbalance_count", 0) or 0),
                self._decision_fmt(info, "target_path_imbalance_penalty", ".2f"),
            )
        lineage = dict(quality.get("lineage", {}) or {})
        frames = dict(quality.get("frames", {}) or {})
        if quality and (changed or periodic):
            frame_rows = []
            for tf, frame in frames.items():
                last_age = frame.get("last_age_sec")
                last_age_txt = "N/A" if last_age is None else "{:.1f}s".format(float(last_age))
                frame_rows.append(
                    "{}:n={} age={} dup={} gap={} badOHLC={} vol={}({})".format(
                        tf,
                        frame.get("bars", 0),
                        last_age_txt,
                        frame.get("duplicates", 0),
                        frame.get("gaps", 0),
                        frame.get("invalid_ohlc", 0),
                        frame.get("volume_status", "?"),
                        frame.get("nonzero_volume_bars", 0),
                    )
                )
            frame_txt = " | ".join(frame_rows)
            native = getattr(getattr(self, "_liq_map", None), "_native_atr_by_tf", {}) or {}
            native_txt = ",".join(f"{tf}={float(native.get(tf,0.0)):.4f}" for tf in ("5m","15m","4h") if tf in native) or "pending"
            logger.info(
                "🔗 DATA_LINEAGE analysis_integrity=%s blockers=%s | analysis=%s/%s fresh=%s age=%s | execution=%s/%s fresh=%s ready=%s blockers=%s status=%s session=%s age=%s | %s | native_ATR[%s]",
                "PASS" if quality.get("ok") else "BLOCK", ",".join(quality.get("blockers", [])),
                lineage.get("analysis_source","?"), lineage.get("analysis_domain","?"),
                "Y" if quality.get("analysis_quote_fresh") else "N",
                "N/A" if quality.get("last_update_age_sec") is None else f"{float(quality.get('last_update_age_sec')):.2f}s",
                lineage.get("execution_source","?"), lineage.get("execution_domain","?"),
                "Y" if quality.get("execution_quote_fresh", quality.get("analysis_quote_fresh")) else "N",
                "Y" if quality.get("execution_ready_for_order", True) else "N",
                ",".join(quality.get("execution_blockers", ["NONE"])),
                str(dict(quality.get("execution_status", {}) or {}).get("status", "DIRECT")),
                quality.get("execution_session_status", "DIRECT"),
                "N/A" if quality.get("execution_last_update_age_sec") is None else f"{float(quality.get('execution_last_update_age_sec')):.2f}s",
                frame_txt, native_txt,
            )
            session_book = dict(quality.get("execution_session_book", {}) or {})
            _gate = str(session_book.get("execution_freshness_gate", ""))
            if _gate:
                ce = dict(session_book.get("call", {}) or {})
                pe = dict(session_book.get("put", {}) or {})
                ce_age = "N/A" if ce.get("ws_age_sec") is None else f"{float(ce.get('ws_age_sec')):.2f}s"
                pe_age = "N/A" if pe.get("ws_age_sec") is None else f"{float(pe.get('ws_age_sec')):.2f}s"
                logger.info(
                    "📡 ICICI_EXECUTION_FRESHNESS state=%s gate=%s | CE=%s ws_fresh=%s age=%s prem=%s bid=%s ask=%s | PE=%s ws_fresh=%s age=%s prem=%s bid=%s ask=%s",
                    session_book.get("status", "MISSING"), _gate, ce.get("symbol", "n/a"), "Y" if ce.get("ws_fresh") else "N", ce_age,
                    self._decision_fmt(ce, "live_premium", ".2f"), self._decision_fmt(ce, "live_bid", ".2f"), self._decision_fmt(ce, "live_ask", ".2f"),
                    pe.get("symbol", "n/a"), "Y" if pe.get("ws_fresh") else "N", pe_age,
                    self._decision_fmt(pe, "live_premium", ".2f"), self._decision_fmt(pe, "live_bid", ".2f"), self._decision_fmt(pe, "live_ask", ".2f"),
                )
        if changed and info.get("raid_side"):
            if not info.get("mss_broken"):
                logger.info(
                    "📐 ICT_GEOMETRY stage=MSS_WAIT side=%s raid=%s | MSS=%s source=%s age=%sb broken=N displacement=%sATR | "
                    "FVG=N/A prerequisite=MSS_BREAK | SL=N/A prerequisite=FVG_REPRICE | TP=N/A prerequisite=EXECUTABLE_GEOMETRY",
                    str(info.get("raid_side")).upper(), raid, self._decision_fmt(info,"mss_level"),
                    str(info.get("mss_source") or "N/A"), int(info.get("mss_age_bars", -1) or -1),
                    self._decision_fmt(info,"displacement_atr",".2f"),
                )
            elif not self._decision_has(info, "fvg_low"):
                logger.info(
                    "📐 ICT_GEOMETRY stage=FVG_WAIT side=%s MSS=%s source=%s age=%sb broken=Y displacement=%sATR | FVG=N/A prerequisite=VALID_REBALANCE_GAP | SL=N/A | TP=N/A",
                    str(info.get("raid_side")).upper(), self._decision_fmt(info,"mss_level"),
                    str(info.get("mss_source") or "N/A"), int(info.get("mss_age_bars", -1) or -1),
                    self._decision_fmt(info,"displacement_atr",".2f"),
                )
            else:
                audit = dict(info.get("target_audit", {}) or {})
                target_model = str(info.get("target_selection_model") or audit.get("target_selection_model") or "N/A")
                cap_raw = info.get("target_policy_max_rr", audit.get("max_structural_rr_reference"))
                try:
                    cap_txt = f"{float(cap_raw):.2f}" if cap_raw is not None and float(cap_raw) > 0.0 else "N/A"
                except Exception:
                    cap_txt = "N/A"
                block_reason = str(info.get("block_reason", "") or "")
                fvg_waiting = (
                    block_reason in ("AWAITING_FVG_REBALANCE", "AWAITING_FVG_EQUILIBRIUM_REBALANCE")
                    or not bool(info.get("fvg_rebalanced"))
                )
                if fvg_waiting and not self._decision_has(info, "structural_stop"):
                    prerequisite = "FVG_EQUILIBRIUM_REBALANCE" if block_reason == "AWAITING_FVG_EQUILIBRIUM_REBALANCE" else "FVG_REBALANCE"
                    logger.info(
                        "📐 ICT_GEOMETRY stage=FVG_REPRICE_WAIT side=%s MSS=%s broken=Y disp=%sATR | FVG=[%s,%s] eq=%s gap=%sATR | "
                        "SL=N/A prerequisite=%s | TP=N/A prerequisite=STRUCTURAL_STOP_AND_TARGET",
                        str(info.get("side", info.get("raid_side", "-"))).upper(), self._decision_fmt(info,"mss_level"),
                        self._decision_fmt(info,"displacement_atr",".2f"), self._decision_fmt(info,"fvg_low"), self._decision_fmt(info,"fvg_high"),
                        self._decision_fmt(info,"fvg_equilibrium"), self._decision_fmt(info,"fvg_distance_atr",".2f"), prerequisite,
                    )
                    return
                logger.info(
                    "📐 AUCTION_GEOMETRY stage=EXECUTABLE side=%s MSS=%s broken=Y disp=%sATR | FVG=[%s,%s] eq=%s gap=%sATR | "
                    "SL=%s clearance=%sATR model=%s+%s×pct | target=%s@%s model=%s eligible=%d/%d positive=%d RR=%s floor=%s cap=%s deliveryScore=%s calibratedP=%s",
                    str(info.get("side", info.get("raid_side", "-"))).upper(), self._decision_fmt(info,"mss_level"),
                    self._decision_fmt(info,"displacement_atr",".2f"), self._decision_fmt(info,"fvg_low"), self._decision_fmt(info,"fvg_high"),
                    self._decision_fmt(info,"fvg_equilibrium"), self._decision_fmt(info,"fvg_distance_atr",".2f"),
                    self._decision_fmt(info,"structural_stop"), self._decision_fmt(info,"stop_clearance_atr",".2f"),
                    self._decision_fmt(info,"stop_clearance_base_atr",".2f"), self._decision_fmt(info,"stop_clearance_pctile_slope_atr",".2f"),
                    info.get("target_timeframe", "N/A"), self._decision_fmt(info,"target_pool_price"), target_model, int(audit.get("eligible",0) or 0),
                    int(audit.get("pool_total",0) or 0), int(audit.get("positive",0) or 0), self._decision_fmt(info,"rr",".2f"),
                    self._decision_fmt(info,"min_structural_rr",".2f"), cap_txt, self._decision_fmt(info,"delivery_score","+.2f"), "Y" if bool(info.get("probability_calibrated")) else "N/A",
                )

    def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):
        """Evaluate unified institutional auction archetypes with data lineage gates."""
        if self._entry_engine is None or self._liq_map is None:
            logger.error("Institutional auction authority unavailable — entries disabled")
            return
        if self.watchdog_trading_frozen:
            if now - self._last_watchdog_freeze_log >= 60.0:
                self._last_watchdog_freeze_log = now
                logger.info("Institutional auction entries paused: watchdog circuit breaker engaged")
            return
        try:
            analysis_getter = getattr(data_manager, "get_analysis_price", None)
            price = float((analysis_getter() if callable(analysis_getter) else data_manager.get_last_price()) or 0.0)
        except Exception:
            return
        if price <= 0:
            return
        candles_by_tf: Dict[str, List[Dict]] = {}
        for tf, limit in (("5m", 2100), ("15m", 700), ("1h", 300), ("4h", 120), ("1d", 90)):
            try:
                candles_by_tf[tf] = data_manager.get_candles(tf, limit=limit) or []
            except Exception:
                candles_by_tf[tf] = []
        quality = self._audit_structural_inputs(data_manager, candles_by_tf, price, now)
        self._last_data_integrity_context = quality
        c5, c15, c4h = candles_by_tf.get("5m", []), candles_by_tf.get("15m", []), candles_by_tf.get("4h", [])
        if len(c5) < QCfg.MIN_5M_BARS() or len(c15) < 20 or len(c4h) < 20:
            if now - self._last_data_warn >= 30.0:
                self._last_data_warn = now
                logger.info("Institutional auction warmup waiting: 5m=%d 15m=%d 4h=%d", len(c5), len(c15), len(c4h))
            return
        self._atr_5m.compute(c5)
        atr = float(self._atr_5m.atr or 0.0)
        if atr <= 1e-10:
            self._log_ict_decision_snapshot({"state":"SCANNING", "block_reason":"INVALID_5M_ATR", "trigger":"WAIT"}, price, now)
            return
        if not quality.get("ok", False):
            self._log_ict_decision_snapshot({
                "state": "SCANNING", "block_reason": "DATA_INTEGRITY_BLOCK:" + ",".join(quality.get("blockers", [])),
                "trigger": "WAIT", "entry_5m_atr": atr, "atr_percentile": self._atr_5m.get_percentile(),
                "authority": "UNIFIED_STRUCTURAL_AUCTION",
            }, price, now)
            return
        # Book/spread is execution evidence, not structural alpha.  Keep
        # structural detection running even when the execution book is stale;
        # stale-book failure is enforced below as a pre-order block.
        spread_ok, _ = self._spread_atr_gate(data_manager)
        try:
            self._liq_map.update(candles_by_tf, price, atr, now)
            snapshot = self._liq_map.get_snapshot(price, atr)
        except Exception as exc:
            logger.warning("Liquidity map refresh blocked: %s", exc)
            return
        pol = active_policy(getattr(self, "_instrument", None))
        structural_min_rr = (
            max(1.0, float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_MIN_RR", 1.15) or 1.15))
            if self._analysis_unit() == "NIFTYpts" else float(pol.min_rr)
        )
        self._entry_engine.set_structural_delivery_policy(structural_min_rr, float(pol.max_rr))
        analysis_unit = self._analysis_unit()
        if analysis_unit == "NIFTYpts":
            estimated_cost_pts, estimated_cost_bps = 0.0, 0.0
        else:
            estimated_cost_pts, estimated_cost_bps = self._roundtrip_cost_points(price, use_maker_entry=True)
        self._entry_engine.set_execution_cost_model(estimated_cost_pts, estimated_cost_bps)
        self._entry_engine.set_atr_pctile(self._atr_5m.get_percentile())
        try:
            micro = build_microstructure_state(
                data_manager.get_orderbook() if hasattr(data_manager, "get_orderbook") else {},
                data_manager.get_recent_trades_raw() if hasattr(data_manager, "get_recent_trades_raw") else [],
                atr, now=now, book_max_age_sec=float(getattr(config, "ORDERBOOK_MAX_AGE_SECONDS", 5.0) or 5.0),
            )
        except Exception:
            micro = None
        self._entry_engine.update(
            snapshot, price, atr, now, candles_5m=c5, candles_15m=c15, candles_4h=c4h,
            candles_1h=candles_by_tf.get("1h", []), candles_1d=candles_by_tf.get("1d", []),
            micro_state=micro,
        )
        signal = self._entry_engine.get_signal()
        info = self._entry_engine.analysis_info or {}
        self._log_ict_decision_snapshot(info, price, now, force=signal is not None or not spread_ok)
        if signal is None:
            return
        side = str(signal.side or "").lower()
        entry, sl, tp = float(signal.entry_price), float(signal.sl_price), float(signal.tp_price)
        # NIFTY analysis is formed from the live underlying, while execution is
        # in the selected CE/PE option. If that selected vehicle is already fresh
        # on its identity-routed websocket, proceed. If the websocket is absent
        # but official exact-contract commit preflight is enabled, allow this
        # same fresh structural signal to reach _enter_trade(), where one exact
        # NFO quote/depth check must pass immediately before GTT cover-OCO
        # placement. There is no delayed queued signal and no stale/naked order.
        if self._analysis_unit() == "NIFTYpts" and not bool(quality.get("execution_ready_for_order", False)):
            _commit_preflight_allowed = bool(getattr(config, "ICICI_EXECUTION_PREFLIGHT_EXACT_QUOTE_ENABLED", True))
            if _commit_preflight_allowed:
                logger.info(
                    "NIFTY structural setup reached execution commitment with websocket vehicle not ready; "
                    "requiring immediate exact-contract NFO REST preflight before protected GTT cover-OCO | blockers=%s",
                    ",".join(str(x) for x in quality.get("execution_blockers", [])),
                )
            else:
                blocked_info = dict(info)
                blocked_info.update({
                    "state": "EXECUTION_BLOCKED",
                    "block_reason": "ICICI_EXECUTION_VEHICLE_NOT_FRESH",
                    "trigger": "PRE_ORDER_ICICI_OPTION_EXECUTION_FRESHNESS",
                    "entry_5m_atr": atr,
                    "atr_percentile": self._atr_5m.get_percentile(),
                    "authority": "UNIFIED_STRUCTURAL_AUCTION",
                    "execution_status": str((quality.get("execution_status") or {}).get("status", "UNKNOWN")),
                })
                self._log_ict_decision_snapshot(blocked_info, price, now, force=True)
                logger.info(
                    "NIFTY structural setup suppressed: neither fresh routed option websocket data nor exact-contract commit preflight is permitted | blockers=%s",
                    ",".join(str(x) for x in quality.get("execution_blockers", [])),
                )
                self._entry_engine.mark_signal_deferred(
                    side, "icici_execution_vehicle_not_fresh",
                    cooldown_sec=float(getattr(config, "ICICI_EXECUTION_SIGNAL_DEFER_COOLDOWN_SEC", 5.0) or 5.0),
                )
                return
        rr = abs(tp - entry) / max(abs(entry - sl), 1e-12)
        correct_geometry = (side == "long" and sl < entry < tp) or (side == "short" and tp < entry < sl)
        signal_rr_floor = self._structural_rr_floor_for_signal(signal, pol)
        if not correct_geometry or rr < signal_rr_floor:
            self._entry_engine.mark_signal_deferred(side, "invalid_structural_geometry_or_policy_rr", cooldown_sec=30.0)
            logger.info("ICT_LIQUIDITY ticket rejected: geometry=%s RR=%.2f floor=%.2f", correct_geometry, rr, signal_rr_floor)
            return
        realism, realism_notes, realism_rejects = self._target_pool_realism(signal, snapshot, side, entry, tp, sl, atr)
        min_realism = (
            float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_MIN_TARGET_REALISM", 0.42) or 0.42)
            if self._is_nifty_trend_sweep_signal(signal) else float(getattr(config, "ICT_MIN_TARGET_REALISM_SCORE", 0.58) or 0.58)
        )
        if realism_rejects or realism < min_realism:
            self._entry_engine.mark_signal_deferred(side, "target_realism_rejected", cooldown_sec=45.0)
            logger.info(
                "ICT_LIQUIDITY ticket rejected: target realism %.2f < %.2f rejects=%s notes=%s",
                realism, min_realism, "; ".join(realism_rejects) or "none", "; ".join(realism_notes) or "none",
            )
            return
        if not spread_ok:
            spread_ctx = dict(getattr(self, "_last_spread_gate_context", {}) or {})
            blocked_info = dict(info)
            blocked_info.update({
                "state": "EXECUTION_BLOCKED",
                "block_reason": "EXECUTION_SPREAD_OR_BOOK_BLOCK",
                "trigger": "PRE_ORDER_EXECUTION_BOOK_FRESHNESS",
                "entry_5m_atr": atr,
                "atr_percentile": self._atr_5m.get_percentile(),
                "authority": "UNIFIED_STRUCTURAL_AUCTION",
                "pre_order_structural_state": str(info.get("state", "")),
                "pre_order_structural_trigger": str(info.get("trigger", "")),
                "execution_book_status": str(spread_ctx.get("book_status", "")),
                "execution_book_age_sec": spread_ctx.get("book_age_sec"),
                "execution_book_max_age_sec": spread_ctx.get("max_book_age_sec"),
                "execution_book_hard_fail_reason": str(spread_ctx.get("hard_fail_reason", "")),
            })
            self._log_ict_decision_snapshot(blocked_info, price, now, force=True)
            logger.info(
                "STRUCTURAL_SETUP_READY_BUT_EXECUTION_BOOK_BLOCK %s entry=%.4f SL=%.4f TP=%.4f RR=%.2f | "
                "book=%s age=%s/%ss spread=%sbps/%sATR",
                side.upper(), entry, sl, tp, rr,
                spread_ctx.get("book_status", "UNKNOWN"),
                self._decision_fmt(spread_ctx, "book_age_sec", ".2f"),
                self._decision_fmt(spread_ctx, "max_book_age_sec", ".2f"),
                self._decision_fmt(spread_ctx, "spread_bps", ".2f"),
                self._decision_fmt(spread_ctx, "spread_atr", ".3f"),
            )
            self._entry_engine.mark_signal_deferred(
                side, "execution_spread_or_book_block",
                cooldown_sec=float(getattr(config, "EXECUTION_BOOK_SIGNAL_DEFER_COOLDOWN_SEC", 5.0) or 5.0),
            )
            return
        bal_info = risk_manager.get_available_balance()
        total_bal = float((bal_info or {}).get("total", (bal_info or {}).get("available", 0.0)) or 0.0)
        allowed, reason = self._risk_gate.can_trade(total_bal)
        if not allowed:
            self._entry_engine.mark_signal_deferred(side, "account_risk_lock", cooldown_sec=30.0)
            logger.info("ICT_LIQUIDITY account control rejected setup: %s", reason)
            return
        self._force_sl, self._force_tp, self._last_entry_signal = sl, tp, signal
        self._active_auction_risk_scalar = max(0.05, min(1.0, float((getattr(signal, "quality", {}) or {}).get("auction_risk_scalar", 1.0) or 1.0)))
        self._active_auction_posture = str((getattr(signal, "quality", {}) or {}).get("execution_posture", "NORMAL") or "NORMAL")
        self._active_market_phase = str((getattr(signal, "quality", {}) or {}).get("market_phase", "UNCLASSIFIED") or "UNCLASSIFIED")
        sig = StructuralEntrySummary()
        sig.atr = atr
        sig.delivery_score = float(getattr(signal, "delivery_score", 0.0) or 0.0)
        sig.probability_calibrated = bool(getattr(signal, "probability_calibrated", False))
        sig.delivery_probability = (float(signal.delivery_probability) if sig.probability_calibrated and signal.delivery_probability is not None else None)
        sig.archetype = str(getattr(signal, "archetype", "") or getattr(signal.entry_type, "value", "STRUCTURAL_AUCTION"))
        sig.structural_validation = signal.structural_validation
        unit = self._analysis_unit()
        logger.info(
            "✅ INSTITUTIONAL_ORDER_THESIS archetype=%s domain=%s %s entry=%s%.4f SL=%s%.4f TP=%s%.4f | risk=%.4f reward=%.4f grossRR=%.2f floor=%.2f "
            "estNetWinR=%s deliveryScore=%s calibratedP=%s target=%s@%.4f | account_balance=%s%.2f execution_conversion=%s evalSource=%s eventDelayMs=%.2f | %s",
            str(getattr(signal, "archetype", "") or getattr(signal.entry_type, "value", "STRUCTURAL")),
            "UNDERLYING" if unit == "NIFTYpts" else "EXECUTION_INSTRUMENT", side.upper(), unit, entry, unit, sl, unit, tp,
            abs(entry-sl), abs(tp-entry), rr, signal_rr_floor,
            f"{float(info.get('target_net_win_r')):.2f}" if info.get('target_net_win_r') is not None else "PREMIUM_PENDING" if unit == "NIFTYpts" else "N/A",
            f"{float(info.get('delivery_score')):.2f}" if info.get('delivery_score') is not None else "N/A",
            "Y" if bool(info.get("probability_calibrated")) else "N/A",
            str(info.get("target_timeframe","-")), self._decision_num(info,"target_pool_price"),
            str(self._position_accounting_context().get("currency_symbol", "$")), total_bal,
            "OPTION_PREMIUM_PENDING" if unit == "NIFTYpts" else "DIRECT", getattr(self, "_last_eval_source", "POLL"), float(getattr(self, "_last_event_eval_delay_ms", 0.0) or 0.0), signal.reason,
        )
        self._entry_engine.on_entry_placed(signal)
        self._launch_entry_async(data_manager, order_manager, risk_manager, side, sig, mode="institutional_auction", setup_grade="STRUCTURAL", prefetched_bal_info=bal_info, entry_now=now)

    @staticmethod
    def _clamp_ladder_value(value: float, lo: float, hi: float) -> float:
        """Local numeric clamp for TP-ladder path math.

        Kept local to QuantStrategy so the ladder-build path never depends on
        private helpers from selector modules. A failure here must never degrade
        the strategy to final-TP-only again.
        """
        try:
            v = float(value)
        except Exception:
            v = float(lo)
        if not math.isfinite(v):
            v = float(lo)
        return max(float(lo), min(float(hi), v))

    def _build_tp_ladder_plan(self, side: str, entry_price: float, sl_price: float,
                              final_tp: float, quantity: float, atr: float,
                              use_maker_entry: bool = True):
        """Build TP1..TPn from internal liquidity; final TP remains unchanged."""
        if build_tp_ladder is None:
            return None
        try:
            pool_report = None
            if getattr(self, "_entry_engine", None) is not None:
                try:
                    pool_report = self._entry_engine.pool_plan_info
                except Exception:
                    pool_report = None
            _qty = max(float(quantity or 0.0), 0.0)
            _min_qty = max(float(QCfg.MIN_QTY() or 0.0), 0.0)
            _min_leg_fraction = (_min_qty / _qty) if (_qty > 0 and _min_qty > 0) else 0.0

            # Internal TP count is not a fixed setting.  It is constrained by
            # (a) executable lot capacity and (b) information capacity of the
            # entry→final path.  A 7–8ATR objective does not justify ten tiny
            # orders if the path contains only a few independent auction zones.
            _lot_capacity = max(0, int(_qty // _min_qty) - 1) if (_qty > 0 and _min_qty > 0) else 0
            _final_dist_atr = abs(float(final_tp) - float(entry_price)) / max(float(atr or 0.0), 1e-9)
            _risk_atr = abs(float(entry_price) - float(sl_price)) / max(float(atr or 0.0), 1e-9)
            _rr = abs(float(final_tp) - float(entry_price)) / max(abs(float(entry_price) - float(sl_price)), 1e-9)
            _path_information = max(0.0, _final_dist_atr) * (0.70 + 0.30 * min(1.0, _rr / 4.0))
            _info_capacity = max(1, int(math.ceil(math.sqrt(max(_path_information, 1.0))))) if _final_dist_atr >= 1.25 else 0
            # Long-run execution hardening: the planner must respect executable
            # lot capacity before it creates/logs internal TP legs.  If total qty
            # can only support the native final runner, do not build a theoretical
            # ladder and leave placement to reject it later.
            if _qty > 0 and _min_qty > 0:
                _max_internal_legs = min(_lot_capacity, _info_capacity) if _lot_capacity > 0 else 0
            else:
                _max_internal_legs = _info_capacity
            _target_spacing_atr = (_final_dist_atr / max(_max_internal_legs + 1, 1)) if _max_internal_legs > 0 else _final_dist_atr
            _min_spacing_atr = self._clamp_ladder_value(max(0.35, 0.58 * _target_spacing_atr, 0.55 * _risk_atr), 0.35, 1.35)
            _rt_cost_bps = 0.0
            try:
                _rt_cost_pts, _rt_cost_bps = self._roundtrip_cost_points(float(entry_price), bool(use_maker_entry))
            except Exception:
                _rt_cost_bps = 0.0
            _fee_floor_mult = 1.20
            try:
                _fee_floor_mult = float(getattr(config, "FEE_FLOOR_ABS_MIN_MULT", 1.20) or 1.20)
            except Exception:
                _fee_floor_mult = 1.20
            plan = build_tp_ladder(
                side=side,
                entry=float(entry_price),
                sl=float(sl_price),
                final_tp=float(final_tp),
                atr=float(atr or 0.0),
                total_quantity=_qty,
                pool_report=pool_report,
                min_leg_fraction=_min_leg_fraction,
                min_spacing_atr=_min_spacing_atr,
                max_internal_legs=_max_internal_legs,
                roundtrip_cost_bps=_rt_cost_bps,
                fee_floor_mult=_fee_floor_mult,
            )
            if plan is not None and getattr(plan, "legs", None):
                logger.info(
                    "🎯 TP_LADDER PLAN [%s] %s | notes=%s",
                    side.upper(), plan.compact(), "; ".join(getattr(plan, "regime_notes", []) or []) or "none",
                )
            return plan
        except Exception as _e:
            logger.error("TP ladder build failed — falling back to final TP only: %s", _e, exc_info=True)
            return None

    def _floor_to_step(self, qty: float, step: float) -> float:
        try:
            qty = float(qty or 0.0)
            step = float(step or 0.0)
            if qty <= 0:
                return 0.0
            if step <= 0:
                return qty
            return math.floor((qty + 1e-12) / step) * step
        except Exception:
            return 0.0

    def _prepare_executable_tp_ladder_legs(self, internal_legs, total_qty: float, ladder_plan=None):
        """Quantise internal TP quantities before placing exchange orders.

        This prevents the exchange/UI from rounding each tiny fractional leg up
        until all lots are offloaded before the selected final TP.  The final
        runner reserve is respected first; only the remaining executable lot
        budget is distributed across internal legs by their planned weights.
        """
        total_qty = max(float(total_qty or 0.0), 0.0)
        if total_qty <= 0 or not internal_legs:
            return []
        step = max(float(QCfg.LOT_STEP() or 0.0), 0.0)
        min_qty = max(float(QCfg.MIN_QTY() or 0.0), step, 0.0)
        if step <= 0:
            step = min_qty if min_qty > 0 else 0.0
        # Use the actual final leg in the plan as the final-runner reserve.  If
        # it is below executable size, reserve at least one executable lot.
        final_qty_planned = 0.0
        try:
            for l in getattr(ladder_plan, "legs", []) or []:
                if str(getattr(l, "role", "")).upper() == "FINAL":
                    final_qty_planned = max(final_qty_planned, float(getattr(l, "quantity", 0.0) or 0.0))
        except Exception:
            final_qty_planned = 0.0
        reserve = max(final_qty_planned, min_qty if total_qty >= 2.0 * min_qty else 0.0)
        if step > 0:
            reserve = math.ceil(reserve / step - 1e-12) * step
        reserve = min(max(reserve, 0.0), total_qty)
        max_internal_qty = max(0.0, total_qty - reserve)
        if max_internal_qty < min_qty:
            logger.info(
                "TP_LADDER final reserve %.8g leaves no executable internal TP budget; native final TP remains live",
                reserve,
            )
            return []
        raw = [max(0.0, float(getattr(l, "quantity", 0.0) or 0.0)) for l in internal_legs]
        raw_sum = sum(raw)
        if raw_sum <= 0:
            return []
        scale = min(1.0, max_internal_qty / raw_sum)
        targets = [q * scale for q in raw]
        floored = [self._floor_to_step(q, step) if step > 0 else q for q in targets]
        # Drop non-executable dust and redistribute the remaining capacity to
        # legs with the largest useful remainder.  Never round up beyond the
        # total internal budget reserved by the final-runner model.
        floored = [q if q >= min_qty else 0.0 for q in floored]
        used = sum(floored)
        capacity = self._floor_to_step(max_internal_qty - used, step) if step > 0 else max_internal_qty - used
        remainders = []
        for i, (leg, target, base) in enumerate(zip(internal_legs, targets, floored)):
            if target < min_qty:
                continue
            remainders.append((target - base, -float(getattr(leg, "distance_atr", 0.0) or 0.0), i))
        remainders.sort(reverse=True)
        for _, __, i in remainders:
            if capacity + 1e-12 < min_qty:
                break
            add = min(step if step > 0 else capacity, capacity)
            if floored[i] + add <= targets[i] + (step if step > 0 else add) + 1e-12:
                floored[i] += add
                capacity -= add
        out = []
        for leg, q in zip(internal_legs, floored):
            if q >= min_qty:
                out.append((leg, q))
            else:
                logger.info(
                    "TP_LADDER skip %s non-executable qty %.8f after final-runner reserve; qty remains for final TP",
                    getattr(leg, "role", "TP"), q,
                )
        planned_internal = sum(raw)
        executable_internal = sum(q for _, q in out)
        if executable_internal + reserve > total_qty + max(step, 1e-12):
            logger.warning(
                "TP_LADDER quantisation guard: internal %.8g + final reserve %.8g exceeds total %.8g; trimming internals",
                executable_internal, reserve, total_qty,
            )
        if abs(executable_internal - planned_internal) > max(min_qty, total_qty * 0.01):
            logger.info(
                "TP_LADDER executable allocation: planned_internal=%.8g executable_internal=%.8g final_reserved=%.8g total=%.8g step=%.8g min=%.8g",
                planned_internal, executable_internal, reserve, total_qty, step, min_qty,
            )
        return out

    def _place_internal_tp_ladder(self, order_manager, side: str, quantity: float,
                                  final_tp: float, native_final_tp_order_id: str,
                                  ladder_plan) -> tuple:
        """
        Place reduce-only internal TP1..TPn orders while leaving the native
        bracket final TP/SL untouched. The SL price is never moved.

        The native bracket TP remains the final/terminal target. Internal legs
        are standalone reduce-only take-profit orders. If Delta rejects them,
        the bot keeps the original bracket as fallback and logs ladder_degraded.
        """
        if ladder_plan is None or not getattr(ladder_plan, "legs", None):
            return [], []
        exit_side = "sell" if side == "long" else "buy"
        placed_ids = []
        leg_dicts = []
        # Place only internal legs. FINAL is already represented by native bracket TP.
        internal_legs = [l for l in ladder_plan.legs if str(getattr(l, "role", "")).upper() != "FINAL"]
        if not internal_legs:
            leg_dicts = [l.as_dict() for l in ladder_plan.legs]
            return leg_dicts, placed_ids
        executable_legs = self._prepare_executable_tp_ladder_legs(internal_legs, float(quantity or 0.0), ladder_plan)
        if not executable_legs:
            leg_dicts = [l.as_dict() for l in ladder_plan.legs]
            return leg_dicts, placed_ids
        for leg, leg_qty in executable_legs:
            try:
                leg_px = float(getattr(leg, "price", 0.0) or 0.0)
                if leg_qty <= 0 or leg_px <= 0:
                    continue
                res = order_manager.place_take_profit(
                    side=exit_side,
                    quantity=leg_qty,
                    trigger_price=leg_px,
                )
                if res and not (isinstance(res, dict) and res.get("_error")):
                    oid = str(res.get("order_id") or res.get("id") or "")
                    if oid:
                        leg.order_id = oid
                        leg.quantity = leg_qty
                        try:
                            leg.qty_fraction = leg_qty / max(float(quantity or 0.0), 1e-12)
                        except Exception:
                            pass
                        leg.placed = True
                        placed_ids.append(oid)
                        logger.info(
                            "✅ TP_LADDER %s placed reduce-only %s qty=%.8g trigger=%.2f oid=%s",
                            getattr(leg, "role", "TP"), exit_side.upper(), leg_qty, leg_px, oid[:10])
                else:
                    logger.warning(
                        "TP_LADDER %s placement failed; native final TP remains live. raw=%s",
                        getattr(leg, "role", "TP"), res)
            except Exception as _e:
                logger.error("TP_LADDER leg placement error — continuing with native final TP: %s", _e, exc_info=True)
        leg_dicts = [l.as_dict() for l in ladder_plan.legs]
        return leg_dicts, placed_ids

    def _cancel_tp_ladder_orders(self, order_manager, pos=None) -> None:
        """Cancel standalone internal TP ladder orders; native bracket cancel is handled separately."""
        p = pos or self._pos
        ids = list(getattr(p, "tp_ladder_order_ids", []) or [])
        if not ids:
            return
        if order_manager is None:
            logger.warning("TP_LADDER cleanup requested but order_manager unavailable; ids=%s", ids)
            return
        for oid in ids:
            try:
                if oid and oid != getattr(p, "tp_order_id", ""):
                    result = order_manager.cancel_order(oid)
                    logger.info("TP_LADDER cancel %s: %s", str(oid)[:10], getattr(result, "value", result))
            except Exception as _e:
                logger.warning("TP_LADDER cancel error %s: %s", str(oid)[:10], _e)

    def _cleanup_tp_ladder_after_position_flat(self, order_manager=None, pos=None, reason: str = "exit") -> None:
        """Cancel all standalone internal TP ladder orders once the position is flat.

        Delta native bracket normally handles the original final TP/SL pair, but
        TP1..TPn are independent reduce-only orders.  When the original SL fires
        or any other full-position exit makes the exchange flat, those standalone
        internal TP orders must be cancelled immediately.  Otherwise stale
        reduce-only conditionals remain on Delta, clutter the book/UI, and can
        confuse later adoption/reconciliation logic.
        """
        p = pos or self._pos
        ids = [str(x) for x in (getattr(p, "tp_ladder_order_ids", []) or []) if str(x)]
        if not ids:
            return
        om = order_manager or getattr(self, "_om", None)
        logger.info(
            "TP_LADDER flat-cleanup [%s]: cancelling %d standalone internal TP orders; "
            "native final TP/SL bracket is exchange-managed",
            reason, len(ids))
        self._cancel_tp_ladder_orders(om, p)
        try:
            for leg in list(getattr(p, "tp_ladder", []) or []):
                if isinstance(leg, dict) and str(leg.get("role", "")).upper() != "FINAL":
                    leg["cancelled_after_flat"] = True
                    leg["cancel_reason"] = reason
        except Exception:
            pass
        try:
            p.tp_ladder_active = False
            p.tp_ladder_order_ids = []
        except Exception:
            pass

    def _compute_leg_net_pnl(self, pos, exit_price: float, qty: float,
                             exit_fee: float = 0.0) -> tuple:
        """Return (net, gross, entry_fee_alloc, exit_fee) for a partial exit."""
        try:
            _is_inverse = str(getattr(pos, "pnl_model", "linear") or "linear").lower() == "inverse_btcusd"
            gross = gross_pnl_usd(
                pos.side,
                float(pos.entry_price or 0.0),
                float(exit_price or 0.0),
                float(qty or 0.0),
                inverse=bool(_is_inverse and exit_price > 0),
            )
            initial_qty = float(getattr(pos, "tp_ladder_initial_qty", 0.0) or getattr(pos, "quantity", 0.0) or 0.0)
            entry_fee_total = float(getattr(pos, "entry_fee_paid", 0.0) or 0.0)
            entry_fee_exact = bool(getattr(pos, "entry_fee_exact", False) or abs(entry_fee_total) > 1e-12)
            entry_fee_alloc = entry_fee_total * (float(qty or 0.0) / max(initial_qty, 1e-9)) if entry_fee_exact else 0.0
            net = gross - entry_fee_alloc - float(exit_fee or 0.0)
            return net, gross, entry_fee_alloc, float(exit_fee or 0.0)
        except Exception:
            gross = ((float(exit_price or 0.0) - pos.entry_price) if pos.side == "long" else (pos.entry_price - float(exit_price or 0.0))) * float(qty or 0.0)
            return gross - float(exit_fee or 0.0), gross, 0.0, float(exit_fee or 0.0)

    def _book_tp_ladder_partials(self, order_manager, closed_qty: float) -> None:
        """Book realised PnL for internal TP ladder fills.

        Delta reduce-only TP legs are separate orders from the native bracket.
        If we only shrink local position size, lifecycle PnL later ignores TP1..TPn.
        This method records each filled internal leg only when the exchange
        order/fill record has propagated. If Delta has not exposed the exact
        fill and commission yet, booking is deferred — no planned-price or
        synthetic-fee PnL is fabricated.
        """
        pos = self._pos
        if closed_qty <= 0 or not getattr(pos, "tp_ladder", None):
            return
        recorded = set(str(x) for x in (getattr(pos, "tp_ladder_recorded_order_ids", []) or []))
        remaining_to_match = float(closed_qty or 0.0)
        booked_any = False
        for leg in list(getattr(pos, "tp_ladder", []) or []):
            try:
                role = str(leg.get("role", "") or "").upper()
                if role == "FINAL":
                    continue
                oid = str(leg.get("order_id", "") or "").strip()
                if oid and oid in recorded:
                    continue
                planned_qty = float(leg.get("quantity", 0.0) or 0.0)
                planned_px = float(leg.get("price", 0.0) or 0.0)
                if planned_qty <= 0 or planned_px <= 0:
                    continue
                fill_px = 0.0; fill_qty = 0.0; fee_paid = 0.0; confirmed = False; fee_exact = False; status = ""
                if oid and order_manager is not None and hasattr(order_manager, "get_fill_details"):
                    try:
                        details = order_manager.get_fill_details(oid) or {}
                        status = str(details.get("status", "") or "").upper()
                        fill_px = float(details.get("fill_price", 0.0) or 0.0)
                        fill_qty = float(details.get("filled_qty", 0.0) or 0.0)
                        fee_paid = float(details.get("paid_commission", 0.0) or 0.0)
                        fee_exact = bool(details.get("paid_commission_exact", False))
                        confirmed = status in ("FILLED", "CLOSED", "PARTIAL_FILL") and fill_px > 0 and fill_qty > 0 and fee_exact
                    except Exception as _fd_e:
                        logger.debug("TP_LADDER fill detail lookup failed for %s: %s", oid[:10], _fd_e)
                if not confirmed:
                    # No synthetic TP-ladder PnL. Delta exposes exact order/fill
                    # fees; if the fill has not propagated yet, defer booking
                    # rather than using the planned TP price or a zero-fee guess.
                    logger.info(
                        "TP_LADDER fill %s not exact yet — deferring realised PnL booking",
                        oid[:10] if oid else role or "TP")
                    continue

                # Delta fill_qty and paid_commission are cumulative for the order.
                # Book ONLY the newly-filled delta so repeated reconcile ticks and
                # true partial-fill updates cannot double-count or under-count P&L.
                booked_qty_by_order = getattr(pos, "tp_ladder_recorded_fill_qty_by_order", {}) or {}
                booked_fee_by_order = getattr(pos, "tp_ladder_recorded_exit_fee_by_order", {}) or {}
                prev_qty = float(booked_qty_by_order.get(oid, 0.0) or 0.0) if oid else 0.0
                prev_fee = float(booked_fee_by_order.get(oid, 0.0) or 0.0) if oid else 0.0
                cumulative_qty = min(max(fill_qty, 0.0), planned_qty)
                delta_qty = max(0.0, cumulative_qty - prev_qty)
                delta_exit_fee = fee_paid - prev_fee
                if delta_qty <= max(QCfg.MIN_QTY() * 0.001, 1e-12):
                    logger.debug(
                        "TP_LADDER fill %s already booked to qty %.8g fee %.6f; status=%s",
                        oid[:10] if oid else role or "TP", prev_qty, prev_fee, status)
                    continue

                net, gross, entry_fee_alloc, exit_fee = self._compute_leg_net_pnl(pos, fill_px, delta_qty, delta_exit_fee)
                with self._lock:
                    pos.tp_ladder_realized_pnl += net
                    pos.tp_ladder_realized_gross += gross
                    pos.tp_ladder_realized_fees += (entry_fee_alloc + exit_fee)
                    pos.tp_ladder_realized_entry_fees += entry_fee_alloc
                    pos.tp_ladder_realized_exit_fees += exit_fee
                    if oid:
                        pos.tp_ladder_recorded_fill_qty_by_order[oid] = cumulative_qty
                        pos.tp_ladder_recorded_exit_fee_by_order[oid] = fee_paid
                        if status in ("FILLED", "CLOSED") or cumulative_qty >= planned_qty - max(QCfg.MIN_QTY() * 0.001, 1e-12):
                            if oid not in pos.tp_ladder_recorded_order_ids:
                                pos.tp_ladder_recorded_order_ids.append(oid)
                            recorded.add(oid)
                booked_any = True
                remaining_to_match = max(0.0, remaining_to_match - delta_qty)
                logger.info(
                    "🎯 TP_LADDER realised %s qty=%.8g fill=%.2f gross=%+.4f fees=%.4f net=%+.4f cumulative_net=%+.4f exact status=%s",
                    role or "TP", delta_qty, fill_px, gross, entry_fee_alloc + exit_fee, net,
                    float(getattr(pos, "tp_ladder_realized_pnl", 0.0) or 0.0), status)
                try:
                    self._send_telegram(
                        format_partial_exit_alert(
                            side=pos.side,
                            role=role or "TP",
                            fill_price=fill_px,
                            qty_closed=delta_qty,
                            qty_remaining=max(float(getattr(pos, "quantity", 0.0) or 0.0) - delta_qty, 0.0),
                            gross=gross,
                            fees=entry_fee_alloc + exit_fee,
                            net=net,
                            cumulative_net=float(getattr(pos, "tp_ladder_realized_pnl", 0.0) or 0.0),
                            sl=float(getattr(pos, "sl_price", 0.0) or 0.0),
                            final_tp=float(getattr(pos, "tp_price", 0.0) or 0.0),
                            exact_fees=True,
                            status=status,
                        ),
                        event_type="tp_ladder",
                    )
                except Exception:
                    pass
                if remaining_to_match <= max(QCfg.MIN_QTY() * 0.25, 1e-12):
                    break
            except Exception as _e:
                logger.debug("TP_LADDER partial booking leg error: %s", _e)
        if not booked_any:
            logger.warning("TP_LADDER partial quantity changed but no internal TP leg could be booked; closed_qty=%.8g", closed_qty)

    def _reconcile_tp_ladder_quantity(self, order_manager, ex_size: float) -> None:
        """
        After TP1/TP2 fills, exchange position size drops while SL price must stay
        fixed. We update local size and ladder state only. We do not trail or
        migrate SL to breakeven.
        """
        try:
            if self._pos.phase != PositionPhase.ACTIVE:
                return
            old_qty = float(self._pos.quantity or 0.0)
            new_qty = abs(float(ex_size or 0.0))
            if old_qty <= 0:
                return
            min_delta = max(QCfg.MIN_QTY(), old_qty * 0.005)
            if new_qty < old_qty - min_delta:
                closed = old_qty - new_qty
                self._book_tp_ladder_partials(order_manager, closed)
                with self._lock:
                    self._pos.quantity = new_qty
                    self._pos.tp_ladder_last_sync_qty = new_qty
                logger.info(
                    "🎯 TP_LADDER partial exit detected: qty %.8g → %.8g (closed %.8g). "
                    "SL price remains fixed at %.2f; fixed-SL policy; TP ladder manages monetisation.",
                    old_qty, new_qty, closed, float(self._pos.sl_price or 0.0))
                # The fill-booking path sends the user-facing TP ladder card once
                # exact Delta fill + fee details are available. Keep this reconcile
                # log local only to avoid plain duplicate Telegram messages.
        except Exception as _e:
            logger.debug("TP ladder quantity reconcile error: %s", _e)


    def _position_accounting_context(self) -> Dict[str, str]:
        """Instrument-scoped settlement metadata for every simultaneous desk."""
        inst = getattr(self, "_instrument", None) or current_instrument()
        ex = _icici_exchange_name(inst)
        symbol = str(getattr(inst, "execution_symbol", "") or getattr(inst, "display_symbol", "") or QCfg.SYMBOL())
        is_inr = ex == "icici"
        if is_inr:
            selected = _icici_selected_contract_dict(inst)
            symbol = str(
                selected.get("selected_symbol")
                or selected.get("TradingSymbol")
                or selected.get("trading_symbol")
                or selected.get("symbol")
                or symbol
            )
        asset = str(getattr(inst, "asset_id", "") or getattr(self, "_asset_id", "") or symbol).upper()
        pnl_model = "inverse_btcusd" if ex == "delta" and symbol.upper() == "BTCUSD" else "linear"
        return {
            "exchange": ex,
            "execution_symbol": symbol,
            "asset_id": asset,
            "currency_code": "INR" if is_inr else "USD",
            "currency_symbol": "₹" if is_inr else "$",
            "pnl_model": pnl_model,
            "quantity_unit": "contracts" if is_inr else (asset or "units"),
        }

    def _enter_trade(self, data_manager, order_manager, risk_manager, side, sig, mode="ict_liquidity",
                     setup_grade: str = "", prefetched_bal_info: dict = None,
                     entry_now: float = 0.0):
        """Execute an approved institutional auction thesis with attached protection.

        The entry authority has already fixed side, invalidation and delivery target.
        This method performs only derivative routing, fee/cost feasibility, structural-risk
        sizing, venue leverage constraints, bracket placement and fill adoption.
        """
        self._last_execution_viability = None
        _icici_chain_mode = bool(getattr(config, "ICICI_LONG_PREMIUM_ONLY", True)) and _is_icici_underlying_chain_instrument(self._instrument)
        _icici_option_mode = bool(getattr(config, "ICICI_LONG_PREMIUM_ONLY", True)) and _is_icici_option_instrument(self._instrument)
        _icici_mode = bool(_icici_chain_mode or _icici_option_mode)
        _icici_thesis_side = str(side or "").lower()
        _icici_underlying_entry = float(getattr(self._last_entry_signal, "entry_price", 0.0) or 0.0)
        _icici_selected_choice = None
        _accounting = self._position_accounting_context()
        _entry_cur = str(_accounting.get("currency_symbol", "$"))
        _icici_underlying_atr = 0.0

        def _release_icici_vehicle_if_unfilled(rejection: str) -> None:
            # Contract activation switches the primary feed from underlying-mode
            # to option-premium execution-mode.  If the entry is rejected before
            # any fill, restore underlying-mode immediately; otherwise later scans
            # could evaluate NIFTY structure against an option premium.
            if _icici_selected_choice is None:
                return
            releaser = getattr(data_manager, "release_icici_execution_vehicle", None)
            if callable(releaser):
                try:
                    releaser()
                    logger.info("ICICI preselected vehicle released before fill: %s", rejection)
                except Exception as exc:
                    logger.error("ICICI pre-fill vehicle release failed [%s]: %s", rejection, exc)
        if _icici_mode:
            _session_open, _session_reason = _icici_market_session_open()
            if not _session_open:
                logger.info("ICICI options entry skipped: %s", _session_reason)
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return
            if _icici_option_mode and not _icici_chain_mode and not _icici_allowed_thesis_side(self._instrument, _icici_thesis_side):
                logger.info(
                    "ICICI options entry skipped: selected %s does not match %s underlying thesis",
                    _icici_option_right(self._instrument) or "unknown",
                    _icici_thesis_side or "unknown",
                )
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return
        price = data_manager.get_last_price()
        _min_price = max(QCfg.TICK_SIZE(), 1e-6) if _icici_mode else 1.0
        if price < _min_price: return
        atr = self._atr_5m.atr
        if atr < 1e-10: return
        if _icici_mode:
            _icici_underlying_atr = float(atr)

        # ── Risk gate ─────────────────────────────────────────────────────────────
        # Bug #5 fix: reuse prefetched balance when available; only call the REST
        # endpoint as a fallback (e.g. when _enter_trade is invoked outside of the
        # normal _evaluate_entry → _launch_entry_async path).
        if prefetched_bal_info is not None:
            bal_info = prefetched_bal_info
        else:
            bal_info = risk_manager.get_available_balance()
        if bal_info is None: return
        total_bal = float(bal_info.get("total", bal_info.get("available", 0.0)))
        self._risk_gate.set_opening_balance(total_bal)
        if _icici_chain_mode:
            available_funds = float(bal_info.get("available_raw", bal_info.get("available", 0.0)) or 0.0)
            if _icici_underlying_entry <= 0.0:
                _icici_underlying_entry = float(data_manager.get_last_price() or 0.0)
            _icici_selected_choice = _icici_select_contract_for_thesis(
                self._instrument,
                data_manager,
                _icici_thesis_side,
                underlying_spot=_icici_underlying_entry,
                available_funds=available_funds,
            )
            if _icici_selected_choice is None:
                logger.info(
                    "ICICI options entry rejected: no affordable %s contract fit live F&O funds %.2f",
                    "call" if _icici_thesis_side == "long" else "put" if _icici_thesis_side == "short" else "option",
                    available_funds,
                )
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return
            _verified_icici_lot = _icici_runtime_lot_size(self._instrument)
            if _verified_icici_lot <= 0:
                logger.critical(
                    "ICICI options entry rejected: selected contract has no verified NFO lot size; refusing unsafe sizing/routing")
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return
            side = "long"
            price = _icici_selected_premium(self._instrument, fallback=0.0)
            if price <= max(QCfg.TICK_SIZE(), 1e-6):
                try:
                    rows = list(data_manager.get_execution_candles("1m", 3) or [])
                    for candle in reversed(rows):
                        px = _icici_float(candle.get("c", candle.get("close")), 0.0)
                        if px > max(QCfg.TICK_SIZE(), 1e-6):
                            price = px
                            break
                except Exception:
                    pass
            if price <= max(QCfg.TICK_SIZE(), 1e-6):
                logger.info("ICICI options entry rejected: selected option premium is unavailable after contract selection")
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return
            exec_atr = _icici_execution_atr(data_manager, entry_premium=price)
            if exec_atr > 1e-10:
                atr = exec_atr
            # Contract activation changes the executable instrument identity from
            # NIFTY underlying to the selected CE/PE. Ledger, P&L and reporting
            # metadata must be bound to that exact NFO option before an order is sent.
            _accounting = self._position_accounting_context()
            _entry_cur = str(_accounting.get("currency_symbol", "₹"))
            logger.info(
                "ICICI preselected session execution vehicle activated: %s %s strike=%s expiry=%s live_premium=%.2f lot=%.0f live_cost=%.2f funds=%.2f",
                getattr(_icici_selected_choice, "right", ""),
                getattr(_icici_selected_choice, "selected_symbol", ""),
                getattr(_icici_selected_choice, "strike", 0.0),
                getattr(_icici_selected_choice, "expiry", ""),
                price,
                _icici_runtime_lot_size(self._instrument),
                _icici_runtime_lot_size(self._instrument) * price,
                available_funds,
            )
        elif _icici_option_mode:
            side = "long"
        # NOTE: risk gate already checked in _evaluate_entry — no duplicate check here

        # ── Statistical calibration boundary ───────────────────────────────────
        # Structural delivery_score is execution evidence, not a win probability.
        # Only replay-calibrated probabilities may affect utility or risk allocation.
        delivery_probability = self._calibrated_signal_probability()

        # ── Limit price: prefer FVG rebalance signal price, fall back to live book ──
        # A valid FVG rebalance price is routed as a maker LIMIT. When that
        # structural repricing level has become stale by more than two ATR,
        # routing falls back to executable book pricing without changing SL/TP geometry.
        use_maker = True   # valid FVG rebalance entries post as maker limits
        tick      = QCfg.TICK_SIZE()
        offset    = float(getattr(config, 'LIMIT_ORDER_OFFSET_TICKS', 3)) * tick

        _sig_entry = getattr(self._last_entry_signal, 'entry_price', 0.0) or 0.0
        if _icici_mode:
            # The signal entry is the NIFTY underlying FVG rebalance level. ICICI routing
            # must price the selected option premium, not send an index level as
            # an option limit.
            _sig_entry = 0.0
        _stale_threshold = 2.0 * atr if atr > 1e-10 else float('inf')
        _sig_is_valid = (
            _sig_entry > 0
            and abs(_sig_entry - price) <= _stale_threshold
        )

        if _sig_is_valid:
            limit_px  = _round_to_tick(_sig_entry)
            mt_reason = f"limit_{side}_ote={limit_px:.1f} (signal.entry_price)"
        else:
            # Fallback: live orderbook offset (original logic)
            try:
                orderbook = data_manager.get_orderbook()
                bids = (orderbook or {}).get("bids", [])
                asks = (orderbook or {}).get("asks", [])
                if bids and asks:
                    def _best_px(lvl):
                        if isinstance(lvl,(list,tuple)): return float(lvl[0])
                        if isinstance(lvl,dict): return float(lvl.get("limit_price") or lvl.get("price") or 0)
                        return 0.0
                    if side == "long":
                        limit_px  = round(_best_px(bids[0]) - offset, 1)
                        mt_reason = f"limit_long@bid-{offset:.1f}={limit_px:.1f} (book fallback)"
                    else:
                        limit_px  = round(_best_px(asks[0]) + offset, 1)
                        mt_reason = f"limit_short@ask+{offset:.1f}={limit_px:.1f} (book fallback)"
                else:
                    raise ValueError("empty book")
            except Exception:
                if side == "long":
                    limit_px = round(price - offset, 1)
                else:
                    limit_px = round(price + offset, 1)
                mt_reason = f"limit_{side}_offset={offset:.1f}pts (no book, no structural repricing)"

        if _sig_entry > 0 and not _sig_is_valid:
            logger.warning(
                f"_enter_trade: signal.entry_price={_entry_cur}{_sig_entry:,.1f} is "
                f"{abs(_sig_entry - price):.0f}pts from market (>{_stale_threshold:.0f}pts=2ATR) "
                f"— falling back to live book. Signal may be from a prior tick."
            )

        if _icici_mode:
            limit_px = _round_to_tick(max(price, QCfg.TICK_SIZE()))
            mt_reason = f"icici_option_limit_premium={limit_px:.2f}"
            use_maker = True

        # Keep fee engine updated for diagnostics and TP gate
        if self._fee_engine is not None:
            try:
                ob = data_manager.get_orderbook()
                if ob:
                    self._fee_engine.update_orderbook(ob, price)
            except Exception:
                pass

        # Bug #34 fix: for book-offset entries, query the fee engine
        # to decide maker vs taker.  structural repricing entries always remain maker
        # (they're limit orders by construction).
        if not _icici_mode and not _sig_is_valid and self._fee_engine is not None and self._fee_engine.is_warmed_up():
            try:
                # Queue urgency is neutral until a replay-calibrated probability exists.
                # A structural evidence score is not allowed to alter routing cost assumptions.
                _urgency = (0.50 if delivery_probability is None else 1.0 - min(1.0, delivery_probability))
                _fe_maker, _fe_lim, _fe_reason = self._fee_engine.decide_entry_type(
                    side=side, quantity=1.0,   # qty not yet known; use 1.0 for fill-prob estimate
                    price=price,
                    orderbook=data_manager.get_orderbook() or {},
                    signal_urgency=_urgency,
                )
                if not _fe_maker:
                    use_maker = False
                    logger.debug(f"FeeEngine: taker entry selected — {_fe_reason}")
            except Exception as _fe_err:
                logger.debug(f"FeeEngine.decide_entry_type error (non-fatal): {_fe_err}")

        if _icici_mode and limit_px > 0:
            limit_px = _round_to_tick(max(limit_px, QCfg.TICK_SIZE()))
        entry_ref = limit_px if limit_px > 0 else price
        logger.info(f"Entry routing: {'LIMIT/maker' if use_maker else 'MARKET/taker'} | {mt_reason}")

        # ── Structural order sequence: bind SL/TP before sizing ────────────────────────────────
        # SL/TP computation does not depend on position size — it uses price, ATR
        # and approved structural geometry. Optional calibrated odds affect cost
        # conservatism only. Computing it first passes the
        # actual structural invalidation distance (not an ATR proxy) into position sizing, which is the
        # correct industry-grade approach: risk-in-dollars / SL-distance = quantity.

        # -- ICT_LIQUIDITY: Use force SL/TP from entry engine if available --
        # The sole entry authority supplies structural invalidation and delivery targets;
        # execution cannot overwrite this geometry.
        _force_sl = getattr(self, '_force_sl', None)
        _force_tp = getattr(self, '_force_tp', None)
        _analysis_sl_level = float(_force_sl or 0.0) if _icici_mode else 0.0
        _analysis_tp_level = float(_force_tp or 0.0) if _icici_mode else 0.0
        _using_force_levels = False
        if _force_sl is not None and _force_tp is not None and _force_sl > 0 and _force_tp > 0:
            if _icici_mode:
                _conv_sl, _conv_tp, _conv_reason = _icici_option_premium_levels(
                    thesis_side=_icici_thesis_side,
                    premium_entry=entry_ref,
                    underlying_entry=_icici_underlying_entry,
                    underlying_sl=float(_force_sl),
                    underlying_tp=float(_force_tp),
                    instrument=self._instrument,
                )
                if _conv_sl is None or _conv_tp is None:
                    logger.info("ICICI options entry rejected: cannot convert NIFTY SL/TP to premium levels (%s)", _conv_reason)
                    self._force_sl = None
                    self._force_tp = None
                    with self._lock:
                        self._last_tp_gate_rejection = time.time()
                    _release_icici_vehicle_if_unfilled("premium_sltp_conversion_rejected")
                    return
                _fsl, _ftp = _round_structural_levels("long", _conv_sl, _conv_tp)
                logger.info("ICICI premium SL/TP converted from NIFTY structure: %s", _conv_reason)
            else:
                _fsl, _ftp = _round_structural_levels(side, _force_sl, _force_tp)
            _dir_ok = False
            if side == "long" and _fsl < entry_ref and _ftp > entry_ref:
                _dir_ok = True
            elif side == "short" and _fsl > entry_ref and _ftp < entry_ref:
                _dir_ok = True
            if _dir_ok:
                sl_price = _fsl
                tp_price = _ftp
                _using_force_levels = True
                logger.info(f"Protective exchange SL/TP armed: SL={_entry_cur}{sl_price:,.1f} TP={_entry_cur}{tp_price:,.1f} | structural target locked")
            self._force_sl = None
            self._force_tp = None

        if not _using_force_levels:
            logger.warning(
                "Entry rejected: ICT/Liquidity authority did not provide executable "
                "liquidity TP + ICT/liquidity SL levels; refusing entry")
            with self._lock:
                self._last_tp_gate_rejection = time.time()
            _release_icici_vehicle_if_unfilled("no_executable_structural_levels")
            return
        else:
            # Force levels active; fee/slippage expectancy is a hard execution gate.
            if self._fee_engine is not None and self._fee_engine.is_warmed_up():
                try:
                    _tp_dist = abs(tp_price - entry_ref)
                    _min_tp = self._fee_engine.min_required_tp_move(
                        price=entry_ref, atr=atr,
                        atr_percentile=self._atr_5m.get_percentile(),
                        use_maker_entry=use_maker,
                        delivery_probability=delivery_probability)
                    if _tp_dist < _min_tp:
                        logger.info(
                            f"Entry rejected by fee floor: TP dist {_tp_dist:.0f} "
                            f"< required {_min_tp:.0f} after fees/slippage")
                        with self._lock:
                            self._last_tp_gate_rejection = time.time()
                        _release_icici_vehicle_if_unfilled("fee_floor_rejected")
                        return
                except Exception:
                    pass
        if sl_price is None:
            with self._lock:
                self._last_tp_gate_rejection = time.time()
            _release_icici_vehicle_if_unfilled("sl_missing")
            return

        sd = abs(entry_ref - sl_price)
        td = abs(entry_ref - tp_price)
        if sd < 1e-10:
            _release_icici_vehicle_if_unfilled("zero_stop_distance")
            return
        rr = td / sd
        _execution_policy = active_policy(getattr(self, "_instrument", None))
        _execution_min_rr = self._structural_rr_floor_for_signal(getattr(self, "_last_entry_signal", None), _execution_policy)
        if rr + 1e-12 < _execution_min_rr:
            logger.info(
                "AUCTION_EXECUTION_REJECT grossRR=%.2f floor=%.2f | conservative tick/premium conversion no longer clears structural R:R floor",
                rr, _execution_min_rr)
            with self._lock:
                self._last_tp_gate_rejection = time.time()
            _release_icici_vehicle_if_unfilled("post_rounding_rr_below_floor")
            return
        if _icici_mode:
            logger.info("ICICI long-premium option: liquidation guard skipped; paid premium is the maximum loss envelope")

        # ── Structural order sequence: size from exact invalidation distance ──────────────────────
        # Now that sl_price is known, size from dollar risk / actual SL distance.
        # Structural delivery and venue-cost scaling are applied within account-risk limits.
        exec_delivery_probability = self._calibrated_signal_probability()

        if not use_maker:
            taker_v = self._execution_viability_model(
                side=side, price=entry_ref, sl_price=sl_price, tp_price=tp_price,
                use_maker_entry=False, delivery_probability=exec_delivery_probability)
            maker_v = self._execution_viability_model(
                side=side, price=entry_ref, sl_price=sl_price, tp_price=tp_price,
                use_maker_entry=True, delivery_probability=exec_delivery_probability)
            if not taker_v.allocation_allowed and maker_v.allocation_allowed:
                use_maker = True
                logger.info(
                    f"Execution route repriced: taker invalid "
                    f"(fee_to_risk={taker_v.fee_to_risk:.2f}R, "
                    f"EU={taker_v.expected_net_utility_r:.2f}R) -> maker "
                    f"(fee_to_risk={maker_v.fee_to_risk:.2f}R, "
                    f"EU={maker_v.expected_net_utility_r:.2f}R)")

        sl_price, tp_price, _geometry_repaired = self._repair_execution_geometry(
            side=side,
            entry_price=entry_ref,
            sl_price=sl_price,
            tp_price=tp_price,
            atr=atr,
            use_maker_entry=use_maker,
            delivery_probability=exec_delivery_probability,
        )
        sd = abs(entry_ref - sl_price)
        td = abs(entry_ref - tp_price)
        if sd < 1e-10:
            _release_icici_vehicle_if_unfilled("post_surface_zero_stop_distance")
            return
        rr = td / sd
        executed_viability = self._execution_viability_model(
            side=side, price=entry_ref, sl_price=sl_price, tp_price=tp_price,
            use_maker_entry=use_maker, delivery_probability=exec_delivery_probability)
        self._last_execution_viability = executed_viability.as_refine_context()
        if not executed_viability.allocation_allowed:
            logger.info(
                "AUCTION_EXECUTION_REJECT grossRR=%.2f netWinR=%s netEU=%s | cost/risk=%.3fR reason=%s",
                rr,
                f"{executed_viability.net_win_r:.2f}" if executed_viability.utility_known else "N/A",
                f"{executed_viability.expected_net_utility_r:+.2f}R" if executed_viability.utility_known else "N/A",
                executed_viability.fee_to_risk, executed_viability.reason)
            _release_icici_vehicle_if_unfilled("execution_cost_geometry_rejected")
            return
        if executed_viability.utility_known and executed_viability.expected_net_utility_r <= 0.0:
            logger.info(
                "AUCTION_EXECUTION_WARNING grossRR=%.2f netWinR=%.2f netLossR=%.2f netEU=%+.2fR | calibrated odds reduce allocation; no thesis veto",
                rr, executed_viability.net_win_r, executed_viability.net_loss_r, executed_viability.expected_net_utility_r)
        logger.info(
            "AUCTION_EXECUTION_ECONOMICS grossRR=%.2f netWinR=%s netLossR=%s netEU=%s cost/risk=%.3fR route=%s",
            rr,
            f"{executed_viability.net_win_r:.2f}" if executed_viability.utility_known else "N/A",
            f"{executed_viability.net_loss_r:.2f}" if executed_viability.utility_known else "N/A",
            f"{executed_viability.expected_net_utility_r:+.2f}R" if executed_viability.utility_known else "N/A",
            executed_viability.fee_to_risk, executed_viability.route)

        qty = self._compute_quantity(
            risk_manager, entry_ref, sig=sig, setup_grade=setup_grade, sl_price=sl_price,
            prefetched_bal_info=bal_info, side=side, tp_price=tp_price,
            use_maker_entry=use_maker, delivery_probability=exec_delivery_probability
        )
        if qty is None or qty < QCfg.MIN_QTY():
            # No order was sent. Treat as a pre-order rejection, not a trade
            # exit/failure. Otherwise the re-entry interval starts and entry
            # evaluation appears to stop after a minimum-lot sizing reject.
            with self._lock:
                self._last_tp_gate_rejection = time.time()
            _release_icici_vehicle_if_unfilled("sizing_rejected")
            return

        _actual_entry_lev = float(getattr(self, "_active_effective_leverage", QCfg.LEVERAGE()) or QCfg.LEVERAGE())
        if not _icici_mode:
            _liq_ok, _liq_px, _liq_guard, _liq_reason = self._sl_liquidation_sanity(
                side, entry_ref, sl_price, leverage_override=_actual_entry_lev)
            if not _liq_ok:
                logger.warning(
                    f"Entry rejected by actual-leverage liquidation guard: {side.upper()} "
                    f"entry={_entry_cur}{entry_ref:,.1f} SL={_entry_cur}{sl_price:,.1f} lev={_actual_entry_lev:.0f}x | {_liq_reason}")
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return

        # Always assert the leverage used by the sizing model before sending a
        # leveraged bracket.  Previous builds only called set_leverage() when
        # _entry_leverage != configured leverage.  That was unsafe: after one
        # trade normalized BTC from 40x -> 9x, the next trade that sized at 40x
        # could skip the reset and the exchange would still be sitting at 9x.
        _entry_leverage = int(max(1, round(float(getattr(self, "_active_effective_leverage", QCfg.LEVERAGE()) or QCfg.LEVERAGE()))))
        _configured_leverage = int(max(1, QCfg.LEVERAGE()))
        _exch_for_leverage = str(QCfg.EXCHANGE()).lower()
        _must_assert_leverage = (
            _entry_leverage > 1
            and hasattr(order_manager, "set_leverage")
            and ("delta" in _exch_for_leverage or "coinswitch" in _exch_for_leverage)
        )
        if _must_assert_leverage:
            try:
                _lev_resp = order_manager.set_leverage(leverage=_entry_leverage)
                _lev_err = ""
                _lev_success = True
                if isinstance(_lev_resp, dict):
                    _lev_err = str(_lev_resp.get("error", "") or _lev_resp.get("message", "") or "")
                    _lev_success = bool(_lev_resp.get("success", True)) and not _lev_err
                if not _lev_success:
                    logger.warning(
                        f"Entry rejected: exchange leverage {_entry_leverage}x "
                        f"not confirmed before bracket: {_lev_err or _lev_resp}")
                    with self._lock:
                        self._last_tp_gate_rejection = time.time()
                    return
                _lev_action = "normalized" if _entry_leverage != _configured_leverage else "asserted"
                logger.info(
                    f"⚖️ Leverage {_lev_action} for structural-risk funding: "
                    f"configured={_configured_leverage}x effective={_entry_leverage}x "
                    f"(SL risk≈{getattr(self, '_active_margin_risk_pct', 0.0) * 100.0:.2f}% of margin)")
            except Exception as _lev_e:
                logger.warning(
                    f"Entry rejected: failed to assert capital-efficient leverage "
                    f"{_entry_leverage}x before order: {_lev_e}")
                with self._lock:
                    self._last_tp_gate_rejection = time.time()
                return
        _sig_diag = str(sig) if sig is not None else ""
        if (getattr(sig, 'vwap_price', 0.0) or 0.0) <= 0:
            _sig_diag = "ICT/Liquidity structural execution"
        logger.info(
            f"ENTERING {side.upper()} @ {_entry_cur}{entry_ref:,.2f} | qty={qty} | "
            f"SL={_entry_cur}{sl_price:,.2f} TP={_entry_cur}{tp_price:,.2f} payoff/risk=1:{rr:.2f} | "
            f"lev={_entry_leverage}x funded_margin_SL_risk≈{getattr(self, '_active_margin_risk_pct', 0.0) * 100.0:.2f}% | "
            f"{'maker' if use_maker else 'taker'} | {_sig_diag}"
        )

        # ── Place entry ────────────────────────────────────────────────────────────
        # Delta: bracket limit order (entry + SL + TP in one API call).
        #   Avoids bad_schema from separate stop/take-profit order placement.
        # CoinSwitch: standard limit entry, SL/TP placed separately after fill.
        #
        # BUG 2 FIX: on_order_placed callback captures the exact moment the
        # limit order hits the exchange (REST 200 OK returned an order_id).
        # The on_tick watchdog switches from Stage A (pre-order, 45 s tolerance)
        # to Stage B (post-order, fill-timeout + 25 %) at that instant.  This
        # prevents the watchdog from firing while the bracket fill-poll is
        # still legitimately running.
        def _on_order_placed(_oid: str) -> None:
            self._entry_order_placed_at = time.time()
            logger.info(
                f"⏱️  Entry order placed on exchange (order_id={_oid[:12]}…) "
                f"— watchdog switched to Stage B (fill-poll)")

        limit_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 45.0))
        is_bracket = False

        # v7 multi-asset protection policy:
        # Every Delta contract (BTC, metals, xStocks, indices/RWA tokens) must use
        # the same native bracket methodology: entry + SL + TP in one Delta order.
        # If the bracket endpoint rejects the order for a non-BTC contract, do NOT
        # fall back to a naked entry followed by standalone SL/TP. That fallback can
        # leave the position temporarily unprotected and behaves differently from BTC.
        # CoinSwitch has no Delta-native bracket endpoint, so only CoinSwitch uses the
        # old fill-then-place-standalone-conditionals path.
        _active_exchange = str(
            getattr(order_manager, "active_exchange", None)
            or getattr(order_manager, "_exchange_name", "")
            or ""
        ).lower()
        _delta_requires_native_bracket = (
            _active_exchange == "delta" and
            bool(getattr(config, "DELTA_REQUIRE_NATIVE_BRACKET", True))
        )
        # ICICI NFO options must never fall back to a naked buy order.  If the
        # official Breeze protected GTT path is disabled or rejected, the entry
        # is refused rather than routed through standalone post-fill protection.
        _icici_requires_protected_oco = (_active_exchange == "icici")

        entry_data = order_manager.place_bracket_limit_entry(
            side=side, quantity=qty,
            limit_price=limit_px,
            sl_price=sl_price, tp_price=tp_price,
            timeout_sec=limit_timeout,
            on_order_placed=_on_order_placed,
        )
        if entry_data is not None:
            is_bracket = bool(entry_data.get("bracket_order", False))
        elif _delta_requires_native_bracket or _icici_requires_protected_oco:
            _bracket_err = getattr(order_manager, "last_order_error", None)
            _err_reason = ""
            _err_stage = ""
            try:
                if isinstance(_bracket_err, dict):
                    if _bracket_err.get("reason"):
                        _err_reason = f" reason={_bracket_err.get('reason')}"
                    _err_stage = str(_bracket_err.get("stage") or "")
            except Exception:
                _err_reason = ""
                _err_stage = ""

            _protected_model = "ICICI official GTT cover-OCO" if _icici_requires_protected_oco else "Delta native bracket"
            if "fill_timeout" in _err_stage:
                logger.warning(
                    f"⚠️ {_protected_model} entry timed out unfilled — order was "
                    "cancelled safely; no non-protected fallback was used, so no "
                    "unprotected position was opened. "
                    f"side={side} qty={qty} entry={_entry_cur}{limit_px:,.2f} "
                    f"SL={_entry_cur}{sl_price:,.2f} TP={_entry_cur}{tp_price:,.2f}{_err_reason}"
                )
            else:
                logger.error(
                    f"❌ {_protected_model} entry failed — refusing non-protected fallback "
                    "so the position is not opened without broker-attached TP/SL. "
                    f"side={side} qty={qty} entry={_entry_cur}{limit_px:,.2f} "
                    f"SL={_entry_cur}{sl_price:,.2f} TP={_entry_cur}{tp_price:,.2f}{_err_reason}"
                )
            self._last_exit_time = time.time()
            return
        else:
            # CoinSwitch / non-Delta path: standard limit entry, then protected
            # standalone SL/TP after fill because native bracket is unavailable.
            entry_data = order_manager.place_limit_entry(
                side=side, quantity=qty,
                limit_price=limit_px,
                timeout_sec=limit_timeout,
                fallback_to_market=False,
                on_order_placed=_on_order_placed,
            )

        if not entry_data:
            logger.error("❌ Entry order failed")
            self._last_exit_time = time.time()  # engage cooldown — prevents hammer-retrying
            _release_icici_vehicle_if_unfilled("entry_order_not_filled_or_rejected")
            return

        if (_delta_requires_native_bracket or _icici_requires_protected_oco) and not is_bracket:
            logger.error(
                "❌ Protected-entry desk returned without bracket_order=True — refusing to "
                "treat it as an active position because TP/SL are not broker-attached."
            )
            self._last_exit_time = time.time()
            return

        # ── Extract fill price ────────────────────────────────────────────────────
        fill_price = (
            float(entry_data.get("fill_price")          or 0)
            or float(entry_data.get("average_price")    or 0)
            or float(entry_data.get("avg_execution_price") or 0)
            or float(entry_data.get("price")            or 0)
            or price
        )
        actual_fill_type = entry_data.get("fill_type", "taker")
        # v8.1: exact entry fee from Delta paid_commission / fill commission.
        # Exactness is a boolean signal; the value can be zero or a maker rebate.
        entry_fee_paid = float(entry_data.get("paid_commission", 0) or 0)
        entry_fee_exact = bool(entry_data.get("paid_commission_exact", False))
        if entry_fee_exact:
            logger.info(f"💰 Entry fee (Delta exact): {_entry_cur}{entry_fee_paid:.4f}")
        else:
            logger.warning("Entry fee exactness missing from order response; PnL will not use a synthetic Delta fee")

        # v4.6 BUG FIX #8: Use actual filled quantity for partial fills
        # order_manager.place_limit_entry returns adjusted quantity on partial fill
        filled_qty = float(entry_data.get("quantity", 0)) if "quantity" in entry_data else 0
        if filled_qty > 0 and filled_qty != qty:
            logger.info(f"⚠️ Partial fill: {filled_qty:.4f} of {qty:.4f} — using filled qty")
            qty = filled_qty

        # ── v13 HARD PROTECTION INVARIANT ─────────────────────────────────────────
        # A Delta bracket order can be filled while child SL/TP orders are still
        # not visible.  In single-BTC mode the old code tolerated this, but in
        # multi-asset mode that is unsafe: the open-orders response can include
        # other products' SL/TP children.  If the exact product's child SL+TP are
        # not verified near the intended prices, flatten immediately and alert.
        if is_bracket and _delta_requires_native_bracket and (
            entry_data.get("_bracket_children_missing") or not entry_data.get("bracket_child_verified", False)
        ):
            _inst = getattr(self, "_instrument", None)
            _asset = str(getattr(_inst, "asset_id", getattr(self, "_asset_id", QCfg.SYMBOL())) or "").upper()
            _sym = str(getattr(_inst, "display_symbol", QCfg.SYMBOL()) or QCfg.SYMBOL())
            _exit_side = "sell" if side == "long" else "buy"
            _msg = (
                f"🚨 <b>PROTECTION FAILURE — POSITION FLATTENED</b>\n"
                f"<b>{_asset}</b> · DELTA:{_sym} · {side.upper()}\n"
                f"Entry order filled but verified bracket children were not found on the same product.\n"
                f"Expected SL <code>{_entry_cur}{sl_price:,.2f}</code> · TP <code>{_entry_cur}{tp_price:,.2f}</code>\n"
                f"Entry <code>{_entry_cur}{fill_price:,.2f}</code> · Qty <code>{qty:.8g}</code>\n"
                f"Action: reduce-only market {_exit_side.upper()} submitted; entries cooled down."
            )
            logger.critical(
                f"[{_asset}|DELTA:{_sym}] PROTECTION FAILURE: verified bracket SL/TP missing; "
                f"flattening immediately entry_order={entry_data.get('order_id')} "
                f"expected_sl={sl_price} expected_tp={tp_price} qty={qty}"
            )
            try:
                self._send_telegram(_msg, event_type="protection_failure")
            except Exception:
                try: send_telegram_message(_msg)
                except Exception: pass
            try:
                flatten = order_manager.place_market_order(side=_exit_side, quantity=qty, reduce_only=True)
                if not flatten:
                    logger.critical(f"[{_asset}|DELTA:{_sym}] EMERGENCY FLATTEN FAILED after bracket protection failure")
                    self._send_telegram(
                        f"🚨 <b>EMERGENCY FLATTEN FAILED</b>\n<b>{_asset}</b> · DELTA:{_sym}\nManual action required immediately.",
                        event_type="protection_failure",
                    )
                else:
                    logger.critical(f"[{_asset}|DELTA:{_sym}] Emergency flatten submitted after bracket protection failure: {flatten}")
            except Exception as _pf_e:
                logger.critical(f"[{_asset}|DELTA:{_sym}] EMERGENCY FLATTEN EXCEPTION after protection failure: {_pf_e}", exc_info=True)
                try:
                    self._send_telegram(
                        f"🚨 <b>EMERGENCY FLATTEN EXCEPTION</b>\n<b>{_asset}</b> · DELTA:{_sym}\n<code>{str(_pf_e)[:400]}</code>",
                        event_type="protection_failure",
                    )
                except Exception: pass
            self._last_exit_time = time.time()
            self._entry_order_placed_at = 0.0
            self._entering_since = 0.0
            return

        # ── Record slippage for fee engine (PATCH 5f) ─────────────────────────────
        if self._fee_engine is not None:
            try:
                self._fee_engine.record_fill(price, fill_price, leg="entry")
            except Exception as e:
                logger.debug(f"record_fill error (non-fatal): {e}")

        # ── Recompute SL/TP from actual fill only on ADVERSE slippage ────────────
        # CRITICAL BUG FIX: The old code used abs(fill_price - price) which fired
        # on FAVORABLE fills too. A SHORT limit at 73,629 filling at 73,683
        # (market moved up, maker got better price) is NOT slippage — it's
        # favorable execution. Recomputing in that case then hit pctile=0.00
        # (ATR percentile drops in the seconds between decision and fill) and
        # the fee floor rejected the now-open position, instantly closing it.
        #
        # Adverse slippage definition:
        #   LONG:  fill_price > price (paid more than the market snapshot)
        #   SHORT: fill_price < price (sold for less than the market snapshot)
        #
        # Favorable execution (market moved in our direction between decision
        # and fill) should NOT trigger recompute — the SL/TP from the original
        # decision are still valid or better.
        is_adverse_slip = (
            (side == "long"  and fill_price > price) or
            (side == "short" and fill_price < price)
        )
        adverse_slip_pct = (abs(fill_price - price) / price) if is_adverse_slip else 0.0

        if is_adverse_slip and adverse_slip_pct > QCfg.SLIPPAGE_TOL():
            logger.info(
                f"⚠️ Adverse slippage {adverse_slip_pct:.4%} > tol {QCfg.SLIPPAGE_TOL():.4%} "
                f"— validating original structural SL/TP against fill {_entry_cur}{fill_price:,.2f}")
            _levels_valid = (
                (side == "long" and sl_price < fill_price and tp_price > fill_price) or
                (side == "short" and sl_price > fill_price and tp_price < fill_price)
            )
            if not _levels_valid:
                logger.warning(
                    f"❌ Post-slippage structural levels invalid — aborting trade "
                    f"(adverse slip={adverse_slip_pct:.4%})")
                exit_side = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                return
        elif not is_adverse_slip and abs(fill_price - price) / price > QCfg.SLIPPAGE_TOL():
            # Favorable fill: market moved our way. Log it but keep original SL/TP.
            fav_pct = abs(fill_price - price) / price
            logger.info(
                f"✅ Favorable fill: {_entry_cur}{fill_price:,.2f} vs snapshot {_entry_cur}{price:,.2f} "
                f"(+{fav_pct:.4%} in our direction) — keeping original SL/TP")

        # ── Place SL/TP (or retrieve bracket child order IDs) ───────────────────
        exit_side = "sell" if side == "long" else "buy"

        if is_bracket:
            # Broker-protected entry: Delta uses native bracket children; ICICI
            # uses official GTT cover-OCO target/stoploss legs.
            sl_order_id_raw = entry_data.get("bracket_sl_order_id", "")
            tp_order_id_raw = entry_data.get("bracket_tp_order_id", "")
            # Use bracket prices if we have them (queried from open_orders),
            # otherwise fall back to the computed prices
            bsl = entry_data.get("bracket_sl_price", 0.0)
            btp = entry_data.get("bracket_tp_price", 0.0)
            if bsl > 0:
                sl_price = bsl
            if btp > 0:
                tp_price = btp
            sl_data = {"order_id": sl_order_id_raw} if sl_order_id_raw else None
            tp_data = {"order_id": tp_order_id_raw} if tp_order_id_raw else None
            if sl_order_id_raw:
                logger.info(f"✅ Bracket SL order: {sl_order_id_raw} @ {_entry_cur}{sl_price:,.2f}")
            if tp_order_id_raw:
                logger.info(f"✅ Bracket TP order: {tp_order_id_raw} @ {_entry_cur}{tp_price:,.2f}")
            if not sl_order_id_raw or not tp_order_id_raw:
                logger.warning(
                    "⚠️ Bracket child order IDs not found after fill — "
                    "TP ladder cannot be verified; check open orders manually.")
        else:
            # CoinSwitch (and non-bracket) path: place SL/TP as separate orders
            sweep = order_manager.cancel_symbol_conditionals()
            if sweep:
                # v4.6 BUG FIX #3: Wait for exchange to process cancellations
                # Without this, old SL/TP can fire against the new position instantly
                time.sleep(1.5)
                filled = [
                    oid for oid, r in sweep.items()
                    if r in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL)
                ]
                if filled:
                    self._last_reconcile_time = 0.0
                    return

            sl_data = order_manager.place_stop_loss(
                side=exit_side, quantity=qty, trigger_price=sl_price)
            if not sl_data:
                order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                return

            if _icici_mode:
                # Legacy compatibility branch only.  Default ICICI execution is
                # mandatory official GTT cover-OCO above; it cannot fall through
                # here while protected-entry policy is active.
                tp_data = None
                logger.info(
                    "ICICI LEGACY single-live-exit branch reached: protective NFO STOPLOSS armed; "
                    "protected GTT routing should be enabled for live trading")
            else:
                tp_data = order_manager.place_take_profit(
                    side=exit_side, quantity=qty, trigger_price=tp_price)
                if not tp_data:
                    order_manager.cancel_order(sl_data["order_id"])
                    order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
                    self._last_exit_time = time.time()
                    return

        # ── Dynamic TP ladder: internal liquidity TP1..TPn, final TP unchanged ─────
        tp_ladder_plan = self._build_tp_ladder_plan(
            side=side, entry_price=fill_price, sl_price=sl_price,
            final_tp=tp_price, quantity=qty, atr=atr,
            use_maker_entry=(str(actual_fill_type or "").lower() == "maker"))
        if _icici_mode:
            tp_ladder_dicts = [l.as_dict() for l in getattr(tp_ladder_plan, "legs", [])] if tp_ladder_plan is not None else []
            tp_ladder_order_ids = []
            logger.info(
                "ICICI TP_LADDER analytical-only: %d levels computed; official broker GTT target/stoploss remain the only exit authority",
                len(tp_ladder_dicts))
        else:
            tp_ladder_dicts, tp_ladder_order_ids = self._place_internal_tp_ladder(
                order_manager=order_manager, side=side, quantity=qty, final_tp=tp_price,
                native_final_tp_order_id=(tp_data or {}).get("order_id", ""),
                ladder_plan=tp_ladder_plan,
            )

        # ── Log execution cost snapshot (PATCH 5g) ────────────────────────────────
        if self._fee_engine is not None:
            try:
                snap = self._fee_engine.diagnostic_snapshot()
                sdf  = abs(fill_price - sl_price)
                logger.info(
                    f"📊 ExecCost | spread={snap['spread_median_bps']:.1f}bps "
                    f"slip={snap['slippage_ewma_bps']:.1f}bps "
                    f"rt_cost_{'maker' if actual_fill_type == 'maker' else 'taker'}"
                    f"={snap['rt_cost_maker_bps' if actual_fill_type == 'maker' else 'rt_cost_taker_bps']:.1f}bps "
                    f"fill_type={actual_fill_type}"
                )
            except Exception as e:
                logger.debug(f"ExecCost snapshot error (non-fatal): {e}")

        sdf = abs(fill_price - sl_price)
        ir = sdf * qty
        # Filled-order risk metrics must be defined from the confirmed execution,
        # not from pre-order marks. Quantity represents executable exposure units
        # for every supported venue; core.pnl applies the venue payoff model.
        dollar_risk = abs(gross_pnl_usd(
            side, fill_price, sl_price, qty,
            inverse=(str(_accounting.get("pnl_model", "linear")).lower() == "inverse_btcusd"),
        ))
        reward_value = abs(gross_pnl_usd(
            side, fill_price, tp_price, qty,
            inverse=(str(_accounting.get("pnl_model", "linear")).lower() == "inverse_btcusd"),
        ))
        rr_a = reward_value / max(dollar_risk, 1e-12)
        entry_session = self._execution_session_label()


        _quality = dict(getattr(getattr(self, "_last_entry_signal", None), "quality", {}) or {})
        _prob_calibrated = bool(_quality.get("probability_calibrated", False))
        _delivery_probability = float(_quality.get("delivery_probability", 0.0) or 0.0) if _prob_calibrated else 0.0
        _delivery_score = float(_quality.get("delivery_score", 0.0) or 0.0)
        _delivery_utility_r = float(_quality.get("delivery_utility_r", 0.0) or 0.0) if _prob_calibrated else 0.0
        _archetype = str(_quality.get("archetype", getattr(getattr(self, "_last_entry_signal", None), "archetype", "STRUCTURAL_AUCTION")) or "STRUCTURAL_AUCTION")

        # ── Update position state ─────────────────────────────────────────────────
        self._pos = PositionState(
            phase           = PositionPhase.ACTIVE,
            side            = side,
            quantity        = qty,
            entry_price     = fill_price,
            sl_price        = sl_price,
            tp_price        = tp_price,
            sl_order_id     = (sl_data or {}).get("order_id", ""),
            tp_order_id     = (tp_data or {}).get("order_id", ""),
            entry_order_id  = entry_data.get("order_id"),
            entry_time      = time.time(),
            initial_risk    = ir,
            initial_sl_dist = sdf,
            entry_signal    = sig,
            entry_atr       = self._atr_5m.atr,
            entry_fill_type = actual_fill_type,  # v4.3: for correct PnL fee calc
            entry_leverage  = float(_entry_leverage),
            entry_fee_paid  = entry_fee_paid,     # v8.1: exact from Delta paid_commission
            entry_fee_exact = entry_fee_exact,    # exact fee present, even if 0/rebate
            entry_session   = entry_session,
            # FIX 8b: capture actual HTF scores at entry so _record_pnl can log them correctly
            entry_htf_15m   = float(getattr(getattr(self, "_last_entry_signal", None), "quality", {}).get("context_15m", 0.0) or 0.0),
            entry_htf_4h    = float(getattr(getattr(self, "_last_entry_signal", None), "quality", {}).get("context_4h", 0.0) or 0.0),
            delivery_probability = _delivery_probability,
            probability_calibrated = _prob_calibrated,
            delivery_score = _delivery_score,
            archetype = _archetype,
            delivery_utility_r = _delivery_utility_r,
            quant_components = _quality,
            tp_ladder = tp_ladder_dicts,
            tp_ladder_order_ids = tp_ladder_order_ids,
            tp_ladder_active = bool(tp_ladder_order_ids),
            tp_ladder_last_sync_qty = qty,
            tp_ladder_initial_qty = qty,
            last_seen_price = fill_price,
            exchange = str(_accounting.get("exchange", "")),
            execution_symbol = str(_accounting.get("execution_symbol", "")),
            asset_id = str(_accounting.get("asset_id", "")),
            currency_code = str(_accounting.get("currency_code", "USD")),
            currency_symbol = str(_accounting.get("currency_symbol", "$")),
            pnl_model = str(_accounting.get("pnl_model", "linear")),
            quantity_unit = str(_accounting.get("quantity_unit", "units")),
            thesis_side = (_icici_thesis_side if _icici_mode else side),
            analysis_entry_price = (_icici_underlying_entry if _icici_mode else fill_price),
            analysis_sl_price = (_analysis_sl_level if _icici_mode else sl_price),
            analysis_tp_price = (_analysis_tp_level if _icici_mode else tp_price),
            analysis_atr = (_icici_underlying_atr if _icici_mode else float(self._atr_5m.atr or 0.0)),
        )
        # ── Reconcile safety: discard any in-flight reconcile data ────────────────
        self._reconcile_data        = None
        self._last_reconcile_time   = time.time()
        self.current_sl_price       = sl_price
        self.current_tp_price       = tp_price
        self._confirm_long          = self._confirm_short = 0
        # Reset duplicate guards for the new position
        self._exit_completed        = False
        self._pnl_recorded_for     = 0.0
        # FIX Bug-C: record_trade_start() AFTER confirmed fill, not before.
        # Moving it here prevents aborted entries (TP-gate, fee-gate, exchange
        # error) from consuming the daily trade cap with no actual order sent.
        self._risk_gate.record_trade_start()
        # Long-run RiskManager bookkeeping: mark the shared risk manager as
        # position-open after the exchange confirms the fill. This makes its
        # deferred midnight reset logic accurate during multi-day unattended runs.
        try:
            _rm_open = getattr(self, '_risk_manager_ref', None)
            if _rm_open is not None and hasattr(_rm_open, 'set_position_open'):
                _rm_open.set_position_open(True)
        except Exception as _rm_open_e:
            logger.debug(f"risk_manager.set_position_open(True) error (non-fatal): {_rm_open_e}")

        if hasattr(self, '_entry_engine') and self._entry_engine is not None:
            self._entry_engine.on_position_opened()


        # ── Institutional auction entry notification ─────────────────────────
        _es = getattr(self, "_last_entry_signal", None)
        _quality = dict(getattr(_es, "quality", {}) or {}) if _es is not None else {}
        _raid = getattr(_es, "sweep_result", None) if _es is not None else None
        _raid_pool = getattr(_raid, "pool", None) if _raid is not None else None
        _raid_label = str(getattr(getattr(_raid_pool, "side", None), "value", "-") or "-")
        _raid_px = float(getattr(_raid_pool, "price", 0.0) or 0.0)
        _entry_cur = str(getattr(self._pos, "currency_symbol", "$") or "$")
        _prob_calibrated = bool(_quality.get("probability_calibrated", False))
        _delivery_p = float(_quality.get("delivery_probability", 0.0) or 0.0) if _prob_calibrated else 0.0
        _delivery_score = float(_quality.get("delivery_score", 0.0) or 0.0)
        _utility = float(_quality.get("delivery_utility_r", 0.0) or 0.0) if _prob_calibrated else 0.0
        _archetype = str(_quality.get("archetype", getattr(_es, "archetype", "STRUCTURAL_AUCTION")) or "STRUCTURAL_AUCTION")
        _disp = float(_quality.get("displacement_atr", 0.0) or 0.0)
        _fee_line = (f"Broker fee exact {_entry_cur}{entry_fee_paid:.4f}" if entry_fee_exact else "Broker fee pending exact execution report")
        _entry_msg = format_entry_alert(
            side=side,
            price=fill_price,
            sl=sl_price,
            tp=tp_price,
            qty=qty,
            leverage=_entry_leverage,
            context_4h=f"{float(_quality.get('context_4h', 0.0) or 0.0):.2f}",
            context_15m=f"{float(_quality.get('context_15m', 0.0) or 0.0):.2f}",
            raid_quality=float(getattr(_raid, "quality", 0.0) or _quality.get("raid_quality", 0.0) or 0.0),
            displacement_atr=_disp,
            delivery_score=_delivery_score,
            delivery_probability=_delivery_p if _prob_calibrated else None,
            delivery_utility_r=_utility,
            probability_calibrated=_prob_calibrated,
            archetype=_archetype,
            rr=rr_a,
            instrument=getattr(self, "_instrument", None),
            risk_usd=dollar_risk,
            margin_used=(qty * fill_price / max(float(_entry_leverage or 1.0), 1.0)),
            fee_status=_fee_line,
            raid_label=_raid_label,
            raid_price=_raid_px,
            target_label="opposing higher-timeframe liquidity targets",
        )
        self._send_telegram(_entry_msg, event_type="entry")
        logger.info("✅ ACTIVE STRUCTURAL_AUCTION %s @ %s%.4f | SL=%s%.4f TP=%s%.4f | R:R=1:%.2f", side.upper(), _entry_cur, fill_price, _entry_cur, sl_price, _entry_cur, tp_price, rr_a)

    def _observe_position_extremes(self, pos, price: float) -> tuple:
        profit = (price - pos.entry_price) if pos.side == "long" else (pos.entry_price - price)
        new_extreme = False
        with self._lock:
            if price > 0:
                pos.last_seen_price = price
            if profit > pos.peak_profit + 1e-9:
                pos.peak_profit = profit
                new_extreme = True

            adverse = max(0.0, -profit)
            if adverse > pos.peak_adverse:
                pos.peak_adverse = adverse

            if pos.side == "long":
                if pos.peak_price_abs < 1e-10 or price > pos.peak_price_abs:
                    pos.peak_price_abs = price
                    new_extreme = True
            else:
                if pos.peak_price_abs < 1e-10 or price < pos.peak_price_abs:
                    pos.peak_price_abs = price
                    new_extreme = True
        return profit, new_extreme

    def _time_decay_exit_reason(self, pos, price: float, now: float) -> str:
        if not bool(getattr(config, "QUANT_TIME_STOP_ENABLED", True)):
            return ""
        entry_time = float(getattr(pos, "entry_time", 0.0) or 0.0)
        if entry_time <= 0.0 or price <= 0.0:
            return ""
        try:
            max_hold = float(policy_value("max_hold_sec", getattr(config, "QUANT_MAX_HOLD_SEC", 3600)) or 0.0)
        except Exception:
            max_hold = float(getattr(config, "QUANT_MAX_HOLD_SEC", 3600) or 0.0)
        if max_hold <= 0.0:
            return ""
        nifty_fast_exit = (
            str(getattr(pos, "asset_id", "") or "").upper() in {"NIFTY", "NIFTY50", "CNXNIFTY"}
            and str(getattr(pos, "archetype", "") or "").upper() == "NIFTY_TREND_SWEEP_SCALP"
        )
        if nifty_fast_exit:
            max_hold = min(max_hold, float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_MAX_HOLD_SEC", 720.0) or 720.0))
        init_r = float(getattr(pos, "initial_sl_dist", 0.0) or 0.0)
        if init_r <= 1e-10:
            init_r = abs(float(getattr(pos, "entry_price", 0.0) or 0.0) - float(getattr(pos, "sl_price", 0.0) or 0.0))
        if init_r <= 1e-10:
            return ""
        side = str(getattr(pos, "side", "") or "").lower()
        entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
        progress_r = ((price - entry) if side == "long" else (entry - price)) / init_r
        mfe_r = float(getattr(pos, "peak_profit", 0.0) or 0.0) / init_r
        age = max(0.0, float(now) - entry_time)

        if nifty_fast_exit:
            early_frac = float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_FAILED_AUCTION_FRACTION", 0.35) or 0.35)
            failed_r = float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_FAILED_AUCTION_R", -0.15) or -0.15)
            failed_mfe_r = float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_FAILED_AUCTION_MAX_MFE_R", 0.30) or 0.30)
            min_delivery_r = float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_MIN_PROGRESS_R", 0.15) or 0.15)
            hard_mult = float(getattr(config, "ICICI_NIFTY_TREND_SWEEP_HARD_MAX_MULT", 1.0) or 1.0)
        else:
            early_frac = float(getattr(config, "QUANT_TIME_STOP_EARLY_FRACTION", 0.55) or 0.55)
            failed_r = float(getattr(config, "QUANT_TIME_STOP_FAILED_AUCTION_R", -0.35) or -0.35)
            failed_mfe_r = float(getattr(config, "QUANT_TIME_STOP_FAILED_AUCTION_MAX_MFE_R", 0.50) or 0.50)
            min_delivery_r = float(getattr(config, "QUANT_TIME_STOP_MIN_PROGRESS_R", 0.20) or 0.20)
            hard_mult = float(getattr(config, "QUANT_TIME_STOP_HARD_MAX_MULT", 1.35) or 1.35)

        if age >= max_hold * max(1.0, hard_mult):
            return "time_stop_hard_max_hold"
        if age >= max_hold and progress_r < min_delivery_r:
            return "time_stop_no_delivery"
        if age >= max_hold * max(0.10, min(early_frac, 0.95)) and progress_r <= failed_r and mfe_r < failed_mfe_r:
            return "time_stop_failed_auction"
        return ""

    def _manage_active(self, data_manager, order_manager, now):
        """Manage protected exposure without introducing a second alpha authority.

        Delta/CoinSwitch positions are monetised through their exchange-resident
        liquidity targets.  ICICI long-premium entries use Breeze's official
        three-leg cover-OCO: once armed, its target and stoploss legs own normal
        TP/SL execution.  Strategy-managed early/time exits may cancel the OCO
        plan first, but must not race the native target on an ordinary TP touch.
        """
        pos = self._pos
        if pos.is_flat():
            return
        try:
            price = float(data_manager.get_last_price() or 0.0)
        except Exception:
            return
        if price <= 0:
            return
        self._last_known_price = price
        self._observe_position_extremes(pos, price)
        time_stop_reason = self._time_decay_exit_reason(pos, price, now)
        if time_stop_reason and pos.phase == PositionPhase.ACTIVE:
            logger.info("Structural auction time-decay exit armed: %s at %.4f", time_stop_reason, price)
            self._exit_trade(order_manager, price, time_stop_reason)
            return

        exchange = str(getattr(pos, "exchange", "") or "").lower()
        side = str(getattr(pos, "side", "") or "").lower()
        tp = float(getattr(pos, "tp_price", 0.0) or 0.0)
        tp_hit = tp > 0.0 and ((side == "long" and price >= tp) or (side == "short" and price <= tp))
        if exchange == "icici" and tp_hit and pos.phase == PositionPhase.ACTIVE:
            cur = str(getattr(pos, "currency_symbol", "") or "₹")
            protected_gtt = (
                str(getattr(pos, "sl_order_id", "") or "").startswith("GTT:")
                or str(getattr(pos, "tp_order_id", "") or "").startswith("GTT:")
            )
            if protected_gtt:
                logger.info(
                    "ICICI broker GTT target zone reached: premium=%s%.4f target=%s%.4f; "
                    "cover-OCO owns normal TP execution; requesting exact reconciliation",
                    cur, price, cur, tp)
                self._last_reconcile_time = 0.0
                return
            logger.warning(
                "ICICI legacy supervised target reached without protected GTT ids: premium=%s%.4f target=%s%.4f; "
                "attempting protected strategy close", cur, price, cur, tp)
            self._exit_trade(order_manager, price, "liquidity_tp_hit")
            return

        try:
            self._book_tp_ladder_partials(order_manager, pos)
            self._reconcile_tp_ladder_quantity(order_manager, pos)
        except Exception as exc:
            logger.debug("Liquidity TP reconciliation skipped: %s", exc)
        return

    def _exit_trade(self, order_manager, reference_price: float, reason: str) -> bool:
        """Submit a full strategy exit without leaving conflicting exit orders live.

        Any strategy-managed close must first cancel broker-resident exits. P&L
        is never booked here; reconciliation records the confirmed execution.
        """
        pos = self._pos
        if pos is None or pos.is_flat() or pos.phase != PositionPhase.ACTIVE:
            return False
        exit_side = "sell" if pos.side == "long" else "buy"
        cur = str(getattr(pos, "currency_symbol", "") or ("₹" if _icici_exchange_name(getattr(self, "_instrument", None)) == "icici" else "$"))
        with self._lock:
            if self._pos.phase != PositionPhase.ACTIVE:
                return False
            self._pos.phase = PositionPhase.EXITING
            self._exiting_since = time.time()
        try:
            self._cancel_tp_ladder_orders(order_manager, pos)
            sl_result, tp_result = order_manager.cancel_all_exit_orders(
                getattr(pos, "sl_order_id", ""), getattr(pos, "tp_order_id", ""))
            terminal = {CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL}
            if sl_result in terminal or tp_result in terminal:
                logger.warning(
                    "Exit request %s deferred: existing exit order filled/partially filled; reconciliation owns final state", reason)
                with self._lock:
                    self._pos.phase = PositionPhase.ACTIVE
                self._last_reconcile_time = 0.0
                return False
            if sl_result == CancelResult.FAILED or tp_result == CancelResult.FAILED:
                logger.critical(
                    "Exit request %s refused: live exit cancellation unverified; no conflicting close submitted", reason)
                with self._lock:
                    self._pos.phase = PositionPhase.ACTIVE
                return False
            close = order_manager.place_market_order(
                side=exit_side, quantity=pos.quantity, reduce_only=True)
            oid = str((close or {}).get("order_id") or "") if isinstance(close, dict) else ""
            if oid:
                with self._lock:
                    pos.manual_exit_order_id = oid
                    pos.manual_exit_reason = str(reason or "manual_exit")
                    pos.manual_exit_requested_at = time.time()
                    pos.manual_exit_reference_price = float(reference_price or 0.0)
                    pos.sl_order_id = ""; pos.tp_order_id = ""
                logger.info(
                    "Protected strategy exit submitted [%s]: %s qty=%.8g reference=%s%.4f order=%s",
                    reason, exit_side.upper(), float(pos.quantity or 0.0), cur, float(reference_price or 0.0), oid[:12])
                return True
            restore = order_manager.place_stop_loss(
                side=exit_side, quantity=pos.quantity, trigger_price=pos.sl_price)
            restore_id = str((restore or {}).get("order_id") or "") if isinstance(restore, dict) else ""
            if restore_id:
                with self._lock:
                    pos.phase = PositionPhase.ACTIVE; pos.sl_order_id = restore_id
                logger.critical(
                    "Exit submission failed [%s]; protective SL restored at %s%.4f order=%s",
                    reason, cur, float(pos.sl_price or 0.0), restore_id[:12])
                return False
            logger.critical(
                "UNPROTECTED EXIT FAILURE [%s]: close not submitted and SL restore failed — manual intervention required", reason)
            return False
        except Exception as exc:
            logger.critical("Protected strategy exit exception [%s]: %s", reason, exc, exc_info=True)
            with self._lock:
                if not str(getattr(pos, "manual_exit_order_id", "") or ""):
                    pos.phase = PositionPhase.ACTIVE
            return False

    def _record_exchange_exit(self, ex_pos):
        """
        v5.1: Exchange-confirmed exit only. No price heuristics. No estimated fees.

        Calls identify_exit_order() which queries GET /v2/orders/{id} for both
        the SL and TP order IDs directly — state:"closed" + paid_commission from
        the exchange response. One retry after 1 s if both orders still show open
        (covers the sub-second propagation window between fill and state update).

        If tracked closing execution has not resolved after retries:
          - Realised P&L is NOT booked from position state, marks, or zero values
          - The lifecycle remains EXITING/pending exact reconciliation
          - If exposure remains open, protected emergency flattening is triggered

        When confirmed (normal path):
          - exit_type, fill_price, fee_paid all from exchange (exact)
          - Gross PnL computed from actual fill price using exact inverse-perp formula
          - Exit fee = paid_commission from Delta (exact USD)
          - Entry fee = commission_rate × entry_notional (exact rate, estimated value
            because we do not yet store paid_commission at entry order placement)
          - fee_breakdown.exact_fees = True signals that exit side is exact
        """
        pos = self._pos
        _cur = str(getattr(pos, "currency_symbol", "") or ("₹" if _icici_exchange_name(getattr(self, "_instrument", None)) == "icici" else "$"))
        if pos.phase == PositionPhase.FLAT:
            logger.debug("_record_exchange_exit skipped — already FLAT")
            return

        # ── ATOMIC EXIT CLAIM ──────────────────────────────────────────────
        # ROOT CAUSE OF DOUBLE NOTIFICATION (observed in logs):
        #   11:47:18.152  sync thread    → enters _record_exchange_exit, sees _exit_completed=False
        #   11:47:18.775  reconcile thread → enters _record_exchange_exit, sees _exit_completed=False
        #   11:47:18.906  sync thread    → finishes identify_exit_order, logs, sends telegram, records PnL
        #   11:47:19.659  reconcile thread → finishes identify_exit_order, logs AGAIN, sends telegram AGAIN
        #
        # The old guard (checking _exit_completed without a lock) was non-atomic:
        # both threads read False before either set True.  _exit_completed was only
        # set inside _record_pnl() which runs AFTER identify_exit_order() (~1s of I/O).
        #
        # FIX: Atomic claim under the lock. The FIRST thread to arrive sets
        # _exit_completed=True and proceeds. All others bail immediately.
        # This happens BEFORE any I/O, logging, or telegram sends.
        with self._lock:
            if self._exit_completed:
                logger.info(
                    "_record_exchange_exit skipped — exit already claimed by another thread "
                    f"(phase={pos.phase.name})")
                return
            self._exit_completed = True   # CLAIM: this thread owns the exit

        # ─── Step 1: Get exchange-confirmed exit data ──────────────────────────
        # Step 1A: manual/supervised-liquidity reduce-only exit.
        # If _exit_trade() submitted a protected close, SL/TP child ids were
        # cancelled and identify_exit_order() cannot identify the fired order.
        # Query the tracked market order first and book exact PnL from it.
        exit_info: Dict = {"confirmed": False}
        manual_exit_id = str(getattr(pos, 'manual_exit_order_id', '') or '').strip()
        if self._om is not None and manual_exit_id:
            for _i, _delay in enumerate([0.0, 1.0, 2.0, 3.0, 5.0]):
                if _delay > 0:
                    time.sleep(_delay)
                try:
                    details = self._om.get_fill_details(manual_exit_id) if hasattr(self._om, 'get_fill_details') else None
                    status = str((details or {}).get('status', '')).upper()
                    fill_price = float((details or {}).get('fill_price') or 0.0)
                    fee_paid = float((details or {}).get('paid_commission') or 0.0)
                    fee_exact = bool((details or {}).get('paid_commission_exact', False))
                    if status in ('FILLED', 'CLOSED') and fill_price > 0:
                        exit_info = {
                            'confirmed': True,
                            'exit_type': 'manual_exit',
                            'exit_reason': str(getattr(pos, 'manual_exit_reason', '') or 'manual_exit'),
                            'fill_price': fill_price,
                            'order_id': manual_exit_id,
                            'fee_paid': fee_paid,
                            'fee_exact': fee_exact,
                        }
                        logger.info(
                            f"✅ Manual exit confirmed on attempt {_i+1}: order={manual_exit_id[:10]}… "
                            f"fill={_cur}{fill_price:,.2f} fee={_cur}{fee_paid:.4f}")
                        break
                except Exception as e:
                    logger.error(f"manual exit order confirmation error attempt {_i+1}: {e}", exc_info=True)

        # Step 1B: exchange SL/TP child exit.
        # Query both order IDs directly. One retry after 1 s.
        if self._om is not None and not exit_info.get('confirmed'):
            try:
                exit_info = self._om.identify_exit_order(
                    sl_order_id  = pos.sl_order_id,
                    tp_order_id  = pos.tp_order_id,
                )
            except Exception as e:
                logger.error(f"identify_exit_order (attempt 1) error: {e}", exc_info=True)

            # v6.0: Exponential backoff retry — 4 additional attempts (1s, 2s, 3s, 5s)
            # Exchange state propagation can take up to 5-8s under load.
            # Old single 1s retry missed ~40% of confirmations (observed in prod logs).
            _retry_delays = [1.0, 2.0, 3.0, 5.0]
            for _retry_idx, _delay in enumerate(_retry_delays):
                if exit_info.get("confirmed"):
                    break
                time.sleep(_delay)
                try:
                    exit_info = self._om.identify_exit_order(
                        sl_order_id  = pos.sl_order_id,
                        tp_order_id  = pos.tp_order_id,
                        )
                    if exit_info.get("confirmed"):
                        logger.info(f"✅ Exit confirmed on retry {_retry_idx + 2} (after {_delay}s)")
                except Exception as e:
                    logger.error(f"identify_exit_order (retry {_retry_idx + 2}) error: {e}", exc_info=True)

        if not exit_info.get("confirmed"):
            # A position snapshot can tell us exposure is gone, but it cannot
            # prove the closing price or broker charges.  Realised P&L is booked
            # only after a tracked exit order resolves to an exact execution.
            _exchange_flat = False
            _ex_still_open = False
            try:
                _ex_pos = ex_pos
                if _ex_pos is None and self._om is not None:
                    _ex_pos = self._om.get_position() if hasattr(self._om, "get_position") else self._om.get_open_position()
                if _ex_pos is not None:
                    _ex_qty = abs(float(_ex_pos.get("size", _ex_pos.get("quantity", 0)) or 0.0))
                    _exchange_flat = _ex_qty < 1e-10
                    _ex_still_open = not _exchange_flat
            except Exception as _pos_e:
                logger.debug("Exit reconciliation position check unavailable: %s", _pos_e)

            if _ex_still_open:
                logger.critical(
                    "💀 EXIT UNCONFIRMED but exchange position is STILL OPEN — "
                    "refusing phantom FLAT and requesting protected emergency flatten")
                self._send_telegram(
                    "🚨 <b>EXIT UNCONFIRMED + EXCHANGE STILL OPEN</b>\n"
                    "Exposure remains open; protected close reconciliation is active.\n"
                    f"Entry: {_cur}{pos.entry_price:,.2f} | Side: {pos.side.upper()}")
                try:
                    if hasattr(self._om, "emergency_flatten"):
                        self._om.emergency_flatten(reason="exit_unconfirmed_still_open")
                    else:
                        logger.critical("No emergency_flatten method exists; keeping position ACTIVE for operator intervention")
                except Exception as _ef_e:
                    logger.error("emergency flatten raised: %s", _ef_e, exc_info=True)
                with self._lock:
                    self._exit_completed = False
                    self._pos.phase = PositionPhase.ACTIVE
                return

            with self._lock:
                pos.unconfirmed_exit_attempts = int(getattr(pos, "unconfirmed_exit_attempts", 0) or 0) + 1
                _notice_due = (time.time() - float(getattr(pos, "unconfirmed_exit_notice_at", 0.0) or 0.0)) >= 60.0
                if _notice_due:
                    pos.unconfirmed_exit_notice_at = time.time()
                self._pos.phase = PositionPhase.EXITING
                self._exit_completed = False
            if _notice_due:
                state = "FLAT" if _exchange_flat else "UNAVAILABLE"
                logger.warning(
                    "EXIT PENDING EXACT RECONCILIATION: exposure_state=%s, no confirmed fill price; "
                    "realised P&L not booked (attempt=%d)", state, pos.unconfirmed_exit_attempts)
                self._send_telegram(
                    "⚠️ <b>EXIT PENDING EXACT RECONCILIATION</b>\n"
                    f"Exposure state: {state}; tracked closing fill has not resolved.\n"
                    "Realised P&L is not recorded until broker execution price is confirmed.\n"
                    f"Entry: {_cur}{pos.entry_price:,.2f} | Side: {pos.side.upper()}")
            return

        # ─── Exchange-confirmed ─────────────────────────────────────────────────
        exit_type  = exit_info["exit_type"]          # "tp" | "sl" | "protective_sl"
        fill_price = float(exit_info["fill_price"])  # exact execution price
        fee_paid   = float(exit_info["fee_paid"])    # paid_commission from Delta
        fired_id   = exit_info["order_id"]
        if self._fee_engine is not None and fill_price > 0:
            try:
                expected_exit = pos.tp_price if exit_type == "tp" else (pos.sl_price if exit_type in ("sl", "protective_sl") else fill_price)
                if expected_exit > 0:
                    self._fee_engine.record_fill(
                        expected_exit, fill_price, leg="exit")
            except Exception as e:
                logger.debug(f"record exit fill error (non-fatal): {e}")

        if exit_type == "tp":
            exit_reason = "tp_hit";       is_tp_hit = True;  is_sl_hit = False
        elif exit_type == "protective_sl":
            exit_reason = "protective_sl_hit"; is_tp_hit = False; is_sl_hit = True
        elif exit_type == "manual_exit":
            # Preserve the broker-confirmed supervised-close reason. A locally
            # supervised ICICI liquidity target is economically a TP even though
            # the broker sees one explicit priced SELL-to-close after SL cancel.
            exit_reason = str(exit_info.get('exit_reason') or getattr(pos, 'manual_exit_reason', '') or 'manual_exit')
            is_tp_hit = exit_reason in ("liquidity_tp_hit", "tp_hit", "final_tp_hit")
            is_sl_hit = False
        else:
            exit_reason = "sl_hit";       is_tp_hit = False; is_sl_hit = True

        # Once the exchange has confirmed a full-position exit, cancel all
        # standalone internal TP ladder orders.  Delta auto-cancels the native
        # bracket peer, but not our independent reduce-only TP1..TPn orders.
        if exit_type in ("sl", "protective_sl", "tp", "manual_exit"):
            self._cleanup_tp_ladder_after_position_flat(self._om, pos, reason=exit_reason)

        _disp = (fired_id[:10] + "…") if len(fired_id) > 10 else fired_id
        _is_inverse = str(getattr(pos, "pnl_model", "linear") or "linear").lower() == "inverse_btcusd"
        logger.info(
            f"✅ Exit confirmed: {exit_reason} @ {_cur}{fill_price:,.2f} "
            f"fee={_cur}{fee_paid:.4f} order={_disp}"
        )

        # ─── Step 2: PnL — actual fill under the filled instrument's payoff model ─
        exit_fee_is_exact = bool(exit_info.get("fee_exact", False))
        if not exit_fee_is_exact:
            logger.warning(
                f"Exit fee not exact for {exit_reason}; no synthetic fee estimate will be booked. "
                "Delta order/fill commission should populate on retry/reconcile."
            )

        gross = gross_pnl_usd(
            pos.side,
            pos.entry_price,
            fill_price,
            pos.quantity,
            inverse=bool(_is_inverse and fill_price > 0),
        )

        # Fees are booked only when returned by the venue.  CoinSwitch and ICICI
        # charges must not be guessed using Delta/global commission constants.
        if not bool(getattr(pos, "entry_fee_exact", False)):
            entry_order_id = str(getattr(pos, "entry_order_id", "") or "").strip()
            if entry_order_id and hasattr(self._om, "get_fill_details"):
                try:
                    entry_details = self._om.get_fill_details(entry_order_id) or {}
                    recovered_exact = bool(entry_details.get("paid_commission_exact", False))
                    if recovered_exact:
                        pos.entry_fee_paid = float(entry_details.get("paid_commission", 0.0) or 0.0)
                        pos.entry_fee_exact = True
                        logger.info(
                            "Recovered exact entry fee before final P&L booking: %s %.4f",
                            str(getattr(pos, "currency_symbol", "$") or "$"),
                            float(pos.entry_fee_paid or 0.0),
                        )
                except Exception as _entry_fee_reconcile_e:
                    logger.debug("Entry fee reconcile skipped for %s: %s", entry_order_id[:10], _entry_fee_reconcile_e)
        _entry_fee_exact = float(getattr(pos, "entry_fee_paid", 0.0) or 0.0)
        entry_fee_is_exact = bool(getattr(pos, "entry_fee_exact", False) or abs(_entry_fee_exact) > 1e-12)
        entry_fee = _entry_fee_exact if entry_fee_is_exact else 0.0
        if not entry_fee_is_exact:
            logger.warning("Entry fee not broker-confirmed for %s:%s; lifecycle P&L is provisional until charges reconcile", getattr(pos, "exchange", "unknown"), getattr(pos, "execution_symbol", "unknown"))
        exit_fee = fee_paid if exit_fee_is_exact else 0.0
        _exit_tag = "exact" if exit_fee_is_exact else "pending"

        ladder_pnl = float(getattr(pos, 'tp_ladder_realized_pnl', 0.0) or 0.0)
        ladder_gross = float(getattr(pos, 'tp_ladder_realized_gross', 0.0) or 0.0)
        ladder_fees = float(getattr(pos, 'tp_ladder_realized_fees', 0.0) or 0.0)
        ladder_entry_fees = float(getattr(pos, 'tp_ladder_realized_entry_fees', 0.0) or 0.0)
        ladder_exit_fees = float(getattr(pos, 'tp_ladder_realized_exit_fees', 0.0) or 0.0)

        # Reporting/P&L fix: TP ladder partials already allocate their share of
        # the exact entry commission. The final residual leg must subtract only
        # the unallocated entry fee, otherwise scaled-out trades double-count
        # entry fees and profitable partials can be reported as losses.
        residual_entry_fee = entry_fee - ladder_entry_fees if entry_fee_is_exact else entry_fee
        residual_pnl = gross - residual_entry_fee - exit_fee
        pnl = residual_pnl + ladder_pnl

        _entry_tag = "exact" if entry_fee_is_exact else "pending"
        fee_breakdown: Dict = {
            "gross_pnl":  round(gross + ladder_gross, 4),
            "entry_fee":  round(entry_fee, 4),
            "entry_fee_residual": round(residual_entry_fee, 4),
            "exit_fee":   round(exit_fee, 4),
            "total_fees": round(residual_entry_fee + exit_fee + ladder_fees, 4),
            "exact_fees": entry_fee_is_exact and exit_fee_is_exact,
            "pnl_provisional": not (entry_fee_is_exact and exit_fee_is_exact),
            "fee_source": "broker_exact_only",
            "residual_pnl": round(residual_pnl, 4),
            "tp_ladder_realized_pnl": round(ladder_pnl, 4),
            "tp_ladder_gross": round(ladder_gross, 4),
            "tp_ladder_fees": round(ladder_fees, 4),
            "tp_ladder_entry_fees": round(ladder_entry_fees, 4),
            "tp_ladder_exit_fees": round(ladder_exit_fees, 4),
        }

        logger.info(
            f"📊 Exit price={_cur}{fill_price:,.2f} reason={exit_reason} "
            f"entry={_cur}{pos.entry_price:,.2f} residual_gross={_cur}{gross:+.4f} "
            f"entry_fee_total={_cur}{entry_fee:.4f}({_entry_tag}) residual_entry_fee={_cur}{residual_entry_fee:.4f} "
            f"exit_fee={_cur}{exit_fee:.4f}({_exit_tag}) ladder_net={_cur}{ladder_pnl:+.4f} lifecycle_net={_cur}{pnl:+.4f}"
        )

        # ─── Step 3: Record PnL and trade history ─────────────────────────────

        self._record_pnl(pnl, exit_reason=exit_reason, exit_price=fill_price,
                         fee_breakdown=fee_breakdown)

        # Bug #10 fix: call risk_manager.record_trade so RiskManager's own
        # counters (consecutive_losses, daily_pnl, winning_trades, last_trade_time)
        # are updated.  Without this call, risk_manager.can_trade() gates —
        # including the loss cooldown, daily loss %, and max consecutive losses —
        # always read stale zeros because record_trade was never invoked.
        # We pass pnl_override so risk_manager does not re-compute PnL from
        # prices (which would use the linear formula for an inverse-perp account).
        try:
            _rm = getattr(self, '_risk_manager_ref', None)
            if _rm is not None:
                if hasattr(_rm, 'set_position_open'):
                    _rm.set_position_open(False)
                _lifecycle_qty_for_record = float(
                    getattr(pos, "tp_ladder_initial_qty", 0.0)
                    or getattr(pos, "quantity", 0.0)
                    or 0.0
                )
                _rm.record_trade(
                    side         = pos.side,
                    entry_price  = pos.entry_price,
                    exit_price   = fill_price,
                    quantity     = _lifecycle_qty_for_record,
                    reason       = exit_reason,
                    pnl_override = pnl,
                    entry_leverage = float(getattr(pos, "entry_leverage", 0.0) or QCfg.LEVERAGE()),
                    pnl_model = str(getattr(pos, "pnl_model", "linear") or "linear"),
                    currency_code = str(getattr(pos, "currency_code", "USD") or "USD"),
                    quantity_unit = str(getattr(pos, "quantity_unit", "units") or "units"),
                )
        except Exception as _rm_rec_e:
            logger.debug(f"risk_manager.record_trade error (non-fatal): {_rm_rec_e}")

        # ─── Step 4: Telegram notification ────────────────────────────────────
        hold_min     = (time.time() - pos.entry_time) / 60.0 if pos.entry_time > 0 else 0.0
        init_sl_dist = (pos.initial_sl_dist if pos.initial_sl_dist > 1e-10
                        else abs(pos.entry_price - pos.sl_price))
        raw_pts      = ((fill_price - pos.entry_price) if pos.side == "long"
                        else (pos.entry_price - fill_price))
        achieved_r   = raw_pts / init_sl_dist if init_sl_dist > 1e-10 else 0.0

        if is_tp_hit:
            result_icon = "🎯"; result_label = "TP HIT";   result_color = "WIN ✅"
        elif is_sl_hit and pnl > 0:
            result_icon = "🔒"
            result_label = "SL HIT (protected)"
            result_color = "WIN ✅"
        else:
            result_icon = "🛑"; result_label = "SL HIT";   result_color = "LOSS ❌"

        mfe_r      = pos.peak_profit / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        tp_dist    = abs(pos.tp_price - pos.entry_price) if pos.tp_price > 0 else 0.0
        planned_rr = tp_dist / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        _orig_sl   = ((pos.entry_price - init_sl_dist) if pos.side == "long"
                      else (pos.entry_price + init_sl_dist))

        # v6.0: Margin-based P&L %
        _exit_margin_pct = 0.0
        _exit_margin_used = 0.0
        try:
            _exit_qty_for_margin = float(getattr(pos, "tp_ladder_initial_qty", 0.0) or getattr(pos, "quantity", 0.0) or 0.0)
            if pos.entry_price > 0 and _exit_qty_for_margin > 0:
                _exit_notional = pos.entry_price * _exit_qty_for_margin
                _exit_lev = max(float(getattr(pos, "entry_leverage", 0.0) or QCfg.LEVERAGE()), 1.0)
                _exit_margin_used = _exit_notional / _exit_lev if _exit_lev > 0 else _exit_notional
                if _exit_margin_used > 1e-10:
                    _exit_margin_pct = (pnl / _exit_margin_used) * 100.0
        except Exception:
            pass

        _lifecycle_qty = float(getattr(pos, "tp_ladder_initial_qty", 0.0) or getattr(pos, "quantity", 0.0) or 0.0)
        _residual_qty = float(getattr(pos, "quantity", 0.0) or 0.0)
        _partial_qty = max(0.0, _lifecycle_qty - _residual_qty)
        _exit_cur = _cur
        _fee_source = (
            f"entry total {_entry_tag} {_exit_cur}{entry_fee:.4f} · residual entry {_exit_cur}{residual_entry_fee:.4f} · "
            f"ladder fees {_exit_cur}{ladder_fees:.4f} · final exit {_exit_tag} {_exit_cur}{exit_fee:.4f}"
        )
        self._send_telegram(
            format_exit_alert(
                side=pos.side,
                entry=pos.entry_price,
                exit_price=fill_price,
                pnl=pnl,
                r_realised=achieved_r,
                mfe_r=mfe_r,
                reason=exit_reason,
                hold_min=hold_min,
                fees=residual_entry_fee + exit_fee + ladder_fees,
                qty=_lifecycle_qty,
                gross=gross + ladder_gross,
                raw_pts=raw_pts,
                planned_rr=planned_rr,
                margin_pct=_exit_margin_pct,
                margin_used=_exit_margin_used,
                fee_source=_fee_source,
                exact_fees=entry_fee_is_exact and exit_fee_is_exact,
                exit_model="structural SL + opposing-liquidity targets",
                portfolio_pnl=self._total_pnl,
                portfolio_open=0,
                residual_qty=_residual_qty,
                partial_qty=_partial_qty,
                tp_ladder_net=ladder_pnl,
                tp_ladder_gross=ladder_gross,
                tp_ladder_fees=ladder_fees,
                residual_net=residual_pnl,
                instrument=getattr(self, "_instrument", None),
            ),
            event_type="exit",
        )
        self._last_exit_side = pos.side
        self._finalise_exit()

    def _record_pnl(self, pnl: float, exit_reason: str = "unknown",
                    exit_price: float = 0.0,
                    fee_breakdown: Optional[Dict] = None) -> bool:
        """Book one broker-resolved close into the venue-scoped realised ledger."""
        pos = self._pos
        entry_time = float(getattr(pos, "entry_time", 0.0) or 0.0)
        if entry_time > 0 and abs(self._pnl_recorded_for - entry_time) < 0.001:
            logger.warning("Realised P&L duplicate suppressed for entry_time=%.3f", entry_time)
            return False
        self._pnl_recorded_for = entry_time
        self._total_trades += 1
        self._total_pnl += float(pnl)
        is_win = float(pnl) > 0.0
        if is_win:
            self._winning_trades += 1
        self._risk_gate.record_trade_result(float(pnl))
        fb = dict(fee_breakdown or {})
        initial_risk = float(getattr(pos, "initial_sl_dist", 0.0) or 0.0)
        hold_min = (time.time() - entry_time) / 60.0 if entry_time > 0 else 0.0
        lifecycle_qty = float(getattr(pos, "tp_ladder_initial_qty", 0.0) or getattr(pos, "quantity", 0.0) or 0.0)
        residual_qty = float(getattr(pos, "quantity", 0.0) or 0.0)
        margin = 0.0
        try:
            margin = float(pos.entry_price) * lifecycle_qty / max(float(getattr(pos, "entry_leverage", 1.0) or 1.0), 1.0)
        except Exception:
            margin = 0.0
        quality = dict(getattr(pos, "quant_components", {}) or {})
        record = {
            "timestamp": time.time(), "currency": str(getattr(pos, "currency_symbol", "$") or "$"),
            "currency_code": str(getattr(pos, "currency_code", "USD") or "USD"),
            "asset": str(getattr(pos, "asset_id", "") or self._asset_id),
            "exchange": str(getattr(pos, "exchange", "") or ""),
            "execution_symbol": str(getattr(pos, "execution_symbol", "") or QCfg.SYMBOL()),
            "symbol": str(getattr(pos, "execution_symbol", "") or QCfg.SYMBOL()),
            "side": str(getattr(pos, "side", "") or ""), "mode": "INSTITUTIONAL_AUCTION_V514",
            "entry": float(getattr(pos, "entry_price", 0.0) or 0.0), "exit": float(exit_price or 0.0),
            "qty": lifecycle_qty, "residual_qty": residual_qty,
            "partial_qty": max(0.0, lifecycle_qty - residual_qty),
            "sl": float(getattr(pos, "sl_price", 0.0) or 0.0),
            "tp": float(getattr(pos, "tp_price", 0.0) or 0.0), "pnl": float(pnl), "is_win": is_win,
            "reason": exit_reason, "hold_min": hold_min, "margin_pnl_pct": (float(pnl) / margin * 100.0 if margin > 0 else 0.0),
            "pnl_model": str(getattr(pos, "pnl_model", "linear") or "linear"),
            "gross_pnl": fb.get("gross_pnl", pnl), "entry_fee": fb.get("entry_fee", 0.0),
            "exit_fee": fb.get("exit_fee", 0.0), "total_fees": fb.get("total_fees", 0.0),
            "exact_fees": bool(fb.get("exact_fees", False)), "pnl_provisional": bool(fb.get("pnl_provisional", False)),
            "fee_source": fb.get("fee_source", "unknown"),
            "context_4h": float(quality.get("context_4h", getattr(pos, "entry_htf_4h", 0.0)) or 0.0),
            "context_15m": float(quality.get("context_15m", getattr(pos, "entry_htf_15m", 0.0)) or 0.0),
            "raid_quality": float(quality.get("raid_quality", 0.0) or 0.0),
            "displacement_atr": float(quality.get("displacement_atr", 0.0) or 0.0),
            "delivery_score": float(getattr(pos, "delivery_score", quality.get("delivery_score", 0.0)) or 0.0),
            "probability_calibrated": bool(getattr(pos, "probability_calibrated", quality.get("probability_calibrated", False))),
            "delivery_probability": float(getattr(pos, "delivery_probability", quality.get("delivery_probability", 0.0)) or 0.0) if bool(getattr(pos, "probability_calibrated", quality.get("probability_calibrated", False))) else None,
            "delivery_utility_r": float(getattr(pos, "delivery_utility_r", quality.get("delivery_utility_r", 0.0)) or 0.0) if bool(getattr(pos, "probability_calibrated", quality.get("probability_calibrated", False))) else None,
            "archetype": str(getattr(pos, "archetype", quality.get("archetype", "STRUCTURAL_AUCTION")) or "STRUCTURAL_AUCTION"),
            "analysis_entry_price": float(getattr(pos, "analysis_entry_price", 0.0) or 0.0),
            "analysis_sl_price": float(getattr(pos, "analysis_sl_price", 0.0) or 0.0),
            "analysis_tp_price": float(getattr(pos, "analysis_tp_price", 0.0) or 0.0),
        }
        self._trade_history.append(record)
        return True

    def _finalise_exit(self):
        if self._entry_engine is not None:
            self._entry_engine.on_position_closed()
        if self._liq_map is not None:
            try:
                self._liq_map.reset_snapshot()
            except Exception as exc:
                logger.debug("Liquidity snapshot reset skipped: %s", exc)
        try:
            rm = getattr(self, "_risk_manager_ref", None)
            if rm is not None and hasattr(rm, "set_position_open"):
                rm.set_position_open(False)
        except Exception as exc:
            logger.debug("risk-manager flat notification skipped: %s", exc)
        if _icici_exchange_name(getattr(self, "_instrument", None)) == "icici":
            try:
                releaser = getattr(getattr(self, "_dm", None), "release_icici_execution_vehicle", None)
                if callable(releaser):
                    releaser()
            except Exception as exc:
                logger.exception("ICICI execution-vehicle release failed: %s", exc)
        self._pos = PositionState()
        self._last_exit_time = time.time()
        self.current_sl_price = 0.0
        self.current_tp_price = 0.0
        self._last_structure_fingerprint = None
        def _clear_claim_window():
            with self._lock:
                if self._pos.phase == PositionPhase.FLAT:
                    self._exit_completed = False
        try:
            timer = threading.Timer(20.0, _clear_claim_window)
            timer.daemon = True
            timer.start()
        except Exception:
            pass
        logger.info("Position closed — FLAT; exact-fill ledger preserved")

    def _roundtrip_cost_points(self, price: float, use_maker_entry: bool) -> Tuple[float, float]:
        fee_engine = getattr(self, "_fee_engine", None)
        if fee_engine is not None and hasattr(fee_engine, "effective_roundtrip_cost_bps"):
            try:
                bps = float(fee_engine.effective_roundtrip_cost_bps(use_maker_entry=use_maker_entry))
                if math.isfinite(bps) and bps >= 0.0:
                    return price * bps / 10_000.0, bps
            except Exception:
                pass

        if use_maker_entry:
            entry_rate = float(getattr(
                config, "DELTA_COMMISSION_RATE_MAKER",
                getattr(config, "COMMISSION_RATE_MAKER", 0.00020),
            ))
        else:
            entry_rate = float(getattr(
                config, "DELTA_COMMISSION_RATE",
                getattr(config, "COMMISSION_RATE", 0.00055),
            ))
        bps = max(0.0, (entry_rate + _stop_exit_fee_rate()) * 10_000.0)
        return price * bps / 10_000.0, bps

    def _execution_viability_model(
        self,
        *,
        side: str,
        price: float,
        sl_price: float,
        tp_price: float = 0.0,
        use_maker_entry: bool = True,
        delivery_probability: Optional[float] = None,
    ) -> ExecutionViability:
        side_l = str(side or "").lower()
        route = "maker" if use_maker_entry else "taker"
        price_f = max(float(price or 0.0), 0.0)
        sl_f = max(float(sl_price or 0.0), 0.0)
        tp_f = max(float(tp_price or 0.0), 0.0)
        sl_dist = abs(price_f - sl_f) if price_f > 0 and sl_f > 0 else 0.0
        reward = abs(tp_f - price_f) if price_f > 0 and tp_f > 0 else 0.0

        rt_cost_pts, rt_cost_bps = self._roundtrip_cost_points(price_f, use_maker_entry)
        fee_soft = float(getattr(config, "FEE_TO_RISK_SOFT_MAX", 0.35))
        fee_no_alloc = max(
            fee_soft + 1e-6,
            float(getattr(config, "FEE_TO_RISK_NO_ALLOC", 0.75)),
        )
        fee_to_risk = rt_cost_pts / max(sl_dist, 1e-9)
        min_viable_sl_dist = rt_cost_pts / max(fee_no_alloc, 1e-9)
        geometry_gap = max(0.0, min_viable_sl_dist - sl_dist)

        if side_l == "long":
            required_sl = price_f - min_viable_sl_dist
            required_entry = sl_f + min_viable_sl_dist
        elif side_l == "short":
            required_sl = price_f + min_viable_sl_dist
            required_entry = sl_f - min_viable_sl_dist
        else:
            required_sl = 0.0
            required_entry = 0.0

        utility_known = False
        delivery_p = None
        net_win_r = 0.0
        net_loss_r = 0.0
        net_delivery_utility = 0.0
        if reward > 0.0 and sl_dist > 1e-9:
            net_win_r = (reward - rt_cost_pts) / sl_dist
            net_loss_r = (sl_dist + rt_cost_pts) / sl_dist
            if delivery_probability is not None:
                try:
                    delivery_p = max(0.01, min(0.99, float(delivery_probability)))
                    utility_known = True
                    net_delivery_utility = delivery_p * net_win_r - (1.0 - delivery_p) * net_loss_r
                except Exception:
                    utility_known = False
                    delivery_p = None

        allocation_allowed = price_f > 0.0 and sl_dist > 1e-9 and side_l in ("long", "short")
        if reward <= 0.0:
            allocation_allowed = False
            reason = "missing executable reward target"
        elif net_win_r <= 0.0:
            allocation_allowed = False
            reason = "non-positive net reward after execution costs"
        elif fee_to_risk >= fee_no_alloc:
            allocation_allowed = False
            reason = "extreme execution-cost drag; no allocation"
        elif fee_to_risk > fee_soft:
            reason = "fee drag above soft band; allocate with execution haircut"
        else:
            reason = "execution geometry viable"
        if price_f <= 0.0 or sl_dist <= 1e-9 or side_l not in ("long", "short"):
            reason = "invalid executable geometry"

        return ExecutionViability(
            route=route,
            side=side_l,
            price=price_f,
            sl_price=sl_f,
            tp_price=tp_f,
            current_sl_dist=sl_dist,
            reward_dist=reward,
            round_trip_cost_pts=rt_cost_pts,
            round_trip_cost_bps=rt_cost_bps,
            fee_to_risk=fee_to_risk,
            fee_soft=fee_soft,
            fee_no_alloc=fee_no_alloc,
            min_viable_sl_dist=min_viable_sl_dist,
            geometry_gap_pts=geometry_gap,
            required_sl_price=required_sl,
            required_entry_price=required_entry,
            delivery_probability=delivery_p,
            net_win_r=net_win_r,
            net_loss_r=net_loss_r,
            expected_net_utility_r=net_delivery_utility,
            utility_known=utility_known,
            allocation_allowed=allocation_allowed,
            reason=reason,
        )

    def _repair_execution_geometry(
        self, side: str, entry_price: float, sl_price: float, tp_price: float,
        atr: float, use_maker_entry: bool, delivery_probability: Optional[float],
    ) -> Tuple[float, float, bool]:
        """Do not modify ICT geometry after approval; execution costs shape size."""
        viability = self._execution_viability_model(side=side, price=entry_price, sl_price=sl_price, tp_price=tp_price, use_maker_entry=use_maker_entry, delivery_probability=delivery_probability)
        self._last_execution_viability = viability.as_refine_context()
        if not viability.allocation_allowed:
            logger.info("STRUCTURAL_AUCTION execution unavailable: invalid executable geometry | %s", viability.reason)
        return sl_price, tp_price, False

    def _compute_quantity(self, risk_manager, price,
                           sig: Optional[StructuralEntrySummary] = None,
                           setup_grade: str = "",
                           sl_price: Optional[float] = None,
                           prefetched_bal_info: dict = None,
                           side: str = "",
                           tp_price: float = 0.0,
                           use_maker_entry: bool = False,
                           delivery_probability: Optional[float] = None) -> Optional[float]:
        """Allocate an approved auction thesis from structural invalidation risk.

        Allocation is bounded by live free cash, venue lot rules, bracket SL
        distance, measured cost, leverage/liquidation safety and account limits.
        A replay-calibrated probability may reduce risk only when supplied.
        """
        # ── SL distance guard — required for risk-based sizing ────────────────
        if sl_price is None or sl_price <= 0:
            logger.warning("_compute_quantity: sl_price required for risk-based sizing — aborting")
            return None
        sl_dist = abs(price - sl_price)
        if sl_dist < 1e-8:
            logger.warning(f"_compute_quantity: sl_dist={sl_dist:.2f} too small — aborting")
            return None

        configured_step = max(float(QCfg.LOT_STEP()), 1e-12)
        step = configured_step
        try:
            if "delta" in QCfg.EXCHANGE().lower():
                contract_step = float(getattr(config, "DELTA_CONTRACT_VALUE_BTC", 0.0) or 0.0)
                if contract_step > 0.0:
                    step = max(configured_step, contract_step)
        except Exception:
            pass
        min_qty = max(float(QCfg.MIN_QTY()), step)
        max_qty = max(min_qty, float(QCfg.MAX_QTY()))
        leverage = max(float(QCfg.LEVERAGE()), 1.0)
        _inst_for_sizing = getattr(self, "_instrument", None)
        is_icici_option = _is_icici_option_instrument(_inst_for_sizing) or _is_icici_underlying_chain_instrument(_inst_for_sizing)
        if is_icici_option:
            lot = _icici_runtime_lot_size(_inst_for_sizing)
            if lot <= 0:
                logger.critical("_compute_quantity: ICICI option contract lacks verified NFO lot size — no allocation")
                return None
            step = max(step, lot)
            min_qty = max(min_qty, lot)
            max_qty = max(min_qty, float(_cfg("ICICI_OPTION_MAX_QTY", 1000000.0)))
            leverage = 1.0

        # ── Institutional risk boundary ───────────────────────────────────────
        # Evidence decides whether a structure exists; it cannot scale capital as
        # though it were a calibrated win probability.  Capital is reduced only
        # by observable venue cost/spread impairment, desk risk policy, and an
        # explicitly calibrated probability model when one is supplied.
        calibrated_p = delivery_probability if delivery_probability is not None else None
        spread_cost_mult = float(getattr(self, "_active_spread_cost_mult", 1.0) or 1.0)
        try:
            policy_risk_mult = float(policy_value("risk_multiplier", 1.0))
        except Exception:
            policy_risk_mult = 1.0
        probability_scalar = (self._capital_allocation_scalar(calibrated_p, 1.0) if calibrated_p is not None else 1.0)
        auction_scalar = max(0.05, min(1.0, float(getattr(self, "_active_auction_risk_scalar", 1.0) or 1.0)))
        allocation_scalar = max(0.05, min(1.0, probability_scalar * spread_cost_mult * policy_risk_mult * auction_scalar))
        logger.info(
            "Auction capital posture phase=%s posture=%s risk_scalar=%.2f execution_cost_scalar=%.2f policy_scalar=%.2f allocation_scalar=%.2f",
            str(getattr(self, "_active_market_phase", "UNCLASSIFIED")),
            str(getattr(self, "_active_auction_posture", "NORMAL")),
            auction_scalar, spread_cost_mult, policy_risk_mult, allocation_scalar,
        )

        # ── Available balance (reuse prefetched — SIG-8 fix) ─────────────────
        bal = prefetched_bal_info if prefetched_bal_info is not None else risk_manager.get_available_balance()
        if bal is None:
            logger.warning("_compute_quantity: get_available_balance returned None")
            return None
        # In multi-asset mode `available` is live-free-cash aware, not an
        # arbitrary equal-slot allocation.  Dollar-risk reporting can still use
        # the portfolio risk base carried by PortfolioRiskManager; margin must
        # be allocated from actual free cash.
        available = float(bal.get("available", 0.0))
        portfolio_scoped = bool(bal.get("portfolio_scoped", False))
        try:
            cash_available = float(bal.get("available_raw", available) if portfolio_scoped else available)
        except Exception:
            cash_available = available
        if not math.isfinite(cash_available) or cash_available <= 0.0:
            cash_available = available
        try:
            # Margin budget must be based on currently available/free funds, not
            # total equity.  Total/risk equity remains available below for risk
            # reporting, but new margin cannot be allocated from locked funds.
            margin_budget_base = float(
                bal.get("available_raw" if portfolio_scoped else "available", available)
                or cash_available
            )
        except Exception:
            margin_budget_base = cash_available
        if not math.isfinite(margin_budget_base) or margin_budget_base <= 0.0:
            margin_budget_base = cash_available
        try:
            risk_available = float(
                bal.get("risk_available",
                        bal.get("total_raw" if portfolio_scoped else "available", available))
                or 0.0
            )
        except Exception:
            risk_available = available
        if not math.isfinite(risk_available) or risk_available <= 0.0:
            risk_available = available
        if cash_available < QCfg.MIN_MARGIN_USDT():
            logger.warning(
                f"_compute_quantity: available {available:.2f} < "
                f"MIN_MARGIN_USDT {QCfg.MIN_MARGIN_USDT():.2f}"
            )
            return None
        # ── Execution-cost reserve ─────────────────────────────────────
        # Reserve: charge a conservative 2× the live taker rate (entry taker
        # worst-case + exit taker) plus a 15 % safety margin for slippage
        # variance.  For a 446-unit notional at COMMISSION_RATE=0.00055 this
        # reserves about 0.56 units — enough to clear Delta's internal commission check
        # with room to spare.  On a 30× leveraged account this represents <1 %
        # of the margin, a negligible position-size reduction for the safety.
        # We don't know qty yet, but we can compute a conservative reserve
        # from the max possible qty given available:
        #   max_notional ≈ available × leverage (all balance as margin)
        #   fee_reserve  ≈ max_notional × taker_rate × 2 × 1.15
        exec_side = side or str(getattr(getattr(self, "_last_entry_signal", None), "side", "") or "")
        viability = self._execution_viability_model(
            side=exec_side,
            price=price,
            sl_price=sl_price,
            tp_price=tp_price,
            use_maker_entry=use_maker_entry,
            delivery_probability=delivery_probability,
        )
        self._last_execution_viability = viability.as_refine_context()
        fee_to_risk = viability.fee_to_risk
        fee_soft = viability.fee_soft
        fee_no_alloc = viability.fee_no_alloc
        fee_drag_mult = 1.0

        if not viability.allocation_allowed:
            eu = (
                f" EU={viability.expected_net_utility_r:.2f}R"
                if viability.utility_known else " EU=unknown"
            )
            logger.info(
                f"No allocation: execution geometry invalid | route={viability.route} "
                f"fee_to_risk={fee_to_risk:.2f}R >= {fee_no_alloc:.2f}R | "
                f"rt_cost={viability.round_trip_cost_pts:.1f}pts "
                f"SL-dist={sl_dist:.1f}pts required={viability.min_viable_sl_dist:.1f}pts "
                f"gap={viability.geometry_gap_pts:.1f}pts | "
                f"repricing: structural_SL={viability.required_sl_price:,.1f} "
                f"or entry={viability.required_entry_price:,.1f}{eu}")
            return None

        if fee_to_risk > fee_soft:
            fee_drag_mult = max(
                0.50,
                1.0 - ((fee_to_risk - fee_soft) / (fee_no_alloc - fee_soft)) * 0.80,
            )
            allocation_scalar *= fee_drag_mult
            logger.info(
                f"Execution-cost allocation haircut: fee_to_risk={fee_to_risk:.2f}R "
                f"soft={fee_soft:.2f} no_alloc={fee_no_alloc:.2f} "
                f"size_mult*={fee_drag_mult:.2f}")

        # ── Structural risk allocation ────────────────────────────────────
        # risk_pct: fraction of allocated margin to risk at SL (e.g. 0.015 = 1.5%)
        raw_risk_pct = float(_cfg("RISK_PER_TRADE", 0.006))
        risk_pct = self._risk_pct_fraction(raw_risk_pct)
        if risk_pct <= 0.0:
            logger.warning(f"Sizing rejected: invalid RISK_PER_TRADE={raw_risk_pct!r}")
            return None
        if raw_risk_pct > 0.05 and raw_risk_pct <= 5.0:
            logger.warning(
                f"RISK_PER_TRADE={raw_risk_pct:.4f} looks percent-style; "
                f"interpreting as {risk_pct:.3%}")
        elif raw_risk_pct > 5.0:
            logger.warning(
                f"RISK_PER_TRADE={raw_risk_pct:.4f} exceeds 5% hard cap; "
                "clamping to 5.000%")

        # ── Dynamic available-funds margin envelope ──────────────────────────
        # One sizing authority only:
        #   • policy margin_pct / QUANT_MARGIN_PCT defines the dynamic target
        #     margin for the active desk/instrument.
        #   • exchange free cash proves feasibility; open brackets naturally
        #     reduce free cash instead of a fixed equal-slot/hard-dollar cap.
        #   • no arbitrary dollar minimum is required; exchange min_qty/step
        #     controls executability.
        try:
            policy_margin_frac = float(QCfg.MARGIN_PCT())
        except Exception:
            policy_margin_frac = float(_cfg("QUANT_MARGIN_PCT", 0.20))
        if policy_margin_frac > 1.0 and policy_margin_frac <= 100.0:
            policy_margin_frac = policy_margin_frac / 100.0
        policy_margin_frac = max(0.0, min(1.0, policy_margin_frac))

        min_trade_margin = max(0.0, float(QCfg.MIN_MARGIN_USDT()))

        policy_target_margin = margin_budget_base * policy_margin_frac
        margin_capacity = cash_available

        # Measured execution costs and an optional replay-calibrated model can
        # reduce allocation, but evidence scores cannot increase or decrease risk.
        allocation_intensity = self._capital_allocation_scalar(calibrated_p, fee_drag_mult)
        allocation_intensity = max(0.05, min(1.0, allocation_intensity * spread_cost_mult * policy_risk_mult))
        allocation_scalar = min(allocation_scalar, allocation_intensity)
        target_margin_budget = min(margin_capacity, policy_target_margin * allocation_intensity)
        effective_risk_pct = risk_pct
        target_risk_base = target_margin_budget
        risk_capital = target_risk_base * effective_risk_pct

        # ── Capital-efficient structural leverage ────────────────────────────
        # Select only the leverage required to fund the structural risk quantity
        # inside allocated margin.  It is then clipped by venue and liquidation
        # safety caps; there is no fixed leverage floor or ROE target.
        configured_leverage = max(float(QCfg.LEVERAGE()), 1.0)
        liquidation_leverage_cap = self._liquidation_safe_leverage_cap(
            exec_side, price, sl_price, configured_leverage=configured_leverage,
        )
        required_capital_leverage = max(
            1.0,
            ((risk_capital / max(sl_dist, 1e-12)) * price) / max(target_margin_budget, 1e-12),
        ) if target_margin_budget > 0.0 and risk_capital > 0.0 else 1.0
        if liquidation_leverage_cap <= 0.0:
            logger.warning(
                f"Sizing rejected: SL is beyond liquidation guard | "
                f"risk_budget={risk_capital:.2f} risk_pct={effective_risk_pct:.3%} "
                f"price={price:.2f} SL-dist={sl_dist:.2f}pts exchange_max={configured_leverage:.0f}x")
            return None
        effective_leverage = self._structural_funding_leverage(
            price, sl_dist, configured_leverage=configured_leverage,
            side=exec_side, sl_price=sl_price,
            target_margin_budget=target_margin_budget, risk_capital=risk_capital,
        )
        if not math.isfinite(effective_leverage) or effective_leverage < 1.0:
            logger.warning(
                f"Sizing rejected: no executable capital-efficient leverage | "
                f"required={required_capital_leverage:.2f}x liq_cap={liquidation_leverage_cap:.1f}x "
                f"exchange_max={configured_leverage:.0f}x")
            return None
        leverage = float(effective_leverage)
        self._active_effective_leverage = float(effective_leverage)
        self._active_margin_risk_pct = (sl_dist * leverage / price) if price > 0 else 0.0

        if target_margin_budget <= 1e-9:
            logger.warning(
                f"Sizing rejected: dynamic margin target is zero | "
                f"cash_available={cash_available:.2f} available={available:.2f} "
                f"policy_margin={policy_margin_frac:.1%} margin_base={margin_budget_base:.2f} "
                f"allocation_intensity={allocation_intensity:.2f}")
            return None
        if margin_capacity <= 1e-9:
            logger.warning(
                f"Sizing rejected: no live margin capacity | "
                f"cash_available={cash_available:.2f} available={available:.2f}")
            return None

        taker_rate = abs(float(_cfg("COMMISSION_RATE", 0.00055)))
        reserve_fee_per_btc = max(
            float(viability.round_trip_cost_pts),
            price * taker_rate * 2.0 * 1.15,
        )
        margin_per_btc = price / leverage
        cash_per_btc = margin_per_btc + reserve_fee_per_btc

        # Cash feasibility uses live exchange free cash.  Margin feasibility uses
        # the dynamic margin capacity, so fees cannot eat the margin budget.
        max_qty_cash = math.floor((cash_available / max(cash_per_btc, 1e-12)) / step) * step
        max_qty_margin = math.floor(((margin_capacity * leverage / price) / step)) * step
        executable_qty_cap = round(max(0.0, min(max_qty, max_qty_cash, max_qty_margin)), 8)
        if executable_qty_cap < min_qty:
            logger.warning(
                f"Sizing rejected: cash/margin/lot envelope below minimum | "
                f"cap_qty={executable_qty_cap:.8f} min_qty={min_qty:.8f} "
                f"target_margin={target_margin_budget:.2f} margin_capacity={margin_capacity:.2f} "
                f"cash_available={cash_available:.2f} slot_available={available:.2f} step={step:.8f}")
            return None

        # Risk capital is fixed before leverage selection and quantity rounding;
        # leverage only governs funded margin, never the approved stop-loss budget.
        qty_by_risk = risk_capital / sl_dist
        qty_by_target_margin = target_margin_budget * leverage / price
        qty_raw = min(qty_by_risk, qty_by_target_margin, executable_qty_cap)
        max_allowed_margin = margin_capacity
        min_lot_risk_mult = float(_cfg("PORTFOLIO_MIN_LOT_MAX_RISK_MULT", 1.15))
        max_risk_cap = max(
            risk_capital * 1.15,
            target_margin_budget * effective_risk_pct * max(1.0, min_lot_risk_mult),
        )

        # ── Lot-step + hard limits ────────────────────────────────────────────
        def _floor_step(q: float) -> float:
            return round(math.floor(max(q, 0.0) / step + 1e-12) * step, 8)

        qty = _floor_step(qty_raw)
        if qty < min_qty - 1e-12:
            # Minimum-lot rescue is allowed only if that lot also satisfies the
            # configured minimum margin and the hard raw-risk cap.  This removes
            # the old dust fallback that allowed dust-size positions through.
            rescue_qty = round(min_qty, 8)
            rescue_margin = rescue_qty * price / leverage
            rescue_cash = rescue_margin + rescue_qty * reserve_fee_per_btc
            rescue_risk = rescue_qty * sl_dist
            if (rescue_qty <= executable_qty_cap + 1e-12 and
                    rescue_margin >= min_trade_margin - 1e-9 and
                    rescue_margin <= max_allowed_margin + 1e-9 and
                    rescue_cash <= cash_available + 1e-9 and
                    rescue_risk <= max_risk_cap + 1e-9):
                qty = rescue_qty
            else:
                logger.warning(
                    f"Sizing rejected: dynamic allocation below exchange lot/min margin | "
                    f"raw_qty={qty_raw:.6f} risk_qty={qty_by_risk:.6f} "
                    f"margin_qty={qty_by_target_margin:.6f} min_qty={min_qty:.8f} "
                    f"target_risk={risk_capital:.2f} hard_risk_cap={max_risk_cap:.2f} "
                    f"target_margin={target_margin_budget:.2f} min_margin={min_trade_margin:.2f}")
                return None

        # If floor-to-step put the allocation below the configured trade-margin
        # floor, try the smallest lot that reaches the floor, but never violate
        # risk, cash, margin cap, or exchange max.
        margin_used_pre = qty * price / leverage
        if margin_used_pre < min_trade_margin - 1e-9:
            qty_for_min_margin = _floor_step(math.ceil((min_trade_margin * leverage / price) / step - 1e-12) * step)
            if qty_for_min_margin < (min_trade_margin * leverage / price) - 1e-12:
                qty_for_min_margin = round(qty_for_min_margin + step, 8)
            cand = qty_for_min_margin
            cand_margin = cand * price / leverage
            cand_cash = cand_margin + cand * reserve_fee_per_btc
            cand_risk = cand * sl_dist
            if (cand >= min_qty - 1e-12 and cand <= executable_qty_cap + 1e-12 and
                    cand_margin <= max_allowed_margin + 1e-9 and
                    cand_cash <= cash_available + 1e-9 and
                    cand_risk <= max_risk_cap + 1e-9):
                qty = cand
            else:
                logger.warning(
                    f"Sizing rejected: allocation below configured minimum margin | "
                    f"qty={qty:.8f} margin={margin_used_pre:.2f} min_margin={min_trade_margin:.2f} "
                    f"needed_qty={cand:.8f} needed_risk={cand_risk:.2f} "
                    f"hard_risk_cap={max_risk_cap:.2f} hard_margin_cap={max_allowed_margin:.2f} "
                    f"target_margin={target_margin_budget:.2f}")
                return None

        # ── Margin feasibility guard ─────────────────────────────────────────
        # This is not an arbitrary per-trade ceiling.  It only proves the order
        # can be funded by live free cash after fees.
        required_margin = qty * price / leverage
        required_cash = required_margin + qty * reserve_fee_per_btc
        if required_margin > max_allowed_margin:
            logger.warning(
                f"Sizing guard: required margin {required_margin:.2f} > "
                f"dynamic margin capacity {max_allowed_margin:.2f} "
                f"(target={target_margin_budget:.2f}, cash_available={cash_available:.2f})"
            )
            return None

        if min_trade_margin > 0.0 and required_margin < min_trade_margin - 1e-9:
            logger.warning(
                f"Sizing guard: required margin {required_margin:.2f} < "
                f"configured minimum {min_trade_margin:.2f} — rejecting dust allocation")
            return None

        if required_cash > cash_available + 1e-9:
            logger.warning(
                f"Sizing guard: margin+fees {required_cash:.2f} > "
                f"cash available {cash_available:.2f} (slot={available:.2f})")
            return None

        if qty < min_qty:
            return None

        # ── Dollar-risk verification ──────────────────────────────────────────
        dollar_risk   = sl_dist * qty
        risk_pct_act  = dollar_risk / target_risk_base * 100.0 if target_risk_base > 0 else 0.0
        slot_risk_pct = dollar_risk / available * 100.0 if available > 0 else 0.0
        margin_used   = qty * price / leverage
        actual_fees   = qty * viability.round_trip_cost_pts
        if dollar_risk > max_risk_cap:
            logger.warning(
                f"Sizing rejected: exchange min lot would over-risk account | "
                f"qty={qty} SL-dist={sl_dist:.1f}pts risk={dollar_risk:.2f} "
                f"cap={max_risk_cap:.2f} ({risk_pct_act:.2f}% of risk_base {target_risk_base:.2f}; "
                f"{slot_risk_pct:.2f}% of slot {available:.2f})")
            return None

        logger.info(
            f"✅ Sizing [ict_liquidity_structural_risk] | "
            f"RISK_BASE={risk_pct:.3%} RISK_EFF={effective_risk_pct:.3%} | "
            f"calibratedP={calibrated_p if calibrated_p is not None else 'N/A'} "
            f"allocation_scalar={allocation_scalar:.2f} allocation_intensity={allocation_intensity:.2f} "
            f"(probability_control={'ACTIVE' if calibrated_p is not None else 'OFF'} fee={fee_drag_mult:.2f}) | "
            f"target_risk={risk_capital:.2f} risk_qty={qty_by_risk:.4f} "
            f"margin_qty={qty_by_target_margin:.4f} raw_qty={qty_raw:.4f} | "
            f"SL-dist={sl_dist:.1f}pts | risk={dollar_risk:.2f} "
            f"({risk_pct_act:.2f}% risk-base; {slot_risk_pct:.2f}% slot) | "
            f"margin={margin_used:.2f} target={target_margin_budget:.2f} "
            f"policy_target={policy_target_margin:.2f} min={min_trade_margin:.2f} cap={max_allowed_margin:.2f} | "
            f"lev={leverage:.0f}x required={required_capital_leverage:.2f}x "
            f"liq_cap={liquidation_leverage_cap:.1f}x venue_cap={configured_leverage:.0f}x "
            f"margin_risk={self._active_margin_risk_pct:.2%} | "
            f"fees≈{actual_fees:.3f} ({fee_to_risk:.2f}R) | "
            f"cash={required_cash:.2f}/{cash_available:.2f} | "
            f"risk_base={target_risk_base:.2f} margin_base={margin_budget_base:.2f} slot_available={available:.2f} | "
            f"headroom={cash_available - required_cash:.2f} | qty={qty}"
        )
        return qty

    @staticmethod
    def _estimate_pnl(pos, exit_price, entry_fill_type="taker"):
        """
        Corrected PnL formula — v5.1.

        ROOT CAUSE OF PREVIOUS BUG:
        The old Delta branch computed:
            contracts = pos.quantity / DELTA_CONTRACT_VALUE_BTC   # e.g. 0.005/0.001 = 5
        But Delta BTCUSD inverse perp has 1 USD per contract — NOT 0.001 BTC per contract.
        To hold 0.005 BTC exposure at 68,856, you need 0.005 × 68,856 = 344 USD contracts.
        Dividing by 0.001 gave 5 contracts = 5 USD notional instead of 344 USD notional.
        Result: gross PnL was ~68× too small; net was always dominated by fees → showed loss
        even when SL migration locked 98 points of profit.

        FIX:
        For Delta BTCUSD inverse perp, convert BTC quantity to USD contracts by
        multiplying by entry_price (the correct economic relationship):
            usd_contracts = pos.quantity × pos.entry_price
        Then apply the standard inverse-perp formula.

        Mathematical note: for moves < 3% (all our trades), the inverse-perp formula
        is equivalent to the linear formula to 3 significant figures:
            gross ≈ pos.quantity × |exit_price − entry_price|
        We use the exact inverse formula for correctness, but the linear approximation
        is included as a sanity check in debug logs.

        Both Delta and CoinSwitch paths now produce identical results for small moves
        because the inverse-perp formula converges to linear.

        Fee basis: notional is measured at entry price (standard industry practice).
        """
        _is_inverse = str(getattr(pos, "pnl_model", "linear") or "linear").lower() == "inverse_btcusd"
        _exchange = str(getattr(pos, "exchange", "") or "").lower()

        # This estimator is retained for non-realised display only.  Use only
        # venue-specific configured rates; realised booking requires broker fees.
        if entry_fill_type == "maker":
            entry_rate = (float(getattr(config, "DELTA_COMMISSION_RATE_MAKER", -0.00020))
                          if _exchange == "delta" else QCfg.COMMISSION_RATE() * 0.40)
        else:
            entry_rate = (float(getattr(config, "DELTA_COMMISSION_RATE", 0.00050))
                          if _exchange == "delta" else QCfg.COMMISSION_RATE())
        exit_rate = (float(getattr(config, "DELTA_COMMISSION_RATE", 0.00050))
                     if _exchange == "delta" else QCfg.COMMISSION_RATE())

        gross = gross_pnl_usd(
            pos.side,
            pos.entry_price,
            exit_price,
            pos.quantity,
            inverse=bool(_is_inverse),
        )
        entry_fee = pos.entry_price * pos.quantity * entry_rate
        exit_fee  = exit_price      * pos.quantity * exit_rate

        realized_pnl = gross - entry_fee - exit_fee
        logger.debug(
            f"PnL calc: {pos.side} qty={pos.quantity} entry={getattr(pos, 'currency_symbol', '$')}{pos.entry_price:,.2f} "
            f"exit={getattr(pos, 'currency_symbol', '$')}{exit_price:,.2f} gross={getattr(pos, 'currency_symbol', '$')}{gross:.4f} fees={getattr(pos, 'currency_symbol', '$')}{entry_fee+exit_fee:.4f} "
            f"net={getattr(pos, 'currency_symbol', '$')}{realized_pnl:.4f} [{'inverse_btcusd' if _is_inverse else 'linear'}]")
        return realized_pnl

    def _is_inverse_btc_contract(self, pos: Optional[PositionState] = None) -> bool:
        """True only for Delta BTCUSD inverse-style accounting.

        All Telegram reports must use the same mark-to-market formula as final
        trade accounting.  Keeping this in the strategy prevents each report from
        inventing its own linear approximation and drifting over long runs.
        """
        if pos is not None:
            return str(getattr(pos, "pnl_model", "linear") or "linear").lower() == "inverse_btcusd"
        try:
            inst = getattr(self, "_instrument", None) or current_instrument()
            ex = str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", ""))).lower()
            sym = str(getattr(inst, "execution_symbol", "") or getattr(config, "DELTA_SYMBOL", "BTCUSD")).upper()
            return ex == "delta" and sym == "BTCUSD"
        except Exception:
            return (str(getattr(config, "EXECUTION_EXCHANGE", "")).lower() == "delta"
                    and str(getattr(config, "DELTA_SYMBOL", "BTCUSD")).upper() == "BTCUSD")

    def _unrealised_pnl_usd(self, mark_price: Optional[float] = None, pos: Optional[PositionState] = None) -> float:
        """Authoritative open-position mark-to-market P&L for objects or snapshots."""
        p = pos or getattr(self, "_pos", None)
        if p is None:
            return 0.0
        def _value(key: str, default=0.0):
            return p.get(key, default) if isinstance(p, dict) else getattr(p, key, default)
        if isinstance(p, dict):
            if str(_value("phase", "ACTIVE")).upper() == "FLAT":
                return 0.0
        elif p.is_flat():
            return 0.0
        try:
            price = float(mark_price if mark_price is not None else getattr(self, "_last_known_price", 0.0) or 0.0)
            entry = float(_value("entry_price", 0.0) or 0.0)
            qty = float(_value("quantity", 0.0) or 0.0)
            if price <= 0 or entry <= 0 or qty <= 0:
                return 0.0
            model = str(_value("pnl_model", "linear") or "linear").lower()
            return gross_pnl_usd(
                str(_value("side", "")), entry, price, qty,
                inverse=(model == "inverse_btcusd"),
            )
        except Exception:
            try:
                entry = float(_value("entry_price", 0.0) or 0.0)
                qty = float(_value("quantity", 0.0) or 0.0)
                side = str(_value("side", "")).lower()
                move = (float(mark_price or 0.0) - entry) if side == "long" else (entry - float(mark_price or 0.0))
                return move * qty
            except Exception:
                return 0.0

    def _win_rate(self): return self._winning_trades/self._total_trades if self._total_trades else 0.0

    def get_stats(self):
        """Returns stats based on CLOSED trades only — correct win-rate denominator."""
        return {
            "total_trades":   self._total_trades,
            "winning_trades": self._winning_trades,
            "win_rate":       f"{self._win_rate():.1%}",
            "total_pnl":      round(self._total_pnl, 2),
            "daily_trades":   self._risk_gate.daily_trades,
            "consec_losses":  self._risk_gate.consec_losses,
            "current_phase":  self._pos.phase.name,
            "last_signal":    str(self._last_sig),
            "atr_5m":         round(self._atr_5m.atr, 2),
            "atr_1m":         round(self._atr_1m.atr, 2),
            "atr_pctile":     f"{self._atr_5m.get_percentile():.0%}",
            "regime_ok":      self._atr_5m.regime_valid(),
        }

    def format_status_report(self, current_price: float = 0.0, balance: float = 0.0, **kwargs) -> str:
        p = self._pos
        price = float(current_price or self._last_known_price or 0.0)
        cur = str(getattr(p, "currency_symbol", self._position_accounting_context().get("currency_symbol", "$")) or "$")
        state = self._entry_engine.state if self._entry_engine is not None else "UNAVAILABLE"
        analysis = self._entry_engine.analysis_info if self._entry_engine is not None else {}
        atr = float(self._atr_5m.atr or 0.0)
        lines = [
            "🏛 <b>INSTITUTIONAL AUCTION STATUS</b>",
            f"Authority: liquidity-raid reversal | displacement continuation | liquidity-expansion retest",
            f"State: {state} | Price: {cur}{price:,.4f} | ATR(5m): {cur}{atr:,.4f}",
            f"Context: 4H={analysis.get('context_4h', '-')} | 15m={analysis.get('context_15m', '-')} | Bias={analysis.get('context_bias_path', 'AWAITING_5M_DOL')} | Trigger={analysis.get('trigger', 'WAIT')}",
            f"Closed trades: {self._total_trades} | Wins: {self._winning_trades} | Realised P&L: {cur}{self._total_pnl:+,.2f}",
        ]
        if not p.is_flat():
            upnl = self._unrealised_pnl_usd(price, p)
            lines.extend([
                f"Position: {p.side.upper()} {p.quantity:g} {p.execution_symbol or self._asset_id}",
                f"Entry: {cur}{p.entry_price:,.4f} | SL: {cur}{p.sl_price:,.4f} | TP: {cur}{p.tp_price:,.4f}",
                f"Open P&L: {cur}{upnl:+,.2f} | Protection: fixed structural SL + liquidity targets",
            ])
        if analysis.get("entry"):
            lines.append(f"Candidate: entry={cur}{float(analysis.get('entry',0)):.4f} SL={cur}{float(analysis.get('sl',0)):.4f} TP={cur}{float(analysis.get('tp',0)):.4f} RR={float(analysis.get('rr',0)):.2f}")
            lines.append(f"Delivery score={float(analysis.get('delivery_score',0) or 0):.2f} | Calibrated probability={'available' if analysis.get('probability_calibrated') else 'not claimed'}")
        return "\n".join(lines)

    def _reconcile_query_thread(self, order_manager):
        try:
            ex_pos = order_manager.get_open_position()
            if ex_pos is None: return
            ex_size = float(ex_pos.get("size",0.0)); open_orders = None
            if ex_size >= float(getattr(config,"MIN_POSITION_SIZE",0.001)):
                try: open_orders = order_manager.get_open_orders()
                except Exception: pass
            with self._lock: self._reconcile_data = {"ex_pos":ex_pos,"open_orders":open_orders}
        except Exception as e: logger.warning(f"Reconcile error: {e}")
        finally: self._reconcile_pending = False

    def _reconcile_apply(self, order_manager, data):
        ex_pos=data["ex_pos"]; open_orders=data.get("open_orders")

        # FIX (CRITICAL-6): prefer the adapter's BTC-unit fields. The Delta
        # adapter now returns size in BTC (converted from contracts) and
        # size_signed preserving direction. CoinSwitch adapter returns
        # size in BTC natively. Either way, we want BTC here.
        ex_size     = abs(float(ex_pos.get("size", 0.0)))
        ex_size_raw = float(ex_pos.get("size_signed",
                                       ex_pos.get("size", 0.0)))
        ex_side     = str(ex_pos.get("side") or "").upper()
        phase       = self._pos.phase

        # Delta bracket child order type names (covers both bracket and standalone):
        # "stop_market_order", "stop_loss_order", "STOP_MARKET", "STOP", "STOP_LOSS_MARKET"
        def _is_sl(ot):
            return (ot in ("STOP_MARKET","STOP","STOP_LOSS_MARKET",
                           "STOP_MARKET_ORDER","STOP_LOSS_ORDER") or
                    ("STOP" in ot and "PROFIT" not in ot and "TAKE" not in ot))
        def _is_tp(ot):
            return (ot in ("TAKE_PROFIT_MARKET","TAKE_PROFIT",
                           "TAKE_PROFIT_MARKET_ORDER","TAKE_PROFIT_ORDER") or
                    ("PROFIT" in ot or "TAKE_PROFIT" in ot))
        if (phase == PositionPhase.FLAT and ex_size < QCfg.MIN_QTY()
                and _icici_exchange_name(getattr(self, "_instrument", None)) == "icici"
                and bool(ex_pos.get("position_scope_verified"))):
            # A strict NFO PortfolioPositions verification supersedes any stale
            # ghost/unmanaged alarm retained from an earlier malformed response.
            self._last_unmanaged_external_position = None
        if phase==PositionPhase.FLAT and ex_size>=QCfg.MIN_QTY():
            settle_sec = float(getattr(config, "RECONCILE_EXIT_SETTLE_SEC", 15.0))
            last_exit = float(getattr(self, "_last_exit_time", 0.0) or 0.0)
            since_exit = time.time() - last_exit if last_exit > 0.0 else 999.0
            if bool(getattr(self, "_exit_completed", False)) and 0.0 <= since_exit < settle_sec:
                logger.info(
                    "Reconcile: deferring FLAT adoption %.1fs after local exit "
                    "(settlement window %.1fs)",
                    since_exit,
                    settle_sec,
                )
                return
            _is_icici_reconcile = _icici_exchange_name(getattr(self, "_instrument", None)) == "icici"
            if _is_icici_reconcile:
                _session_open, _session_reason = _icici_market_session_open()
                if not _session_open:
                    self._last_unmanaged_external_position = dict(ex_pos)
                    logger.critical(
                        "ICICI reconcile found a broker position while NSE/NFO is closed (%s); "
                        "refusing bot adoption/analysis. Manual broker review required.",
                        _session_reason,
                    )
                    try:
                        self._send_telegram(
                            "🚨 <b>ICICI POSITION NOT ADOPTED</b>\n"
                            "<b>NIFTY option desk is closed</b>\n"
                            f"Reason: <code>{_session_reason}</code>\n"
                            "The bot will not analyse or manage this after-hours position automatically. "
                            "Manual broker review is required.",
                            event_type="reconcile_guard",
                        )
                    except Exception:
                        pass
                    self._last_exit_time = time.time()
                    return
                if bool(ex_pos.get("unadoptable")):
                    self._last_unmanaged_external_position = dict(ex_pos)
                    logger.critical(
                        "ICICI reconcile found broker position qty=%.8g entry=%.2f but Breeze did not provide "
                        "exact strike/right/expiry; refusing adoption to avoid ghost position.",
                        ex_size,
                        float(ex_pos.get("entry_price", 0.0) or 0.0),
                    )
                    try:
                        self._send_telegram(
                            "🚨 <b>ICICI POSITION NOT ADOPTED</b>\n"
                            "Broker position detected, but exact option identity is missing.\n"
                            f"Qty: <code>{ex_size:.8g}</code> · Entry: <code>₹{float(ex_pos.get('entry_price', 0.0) or 0.0):,.2f}</code>\n"
                            "Required fields: <code>strike/right/expiry</code>. No order will be sent.",
                            event_type="reconcile_guard",
                        )
                    except Exception:
                        pass
                    self._last_exit_time = time.time()
                    return
                if not _icici_ensure_adoptable_contract(getattr(self, "_instrument", None), ex_pos):
                    self._last_unmanaged_external_position = dict(ex_pos)
                    logger.critical(
                        "ICICI reconcile found a broker position but exact option contract metadata "
                        "(strike/right/expiry) is missing; refusing adoption to avoid wrong-contract "
                        "protection or failed flatten."
                    )
                    try:
                        self._send_telegram(
                            "🚨 <b>ICICI POSITION NOT ADOPTED</b>\n"
                            "Exact option vehicle metadata is missing: <code>strike/right/expiry</code>.\n"
                            "The bot will not guess the contract. Manual broker review is required.",
                            event_type="reconcile_guard",
                        )
                    except Exception:
                        pass
                    self._last_exit_time = time.time()
                    return
            ex_entry=float(ex_pos.get("entry_price",0.0)); ex_upnl=float(ex_pos.get("unrealized_pnl",0.0))
            # Guard: CoinSwitch sometimes returns entry_price=0 for a position that
            # has been filled but not yet fully settled in the position feed.
            _min_adopt_entry = max(QCfg.TICK_SIZE(), 1e-6) if _is_icici_reconcile else 1.0
            if ex_entry < _min_adopt_entry:
                logger.warning(
                    f"Reconcile: skipping adoption of {ex_side} size={ex_size} "
                    f"— entry_price={ex_entry:.2f} not yet settled on exchange")
                return

            # ── FIX (third-trade bug): refuse ambiguous-side adoption ─────────
            # The original code did `"long" if ex_side=="LONG" else "short"`,
            # which silently produced SHORT whenever ex_side was anything other
            # than the exact literal "LONG" — including empty string, missing
            # key, or lowercase. In the third-trade incident the exchange
            # returned an empty side on a genuinely-LONG position and the bot
            # adopted it as SHORT, then tracked an inverted phantom for 27m.
            #
            # New policy: resolve side from TWO independent sources and refuse
            # to adopt if they disagree or both are ambiguous.
            #   Source 1: string side field ("LONG"/"SHORT")
            #   Source 2: sign of raw size (positive = long, negative = short)
            iside_from_str  = None
            if ex_side == "LONG":
                iside_from_str = "long"
            elif ex_side == "SHORT":
                iside_from_str = "short"

            iside_from_size = None
            if ex_size_raw > 0:
                iside_from_size = "long"
            elif ex_size_raw < 0:
                iside_from_size = "short"

            if iside_from_str and iside_from_size and iside_from_str != iside_from_size:
                logger.error(
                    f"🚨 Reconcile: side conflict — str={ex_side} signed_size={ex_size_raw} "
                    f"— REFUSING adoption. Will retry on next reconcile cycle.")
                return

            iside = iside_from_str or iside_from_size
            if iside is None:
                logger.error(
                    f"🚨 Reconcile: ambiguous side (str={ex_side!r}, "
                    f"size={ex_size_raw}) — REFUSING adoption of size={ex_size} "
                    f"at entry={ex_entry:,.2f}. Will retry on next reconcile cycle.")
                return
            # ─────────────────────────────────────────────────────────────────

            sl_oid=tp_oid=None; sl_p=tp_p=0.0

            if open_orders:
                for o in open_orders:
                    ot=(o.get("type") or (o.get("raw") or {}).get("order_type") or "").upper().replace(" ","_").replace("-","_")
                    trig=float(o.get("trigger_price") or (o.get("raw") or {}).get("stop_price") or 0)
                    if _is_sl(ot): sl_oid=o["order_id"]; sl_p=trig
                    elif _is_tp(ot): tp_oid=o["order_id"]; tp_p=trig

            # Sanity check: SL must be on the protective side of entry for the
            # adopted side. A long's SL is BELOW entry; a short's SL is ABOVE.
            # If the orphan SL contradicts the adopted side, drop the SL oid
            # and let a fresh one be placed rather than tracking a wrong-side SL.
            if sl_oid and sl_p > 0:
                _sl_ok = ((iside == "long"  and sl_p < ex_entry) or
                          (iside == "short" and sl_p > ex_entry))
                if not _sl_ok:
                    logger.warning(
                        f"⚠️ Reconcile: discovered SL @ {sl_p:,.2f} is on the "
                        f"WRONG side of {iside} entry {ex_entry:,.2f} — "
                        f"ignoring (was likely a prior trade's orphan).")
                    sl_oid = None; sl_p = 0.0

            # Compute initial_sl_dist for the adopted position.
            # When sl_p is known: use the actual distance from entry to SL.
            # When sl_p is zero (no SL on exchange): fall back to 1.5×ATR so the
            # DisabledSLMigrationEngine has a valid R-denominator and doesn't stay
            # permanently in PHASE_0_HANDS_OFF at 0/0 = 0R.
            _adopt_atr = self._atr_5m.atr if (hasattr(self, '_atr_5m') and self._atr_5m and self._atr_5m.atr > 0) else 0.0
            _adopt_sl_dist = (
                abs(ex_entry - sl_p) if sl_p > 0
                else (_adopt_atr * 1.5 if _adopt_atr > 0 else 0.0)
            )
            _adopt_accounting = self._position_accounting_context()
            self._pos = PositionState(phase=PositionPhase.ACTIVE, side=iside, quantity=ex_size,
                entry_price=ex_entry, sl_price=sl_p, tp_price=tp_p, sl_order_id=sl_oid,
                tp_order_id=tp_oid, entry_time=time.time(), initial_sl_dist=_adopt_sl_dist,
                entry_atr=_adopt_atr, entry_session=self._execution_session_label(),
                tp_ladder_initial_qty=ex_size, tp_ladder_last_sync_qty=ex_size,
                last_seen_price=ex_entry,
                exchange=str(_adopt_accounting.get("exchange", "")),
                execution_symbol=str(_adopt_accounting.get("execution_symbol", "")),
                asset_id=str(_adopt_accounting.get("asset_id", "")),
                currency_code=str(_adopt_accounting.get("currency_code", "USD")),
                currency_symbol=str(_adopt_accounting.get("currency_symbol", "$")),
                pnl_model=str(_adopt_accounting.get("pnl_model", "linear")),
                quantity_unit=str(_adopt_accounting.get("quantity_unit", "units")),
                thesis_side=iside)
            self._last_unmanaged_external_position = None
            self.current_sl_price=sl_p; self.current_tp_price=tp_p
            self._confirm_long=self._confirm_short=0
            # Reconcile adoption means the exchange is already carrying risk.
            # Keep RiskManager's open-position flag aligned so midnight reset
            # deferral/booking remains correct even after recovery/adoption paths.
            try:
                _rm_adopt = getattr(self, '_risk_manager_ref', None)
                if _rm_adopt is not None and hasattr(_rm_adopt, 'set_position_open'):
                    _rm_adopt.set_position_open(True)
            except Exception as _rm_adopt_e:
                logger.debug(f"risk_manager.set_position_open(True) adoption error (non-fatal): {_rm_adopt_e}")
            # Reset duplicate guards for the newly adopted position
            self._exit_completed = False
            _adopt_cur = "₹" if _is_icici_reconcile else "$"
            logger.warning(f"⚡ RECONCILE: adopted {iside.upper()} @ {_adopt_cur}{ex_entry:,.2f}")
            self._send_telegram(
                f"⚡ <b>POSITION ADOPTED</b>\n"
                f"Side: {iside.upper()} | Size: {ex_size}\n"
                f"Entry: {_adopt_cur}{ex_entry:,.2f} | uPnL: {_adopt_cur}{ex_upnl:+,.2f}"
            )

            # ── TP-LADDER ADOPTION REPAIR ─────────────────────────────────────
            # If a native bracket fill/child-verification timed out but the
            # exchange later shows an active protected position, the normal
            # post-fill TP ladder hook never ran.  Build/attach the ladder here
            # from the recovered SL/TP.  SL price remains fixed; internal legs
            # are reduce-only monetisation orders only.
            try:
                if (not _is_icici_reconcile) and sl_oid and tp_oid and sl_p > 0.0 and tp_p > 0.0 and _adopt_atr > 0.0:
                    _adopt_ladder = self._build_tp_ladder_plan(
                        side=iside,
                        entry_price=ex_entry,
                        sl_price=sl_p,
                        final_tp=tp_p,
                        quantity=ex_size,
                        atr=_adopt_atr,
                    )
                    _ladder_dicts, _ladder_ids = self._place_internal_tp_ladder(
                        order_manager=order_manager,
                        side=iside,
                        quantity=ex_size,
                        final_tp=tp_p,
                        native_final_tp_order_id=str(tp_oid or ""),
                        ladder_plan=_adopt_ladder,
                    )
                    if _ladder_dicts:
                        self._pos.tp_ladder = _ladder_dicts
                        self._pos.tp_ladder_order_ids = list(_ladder_ids or [])
                        self._pos.tp_ladder_active = bool(_ladder_ids)
                        self._pos.tp_ladder_last_sync_qty = ex_size
                        self._pos.tp_ladder_initial_qty = ex_size
                        if _ladder_ids:
                            logger.info(
                                "✅ TP_LADDER adoption repair placed %d internal reduce-only legs; fixed SL remains %.2f",
                                len(_ladder_ids), sl_p)
                        else:
                            logger.info(
                                "TP_LADDER adoption repair produced final-only plan; fixed SL remains %.2f. notes=%s",
                                sl_p,
                                "; ".join(getattr(_adopt_ladder, "regime_notes", []) or []) if _adopt_ladder is not None else "none")
            except Exception as _adopt_ladder_e:
                logger.warning("TP_LADDER adoption repair failed; native bracket remains live: %s", _adopt_ladder_e, exc_info=True)

            # ── FIX-ADOPT-ENGINE: Wire all per-position stateful engines at adoption.
            # ─────────────────────────────────────────────────────────────────────────
            # The original _reconcile_apply set pos.phase = ACTIVE and returned.
            # It did NOT call any of the per-position engine lifecycle hooks that
            # _enter_trade calls after a normal order fill. This created three distinct
            # failure modes, all presenting as "bot stuck after adopted trade closes":
            #
            # (A) EntryEngine stays in EngineState.SCANNING (on_position_opened() never
            #     called → never transitions to IN_POSITION). The 14400s stuck-state
            #     watchdog inside entry_engine.update() only fires for IN_POSITION, so
            #     a 4h+ adoption never triggers self-recovery. If _finalise_exit() ever
            #     throws between setting pos=FLAT and calling on_position_closed(), the
            #     engine stays in SCANNING permanently — the state is already correct but
            #     on_position_closed() → _reset() → purge _processed_sweeps is skipped,
            #     leaving stale sweep holds that block re-entry.
            #

            if hasattr(self, '_liq_map') and self._liq_map is not None:
                try:
                    self._liq_map.reset_snapshot()
                except Exception as _lm_e:
                    logger.debug(f"liq_map.reset_snapshot() at adopt error: {_lm_e}")


            return
        if phase==PositionPhase.ACTIVE and ex_size<QCfg.MIN_QTY():
            logger.info("📡 Reconcile: exchange FLAT → TP/SL fired")
            self._record_exchange_exit(ex_pos); return
        if phase==PositionPhase.ACTIVE and ex_size>=QCfg.MIN_QTY():
            self._reconcile_tp_ladder_quantity(order_manager, ex_size)
            if (not self._pos.sl_order_id or not self._pos.tp_order_id) and open_orders:
                for o in open_orders:
                    ot=(o.get("type") or (o.get("raw") or {}).get("order_type") or "").upper().replace(" ","_").replace("-","_")
                    trig=float(o.get("trigger_price") or (o.get("raw") or {}).get("stop_price") or 0)
                    if not self._pos.sl_order_id and _is_sl(ot):
                        # Side-sanity check also on recovery path
                        _side = self._pos.side
                        _ep = self._pos.entry_price or 0.0
                        _ok = (_ep <= 0) or (
                            (_side == "long"  and trig < _ep) or
                            (_side == "short" and trig > _ep))
                        if not _ok:
                            logger.warning(
                                f"Reconcile: recovered SL @ {trig:,.2f} contradicts "
                                f"{_side} entry {_ep:,.2f} — ignoring")
                            continue
                        # Bug #8 fix: write sl_price under self._lock so the trail
                        # thread (which also writes sl_price under this lock) cannot
                        # observe a torn state where sl_order_id is set but sl_price
                        # is not yet updated (or vice versa).
                        with self._lock:
                            self._pos.sl_order_id  = o["order_id"]
                            self._pos.sl_price     = trig
                            self.current_sl_price  = trig
                            if self._pos.initial_sl_dist == 0 and _ep > 0:
                                self._pos.initial_sl_dist = abs(_ep - trig)
                        logger.info(f"Reconcile: recovered SL order {o['order_id'][:8]}… @ {trig:.2f}")
                    elif (not _is_icici_reconcile) and not self._pos.tp_order_id and _is_tp(ot):
                        # Bug #8 fix: same atomic write for TP fields.
                        with self._lock:
                            self._pos.tp_order_id  = o["order_id"]
                            self._pos.tp_price     = trig
                            self.current_tp_price  = trig
                        logger.info(f"Reconcile: recovered TP order {o['order_id'][:8]}… @ {trig:.2f}")

    def _sync_position(self, order_manager):
        try: ex_pos = order_manager.get_open_position()
        except Exception: return
        if ex_pos is None: return
        ex_size = float(ex_pos.get("size",0.0))
        if self._pos.phase==PositionPhase.ACTIVE:
            if ex_size<QCfg.MIN_QTY():
                logger.info("📡 Sync: exchange FLAT → TP/SL fired")
                self._record_exchange_exit(ex_pos)
            else:
                self._reconcile_tp_ladder_quantity(order_manager, ex_size)
        elif self._pos.phase==PositionPhase.EXITING:
            if ex_size<QCfg.MIN_QTY():
                # v8.0 FIX: call _record_exchange_exit, NOT _finalise_exit.
                # The old code skipped PnL recording entirely for the normal
                # EXITING→flat sync path.  _exit_trade sends estimated PnL via
                # telegram but defers actual recording to this confirmation.
                # Calling _finalise_exit directly meant PnL was never recorded.
                logger.info("📡 Sync: EXITING confirmed FLAT → recording exit")
                self._record_exchange_exit(ex_pos)
