"""
core/market_policy.py — instrument-native runtime policy
=========================================================

Centralised, asset-class aware execution/risk/runtime policy.  This removes the
old failure mode where BTC-calibrated constants were reused for xStock, metal,
index-token and crypto contracts.

The policy is intentionally derived from the confirmed TradableInstrument plus
config base priors.  Config remains the default prior source; this module turns
those priors into per-contract runtime values.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import math

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import AssetClass, ExchangeName, TradableInstrument, current_instrument


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _f(name: str, default: float) -> float:
    try:
        v = float(_cfg(name, default))
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def _i(name: str, default: int) -> int:
    try:
        return int(_cfg(name, default))
    except Exception:
        return int(default)


def _b(name: str, default: bool) -> bool:
    try:
        return bool(_cfg(name, default))
    except Exception:
        return bool(default)


@dataclass(frozen=True)
class InstrumentPolicy:
    asset_id: str
    asset_class: str
    leverage: int
    margin_pct: float
    risk_multiplier: float
    min_margin_usd: float
    tick_eval_sec: float
    loop_interval_sec: float
    min_1m_bars: int
    min_5m_bars: int
    atr_min_pctile: float
    atr_max_pctile: float
    max_hold_sec: int
    cooldown_sec: int
    loss_lockout_sec: int
    min_rr: float
    max_rr: float
    sl_buffer_atr_mult: float
    trail_min_move_atr: float
    slippage_tolerance: float
    spread_soft_atr_ratio: float
    spread_max_atr_ratio: float
    spread_max_bps: float
    spread_max_ticks: float
    spread_min_size_mult: float
    spread_haircut_max: float
    ob_depth_levels: int
    tick_agg_window_sec: float
    vwap_window: int
    cvd_window: int
    notes: str = ""

    @property
    def evaluation_interval_sec(self) -> float:
        """Backward-compatible alias used by orchestration/Telegram code.

        v9 renamed the per-instrument scan cadence to ``loop_interval_sec``.
        Some v10 notification paths still referenced the old field name.
        Keep this alias so command-center/startup messages cannot crash the bot.
        """
        return float(self.loop_interval_sec)

    def asdict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["evaluation_interval_sec"] = self.evaluation_interval_sec
        return d


def _primary_exchange(inst: Optional[TradableInstrument]) -> Optional[ExchangeName]:
    try:
        return inst.primary_exchange if inst is not None else None
    except Exception:
        return None


def _symbol_keys(inst: Optional[TradableInstrument]) -> tuple[str, ...]:
    if inst is None:
        return ()
    vals = []
    for v in (getattr(inst, "execution_symbol", ""), getattr(inst, "display_symbol", ""), getattr(inst, "asset_id", "")):
        k = "".join(ch for ch in str(v or "").upper() if ch.isalnum())
        if k and k not in vals:
            vals.append(k)
    return tuple(vals)


def _map_lookup(name: str, keys: tuple[str, ...], default: float = 0.0) -> float:
    raw = _cfg(name, {})
    if isinstance(raw, dict):
        for k in keys:
            for candidate in (k, k.replace("USDT", "USD"), k.replace("USD", "")):
                if candidate in raw:
                    try:
                        v = float(raw[candidate])
                        if math.isfinite(v) and v > 0:
                            return v
                    except Exception:
                        pass
    return float(default)


def _venue_configured_max_leverage(inst: Optional[TradableInstrument]) -> float:
    if inst is None:
        return 0.0
    ex = _primary_exchange(inst)
    ac = getattr(inst, "asset_class", AssetClass.CRYPTO)
    if ex == ExchangeName.DELTA:
        keys = _symbol_keys(inst)
        by_symbol = _map_lookup("DELTA_SYMBOL_MAX_LEVERAGE", keys, 0.0)
        if by_symbol > 0:
            return by_symbol
        by_class = _cfg("DELTA_ASSET_CLASS_MAX_LEVERAGE", {})
        if isinstance(by_class, dict):
            try:
                v = float(by_class.get(str(ac.value), 0.0) or 0.0)
                if math.isfinite(v) and v > 0:
                    return v
            except Exception:
                pass
        if ac in (AssetClass.FUTURE, AssetClass.CRYPTO):
            return _f("DELTA_DEFAULT_FUTURE_MAX_LEVERAGE", 0.0)
    if ex == ExchangeName.HYPERLIQUID:
        try:
            mx = float(getattr(inst, "max_leverage", 0.0) or 0.0)
            if math.isfinite(mx) and mx > 0:
                return mx
        except Exception:
            pass
        return _f("HYPERLIQUID_DEFAULT_MAX_LEVERAGE", 40.0)
    return 0.0


def _confirmed_max_leverage(inst: Optional[TradableInstrument]) -> float:
    if inst is None:
        return max(1.0, _f("MAX_POLICY_LEVERAGE", 40.0))
    ex = _primary_exchange(inst)
    ac = getattr(inst, "asset_class", AssetClass.CRYPTO)
    if ex == ExchangeName.ICICI or ac in (AssetClass.CASH, AssetClass.OPTION):
        return 1.0
    try:
        mx = float(getattr(inst, "max_leverage", 0.0) or 0.0)
        if math.isfinite(mx) and mx > 0:
            return max(1.0, mx)
    except Exception:
        pass
    configured = _venue_configured_max_leverage(inst)
    if configured > 0:
        return configured
    return 1.0


def _leverage_utilisation(inst: Optional[TradableInstrument]) -> float:
    if inst is None:
        return 0.25
    ac = getattr(inst, "asset_class", AssetClass.CRYPTO)
    ex = _primary_exchange(inst)
    if ex == ExchangeName.ICICI or ac in (AssetClass.CASH, AssetClass.OPTION):
        return 1.0
    if ex == ExchangeName.DELTA:
        sym_util = _map_lookup("DELTA_SYMBOL_LEVERAGE_UTIL", _symbol_keys(inst), 0.0)
        if sym_util > 0:
            return min(1.0, max(0.0, sym_util))
    if ac == AssetClass.CRYPTO:
        return _f("POLICY_CRYPTO_LEVERAGE_UTIL", 0.28)
    if ac == AssetClass.FUTURE:
        return _f("POLICY_FUTURE_LEVERAGE_UTIL", 0.28)
    if ac == AssetClass.COMMODITY:
        return _f("POLICY_COMMODITY_LEVERAGE_UTIL", 0.40)
    if ac == AssetClass.EQUITY:
        return _f("POLICY_EQUITY_LEVERAGE_UTIL", 0.32)
    if ac == AssetClass.INDEX:
        return _f("POLICY_INDEX_LEVERAGE_UTIL", 0.35)
    return _f("POLICY_GENERIC_LEVERAGE_UTIL", 0.35)


def _target_leverage(inst: Optional[TradableInstrument]) -> int:
    venue_cap = _confirmed_max_leverage(inst)
    firm_cap = max(1.0, _f("MAX_POLICY_LEVERAGE", venue_cap))
    util = max(0.0, min(1.0, _leverage_utilisation(inst)))
    # Institutional leverage uses the venue/product cap first, then applies a
    # utilisation haircut, then the firm-level cap.  BTC example: 200x venue cap
    # × 0.20 utilisation = 40x, capped by MAX_POLICY_LEVERAGE=40.
    raw = min(venue_cap * util, firm_cap)
    return max(1, int(math.floor(raw)))


def _margin_pct_for(inst: Optional[TradableInstrument], base_margin: float, configured_name: str, configured_default: float) -> float:
    # Margin budget is capital allocation, not leverage. Fully funded products
    # still receive a position budget, but P&L/margin calculations remain 1x.
    ex = _primary_exchange(inst)
    ac = getattr(inst, "asset_class", AssetClass.CRYPTO) if inst is not None else AssetClass.CRYPTO
    if ex == ExchangeName.ICICI or ac in (AssetClass.CASH, AssetClass.OPTION):
        return min(base_margin, _f("POLICY_ICICI_CASH_MARGIN_PCT", 0.06))
    return min(base_margin, _f(configured_name, configured_default))


def build_instrument_policy(inst: Optional[TradableInstrument]) -> InstrumentPolicy:
    """Build the active policy for one contract.

    Design intent:
      • Crypto/BTC keeps the tight, fast settings.
      • xStock/RWA contracts use slower cadence, lower margin allocation, and
        spread impairment instead of BTC-style hard blocking.
      • Metals/tokens sit between crypto and equity.
      • All values are centralised here instead of scattered inside strategy.
    """
    ac = getattr(inst, 'asset_class', AssetClass.CRYPTO) if inst is not None else AssetClass.CRYPTO
    asset_id = getattr(inst, 'asset_id', 'GLOBAL') if inst is not None else 'GLOBAL'

    lev = _target_leverage(inst)
    base_margin = _f('QUANT_MARGIN_PCT', 0.20)
    base_rr = _f('MIN_RISK_REWARD_RATIO', 2.0)
    base_cooldown = _i('QUANT_COOLDOWN_SEC', 300)


    if ac == AssetClass.CASH:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=1,
            margin_pct=_margin_pct_for(inst, base_margin, 'POLICY_ICICI_CASH_MARGIN_PCT', 0.06),
            risk_multiplier=_f('POLICY_CASH_RISK_MULT', 0.30),
            min_margin_usd=_f('POLICY_CASH_MIN_MARGIN_USD', 1.0),
            tick_eval_sec=_f('POLICY_CASH_TICK_EVAL_SEC', 1.0),
            loop_interval_sec=_f('POLICY_CASH_LOOP_INTERVAL_SEC', 1.0),
            min_1m_bars=_i('POLICY_CASH_MIN_1M_BARS', 120),
            min_5m_bars=_i('POLICY_CASH_MIN_5M_BARS', 80),
            atr_min_pctile=_f('POLICY_CASH_ATR_MIN_PCTILE', 0.05),
            atr_max_pctile=_f('POLICY_CASH_ATR_MAX_PCTILE', 0.98),
            max_hold_sec=_i('POLICY_CASH_MAX_HOLD_SEC', 3600),
            cooldown_sec=_i('POLICY_CASH_COOLDOWN_SEC', 300),
            loss_lockout_sec=_i('POLICY_CASH_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.25, min(base_rr, _f('POLICY_CASH_MIN_RR', 1.60))),
            max_rr=_f('POLICY_CASH_MAX_RR', 4.0),
            sl_buffer_atr_mult=_f('POLICY_CASH_SL_BUFFER_ATR', 0.65),
            trail_min_move_atr=_f('POLICY_CASH_TRAIL_MIN_MOVE_ATR', 0.10),
            slippage_tolerance=_f('POLICY_CASH_SLIPPAGE_TOL', 0.0015),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_CASH', 0.60),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_CASH', 3.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_CASH', 60.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_CASH', 12.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.60),
            ob_depth_levels=_i('POLICY_CASH_OB_DEPTH_LEVELS', 3),
            tick_agg_window_sec=_f('POLICY_CASH_TICK_AGG_WINDOW_SEC', 60.0),
            vwap_window=_i('POLICY_CASH_VWAP_WINDOW', 80),
            cvd_window=_i('POLICY_CASH_CVD_WINDOW', 40),
            notes='cash-market policy; fully funded 1x; no derivatives leverage',
        )

    if ac == AssetClass.OPTION:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=1,
            margin_pct=_margin_pct_for(inst, base_margin, 'POLICY_OPTION_MARGIN_PCT', 0.06),
            risk_multiplier=_f('POLICY_OPTION_RISK_MULT', 0.35),
            min_margin_usd=_f('POLICY_OPTION_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_f('POLICY_OPTION_TICK_EVAL_SEC', 1.0),
            loop_interval_sec=_f('POLICY_OPTION_LOOP_INTERVAL_SEC', 1.0),
            min_1m_bars=_i('POLICY_OPTION_MIN_1M_BARS', 120),
            min_5m_bars=_i('POLICY_OPTION_MIN_5M_BARS', 80),
            atr_min_pctile=_f('POLICY_OPTION_ATR_MIN_PCTILE', 0.08),
            atr_max_pctile=_f('POLICY_OPTION_ATR_MAX_PCTILE', 0.99),
            max_hold_sec=_i('POLICY_OPTION_MAX_HOLD_SEC', 2400),
            cooldown_sec=_i('POLICY_OPTION_COOLDOWN_SEC', 240),
            loss_lockout_sec=_i('POLICY_OPTION_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.25, min(base_rr, _f('POLICY_OPTION_MIN_RR', 1.60))),
            max_rr=_f('POLICY_OPTION_MAX_RR', 4.0),
            sl_buffer_atr_mult=_f('POLICY_OPTION_SL_BUFFER_ATR', 0.75),
            trail_min_move_atr=_f('POLICY_OPTION_TRAIL_MIN_MOVE_ATR', 0.12),
            slippage_tolerance=_f('POLICY_OPTION_SLIPPAGE_TOL', 0.0025),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_OPTION', 0.80),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_OPTION', 5.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_OPTION', 120.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_OPTION', 20.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.65),
            ob_depth_levels=_i('POLICY_OPTION_OB_DEPTH_LEVELS', 3),
            tick_agg_window_sec=_f('POLICY_OPTION_TICK_AGG_WINDOW_SEC', 60.0),
            vwap_window=_i('POLICY_OPTION_VWAP_WINDOW', 80),
            cvd_window=_i('POLICY_OPTION_CVD_WINDOW', 40),
            notes='ICICI/options are fully funded in this bot; leverage disabled; wider spread envelope, no BTC assumptions',
        )

    if ac in (AssetClass.INDEX, AssetClass.FUTURE):
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=getattr(ac, 'value', str(ac)), leverage=lev,
            margin_pct=_margin_pct_for(inst, base_margin, 'POLICY_FUTURE_MARGIN_PCT', 0.10),
            risk_multiplier=_f('POLICY_FUTURE_RISK_MULT', 0.60),
            min_margin_usd=_f('POLICY_FUTURE_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_f('POLICY_FUTURE_TICK_EVAL_SEC', 0.75),
            loop_interval_sec=_f('POLICY_FUTURE_LOOP_INTERVAL_SEC', 0.75),
            min_1m_bars=_i('POLICY_FUTURE_MIN_1M_BARS', 100),
            min_5m_bars=_i('POLICY_FUTURE_MIN_5M_BARS', 75),
            atr_min_pctile=_f('POLICY_FUTURE_ATR_MIN_PCTILE', 0.05),
            atr_max_pctile=_f('POLICY_FUTURE_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_i('POLICY_FUTURE_MAX_HOLD_SEC', 4200),
            cooldown_sec=min(base_cooldown, _i('POLICY_FUTURE_COOLDOWN_SEC', 210)),
            loss_lockout_sec=_i('POLICY_FUTURE_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.45, min(base_rr, _f('POLICY_FUTURE_MIN_RR', 1.75))),
            max_rr=_f('POLICY_FUTURE_MAX_RR', 5.0),
            sl_buffer_atr_mult=_f('POLICY_FUTURE_SL_BUFFER_ATR', 0.55),
            trail_min_move_atr=_f('POLICY_FUTURE_TRAIL_MIN_MOVE_ATR', 0.08),
            slippage_tolerance=_f('POLICY_FUTURE_SLIPPAGE_TOL', 0.0012),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_FUTURE', 0.60),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_FUTURE', 3.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_FUTURE', 65.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_FUTURE', 14.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
            ob_depth_levels=_i('POLICY_FUTURE_OB_DEPTH_LEVELS', 4),
            tick_agg_window_sec=_f('POLICY_FUTURE_TICK_AGG_WINDOW_SEC', 45.0),
            vwap_window=_i('POLICY_FUTURE_VWAP_WINDOW', 70),
            cvd_window=_i('POLICY_FUTURE_CVD_WINDOW', 35),
            notes='index/futures policy; instrument-native tick/lot/ATR',
        )

    if ac == AssetClass.EQUITY:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=_margin_pct_for(inst, base_margin, 'POLICY_EQUITY_MARGIN_PCT', 0.12),
            risk_multiplier=_f('POLICY_EQUITY_RISK_MULT', 0.55),
            min_margin_usd=_f('POLICY_EQUITY_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_f('POLICY_EQUITY_TICK_EVAL_SEC', 0.75),
            loop_interval_sec=_f('POLICY_EQUITY_LOOP_INTERVAL_SEC', 0.75),
            min_1m_bars=_i('POLICY_EQUITY_MIN_1M_BARS', 90),
            min_5m_bars=_i('POLICY_EQUITY_MIN_5M_BARS', 70),
            atr_min_pctile=_f('POLICY_EQUITY_ATR_MIN_PCTILE', 0.03),
            atr_max_pctile=_f('POLICY_EQUITY_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_i('POLICY_EQUITY_MAX_HOLD_SEC', 5400),
            cooldown_sec=min(base_cooldown, _i('POLICY_EQUITY_COOLDOWN_SEC', 180)),
            loss_lockout_sec=_i('POLICY_EQUITY_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.45, min(base_rr, _f('POLICY_EQUITY_MIN_RR', 1.75))),
            max_rr=_f('POLICY_EQUITY_MAX_RR', 5.0),
            sl_buffer_atr_mult=_f('POLICY_EQUITY_SL_BUFFER_ATR', 0.55),
            trail_min_move_atr=_f('POLICY_EQUITY_TRAIL_MIN_MOVE_ATR', 0.06),
            slippage_tolerance=_f('POLICY_EQUITY_SLIPPAGE_TOL', 0.0010),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_EQUITY', 0.50),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_EQUITY', 4.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_EQUITY', 35.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_EQUITY', 8.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
            ob_depth_levels=_i('POLICY_EQUITY_OB_DEPTH_LEVELS', 3),
            tick_agg_window_sec=_f('POLICY_EQUITY_TICK_AGG_WINDOW_SEC', 45.0),
            vwap_window=_i('POLICY_EQUITY_VWAP_WINDOW', 60),
            cvd_window=_i('POLICY_EQUITY_CVD_WINDOW', 30),
            notes='xStock/RWA equity policy; no BTC lot/risk assumptions',
        )

    if ac == AssetClass.COMMODITY:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=_margin_pct_for(inst, base_margin, 'POLICY_COMMODITY_MARGIN_PCT', 0.14),
            risk_multiplier=_f('POLICY_COMMODITY_RISK_MULT', 0.70),
            min_margin_usd=_f('POLICY_COMMODITY_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_f('POLICY_COMMODITY_TICK_EVAL_SEC', 0.50),
            loop_interval_sec=_f('POLICY_COMMODITY_LOOP_INTERVAL_SEC', 0.50),
            min_1m_bars=_i('POLICY_COMMODITY_MIN_1M_BARS', 85),
            min_5m_bars=_i('POLICY_COMMODITY_MIN_5M_BARS', 65),
            atr_min_pctile=_f('POLICY_COMMODITY_ATR_MIN_PCTILE', 0.04),
            atr_max_pctile=_f('POLICY_COMMODITY_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_i('POLICY_COMMODITY_MAX_HOLD_SEC', 4800),
            cooldown_sec=min(base_cooldown, _i('POLICY_COMMODITY_COOLDOWN_SEC', 210)),
            loss_lockout_sec=_i('POLICY_COMMODITY_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.55, min(base_rr, _f('POLICY_COMMODITY_MIN_RR', 1.85))),
            max_rr=_f('POLICY_COMMODITY_MAX_RR', 5.0),
            sl_buffer_atr_mult=_f('POLICY_COMMODITY_SL_BUFFER_ATR', 0.50),
            trail_min_move_atr=_f('POLICY_COMMODITY_TRAIL_MIN_MOVE_ATR', 0.07),
            slippage_tolerance=_f('POLICY_COMMODITY_SLIPPAGE_TOL', 0.0008),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_COMMODITY', 0.50),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_COMMODITY', 2.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_COMMODITY', 45.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_COMMODITY', 10.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
            ob_depth_levels=_i('POLICY_COMMODITY_OB_DEPTH_LEVELS', 4),
            tick_agg_window_sec=_f('POLICY_COMMODITY_TICK_AGG_WINDOW_SEC', 40.0),
            vwap_window=_i('POLICY_COMMODITY_VWAP_WINDOW', 55),
            cvd_window=_i('POLICY_COMMODITY_CVD_WINDOW', 25),
            notes='commodity-token policy; instrument-native tick/lot/ATR',
        )

    # Crypto/default policy keeps the original aggressive BTC runtime prior.
    return InstrumentPolicy(
        asset_id=asset_id, asset_class=getattr(ac, 'value', str(ac)), leverage=lev,
        margin_pct=base_margin,
        risk_multiplier=_f('POLICY_CRYPTO_RISK_MULT', 1.0),
        min_margin_usd=_f('MIN_MARGIN_PER_TRADE', 1.0),
        tick_eval_sec=_f('ENTRY_EVALUATION_INTERVAL_SECONDS', 1.0),
        loop_interval_sec=_f('POLICY_CRYPTO_LOOP_INTERVAL_SEC', float(_cfg('SCANNER_TICK_SLEEP_SEC', 0.25))),
        min_1m_bars=_i('MIN_CANDLES_1M', 80),
        min_5m_bars=_i('MIN_CANDLES_5M', 60),
        atr_min_pctile=_f('QUANT_ATR_MIN_PCTILE', 0.05),
        atr_max_pctile=_f('QUANT_ATR_MAX_PCTILE', 0.97),
        max_hold_sec=_i('QUANT_MAX_HOLD_SEC', 3600),
        cooldown_sec=base_cooldown,
        loss_lockout_sec=_i('QUANT_LOSS_LOCKOUT_SEC', 1800),
        min_rr=base_rr,
        max_rr=_f('QUANT_TP_MAX_RR', 3.5),
        sl_buffer_atr_mult=_f('QUANT_SL_BUFFER_ATR_MULT', 0.4),
        trail_min_move_atr=_f('SL_MIN_IMPROVEMENT_ATR_MULT', 0.08),
        slippage_tolerance=_f('QUANT_SLIPPAGE_TOLERANCE', 0.0005),
        spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_CRYPTO', 0.30),
        spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO', 0.50),
        spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_CRYPTO', 12.0),
        spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_CRYPTO', 10.0),
        spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
        spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
        ob_depth_levels=_i('QUANT_OB_DEPTH_LEVELS', 5),
        tick_agg_window_sec=_f('QUANT_TICK_AGG_WINDOW_SEC', 30.0),
        vwap_window=_i('QUANT_VWAP_WINDOW', 50),
        cvd_window=_i('QUANT_CVD_WINDOW', 20),
        notes='crypto policy; leverage derived from confirmed venue cap and asset utilisation',
    )


def active_policy(inst: Optional[TradableInstrument] = None) -> InstrumentPolicy:
    return build_instrument_policy(inst if inst is not None else current_instrument())


def policy_value(name: str, default: Any = None, inst: Optional[TradableInstrument] = None) -> Any:
    pol = active_policy(inst)
    return getattr(pol, name, default)
