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

from dataclasses import dataclass, asdict, replace
from typing import Any, Dict, Optional
import math

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import AssetClass, TradableInstrument, current_instrument


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
    # Portfolio mechanics. These are not alpha filters; they define how much
    # account cash/risk a confirmed instrument may consume once its posterior
    # is accepted. Values are intentionally ticker-overridable so BTC, metals,
    # ETFs, high-beta xStocks and lower-liquidity RWA contracts do not share
    # the same capital sleeve.
    portfolio_weight: float = 1.0
    max_position_margin_pct: float = 0.20
    max_trade_risk_pct: float = 0.005
    min_lot_elastic_margin_pct: float = 0.0
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



# Per-ticker institutional portfolio priors.  These are allocation/risk priors,
# not entry filters.  They prevent BTC-calibrated margin/risk from leaking into
# PAXG/SLVON and xStock products.  Users can override any value through
# config.TICKER_RISK_POLICY without touching strategy code.
_DEFAULT_TICKER_RISK_POLICY: Dict[str, Dict[str, float]] = {
    # Crypto: deep book, tight spread, lowest contract floor friction.
    "BTC":   {"risk_multiplier": 1.00, "margin_pct": 0.20, "portfolio_weight": 1.30, "max_position_margin_pct": 0.24, "max_trade_risk_pct": 0.0060, "min_lot_elastic_margin_pct": 0.18},

    # Metals / tokenised commodities: slower auction, gap/slippage-aware sizing.
    "GOLD":  {"risk_multiplier": 0.72, "margin_pct": 0.15, "portfolio_weight": 0.95, "max_position_margin_pct": 0.18, "max_trade_risk_pct": 0.0045, "min_lot_elastic_margin_pct": 0.16},
    "SILVER":{"risk_multiplier": 0.64, "margin_pct": 0.14, "portfolio_weight": 0.85, "max_position_margin_pct": 0.17, "max_trade_risk_pct": 0.0040, "min_lot_elastic_margin_pct": 0.16},
    "OIL":   {"risk_multiplier": 0.60, "margin_pct": 0.13, "portfolio_weight": 0.75, "max_position_margin_pct": 0.16, "max_trade_risk_pct": 0.0038, "min_lot_elastic_margin_pct": 0.14},

    # ETF/index-like xStock derivatives.
    "SPY":   {"risk_multiplier": 0.46, "margin_pct": 0.105, "portfolio_weight": 0.64, "max_position_margin_pct": 0.14, "max_trade_risk_pct": 0.0032, "min_lot_elastic_margin_pct": 0.20},
    "QQQ":   {"risk_multiplier": 0.52, "margin_pct": 0.110, "portfolio_weight": 0.70, "max_position_margin_pct": 0.15, "max_trade_risk_pct": 0.0035, "min_lot_elastic_margin_pct": 0.22},

    # Single-name xStock derivatives.  High beta/liquidity gets more budget;
    # wider/less stable RWA contracts get less budget and stricter risk.
    "AAPL":  {"risk_multiplier": 0.42, "margin_pct": 0.095, "portfolio_weight": 0.55, "max_position_margin_pct": 0.13, "max_trade_risk_pct": 0.0028, "min_lot_elastic_margin_pct": 0.22},
    "NVDA":  {"risk_multiplier": 0.62, "margin_pct": 0.120, "portfolio_weight": 0.82, "max_position_margin_pct": 0.17, "max_trade_risk_pct": 0.0042, "min_lot_elastic_margin_pct": 0.26},
    "TSLA":  {"risk_multiplier": 0.54, "margin_pct": 0.115, "portfolio_weight": 0.72, "max_position_margin_pct": 0.16, "max_trade_risk_pct": 0.0037, "min_lot_elastic_margin_pct": 0.25},
    "AMZN":  {"risk_multiplier": 0.40, "margin_pct": 0.095, "portfolio_weight": 0.52, "max_position_margin_pct": 0.13, "max_trade_risk_pct": 0.0027, "min_lot_elastic_margin_pct": 0.21},
    "META":  {"risk_multiplier": 0.44, "margin_pct": 0.100, "portfolio_weight": 0.58, "max_position_margin_pct": 0.14, "max_trade_risk_pct": 0.0030, "min_lot_elastic_margin_pct": 0.22},
    "COIN":  {"risk_multiplier": 0.56, "margin_pct": 0.115, "portfolio_weight": 0.74, "max_position_margin_pct": 0.16, "max_trade_risk_pct": 0.0038, "min_lot_elastic_margin_pct": 0.25},
    "CRCL":  {"risk_multiplier": 0.36, "margin_pct": 0.090, "portfolio_weight": 0.44, "max_position_margin_pct": 0.12, "max_trade_risk_pct": 0.0024, "min_lot_elastic_margin_pct": 0.18},
    "GOOGL": {"risk_multiplier": 0.40, "margin_pct": 0.095, "portfolio_weight": 0.52, "max_position_margin_pct": 0.13, "max_trade_risk_pct": 0.0027, "min_lot_elastic_margin_pct": 0.21},
}


def _ticker_policy_overrides(asset_id: str) -> Dict[str, float]:
    key = str(asset_id or "").upper()
    merged: Dict[str, float] = dict(_DEFAULT_TICKER_RISK_POLICY.get(key, {}))
    raw = _cfg("TICKER_RISK_POLICY", {})
    try:
        if isinstance(raw, dict):
            user_row = raw.get(key) or raw.get(key.lower()) or {}
            if isinstance(user_row, dict):
                merged.update(user_row)
    except Exception:
        pass
    return merged


def _clamp(v: Any, lo: float, hi: float, default: float) -> float:
    try:
        f = float(v)
        if math.isfinite(f):
            return max(lo, min(hi, f))
    except Exception:
        pass
    return float(default)


def _with_ticker_policy(policy: InstrumentPolicy) -> InstrumentPolicy:
    row = _ticker_policy_overrides(policy.asset_id)
    if not row:
        return policy
    changes: Dict[str, Any] = {}
    if "risk_multiplier" in row:
        changes["risk_multiplier"] = _clamp(row["risk_multiplier"], 0.05, 2.00, policy.risk_multiplier)
    if "margin_pct" in row:
        changes["margin_pct"] = _clamp(row["margin_pct"], 0.01, 0.60, policy.margin_pct)
    if "portfolio_weight" in row:
        changes["portfolio_weight"] = _clamp(row["portfolio_weight"], 0.05, 5.00, policy.portfolio_weight)
    if "max_position_margin_pct" in row:
        changes["max_position_margin_pct"] = _clamp(row["max_position_margin_pct"], 0.02, 0.75, policy.max_position_margin_pct)
    if "max_trade_risk_pct" in row:
        changes["max_trade_risk_pct"] = _clamp(row["max_trade_risk_pct"], 0.0005, 0.0300, policy.max_trade_risk_pct)
    if "min_lot_elastic_margin_pct" in row:
        changes["min_lot_elastic_margin_pct"] = _clamp(row["min_lot_elastic_margin_pct"], 0.0, 0.60, policy.min_lot_elastic_margin_pct)
    if changes:
        suffix = f"ticker-capital={policy.asset_id}"
        notes = policy.notes or ""
        changes["notes"] = notes if suffix in notes else (notes + ("; " if notes else "") + suffix)
        return replace(policy, **changes)
    return policy

def _cap_leverage(inst: Optional[TradableInstrument], default: int) -> int:
    base = max(1, int(default))
    if inst is None:
        return base
    try:
        mx = float(getattr(inst, 'max_leverage', 0.0) or 0.0)
        if mx > 0:
            return max(1, min(base, int(mx)))
    except Exception:
        pass
    if inst.asset_class in (AssetClass.EQUITY, AssetClass.COMMODITY, AssetClass.INDEX):
        return min(base, 25)
    return base


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

    lev = _cap_leverage(inst, _i('LEVERAGE', 30))
    base_margin = _f('QUANT_MARGIN_PCT', 0.20)
    base_rr = _f('MIN_RISK_REWARD_RATIO', 2.0)
    base_cooldown = _i('QUANT_COOLDOWN_SEC', 300)

    if ac == AssetClass.EQUITY:
        return _with_ticker_policy(InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=min(base_margin, _f('POLICY_EQUITY_MARGIN_PCT', 0.12)),
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
        ))

    if ac == AssetClass.COMMODITY:
        return _with_ticker_policy(InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=min(base_margin, _f('POLICY_COMMODITY_MARGIN_PCT', 0.14)),
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
        ))

    # Crypto/default policy keeps the original aggressive BTC runtime prior.
    return _with_ticker_policy(InstrumentPolicy(
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
        notes='crypto policy',
    ))


def active_policy(inst: Optional[TradableInstrument] = None) -> InstrumentPolicy:
    return build_instrument_policy(inst if inst is not None else current_instrument())


def policy_value(name: str, default: Any = None, inst: Optional[TradableInstrument] = None) -> Any:
    pol = active_policy(inst)
    return getattr(pol, name, default)
